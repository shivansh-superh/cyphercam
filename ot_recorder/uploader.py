"""
Upload worker.

Runs in a background thread. Picks up chunks from the manifest queue,
uploads to S3, then deletes the local file.

Retry strategy: exponential backoff, max MAX_RETRIES attempts.
After MAX_RETRIES the chunk is marked 'failed' and skipped —
it won't block subsequent chunks.
"""

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import Config
from .manifest import Manifest

if TYPE_CHECKING:
    from .thingsboard_client import ThingsBoardClient

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_BASE_DELAY = 5   # seconds
RETRY_MAX_DELAY = 120  # seconds
VARIANT_PREVIEW = "preview"


@dataclass
class UploadJob:
    chunk_id: int
    surgery_id: str
    chunk_sequence: int
    variant: str
    local_path: str
    recorded_at: str


class Uploader:
    def __init__(
        self,
        cfg: Config,
        manifest: Manifest,
        thingsboard: "ThingsBoardClient",
    ):
        self.cfg = cfg
        self.manifest = manifest
        self.thingsboard = thingsboard
        self._queue: queue.Queue[UploadJob | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._s3 = boto3.client("s3", region_name=cfg.aws_region)

    def start(self):
        self._thread = threading.Thread(
            target=self._run,
            name="uploader",
            daemon=True,
        )
        self._thread.start()
        logger.info("Uploader started")

    def stop(self, drain_timeout: int = 120):
        """
        Signal the uploader to stop and wait for the queue to drain.
        Pass None as sentinel to unblock the queue.get() call.
        """
        logger.info("Uploader draining queue...")
        self._queue.put(None)
        if self._thread:
            self._thread.join(timeout=drain_timeout)
            if self._thread.is_alive():
                logger.warning("Uploader did not drain within timeout")

    def enqueue(
        self,
        chunk_id: int,
        surgery_id: str,
        chunk_sequence: int,
        variant: str,
        local_path: str,
        recorded_at: str,
    ):
        job = UploadJob(
            chunk_id=chunk_id,
            surgery_id=surgery_id,
            chunk_sequence=chunk_sequence,
            variant=variant,
            local_path=local_path,
            recorded_at=recorded_at,
        )
        self._queue.put(job)
        logger.debug(f"Enqueued chunk {chunk_sequence} for surgery {surgery_id}")

    def enqueue_from_manifest(self):
        """
        On startup, re-enqueue any chunks that were pending or mid-upload
        when we last crashed. Called before start() so they go in first.
        """
        for local_path in self.manifest.migrate_legacy_uploaded():
            self._delete_local_file(local_path)

        pending = self.manifest.get_pending_chunks()
        if pending:
            logger.info(f"Recovering {len(pending)} unfinished chunks from manifest")
        for chunk in pending:
            self.enqueue(
                chunk_id=chunk["id"],
                surgery_id=chunk["surgery_id"],
                chunk_sequence=chunk["chunk_sequence"],
                variant=chunk.get("variant") or "original",
                local_path=chunk["local_path"],
                recorded_at=chunk["recorded_at"] or "",
            )

    def _run(self):
        while True:
            job = self._queue.get()
            if job is None:
                logger.info("Uploader received stop signal")
                break
            self._upload_with_retry(job)

    def _upload_with_retry(self, job: UploadJob):
        s3_key = self._build_s3_key(job)
        self.manifest.mark_uploading(job.chunk_id)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._upload_to_s3(job, s3_key)
                needs_analyze = self.cfg.ai_analysis_enabled and job.variant == VARIANT_PREVIEW
                self.manifest.mark_complete(
                    job.chunk_id, s3_key, needs_analyze=needs_analyze
                )
                logger.info(
                    f"Uploaded chunk {job.chunk_sequence} ({job.variant}) → "
                    f"s3://{self.cfg.s3_bucket}/{s3_key}"
                )
                if needs_analyze:
                    self._trigger_preview_analysis(job.chunk_id, job.surgery_id, s3_key)
                self._delete_local_file(job.local_path)
                return

            except (ClientError, BotoCoreError, OSError) as e:
                self.manifest.increment_retry(job.chunk_id)
                delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
                logger.warning(
                    f"Upload attempt {attempt}/{MAX_RETRIES} failed for chunk "
                    f"{job.chunk_sequence}: {e}. Retrying in {delay}s"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Chunk {job.chunk_sequence} for surgery {job.surgery_id} "
                        f"failed after {MAX_RETRIES} attempts. Marking failed."
                    )
                    self.manifest.mark_failed(job.chunk_id, str(e))

    def _upload_to_s3(self, job: UploadJob, s3_key: str):
        if not os.path.exists(job.local_path):
            raise FileNotFoundError(f"Chunk file missing: {job.local_path}")

        file_size = os.path.getsize(job.local_path)
        logger.debug(
            f"Uploading {job.local_path} ({file_size / 1024 / 1024:.1f}MB) → {s3_key}"
        )

        self._s3.upload_file(
            Filename=job.local_path,
            Bucket=self.cfg.s3_bucket,
            Key=s3_key,
            ExtraArgs={
                "Metadata": {
                    "surgery-id": job.surgery_id,
                    "ot-location-id": self.cfg.ot_location_id,
                    "hospital-id": self.cfg.ot_hospital_id,
                    "chunk-sequence": str(job.chunk_sequence),
                    "variant": job.variant,
                    "recorded-at": job.recorded_at,
                }
            },
        )

    def _build_s3_key(self, job: UploadJob) -> str:
        filename = os.path.basename(job.local_path)
        return (
            f"{self.cfg.s3_prefix}/"
            f"{self.cfg.ot_hospital_id}/"
            f"{self.cfg.ot_location_id}/"
            f"{job.surgery_id}/"
            f"{job.variant}/"
            f"{filename}"
        )

    def _generate_presigned_url(self, s3_key: str) -> str:
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.cfg.s3_bucket, "Key": s3_key},
            ExpiresIn=self.cfg.presigned_url_expiry,
        )

    def _video_id_for_s3_key(self, surgery_id: str, s3_key: str) -> str:
        filename_stem = os.path.splitext(os.path.basename(s3_key))[0]
        return f"{surgery_id}-{filename_stem}"

    def _trigger_preview_analysis(
        self, chunk_id: int, surgery_id: str, s3_key: str
    ) -> bool:
        video_id = self._video_id_for_s3_key(surgery_id, s3_key)
        presigned_url = self._generate_presigned_url(s3_key)

        if self.thingsboard.publish_analyze_ot_video(
            video_id=video_id,
            presigned_url=presigned_url,
            ipd_appointment_id=surgery_id,
        ):
            self.manifest.mark_analyze_complete(chunk_id)
            return True
        return False

    def retry_pending_analyze(self):
        if not self.cfg.ai_analysis_enabled:
            return

        pending = self.manifest.get_pending_analyze_chunks()
        if not pending:
            return

        logger.info(
            f"Retrying analyze-ot-video telemetry for {len(pending)} preview chunks"
        )
        for chunk in pending:
            self._trigger_preview_analysis(
                chunk["id"], chunk["surgery_id"], chunk["s3_key"]
            )

    def _delete_local_file(self, path: str):
        try:
            os.remove(path)
            logger.debug(f"Deleted local chunk: {path}")
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(f"Could not delete local chunk {path}: {e}")
