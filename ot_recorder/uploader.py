"""
Upload worker.

Runs in a background thread. Picks up chunks from the manifest queue,
uploads to S3, notifies HMS, then deletes the local file.

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
from typing import Optional

import boto3
import httpx
from botocore.exceptions import BotoCoreError, ClientError

from .config import Config
from .manifest import Manifest

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_BASE_DELAY = 5   # seconds
RETRY_MAX_DELAY = 120  # seconds


@dataclass
class UploadJob:
    chunk_id: int
    surgery_id: str
    chunk_sequence: int
    local_path: str
    recorded_at: str
    s3_key: Optional[str] = None  # set after upload, used for HMS notify retry


class Uploader:
    def __init__(self, cfg: Config, manifest: Manifest):
        self.cfg = cfg
        self.manifest = manifest
        self._queue: queue.Queue[Optional[UploadJob]] = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._s3 = boto3.client("s3", region_name=cfg.aws_region)
        self._http = httpx.Client(
            headers={"X-API-Key": cfg.hms_api_key},
            timeout=10.0,
        )

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

    def enqueue(self, chunk_id: int, surgery_id: str, chunk_sequence: int,
                local_path: str, recorded_at: str):
        job = UploadJob(
            chunk_id=chunk_id,
            surgery_id=surgery_id,
            chunk_sequence=chunk_sequence,
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
        pending = self.manifest.get_pending_chunks()
        if pending:
            logger.info(f"Recovering {len(pending)} unfinished chunks from manifest")
        for chunk in pending:
            self.enqueue(
                chunk_id=chunk["id"],
                surgery_id=chunk["surgery_id"],
                chunk_sequence=chunk["chunk_sequence"],
                local_path=chunk["local_path"],
                recorded_at=chunk["recorded_at"] or "",
            )

        # Also retry HMS notifications for chunks already on S3
        uploaded = self.manifest.get_uploaded_not_notified()
        if uploaded:
            logger.info(f"Retrying HMS notification for {len(uploaded)} chunks")
        for chunk in uploaded:
            job = UploadJob(
                chunk_id=chunk["id"],
                surgery_id=chunk["surgery_id"],
                chunk_sequence=chunk["chunk_sequence"],
                local_path=chunk["local_path"],
                recorded_at=chunk["recorded_at"] or "",
                s3_key=chunk["s3_key"],
            )
            self._queue.put(job)

    def _run(self):
        while True:
            job = self._queue.get()
            if job is None:
                logger.info("Uploader received stop signal")
                break
            self._process_job(job)

    def _process_job(self, job: UploadJob):
        # If s3_key is already set, the upload succeeded but HMS wasn't notified
        if job.s3_key:
            self._notify_hms_with_retry(job)
            return

        self._upload_with_retry(job)

    # -------------------------------------------------------------------------
    # S3 upload
    # -------------------------------------------------------------------------

    def _upload_with_retry(self, job: UploadJob):
        s3_key = self._build_s3_key(job)
        self.manifest.mark_uploading(job.chunk_id)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._upload_to_s3(job, s3_key)
                self.manifest.mark_uploaded(job.chunk_id, s3_key)
                job.s3_key = s3_key
                logger.info(f"Uploaded chunk {job.chunk_sequence} → s3://{self.cfg.s3_bucket}/{s3_key}")
                self._notify_hms_with_retry(job)
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
        logger.debug(f"Uploading {job.local_path} ({file_size / 1024 / 1024:.1f}MB) → {s3_key}")

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
            f"{filename}"
        )

    # -------------------------------------------------------------------------
    # HMS notification
    # -------------------------------------------------------------------------

    def _notify_hms_with_retry(self, job: UploadJob):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._notify_hms(job)
                self.manifest.mark_hms_notified(job.chunk_id)
                self._delete_local_file(job.local_path)
                return

            except Exception as e:
                delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
                logger.warning(
                    f"HMS notification attempt {attempt}/{MAX_RETRIES} failed "
                    f"for chunk {job.chunk_sequence}: {e}. Retrying in {delay}s"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(delay)
                else:
                    logger.error(
                        f"HMS notification failed after {MAX_RETRIES} attempts "
                        f"for chunk {job.chunk_sequence}. Chunk is on S3 but HMS not notified. "
                        f"s3_key={job.s3_key}"
                    )
                    # Don't mark as failed — leave as 'uploaded' so it gets
                    # retried on next restart via enqueue_from_manifest()

    def _notify_hms(self, job: UploadJob):
        url = f"{self.cfg.hms_api_base_url}/api/recordings/chunk"
        payload = {
            "surgery_id": job.surgery_id,
            "chunk_sequence": job.chunk_sequence,
            "s3_key": job.s3_key,
            "s3_bucket": self.cfg.s3_bucket,
            "ot_location_id": self.cfg.ot_location_id,
            "hospital_id": self.cfg.ot_hospital_id,
            "recorded_at": job.recorded_at,
        }
        response = self._http.post(url, json=payload)
        response.raise_for_status()
        logger.debug(f"HMS notified for chunk {job.chunk_sequence} of surgery {job.surgery_id}")

    def _delete_local_file(self, path: str):
        try:
            os.remove(path)
            logger.debug(f"Deleted local chunk: {path}")
        except FileNotFoundError:
            pass  # already gone, fine
        except OSError as e:
            logger.warning(f"Could not delete local chunk {path}: {e}")
