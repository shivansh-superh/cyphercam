"""
Main recorder process.

Wires together: config → preflight → manifest → uploader → ffmpeg → ThingsBoard client.

Lifecycle:
  1. Load config and validate env vars
  2. Run preflight checks (camera, disk, ffmpeg)
  3. Load manifest, recover any unfinished uploads from last run
  4. Start uploader thread
  5. Connect to ThingsBoard (MQTT RPC)
  6. Wait for startRecording RPC
  7. On start RPC: accept immediately, begin recording in background
  8. On stop RPC: accept immediately, stop ffmpeg and drain uploads in background
  9. On SIGTERM: trigger stop and wait briefly for background work, then exit
"""

import logging
import os
import signal
import threading
import time
from typing import Optional

from .config import load_config
from .ffmpeg_manager import FFmpegManager
from .manifest import Manifest
from .preflight import wait_until_ready
from .thingsboard_client import ThingsBoardClient
from .uploader import Uploader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class Recorder:
    def __init__(self):
        self.cfg = load_config()
        self.manifest = Manifest(self.cfg.manifest_path)
        self.thingsboard = ThingsBoardClient(
            cfg=self.cfg,
            on_start=self.trigger_start,
            on_stop=self.trigger_stop,
            get_status=self._get_status,
        )
        self.uploader = Uploader(self.cfg, self.manifest, self.thingsboard)
        self.thingsboard.on_connected = self.uploader.retry_pending_analyze
        self.ffmpeg = FFmpegManager(self.cfg, on_chunk_complete=self._on_chunk_complete)

        self._current_surgery_id: Optional[str] = None
        self._state: str = "idle"  # idle | starting | recording | stopping
        self._state_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._auto_stop_timer: Optional[threading.Timer] = None
        self._worker_thread: Optional[threading.Thread] = None

    # -------------------------------------------------------------------------
    # Start / stop triggers (return immediately; work runs in background)
    # -------------------------------------------------------------------------

    def trigger_start(self, surgery_id: str, scheduled_duration_minutes: Optional[int]):
        with self._state_lock:
            if self._state != "idle":
                logger.warning(
                    f"Start ignored for {surgery_id}: recorder is {self._state}"
                )
                return
            self._current_surgery_id = surgery_id
            self._state = "starting"

        logger.info(f"Recording start accepted for surgery {surgery_id}")
        self.thingsboard.publish_status()
        self._spawn_worker(
            "record-start",
            self._run_start,
            (surgery_id, scheduled_duration_minutes),
        )

    def trigger_stop(self, surgery_id: str):
        with self._state_lock:
            if self._state == "stopping":
                logger.info(f"Stop already in progress for {surgery_id}")
                return
            if self._state not in ("recording", "starting"):
                logger.warning(
                    f"Stop ignored for {surgery_id}: recorder is {self._state}"
                )
                return
            if self._current_surgery_id != surgery_id:
                logger.warning(
                    f"Stop ignored: active surgery is {self._current_surgery_id}, "
                    f"not {surgery_id}"
                )
                return
            self._state = "stopping"

        self._cancel_auto_stop_timer()
        logger.info(f"Recording stop accepted for surgery {surgery_id}")
        self.thingsboard.publish_status()
        self._spawn_worker("record-stop", self._run_stop, (surgery_id,))

    def _spawn_worker(self, name: str, target, args: tuple):
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        self._worker_thread = thread
        thread.start()

    def _run_start(self, surgery_id: str, scheduled_duration_minutes: Optional[int]):
        try:
            self.manifest.start_session(
                surgery_id=surgery_id,
                ot_location_id=self.cfg.ot_location_id,
                hospital_id=self.cfg.ot_hospital_id,
            )
            self.ffmpeg.start(surgery_id)
            with self._state_lock:
                if self._state != "starting":
                    logger.info(
                        f"Start aborted for {surgery_id}: state is {self._state}"
                    )
                    self.ffmpeg.stop()
                    return
                self._state = "recording"
            logger.info(f"Recording started for surgery {surgery_id}")
            self.thingsboard.publish_status()

            if scheduled_duration_minutes:
                stop_after = scheduled_duration_minutes * 60 + 300  # +5 min grace
                self._auto_stop_timer = threading.Timer(
                    stop_after, self._auto_stop, args=[surgery_id]
                )
                self._auto_stop_timer.daemon = True
                self._auto_stop_timer.start()
                logger.info(f"Auto-stop scheduled in {stop_after}s")
        except Exception:
            logger.exception(f"Failed to start recording for surgery {surgery_id}")
            self._cancel_auto_stop_timer()
            with self._state_lock:
                self._current_surgery_id = None
                self._state = "idle"
            self.thingsboard.publish_status()

    def _run_stop(self, surgery_id: str):
        try:
            logger.info(f"Stopping recording for surgery {surgery_id}...")
            self.ffmpeg.stop()
            logger.info("ffmpeg stopped, waiting for upload queue to drain...")
            self._wait_for_uploads(surgery_id, timeout=300)
            self.manifest.stop_session(surgery_id)
            with self._state_lock:
                self._current_surgery_id = None
                self._state = "idle"
            logger.info(f"Recording complete for surgery {surgery_id}")
        except Exception:
            logger.exception(f"Failed to stop recording for surgery {surgery_id}")
            with self._state_lock:
                self._state = "idle"
        finally:
            self.thingsboard.publish_status()

    def _cancel_auto_stop_timer(self):
        if self._auto_stop_timer:
            self._auto_stop_timer.cancel()
            self._auto_stop_timer = None

    def _auto_stop(self, surgery_id: str):
        logger.warning(
            f"Auto-stop triggered for surgery {surgery_id} (scheduled duration exceeded)"
        )
        self.trigger_stop(surgery_id)

    # -------------------------------------------------------------------------
    # Chunk callback (called from ffmpeg monitor thread)
    # -------------------------------------------------------------------------

    def _on_chunk_complete(
        self,
        local_path: str,
        recorded_at: str,
        variant: str,
        chunk_sequence: int,
    ):
        with self._state_lock:
            surgery_id = self._current_surgery_id

        if not surgery_id:
            logger.warning(f"Chunk completed but no active surgery: {local_path}")
            return

        self.manifest.register_chunk(
            surgery_id=surgery_id,
            chunk_sequence=chunk_sequence,
            variant=variant,
            local_path=local_path,
            recorded_at=recorded_at,
        )

        chunks = self.manifest.get_chunks_for_surgery(surgery_id)
        chunk = next(
            (
                c
                for c in chunks
                if c["chunk_sequence"] == chunk_sequence and c["variant"] == variant
            ),
            None,
        )
        if not chunk:
            logger.error(
                f"Could not find registered chunk seq={chunk_sequence} "
                f"variant={variant} in manifest"
            )
            return

        self.uploader.enqueue(
            chunk_id=chunk["id"],
            surgery_id=surgery_id,
            chunk_sequence=chunk_sequence,
            variant=variant,
            local_path=local_path,
            recorded_at=recorded_at,
        )

    # -------------------------------------------------------------------------
    # Status (published as ThingsBoard telemetry)
    # -------------------------------------------------------------------------

    def _get_status(self) -> dict:
        with self._state_lock:
            return {
                "status": self._state,
                "surgery_id": self._current_surgery_id,
            }

    # -------------------------------------------------------------------------
    # Upload drain helper
    # -------------------------------------------------------------------------

    def _wait_for_uploads(self, surgery_id: str, timeout: int):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.manifest.has_incomplete_uploads(surgery_id):
                logger.info("All chunks uploaded successfully")
                return
            time.sleep(3)
        logger.warning(
            f"Upload drain timed out after {timeout}s. "
            "Some chunks may still be pending — they will be retried on next start."
        )

    # -------------------------------------------------------------------------
    # Signal handling (SIGTERM from systemd)
    # -------------------------------------------------------------------------

    def _setup_signal_handlers(self):
        def handle_sigterm(signum, frame):
            logger.info("SIGTERM received, shutting down cleanly...")
            with self._state_lock:
                surgery_id = self._current_surgery_id
                state = self._state

            if state in ("recording", "starting") and surgery_id:
                self.trigger_stop(surgery_id)

            self._shutdown_event.set()

        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigterm)

    # -------------------------------------------------------------------------
    # Crash recovery
    # -------------------------------------------------------------------------

    def _recover_from_crash(self):
        active = self.manifest.get_active_session()
        if active:
            logger.warning(
                f"Found interrupted session for surgery {active['surgery_id']} — "
                "recovering unfinished uploads"
            )
            # Don't restart ffmpeg — the session already ended when we crashed.
            # Just make sure all chunks that made it to disk get uploaded.
            self.manifest.stop_session(active["surgery_id"])

        self.uploader.enqueue_from_manifest()

    # -------------------------------------------------------------------------
    # Main run loop
    # -------------------------------------------------------------------------

    def run(self):
        logger.info(
            f"OT Recorder starting — "
            f"{self.cfg.ot_location_name} ({self.cfg.ot_location_id}) "
            f"at {self.cfg.ot_hospital_id}"
        )

        self._setup_signal_handlers()
        wait_until_ready(self.cfg, self._shutdown_event)
        if self._shutdown_event.is_set():
            logger.info("Exiting before recorder was ready.")
            return

        self._recover_from_crash()

        self.uploader.start()
        self.thingsboard.start()

        logger.info("Recorder ready. Waiting for ThingsBoard RPC...")

        # Block main thread until SIGTERM
        self._shutdown_event.wait()

        logger.info("Shutting down...")
        self._cancel_auto_stop_timer()
        if self._worker_thread and self._worker_thread.is_alive():
            logger.info("Waiting for in-flight start/stop work...")
            self._worker_thread.join(timeout=330)
        self.thingsboard.stop()
        self.uploader.stop(drain_timeout=120)
        logger.info("Recorder exited cleanly")


def main():
    recorder = Recorder()
    recorder.run()


if __name__ == "__main__":
    main()
