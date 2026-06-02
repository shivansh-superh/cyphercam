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
  7. On start: begin recording, ffmpeg chunks → uploader
  8. On stop: stop ffmpeg cleanly, drain uploader
  9. On SIGTERM: same as stop, then exit
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
from .preflight import PreflightError, run_all as run_preflight
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
        self.uploader = Uploader(self.cfg, self.manifest)
        self.ffmpeg = FFmpegManager(self.cfg, on_chunk_complete=self._on_chunk_complete)
        self.thingsboard = ThingsBoardClient(
            cfg=self.cfg,
            on_start=self._on_start,
            on_stop=self._on_stop,
            get_status=self._get_status,
        )

        self._current_surgery_id: Optional[str] = None
        self._chunk_sequence: int = 0
        self._state: str = "idle"  # idle | recording | stopping
        self._state_lock = threading.Lock()
        self._shutdown_event = threading.Event()

    # -------------------------------------------------------------------------
    # Start / stop (called by ThingsBoard RPC handler)
    # -------------------------------------------------------------------------

    def _on_start(self, surgery_id: str, scheduled_duration_minutes: Optional[int]):
        with self._state_lock:
            self._current_surgery_id = surgery_id
            self._chunk_sequence = 0
            self._state = "recording"

        self.manifest.start_session(
            surgery_id=surgery_id,
            ot_location_id=self.cfg.ot_location_id,
            hospital_id=self.cfg.ot_hospital_id,
        )
        self.ffmpeg.start(surgery_id)
        logger.info(f"Recording started for surgery {surgery_id}")
        self.thingsboard.publish_status()

        if scheduled_duration_minutes:
            # Auto-stop after scheduled duration as a safety net
            # ThingsBoard should also send stopRecording, but this ensures
            # we don't record forever if the stop signal is lost
            stop_after = scheduled_duration_minutes * 60 + 300  # +5 min grace
            t = threading.Timer(stop_after, self._auto_stop, args=[surgery_id])
            t.daemon = True
            t.start()
            logger.info(f"Auto-stop scheduled in {stop_after}s")

    def _on_stop(self, surgery_id: str):
        with self._state_lock:
            if self._state != "recording":
                return
            self._state = "stopping"

        logger.info(f"Stopping recording for surgery {surgery_id}...")
        self.ffmpeg.stop()
        logger.info("ffmpeg stopped, waiting for upload queue to drain...")

        # Wait for uploader to process everything currently in the queue
        self._wait_for_uploads(surgery_id, timeout=300)

        self.manifest.stop_session(surgery_id)

        with self._state_lock:
            self._current_surgery_id = None
            self._state = "idle"

        logger.info(f"Recording complete for surgery {surgery_id}")
        self.thingsboard.publish_status()

    def _auto_stop(self, surgery_id: str):
        logger.warning(f"Auto-stop triggered for surgery {surgery_id} (scheduled duration exceeded)")
        with self._state_lock:
            if self._current_surgery_id != surgery_id or self._state != "recording":
                return
        self._on_stop(surgery_id)

    # -------------------------------------------------------------------------
    # Chunk callback (called from ffmpeg monitor thread)
    # -------------------------------------------------------------------------

    def _on_chunk_complete(self, local_path: str, recorded_at: str):
        with self._state_lock:
            surgery_id = self._current_surgery_id
            self._chunk_sequence += 1
            seq = self._chunk_sequence

        if not surgery_id:
            logger.warning(f"Chunk completed but no active surgery: {local_path}")
            return

        self.manifest.register_chunk(
            surgery_id=surgery_id,
            chunk_sequence=seq,
            local_path=local_path,
            recorded_at=recorded_at,
        )

        # Look up the chunk id just registered
        chunks = self.manifest.get_chunks_for_surgery(surgery_id)
        chunk = next((c for c in chunks if c["chunk_sequence"] == seq), None)
        if not chunk:
            logger.error(f"Could not find registered chunk seq={seq} in manifest")
            return

        self.uploader.enqueue(
            chunk_id=chunk["id"],
            surgery_id=surgery_id,
            chunk_sequence=seq,
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

            if state == "recording" and surgery_id:
                self._on_stop(surgery_id)

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

        run_preflight(self.cfg)

        self._setup_signal_handlers()
        self._recover_from_crash()

        self.uploader.start()
        self.thingsboard.start()

        logger.info("Recorder ready. Waiting for ThingsBoard RPC...")

        # Block main thread until SIGTERM
        self._shutdown_event.wait()

        logger.info("Shutting down...")
        self.thingsboard.stop()
        self.uploader.stop(drain_timeout=120)
        logger.info("Recorder exited cleanly")


def main():
    recorder = Recorder()
    recorder.run()


if __name__ == "__main__":
    main()
