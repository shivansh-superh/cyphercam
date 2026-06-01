"""
Manages the ffmpeg subprocess.

ffmpeg writes chunks named by timestamp into:
  {chunk_dir}/{surgery_id}/YYYYMMDD_HHMMSS.mp4

It also writes a segment list file that we use to detect completed chunks
rather than guessing from file modification time.
"""

import logging
import os
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from .config import Config

logger = logging.getLogger(__name__)


class FFmpegManager:
    def __init__(self, cfg: Config, on_chunk_complete: Callable[[str, str], None]):
        """
        on_chunk_complete(local_path, recorded_at) is called each time
        ffmpeg finishes writing a segment and it's safe to upload.
        """
        self.cfg = cfg
        self.on_chunk_complete = on_chunk_complete
        self._process: Optional[subprocess.Popen] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._surgery_id: Optional[str] = None
        self._output_dir: Optional[Path] = None
        self._segment_list_path: Optional[Path] = None
        self._known_segments: set[str] = set()
        self._stop_event = threading.Event()

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self, surgery_id: str):
        if self.is_running:
            raise RuntimeError(f"ffmpeg already running for surgery {self._surgery_id}")

        self._surgery_id = surgery_id
        self._stop_event.clear()
        self._known_segments = set()

        self._output_dir = Path(self.cfg.chunk_dir) / surgery_id
        self._output_dir.mkdir(parents=True, exist_ok=True)

        self._segment_list_path = self._output_dir / "segments.csv"

        cmd = self._build_ffmpeg_cmd()
        logger.info(f"Starting ffmpeg: {' '.join(cmd)}")

        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self._monitor_thread = threading.Thread(
            target=self._monitor_segments,
            name="ffmpeg-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        stderr_thread = threading.Thread(
            target=self._log_ffmpeg_stderr,
            name="ffmpeg-stderr",
            daemon=True,
        )
        stderr_thread.start()

        logger.info(f"ffmpeg started (pid {self._process.pid}) for surgery {surgery_id}")

    def stop(self, timeout: int = 30) -> bool:
        """
        Gracefully stop ffmpeg. Sends SIGTERM and waits for it to finish
        writing the current segment. Returns True if clean stop, False if
        we had to SIGKILL.
        """
        if not self._process:
            return True

        logger.info("Sending SIGTERM to ffmpeg...")
        self._stop_event.set()
        self._process.terminate()

        try:
            self._process.wait(timeout=timeout)
            logger.info("ffmpeg stopped cleanly")
            return True
        except subprocess.TimeoutExpired:
            logger.warning(f"ffmpeg did not stop within {timeout}s, sending SIGKILL")
            self._process.kill()
            self._process.wait()
            return False
        finally:
            # Do one final segment scan to pick up the last chunk
            self._scan_segment_list()
            self._process = None
            self._surgery_id = None

    def _build_ffmpeg_cmd(self) -> list[str]:
        cfg = self.cfg
        output_pattern = str(self._output_dir / "%Y%m%d_%H%M%S.mp4")

        return [
            "ffmpeg",
            "-f", "v4l2",
            "-input_format", "mjpeg",
            "-video_size", f"{cfg.video_width}x{cfg.video_height}",
            "-framerate", str(cfg.video_fps),
            "-i", cfg.camera_device,

            # Encoding
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",

            # Segmentation
            "-f", "segment",
            "-segment_time", str(cfg.chunk_duration_seconds),
            "-segment_format", "mp4",
            "-reset_timestamps", "1",
            "-strftime", "1",

            # Segment list — completed segments are written here
            "-segment_list", str(self._segment_list_path),
            "-segment_list_type", "csv",
            "-segment_list_flags", "+cache",  # append, don't overwrite

            output_pattern,
        ]

    def _monitor_segments(self):
        """
        Polls the segment list CSV for newly completed segments.
        ffmpeg appends a line to the CSV when it closes a segment file,
        so any line we haven't seen before is a completed, safe-to-upload chunk.

        CSV format: filename,start_time,end_time
        """
        import time
        while not self._stop_event.is_set():
            self._scan_segment_list()

            # Also check if ffmpeg died unexpectedly
            if self._process and self._process.poll() is not None:
                exit_code = self._process.returncode
                if not self._stop_event.is_set():
                    logger.error(f"ffmpeg exited unexpectedly with code {exit_code}")
                break

            time.sleep(2)

    def _scan_segment_list(self):
        if not self._segment_list_path or not self._segment_list_path.exists():
            return

        try:
            lines = self._segment_list_path.read_text().strip().splitlines()
        except OSError:
            return

        for line in lines:
            parts = line.strip().split(",")
            if len(parts) < 1:
                continue
            filename = parts[0].strip()
            if not filename or filename in self._known_segments:
                continue

            full_path = str(self._output_dir / filename) if not os.path.isabs(filename) else filename
            if not os.path.exists(full_path):
                continue

            self._known_segments.add(filename)
            recorded_at = datetime.now(timezone.utc).isoformat()
            logger.info(f"Chunk complete: {full_path}")

            try:
                self.on_chunk_complete(full_path, recorded_at)
            except Exception:
                logger.exception(f"Error in on_chunk_complete callback for {full_path}")

    def _log_ffmpeg_stderr(self):
        """Stream ffmpeg stderr to our logger at DEBUG level."""
        if not self._process or not self._process.stderr:
            return
        for line in self._process.stderr:
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                logger.debug(f"[ffmpeg] {decoded}")
