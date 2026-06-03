"""
Manages the ffmpeg subprocess.

ffmpeg writes two segmented streams per surgery:
  {chunk_dir}/{surgery_id}/original/YYYYMMDD_HHMMSS.mp4  — full resolution
  {chunk_dir}/{surgery_id}/preview/YYYYMMDD_HHMMSS.mp4   — 720p @ 1 fps

Each stream has its own segment list CSV; chunk_sequence is the 1-based line
index in that CSV so original and preview rows with the same filename stem align.
"""

import logging
import os
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from .config import Config

logger = logging.getLogger(__name__)

VARIANT_ORIGINAL = "original"
VARIANT_PREVIEW = "preview"

# drawtext localtime format; colons escaped for ffmpeg filter syntax
_DRAWTEXT_TIME = r"%{localtime\:%Y-%m-%d %H\:%M\:%S}"


@dataclass(frozen=True)
class _StreamOutput:
    variant: str
    output_dir: Path
    segment_list_path: Path


class FFmpegManager:
    def __init__(
        self,
        cfg: Config,
        on_chunk_complete: Callable[[str, str, str, int], None],
    ):
        """
        on_chunk_complete(local_path, recorded_at, variant, chunk_sequence)
        is called when ffmpeg finishes a segment and the file is safe to upload.
        """
        self.cfg = cfg
        self.on_chunk_complete = on_chunk_complete
        self._process: Optional[subprocess.Popen] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._surgery_id: Optional[str] = None
        self._streams: list[_StreamOutput] = []
        self._known_segments: set[tuple[str, str]] = set()
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

        base_dir = Path(self.cfg.chunk_dir) / surgery_id
        self._streams = [
            _StreamOutput(
                variant=VARIANT_ORIGINAL,
                output_dir=base_dir / VARIANT_ORIGINAL,
                segment_list_path=base_dir / VARIANT_ORIGINAL / "segments.csv",
            ),
            _StreamOutput(
                variant=VARIANT_PREVIEW,
                output_dir=base_dir / VARIANT_PREVIEW,
                segment_list_path=base_dir / VARIANT_PREVIEW / "segments.csv",
            ),
        ]
        for stream in self._streams:
            stream.output_dir.mkdir(parents=True, exist_ok=True)

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
            self._scan_all_segment_lists()
            self._process = None
            self._surgery_id = None
            self._streams = []

    def _build_ffmpeg_cmd(self) -> list[str]:
        cfg = self.cfg
        original = self._streams[0]
        preview = self._streams[1]
        original_pattern = str(original.output_dir / "%Y%m%d_%H%M%S.mp4")
        preview_pattern = str(preview.output_dir / "%Y%m%d_%H%M%S.mp4")
        preview_vf = self._build_preview_video_filter(cfg)

        segment_opts = [
            "-f", "segment",
            "-segment_time", str(cfg.chunk_duration_seconds),
            "-segment_format", "mp4",
            "-reset_timestamps", "1",
            "-strftime", "1",
            "-segment_list_type", "csv",
            "-segment_list_flags", "+cache",
        ]

        return [
            "ffmpeg",
            "-f", "v4l2",
            "-input_format", "mjpeg",
            "-video_size", f"{cfg.video_width}x{cfg.video_height}",
            "-framerate", str(cfg.video_fps),
            "-i", cfg.camera_device,

            # Original — full resolution
            "-map", "0:v",
            "-an",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", str(cfg.video_crf),
            *segment_opts,
            "-segment_list", str(original.segment_list_path),
            original_pattern,

            # Preview — drop to 1 fps before scale to minimize CPU
            "-map", "0:v",
            "-an",
            "-vf", preview_vf,
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", str(cfg.preview_crf),
            *segment_opts,
            "-segment_list", str(preview.segment_list_path),
            preview_pattern,
        ]

    def _build_preview_video_filter(self, cfg: Config) -> str:
        """1 fps preview at 720p with wall-clock timestamp (preview only)."""
        font = cfg.preview_timestamp_font.replace(":", r"\:")
        drawtext = (
            f"drawtext=fontfile={font}:text='{_DRAWTEXT_TIME}'"
            ":fontcolor=orange:borderw=2:bordercolor=black:fontsize=22"
            ":x=w-tw-12:y=h-th-12"
        )
        return f"fps={cfg.preview_fps},scale=-2:{cfg.preview_height},{drawtext}"

    def _monitor_segments(self):
        """
        Polls segment list CSVs for newly completed segments.
        ffmpeg appends a line when it closes a segment file.
        """
        import time

        while not self._stop_event.is_set():
            self._scan_all_segment_lists()

            if self._process and self._process.poll() is not None:
                exit_code = self._process.returncode
                if not self._stop_event.is_set():
                    logger.error(f"ffmpeg exited unexpectedly with code {exit_code}")
                break

            time.sleep(2)

    def _scan_all_segment_lists(self):
        for stream in self._streams:
            self._scan_segment_list(stream)

    def _scan_segment_list(self, stream: _StreamOutput):
        path = stream.segment_list_path
        if not path.exists():
            return

        try:
            lines = path.read_text().strip().splitlines()
        except OSError:
            return

        for chunk_sequence, line in enumerate(lines, start=1):
            parts = line.strip().split(",")
            if not parts:
                continue
            filename = parts[0].strip()
            if not filename:
                continue

            key = (stream.variant, filename)
            if key in self._known_segments:
                continue

            full_path = (
                str(stream.output_dir / filename)
                if not os.path.isabs(filename)
                else filename
            )
            if not os.path.exists(full_path):
                continue

            self._known_segments.add(key)
            recorded_at = datetime.now(timezone.utc).isoformat()
            logger.info(f"Chunk complete [{stream.variant}]: {full_path}")

            try:
                self.on_chunk_complete(
                    full_path,
                    recorded_at,
                    stream.variant,
                    chunk_sequence,
                )
            except Exception:
                logger.exception(
                    f"Error in on_chunk_complete for {stream.variant} {full_path}"
                )

    def _log_ffmpeg_stderr(self):
        """Stream ffmpeg stderr to our logger at DEBUG level."""
        if not self._process or not self._process.stderr:
            return
        for line in self._process.stderr:
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                logger.debug(f"[ffmpeg] {decoded}")
