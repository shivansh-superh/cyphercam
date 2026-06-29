"""
Manages the ffmpeg subprocess.

A single ffmpeg process reads the camera and writes two outputs directly to disk:

  * original: MJPEG decode → libx264 (software) → MP4 segments
    ({chunk_dir}/{surgery_id}/original/YYYYMMDD_HHMMSS.mp4)

  * preview:  decode → 1 fps → 720p → libx264 → MP4 segments
    (omitted when PREVIEW_ENABLED=false)

libx264 is used by default (VIDEO_CODEC=libx264). Hardware v4l2m2m encoders
(h264_v4l2m2m, hevc_v4l2m2m) are still supported via VIDEO_CODEC but require
two workarounds: the -bsf:v extract_extradata bitstream filter (VPU does not
emit SPS/PPS extradata at init, only inline in the first IDR) and
-force_key_frames (VPU silently ignores -g). Neither is needed for libx264.

Each stream has its own segment list CSV; chunk_sequence is the 1-based line
index in that CSV so original and preview rows with the same index align.
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

# Default FFmpeg localtime format (no fmt arg — colons in fmt break filter parsing on 6.x)
_DRAWTEXT_TIME = r"%{localtime}"


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
        on_unexpected_stop: Optional[Callable[[], None]] = None,
    ):
        """
        on_chunk_complete(local_path, recorded_at, variant, chunk_sequence)
        is called when ffmpeg finishes a segment and the file is safe to upload.

        on_unexpected_stop is called when ffmpeg exits without stop() being called
        (e.g. crash). Used by ER mode to auto-restart the session.
        """
        self.cfg = cfg
        self.on_chunk_complete = on_chunk_complete
        self.on_unexpected_stop = on_unexpected_stop
        self._capture: Optional[subprocess.Popen] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._surgery_id: Optional[str] = None
        self._streams: list[_StreamOutput] = []
        self._known_segments: set[tuple[str, str]] = set()
        self._stop_event = threading.Event()

    @property
    def is_running(self) -> bool:
        return self._capture is not None and self._capture.poll() is None

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
        ]
        if self.cfg.preview_enabled:
            self._streams.append(
                _StreamOutput(
                    variant=VARIANT_PREVIEW,
                    output_dir=base_dir / VARIANT_PREVIEW,
                    segment_list_path=base_dir / VARIANT_PREVIEW / "segments.csv",
                )
            )
        for stream in self._streams:
            stream.output_dir.mkdir(parents=True, exist_ok=True)

        # Audio is best-effort: if the mic is disabled or unreachable we still
        # record video. Probe the ALSA device first so a missing/busy mic never
        # takes down the whole ffmpeg pipeline.
        use_audio = False
        if self.cfg.audio_enabled:
            if self._audio_available():
                use_audio = True
                logger.info(f"Audio enabled: {self.cfg.audio_device}")
            else:
                logger.warning(
                    f"Audio device {self.cfg.audio_device} unavailable — "
                    "recording video only for this session."
                )

        capture_cmd = self._build_capture_cmd(use_audio)
        logger.info(f"Starting ffmpeg: {' '.join(capture_cmd)}")

        self._capture = subprocess.Popen(
            capture_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        self._monitor_thread = threading.Thread(
            target=self._monitor_segments,
            name="ffmpeg-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        threading.Thread(
            target=self._log_ffmpeg_stderr,
            args=(self._capture, "ffmpeg"),
            name="ffmpeg-stderr",
            daemon=True,
        ).start()

        logger.info(f"ffmpeg started (pid {self._capture.pid}) for surgery {surgery_id}")

    def stop(self, timeout: int = 30) -> bool:
        """
        Gracefully stop recording. SIGTERMs ffmpeg so it finalizes the current
        segment before exiting. Returns True if it stopped cleanly, False if we
        had to SIGKILL it.
        """
        if not self._capture:
            return True

        self._stop_event.set()
        clean = True

        logger.info("Sending SIGTERM to ffmpeg...")
        self._capture.terminate()
        try:
            self._capture.wait(timeout=timeout)
            logger.info("ffmpeg stopped cleanly")
        except subprocess.TimeoutExpired:
            logger.warning(
                f"ffmpeg did not stop within {timeout}s, sending SIGKILL"
            )
            self._capture.kill()
            self._capture.wait()
            clean = False

        self._scan_all_segment_lists()
        self._capture = None
        self._surgery_id = None
        self._streams = []
        return clean

    def _build_capture_cmd(self, use_audio: bool) -> list[str]:
        """
        Reads the camera and writes two outputs directly to disk:
          * original: libx264 (or hw codec via VIDEO_CODEC) + AAC → segmented MP4
          * preview:  1 fps / 720p libx264 MP4 segments (omitted when PREVIEW_ENABLED=false)

        Audio (when enabled) is captured from a separate ALSA device and added
        to the original only; the preview stays silent.
        """
        cfg = self.cfg
        original = self._streams[0]
        original_pattern = str(original.output_dir / "%Y%m%d_%H%M%S.mp4")
        original_gop = cfg.video_fps * cfg.chunk_duration_seconds

        is_hw_encoder = "v4l2m2m" in cfg.video_codec

        # The ALSA mic and the V4L2 camera run on independent clocks, so over a
        # long surgery the audio would drift out of sync. aresample=async=1 keeps
        # audio locked to the timeline by stretching/padding to match its PTS.
        audio_inputs: list[str] = []
        audio_output: list[str] = []
        if use_audio:
            audio_inputs = [
                "-thread_queue_size", "1024",
                "-f", "alsa",
                "-ac", str(cfg.audio_channels),
                "-ar", str(cfg.audio_sample_rate),
                "-i", cfg.audio_device,
            ]
            audio_output = [
                "-map", "1:a",
                "-c:a", "aac",
                "-b:a", f"{cfg.audio_bitrate_kbps}k",
                "-af", "aresample=async=1",
            ]

        preview_output: list[str] = []
        if cfg.preview_enabled:
            preview = self._streams[1]
            preview_pattern = str(preview.output_dir / "%Y%m%d_%H%M%S.mp4")
            preview_vf = self._build_preview_video_filter(cfg)
            preview_gop = cfg.preview_fps * cfg.chunk_duration_seconds
            preview_output = [
                "-map", "0:v",
                "-an",
                "-vf", preview_vf,
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-crf", str(cfg.preview_crf),
                "-g", str(preview_gop),
                "-f", "segment",
                "-segment_time", str(cfg.chunk_duration_seconds),
                "-segment_format", "mp4",
                "-reset_timestamps", "1",
                "-strftime", "1",
                "-segment_list_type", "csv",
                "-segment_list_flags", "+cache",
                "-segment_list", str(preview.segment_list_path),
                preview_pattern,
            ]

        # v4l2m2m encoders require yuv420p and silently ignore -g, so we use
        # -force_key_frames instead and extract_extradata to fix missing avcC data.
        # libx264 handles all of this natively.
        if is_hw_encoder:
            original_encode = [
                "-vf", "format=yuv420p",
                "-c:v", cfg.video_codec,
                "-b:v", f"{cfg.original_video_bitrate_kbps}k",
                "-maxrate", f"{cfg.original_video_bitrate_kbps}k",
                "-bufsize", f"{cfg.original_video_bufsize_kbps}k",
                "-g", str(original_gop),
                "-force_key_frames", f"expr:gte(t,n_forced*{cfg.chunk_duration_seconds})",
                *audio_output,
                "-bsf:v", "extract_extradata",
            ]
        else:
            original_encode = [
                "-vf", "format=yuv420p",
                "-c:v", cfg.video_codec,
                "-preset", cfg.video_encoder_preset,
                "-b:v", f"{cfg.original_video_bitrate_kbps}k",
                "-maxrate", f"{cfg.original_video_bitrate_kbps}k",
                "-bufsize", f"{cfg.original_video_bufsize_kbps}k",
                "-g", str(original_gop),
                *audio_output,
            ]

        return [
            "ffmpeg",
            "-hide_banner",
            "-thread_queue_size", "1024",
            "-f", "v4l2",
            "-input_format", "mjpeg",
            "-video_size", f"{cfg.video_width}x{cfg.video_height}",
            "-framerate", str(cfg.video_fps),
            "-i", cfg.camera_device,
            *audio_inputs,
            "-map", "0:v",
            *original_encode,
            "-f", "segment",
            "-segment_time", str(cfg.chunk_duration_seconds),
            "-segment_format", "mp4",
            "-reset_timestamps", "1",
            "-strftime", "1",
            "-segment_list_type", "csv",
            "-segment_list_flags", "+cache",
            "-segment_list", str(original.segment_list_path),
            original_pattern,
            *preview_output,
        ]

    def _audio_available(self) -> bool:
        """
        Probe the ALSA mic with a brief capture. Returns True only if ffmpeg can
        actually open and read the device, so a missing/busy/misconfigured mic
        degrades to video-only instead of taking down the whole pipeline.
        """
        cfg = self.cfg
        probe_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-f", "alsa",
            "-ac", str(cfg.audio_channels),
            "-ar", str(cfg.audio_sample_rate),
            "-i", cfg.audio_device,
            "-t", "0.3",
            "-f", "null",
            "-",
        ]
        try:
            result = subprocess.run(
                probe_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, OSError) as exc:
            logger.warning(f"Audio probe errored for {cfg.audio_device}: {exc}")
            return False

    def _build_preview_video_filter(self, cfg: Config) -> str:
        """1 fps preview at 720p with wall-clock timestamp (preview only)."""
        font = cfg.preview_timestamp_font.replace(":", r"\:")
        drawtext = (
            f"drawtext=fontfile={font}:text='{_DRAWTEXT_TIME}'"
            ":fontcolor=0xFFA500:borderw=2:bordercolor=black:fontsize=22"
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

            if self._capture and self._capture.poll() is not None:
                if not self._stop_event.is_set():
                    logger.error(
                        f"ffmpeg exited unexpectedly with code {self._capture.returncode}"
                    )
                    if self.on_unexpected_stop:
                        try:
                            self.on_unexpected_stop()
                        except Exception:
                            logger.exception("on_unexpected_stop callback failed")
                return

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

    def _log_ffmpeg_stderr(self, proc: subprocess.Popen, tag: str):
        """Stream an ffmpeg process's stderr to our logger for diagnostics."""
        if not proc.stderr:
            return
        for line in proc.stderr:
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                logger.warning(f"[{tag}] {decoded}")
