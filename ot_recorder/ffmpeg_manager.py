"""
Manages the ffmpeg subprocess(es).

Two ffmpeg processes are chained by a pipe:

  capture  — reads the camera once and produces two outputs:
    * original: MJPEG decode → H.264/HEVC via the Pi VPU (h264_v4l2m2m),
      written to stdout as an MPEG-TS elementary stream (pipe, never on disk)
    * preview:  decode → 1 fps → 720p → libx264, written directly as MP4 segments

  remux    — reads the original MPEG-TS from the pipe and stream-copies it into
    MP4 segments ({chunk_dir}/{surgery_id}/original/YYYYMMDD_HHMMSS.mp4)

Why the extra remux hop: the v4l2m2m encoder cannot hand the MP4 muxer its
SPS/PPS extradata at init time (it only emits parameter sets after the first
frame is encoded). Muxing the VPU output straight to segmented MP4 therefore
leaves every segment's avcC box empty, so only the first chunk — which happens
to carry SPS/PPS inline — decodes, and all later chunks play back black. Going
through MPEG-TS preserves timestamps and keeps SPS/PPS inline at every keyframe;
the remux step's TS demuxer recovers them into real extradata, giving each MP4
segment a valid avcC. See raspberrypi/linux#5150. Preview uses libx264, which
populates extradata correctly, so it muxes to MP4 directly.

Each stream has its own segment list CSV; chunk_sequence is the 1-based line
index in that CSV so original and preview rows with the same index align.
"""

import fcntl
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

# Linux fcntl constant — not exposed by name in the fcntl module.
_F_SETPIPE_SZ = 1031
# 4 MB gives remux a large window to absorb SD-card write stalls before capture
# ever blocks and drops frames from the V4L2 buffer.
_PIPE_BUF_SIZE = 4 * 1024 * 1024


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
        # _capture reads the camera and emits original (MPEG-TS via pipe) + preview (MP4).
        # _remux reads the piped MPEG-TS and writes the original MP4 segments.
        self._capture: Optional[subprocess.Popen] = None
        self._remux: Optional[subprocess.Popen] = None
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
        remux_cmd = self._build_remux_cmd()
        logger.info(f"Starting ffmpeg (capture): {' '.join(capture_cmd)}")
        logger.info(f"Starting ffmpeg (remux): {' '.join(remux_cmd)}")

        # Capture writes the original MPEG-TS to stdout; remux consumes it from stdin.
        self._capture = subprocess.Popen(
            capture_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Enlarge the OS pipe buffer so that brief remux disk stalls (e.g. when
        # closing an MP4 segment on an SD card) don't back-pressure capture and
        # cause V4L2 frame drops. F_SETPIPE_SZ is Linux-specific; fail silently
        # on other platforms (macOS dev machines, etc.).
        if self._capture.stdout:
            try:
                fcntl.fcntl(self._capture.stdout.fileno(), _F_SETPIPE_SZ, _PIPE_BUF_SIZE)
                logger.info(f"Pipe buffer set to {_PIPE_BUF_SIZE // 1024} KB")
            except OSError as exc:
                logger.warning(f"Could not enlarge pipe buffer: {exc}")

        self._remux = subprocess.Popen(
            remux_cmd,
            stdin=self._capture.stdout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        # Close our copy of the pipe's read end so only remux holds it; this lets
        # capture receive SIGPIPE if remux dies, and lets remux see EOF when
        # capture exits.
        if self._capture.stdout:
            self._capture.stdout.close()

        self._monitor_thread = threading.Thread(
            target=self._monitor_segments,
            name="ffmpeg-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        for proc, tag in ((self._capture, "ffmpeg"), (self._remux, "remux")):
            threading.Thread(
                target=self._log_ffmpeg_stderr,
                args=(proc, tag),
                name=f"{tag}-stderr",
                daemon=True,
            ).start()

        logger.info(
            f"ffmpeg started (capture pid {self._capture.pid}, "
            f"remux pid {self._remux.pid}) for surgery {surgery_id}"
        )

    def stop(self, timeout: int = 30) -> bool:
        """
        Gracefully stop recording. SIGTERMs the capture process so it finalizes
        the current preview segment and closes the pipe; remux then drains the
        remaining MPEG-TS, finalizes its last original segment on EOF, and exits.
        Returns True if both stopped cleanly, False if we had to SIGKILL either.
        """
        if not self._capture and not self._remux:
            return True

        self._stop_event.set()
        clean = True

        # Stop capture first so remux gets a clean EOF and flushes its last chunk.
        if self._capture:
            logger.info("Sending SIGTERM to ffmpeg (capture)...")
            self._capture.terminate()
            try:
                self._capture.wait(timeout=timeout)
                logger.info("ffmpeg (capture) stopped cleanly")
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"ffmpeg (capture) did not stop within {timeout}s, sending SIGKILL"
                )
                self._capture.kill()
                self._capture.wait()
                clean = False

        # Remux should exit on its own once the pipe closes; give it a window.
        if self._remux:
            try:
                self._remux.wait(timeout=timeout)
                logger.info("ffmpeg (remux) stopped cleanly")
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"ffmpeg (remux) did not drain within {timeout}s, sending SIGTERM"
                )
                self._remux.terminate()
                try:
                    self._remux.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.warning("ffmpeg (remux) ignored SIGTERM, sending SIGKILL")
                    self._remux.kill()
                    self._remux.wait()
                    clean = False

        self._scan_all_segment_lists()
        self._capture = None
        self._remux = None
        self._surgery_id = None
        self._streams = []
        return clean

    def _build_capture_cmd(self, use_audio: bool) -> list[str]:
        """
        Reads the camera once and produces:
          * original: VPU-encoded H.264/HEVC (+ AAC audio when use_audio) as an
            MPEG-TS elementary stream on stdout (consumed by remux — never on disk)
          * preview:  1 fps / 720p libx264 MP4 segments written directly to disk
            (omitted when cfg.preview_enabled is False)

        Audio (when enabled) is captured from a separate ALSA device and added to
        the original only; the preview stays silent.
        """
        cfg = self.cfg

        # Keyframe interval aligned to chunk boundaries so the downstream segment
        # muxer can cut at exactly chunk_duration_seconds on an IDR.
        # -g is silently ignored by v4l2m2m encoders; -force_key_frames is the workaround.
        original_gop = cfg.video_fps * cfg.chunk_duration_seconds

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
            # Preview — drop to 1 fps before scale to minimize CPU. libx264 supplies
            # extradata correctly, so this muxes straight to MP4 segments.
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

            # Original — hardware encode via Pi VPU, emitted as MPEG-TS on stdout.
            # bcm2835-codec requires yuv420p; MJPEG decodes to yuvj422p so we convert explicitly.
            # -force_key_frames guarantees an IDR at every chunk boundary since -g is ignored.
            # MPEG-TS keeps SPS/PPS inline at each IDR and preserves timestamps, which the
            # remux step needs to produce self-contained, correctly-cut MP4 chunks.
            "-map", "0:v",
            "-vf", "format=yuv420p",
            "-c:v", cfg.video_codec,
            "-b:v", f"{cfg.original_video_bitrate_kbps}k",
            "-maxrate", f"{cfg.original_video_bitrate_kbps}k",
            "-bufsize", f"{cfg.original_video_bufsize_kbps}k",
            "-g", str(original_gop),
            "-force_key_frames", f"expr:gte(t,n_forced*{cfg.chunk_duration_seconds})",
            *audio_output,
            "-f", "mpegts",
            "pipe:1",

            *preview_output,
        ]

    def _build_remux_cmd(self) -> list[str]:
        """
        Reads the original MPEG-TS from stdin and stream-copies it into MP4
        segments. Demuxing TS recovers SPS/PPS into real extradata, so every MP4
        chunk gets a valid avcC box (unlike muxing the VPU output to MP4 directly,
        which leaves segments 2+ black). No re-encode happens here.
        """
        cfg = self.cfg
        original = self._streams[0]
        original_pattern = str(original.output_dir / "%Y%m%d_%H%M%S.mp4")

        # -map 0:a? is optional: it copies the audio track when the capture stage
        # included one and is a no-op when recording video only.
        return [
            "ffmpeg",
            "-hide_banner",
            "-f", "mpegts",
            "-i", "pipe:0",
            "-map", "0:v",
            "-map", "0:a?",
            "-c", "copy",
            "-f", "segment",
            "-segment_time", str(cfg.chunk_duration_seconds),
            "-segment_format", "mp4",
            "-reset_timestamps", "1",
            "-strftime", "1",
            "-segment_list_type", "csv",
            "-segment_list_flags", "+cache",
            "-segment_list", str(original.segment_list_path),
            original_pattern,
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

            for proc, tag in ((self._capture, "capture"), (self._remux, "remux")):
                if proc and proc.poll() is not None:
                    if not self._stop_event.is_set():
                        logger.error(
                            f"ffmpeg ({tag}) exited unexpectedly with code "
                            f"{proc.returncode}"
                        )
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
