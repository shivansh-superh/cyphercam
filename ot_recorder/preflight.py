"""
Preflight checks. All must pass before the recorder starts.
Each check raises PreflightError with a clear message on failure.
"""

import logging
import os
import shutil
import threading

from .config import Config

logger = logging.getLogger(__name__)


class PreflightError(Exception):
    pass


def check_camera(cfg: Config):
    if not os.path.exists(cfg.camera_device):
        raise PreflightError(
            f"Camera device {cfg.camera_device} not found. "
            "Is the OBSBOT plugged in and the uvcvideo driver loaded?"
        )
    if not os.access(cfg.camera_device, os.R_OK):
        raise PreflightError(
            f"No read permission on {cfg.camera_device}. "
            "Add the service user to the 'video' group: usermod -aG video pi"
        )
    logger.info(f"Camera check passed: {cfg.camera_device}")


def check_disk_space(cfg: Config):
    os.makedirs(cfg.chunk_dir, exist_ok=True)
    usage = shutil.disk_usage(cfg.chunk_dir)
    free_mb = usage.free // (1024 * 1024)
    if free_mb < cfg.min_free_disk_mb:
        raise PreflightError(
            f"Insufficient disk space: {free_mb}MB free, need {cfg.min_free_disk_mb}MB. "
            f"Clean up {cfg.chunk_dir} or increase the disk."
        )
    logger.info(f"Disk check passed: {free_mb}MB free")


def check_preview_timestamp_font(cfg: Config):
    if not os.path.isfile(cfg.preview_timestamp_font):
        raise PreflightError(
            f"Preview timestamp font not found: {cfg.preview_timestamp_font}. "
            "Install fonts-dejavu-core or set PREVIEW_TIMESTAMP_FONT."
        )
    logger.info(f"Timestamp font check passed: {cfg.preview_timestamp_font}")


def check_ffmpeg():
    if not shutil.which("ffmpeg"):
        raise PreflightError(
            "ffmpeg not found in PATH. Install with: sudo apt install ffmpeg"
        )
    logger.info("ffmpeg check passed")


def check_audio(cfg: Config):
    """
    Non-fatal: audio is best-effort. The mic is probed again at recording start,
    where an unreachable device falls back to video-only. This just surfaces the
    configured intent in the logs.
    """
    if not cfg.audio_enabled:
        logger.info("Audio disabled (AUDIO_ENABLED=false)")
        return
    logger.info(f"Audio enabled — device {cfg.audio_device} (probed at record start)")


def run_all(cfg: Config):
    logger.info("Running preflight checks...")
    check_ffmpeg()
    if cfg.preview_enabled:
        check_preview_timestamp_font(cfg)
    check_camera(cfg)
    check_disk_space(cfg)
    check_audio(cfg)
    logger.info("All preflight checks passed.")


def wait_until_ready(
    cfg: Config,
    shutdown_event: threading.Event,
    interval_sec: float = 5.0,
) -> None:
    """
    Block until preflight passes or shutdown_event is set (SIGTERM).
    Retries camera/disk checks so hot-plug works without systemd device units.
    """
    logger.info("Running preflight checks...")
    check_ffmpeg()
    while not shutdown_event.is_set():
        try:
            if cfg.preview_enabled:
                check_preview_timestamp_font(cfg)
            check_camera(cfg)
            check_disk_space(cfg)
            check_audio(cfg)
            logger.info("All preflight checks passed.")
            return
        except PreflightError as exc:
            logger.warning("%s — retrying in %ss", exc, interval_sec)
            shutdown_event.wait(interval_sec)
    logger.info("Shutdown requested before preflight completed.")
