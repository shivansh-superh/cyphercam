"""
Preflight checks. All must pass before the recorder starts.
Each check raises PreflightError with a clear message on failure.
"""

import logging
import os
import shutil

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


def check_ffmpeg():
    if not shutil.which("ffmpeg"):
        raise PreflightError(
            "ffmpeg not found in PATH. Install with: sudo apt install ffmpeg"
        )
    logger.info("ffmpeg check passed")


def run_all(cfg: Config):
    logger.info("Running preflight checks...")
    check_ffmpeg()
    check_camera(cfg)
    check_disk_space(cfg)
    logger.info("All preflight checks passed.")
