"""
Notify the AI pipeline after a preview chunk is uploaded to S3.
"""

import json
import logging
import urllib.error
import urllib.request

from .config import Config

logger = logging.getLogger(__name__)


def trigger_analyze(
    cfg: Config,
    *,
    video_id: str,
    presigned_url: str,
    ipd_appointment_id: str,
) -> bool:
    if not cfg.ai_pipeline_base_url or not cfg.ai_pipeline_api_key:
        logger.debug(
            "AI pipeline not configured — skipping analyze-ot-video for %s",
            video_id,
        )
        return False

    url = f"{cfg.ai_pipeline_base_url.rstrip('/')}/analyze-ot-video"
    payload = {
        "video_id": video_id,
        "presigned_url": presigned_url,
        "ipd_appointment_id": ipd_appointment_id,
    }
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": cfg.ai_pipeline_api_key,
        },
        method="POST",
    )

    logger.info(
        "Calling AI pipeline analyze-ot-video for video_id=%s ipd_appointment_id=%s",
        video_id,
        ipd_appointment_id,
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status >= 400:
                logger.error(
                    "AI pipeline analyze-ot-video returned HTTP %s for video_id=%s",
                    resp.status,
                    video_id,
                )
                return False
            return True
    except urllib.error.HTTPError as e:
        logger.error(
            "AI pipeline analyze-ot-video failed (HTTP %s) for video_id=%s: %s",
            e.code,
            video_id,
            e.read().decode("utf-8", errors="replace"),
        )
        return False
    except urllib.error.URLError as e:
        logger.error(
            "AI pipeline analyze-ot-video request failed for video_id=%s: %s",
            video_id,
            e.reason,
        )
        return False
