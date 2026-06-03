import os
from dataclasses import dataclass


REQUIRED_VARS = [
    "OT_LOCATION_ID",
    "OT_LOCATION_NAME",
    "OT_HOSPITAL_ID",
    "S3_BUCKET",
    "S3_PREFIX",
    "AWS_DEFAULT_REGION",
    "TB_HOST",
    "TB_ACCESS_TOKEN",
]


@dataclass
class Config:
    ot_location_id: str
    ot_location_name: str
    ot_hospital_id: str

    s3_bucket: str
    s3_prefix: str
    aws_region: str

    tb_host: str
    tb_access_token: str
    tb_mqtt_port: int
    tb_mqtt_use_tls: bool
    tb_mqtt_ca_file: str | None

    camera_device: str
    video_width: int
    video_height: int
    video_fps: int
    video_crf: int
    preview_height: int
    preview_fps: int
    preview_crf: int
    preview_timestamp_font: str
    chunk_duration_seconds: int
    chunk_dir: str
    manifest_path: str

    min_free_disk_mb: int

    ai_pipeline_base_url: str | None
    ai_pipeline_api_key: str | None
    ai_pipeline_presigned_url_expiry: int


def _normalize_tb_host(raw: str) -> str:
    host = raw.strip().rstrip("/")
    for prefix in ("https://", "http://", "mqtts://", "mqtt://"):
        if host.lower().startswith(prefix):
            host = host[len(prefix) :]
    return host.split("/")[0]


def load_config() -> Config:
    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    tb_mqtt_use_tls = os.environ.get("TB_MQTT_USE_TLS", "false").lower() in (
        "1",
        "true",
        "yes",
    )
    tb_mqtt_port = int(
        os.environ.get("TB_MQTT_PORT", "8883" if tb_mqtt_use_tls else "1883")
    )

    return Config(
        ot_location_id=os.environ["OT_LOCATION_ID"],
        ot_location_name=os.environ["OT_LOCATION_NAME"],
        ot_hospital_id=os.environ["OT_HOSPITAL_ID"],

        s3_bucket=os.environ["S3_BUCKET"],
        s3_prefix=os.environ["S3_PREFIX"],
        aws_region=os.environ["AWS_DEFAULT_REGION"],

        tb_host=_normalize_tb_host(os.environ["TB_HOST"]),
        tb_access_token=os.environ["TB_ACCESS_TOKEN"].strip(),
        tb_mqtt_use_tls=tb_mqtt_use_tls,
        tb_mqtt_port=tb_mqtt_port,
        tb_mqtt_ca_file=os.environ.get("TB_MQTT_CA_FILE") or None,

        camera_device=os.environ.get("CAMERA_DEVICE", "/dev/video0"),
        video_width=int(os.environ.get("VIDEO_WIDTH", "1920")),
        video_height=int(os.environ.get("VIDEO_HEIGHT", "1080")),
        video_fps=int(os.environ.get("VIDEO_FPS", "30")),
        video_crf=int(os.environ.get("VIDEO_CRF", "23")),
        preview_height=int(os.environ.get("PREVIEW_HEIGHT", "720")),
        preview_fps=int(os.environ.get("PREVIEW_FPS", "1")),
        preview_crf=int(os.environ.get("PREVIEW_CRF", "28")),
        preview_timestamp_font=os.environ.get(
            "PREVIEW_TIMESTAMP_FONT",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ),
        chunk_duration_seconds=int(os.environ.get("CHUNK_DURATION_SECONDS", "300")),
        chunk_dir=os.environ.get("CHUNK_DIR", "/tmp/ot-chunks"),
        manifest_path=os.environ.get("MANIFEST_PATH", "/var/lib/ot-recorder/manifest.db"),

        min_free_disk_mb=int(os.environ.get("MIN_FREE_DISK_MB", "2048")),

        ai_pipeline_base_url=os.environ.get("AI_PIPELINE_BASE_URL") or None,
        ai_pipeline_api_key=os.environ.get("AI_PIPELINE_API_KEY") or None,
        ai_pipeline_presigned_url_expiry=int(
            os.environ.get("AI_PIPELINE_PRESIGNED_URL_EXPIRY", "3600")
        ),
    )
