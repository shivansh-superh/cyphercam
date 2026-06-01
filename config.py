import os
from dataclasses import dataclass


REQUIRED_VARS = [
    "OT_LOCATION_ID",
    "OT_LOCATION_NAME",
    "OT_HOSPITAL_ID",
    "S3_BUCKET",
    "S3_PREFIX",
    "AWS_DEFAULT_REGION",
    "HMS_API_BASE_URL",
    "HMS_API_KEY",
]


@dataclass
class Config:
    ot_location_id: str
    ot_location_name: str
    ot_hospital_id: str

    s3_bucket: str
    s3_prefix: str
    aws_region: str

    hms_api_base_url: str
    hms_api_key: str

    camera_device: str
    video_width: int
    video_height: int
    video_fps: int
    chunk_duration_seconds: int
    chunk_dir: str
    manifest_path: str

    trigger_port: int
    min_free_disk_mb: int


def load_config() -> Config:
    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    return Config(
        ot_location_id=os.environ["OT_LOCATION_ID"],
        ot_location_name=os.environ["OT_LOCATION_NAME"],
        ot_hospital_id=os.environ["OT_HOSPITAL_ID"],

        s3_bucket=os.environ["S3_BUCKET"],
        s3_prefix=os.environ["S3_PREFIX"],
        aws_region=os.environ["AWS_DEFAULT_REGION"],

        hms_api_base_url=os.environ["HMS_API_BASE_URL"].rstrip("/"),
        hms_api_key=os.environ["HMS_API_KEY"],

        camera_device=os.environ.get("CAMERA_DEVICE", "/dev/video0"),
        video_width=int(os.environ.get("VIDEO_WIDTH", "1920")),
        video_height=int(os.environ.get("VIDEO_HEIGHT", "1080")),
        video_fps=int(os.environ.get("VIDEO_FPS", "30")),
        chunk_duration_seconds=int(os.environ.get("CHUNK_DURATION_SECONDS", "300")),
        chunk_dir=os.environ.get("CHUNK_DIR", "/tmp/ot-chunks"),
        manifest_path=os.environ.get("MANIFEST_PATH", "/var/lib/ot-recorder/manifest.db"),

        trigger_port=int(os.environ.get("TRIGGER_PORT", "8080")),
        min_free_disk_mb=int(os.environ.get("MIN_FREE_DISK_MB", "2048")),
    )
