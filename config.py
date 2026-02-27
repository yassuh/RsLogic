from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv(override=True)

LOCKED_WAITING_BUCKET_NAME = "drone-imagery-waiting"
LOCKED_PROCESSED_BUCKET_NAME = "drone-imagery"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _derive_postgres_url() -> str:
    host = _env("POSTGRES_HOST", "postgis")
    port = _env("POSTGRES_PORT", "5432")
    database = _env("POSTGRES_DB", "rslogic")
    user = _env("POSTGRES_USER", "postgres")
    password = _env("POSTGRES_PASSWORD", "")
    return (
        "postgresql+psycopg://"
        f"{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
    )


def _derive_redis_url() -> str:
    explicit = _env("RSLOGIC_REDIS_URL", _env("REDIS_URL", ""))
    if explicit:
        return explicit
    host = _env("RSLOGIC_REDIS_HOST", _env("REDIS_HOST", "localhost"))
    port = _env("RSLOGIC_REDIS_PORT", _env("REDIS_PORT", "6379"))
    database = _env("RSLOGIC_REDIS_DB", _env("REDIS_DB", "0"))
    password = _env("RSLOGIC_REDIS_PASSWORD", _env("REDIS_PASSWORD", "")).strip()
    if password:
        return f"redis://:{quote_plus(password)}@{host}:{port}/{database}"
    return f"redis://{host}:{port}/{database}"


@dataclass(frozen=True)
class S3Config:
    """S3 configuration used by upload and ingest flows."""

    region: str
    bucket_name: str
    processed_bucket_name: str
    scratchpad_prefix: str
    endpoint_url: Optional[str]
    multipart_part_size: int
    multipart_concurrency: int
    resume_uploads: bool
    manifest_dir: str


@dataclass(frozen=True)
class QueueConfig:
    """Queue configuration for background job processing."""

    worker_count: int
    poll_interval_seconds: int
    backend: str
    redis_url: str
    redis_queue_key: str
    redis_block_timeout_seconds: int
    start_local_workers: bool


@dataclass(frozen=True)
class ControlConfig:
    """Redis command bus for processing jobs and remote rsnode workers."""

    command_queue_key: str
    result_queue_key: str
    block_timeout_seconds: int
    result_ttl_seconds: int
    request_timeout_seconds: int


@dataclass(frozen=True)
class RsToolsConfig:
    """Adapter configuration for the RsTools command-line integration."""

    executable_path: Optional[str]
    working_root: str
    mode: str
    sdk_base_url: Optional[str]
    sdk_client_id: Optional[str]
    sdk_app_token: Optional[str]
    sdk_auth_token: Optional[str]


@dataclass(frozen=True)
class LabelDbConfig:
    """Label-db connection and migration settings shared by services."""

    migration_root: str
    alembic_ini: str
    database_url: str


@dataclass(frozen=True)
class ApiConfig:
    """API endpoint settings used by local tools like the interactive TUI."""

    base_url: str


@dataclass(frozen=True)
class LogConfig:
    """Logging configuration for API and background workers."""

    level: str
    format: str


@dataclass(frozen=True)
class AppConfig:
    """Top-level strongly-typed settings object for the service."""

    app_name: str
    default_group_name: str
    log: LogConfig
    s3: S3Config
    queue: QueueConfig
    control: ControlConfig
    rstools: RsToolsConfig
    label_db: LabelDbConfig
    api: ApiConfig


def load_config() -> AppConfig:
    """Load all config dataclasses from environment variables and defaults."""

    label_db_url = _env(
        "RSLOGIC_LABEL_DB_DATABASE_URL",
        _env("RSLOGIC_DATABASE_URL", _env("DATABASE_URL", "")),
    )
    if not label_db_url:
        label_db_url = _derive_postgres_url()

    # Compatibility default: keep queue worker defaults from explicit config with sane minimums.
    s3 = S3Config(
        region=_env("RSLOGIC_S3_REGION", _env("S3_REGION", "us-east-1")),
        bucket_name=LOCKED_WAITING_BUCKET_NAME,
        processed_bucket_name=_env(
            "RSLOGIC_S3_PROCESSED_BUCKET_NAME",
            _env("S3_PROCESSED_BUCKET_NAME", LOCKED_PROCESSED_BUCKET_NAME),
        ),
        scratchpad_prefix=_env("RSLOGIC_S3_SCRATCHPAD_PREFIX", _env("S3_SCRATCHPAD_PREFIX", "scratchpad")),
        endpoint_url=os.getenv("RSLOGIC_S3_ENDPOINT_URL", os.getenv("S3_ENDPOINT_URL")),
        multipart_part_size=max(_env_int("RSLOGIC_S3_MULTIPART_PART_SIZE", 16 * 1024 * 1024), 5 * 1024 * 1024),
        multipart_concurrency=max(_env_int("RSLOGIC_S3_MULTIPART_CONCURRENCY", 24), 1),
        resume_uploads=_env_bool("RSLOGIC_S3_RESUME_UPLOADS", True),
        manifest_dir=_env("RSLOGIC_S3_MANIFEST_DIR", str(Path.home() / ".rslogic" / "upload-state")),
    )

    queue = QueueConfig(
        worker_count=max(_env_int("RSLOGIC_WORKER_COUNT", 4), 1),
        poll_interval_seconds=max(_env_int("RSLOGIC_QUEUE_POLL_SECONDS", 2), 1),
        backend=_env("RSLOGIC_QUEUE_BACKEND", "redis").strip().lower(),
        redis_url=_derive_redis_url(),
        redis_queue_key=_env("RSLOGIC_REDIS_QUEUE_KEY", "rslogic:jobs:queue"),
        redis_block_timeout_seconds=max(_env_int("RSLOGIC_REDIS_BLOCK_TIMEOUT_SECONDS", 1), 1),
        start_local_workers=_env_bool("RSLOGIC_QUEUE_START_LOCAL_WORKERS", True),
    )
    control = ControlConfig(
        command_queue_key=_env("RSLOGIC_CONTROL_COMMAND_QUEUE", "rslogic:control:commands"),
        result_queue_key=_env("RSLOGIC_CONTROL_RESULT_QUEUE", "rslogic:control:results"),
        block_timeout_seconds=max(_env_int("RSLOGIC_CONTROL_BLOCK_TIMEOUT_SECONDS", 2), 1),
        result_ttl_seconds=max(_env_int("RSLOGIC_CONTROL_RESULT_TTL_SECONDS", 3600), 1),
        request_timeout_seconds=max(_env_int("RSLOGIC_CONTROL_REQUEST_TIMEOUT_SECONDS", 7200), 1),
    )

    rstools = RsToolsConfig(
        executable_path=os.getenv("RSLOGIC_RSTOOLS_EXECUTABLE"),
        working_root=_env("RSLOGIC_RSTOOLS_WORKING_ROOT", "/tmp/rslogic-jobs"),
        mode=_env("RSLOGIC_RSTOOLS_MODE", "stub").lower(),
        sdk_base_url=os.getenv("RSLOGIC_RSTOOLS_SDK_BASE_URL"),
        sdk_client_id=os.getenv("RSLOGIC_RSTOOLS_SDK_CLIENT_ID"),
        sdk_app_token=os.getenv("RSLOGIC_RSTOOLS_SDK_APP_TOKEN"),
        sdk_auth_token=os.getenv("RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN"),
    )

    label_db_root = _env("RSLOGIC_LABEL_DB_ROOT", "internal_tools/label-db/studio-db")
    label_db_root_path = Path(label_db_root)
    if not label_db_root_path.is_absolute():
        label_db_root_path = Path(__file__).resolve().parent / label_db_root_path
    label_db = LabelDbConfig(
        migration_root=str(label_db_root_path),
        alembic_ini=_env(
            "RSLOGIC_LABEL_DB_ALEMBIC_INI",
            str(label_db_root_path / "alembic.ini"),
        ),
        database_url=label_db_url,
    )
    api = ApiConfig(
        base_url=_env("RSLOGIC_API_BASE_URL", "http://localhost:8000").rstrip("/"),
    )
    log = LogConfig(
        level=_env("RSLOGIC_LOG_LEVEL", "INFO").upper(),
        format=_env(
            "RSLOGIC_LOG_FORMAT",
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
        ),
    )

    return AppConfig(
        app_name=_env("RSLOGIC_APP_NAME", "RsLogic Service"),
        default_group_name=_env("RSLOGIC_DEFAULT_GROUP_NAME", "default-group"),
        log=log,
        s3=s3,
        queue=queue,
        control=control,
        rstools=rstools,
        label_db=label_db,
        api=api,
    )


CONFIG = load_config()
