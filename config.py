from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv(override=False)


def _read_local_dotenv_value(name: str, default: str) -> str:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return default
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() != name:
                continue
            return value.strip().strip("\"'")
    except Exception:
        return default
    return default


def _normalize_redis_port(host: str, port: str) -> str:
    # Compose maps redis as 9002 externally, but redis itself listens on 6379.
    if host and host.strip().lower() == "redis" and str(port).strip() == "9002":
        return "6379"
    return str(port).strip()


def _normalize_postgres_port(host: str, port: str) -> str:
    # Compose publishes postgis on 9000 externally, but service listens on 5432.
    if host and host.strip().lower() == "postgis" and str(port).strip() == "9000":
        return "5432"
    return str(port).strip()


LOCKED_WAITING_BUCKET_NAME = "drone-imagery-waiting"
LOCKED_PROCESSED_BUCKET_NAME = "drone-imagery"


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        fallback = _read_local_dotenv_value(name, default)
        return fallback if fallback else default
    return str(value)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    if not str(value).strip():
        return default
    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    if not str(value).strip():
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _derive_postgres_url() -> str:
    host = _env("POSTGRES_HOST", "postgis")
    port = _env("POSTGRES_PORT", "5432")
    if not os.getenv("POSTGRES_HOST") and not os.getenv("POSTGRES_PORT"):
        host = _read_local_dotenv_value("POSTGRES_HOST", host)
        port = _read_local_dotenv_value("POSTGRES_PORT", port)
    port = _normalize_postgres_port(host, port)
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
    host = _env("RSLOGIC_REDIS_HOST", _env("REDIS_HOST", "redis"))
    port = _env("RSLOGIC_REDIS_PORT", _env("REDIS_PORT", "6379"))
    if not os.getenv("RSLOGIC_REDIS_HOST") and not os.getenv("RSLOGIC_REDIS_PORT"):
        host = _read_local_dotenv_value("REDIS_HOST", host)
        port = _read_local_dotenv_value("REDIS_PORT", port)
    port = _normalize_redis_port(host, port)
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
    executable_args: Optional[str]
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
    cpu_default_workers = max(os.cpu_count() or 1, 1)

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
        multipart_concurrency=max(_env_int("RSLOGIC_S3_MULTIPART_CONCURRENCY", cpu_default_workers), 1),
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
        executable_args=_env("RSLOGIC_RSTOOLS_EXECUTABLE_ARGS", None),
        working_root=_env("RSLOGIC_RSTOOLS_WORKING_ROOT", "/tmp/rslogic-jobs"),
        mode=_env("RSLOGIC_RSTOOLS_MODE", "stub").lower(),
        sdk_base_url=os.getenv("RSLOGIC_RSTOOLS_SDK_BASE_URL"),
        sdk_client_id=os.getenv("RSLOGIC_RSTOOLS_SDK_CLIENT_ID"),
        sdk_app_token=os.getenv("RSLOGIC_RSTOOLS_SDK_APP_TOKEN"),
        sdk_auth_token=os.getenv("RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN"),
    )

    label_db_root = _env("RSLOGIC_LABEL_DB_ROOT", "rslogic/internal_tools/label-db/studio-db")
    if not Path(label_db_root).exists():
        base_root = Path(__file__).resolve().parent
        fallback_candidates = [
            base_root / "internal_tools" / "label-db" / "studio-db",
            base_root / "rslogic" / "internal_tools" / "label-db" / "studio-db",
        ]
        for candidate in fallback_candidates:
            if candidate.exists():
                label_db_root = str(candidate)
                break
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
