from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "env_overrides",
    [
        {
            "RSLOGIC_S3_REGION": "eu-west-1",
            "RSLOGIC_S3_PROCESSED_BUCKET_NAME": "processed-bucket",
            "RSLOGIC_LABEL_DB_DATABASE_URL": "postgresql+psycopg://test:test@db:5432/testdb",
            "RSLOGIC_QUEUE_BACKEND": "redis",
            "RSLOGIC_REDIS_HOST": "redis-host",
            "RSLOGIC_REDIS_PORT": "6380",
            "RSLOGIC_REDIS_DB": "7",
            "RSLOGIC_REDIS_PASSWORD": "s3cr3t",
            "RSLOGIC_CONTROL_COMMAND_QUEUE": "rslogic:unit:commands",
            "RSLOGIC_API_BASE_URL": "http://127.0.0.1:9000",
        }
    ],
)
def test_load_config_resolves_expected_values(monkeypatch, tmp_path, env_overrides: dict[str, str]):
    # Keep path-derived defaults stable for assertion.
    root = tmp_path / "label-db"
    root.mkdir(parents=True)
    (tmp_path / ".env").write_text("", encoding="utf-8")

    keys_to_clear = [
        "S3_REGION",
        "S3_PROCESSED_BUCKET_NAME",
        "S3_SCRATCHPAD_PREFIX",
        "RSLOGIC_S3_SCRATCHPAD_PREFIX",
        "RSLOGIC_S3_ENDPOINT_URL",
        "RSLOGIC_S3_MULTIPART_PART_SIZE",
        "RSLOGIC_S3_MULTIPART_CONCURRENCY",
        "RSLOGIC_LABEL_DB_DATABASE_URL",
        "RSLOGIC_DATABASE_URL",
        "DATABASE_URL",
        "RSLOGIC_WORKER_COUNT",
        "RSLOGIC_QUEUE_POLL_SECONDS",
        "RSLOGIC_REDIS_URL",
        "RSLOGIC_CONTROL_REQUEST_TIMEOUT_SECONDS",
        "RSLOGIC_RSTOOLS_WORKING_ROOT",
        "RSLOGIC_LABEL_DB_ROOT",
    ]
    for key in keys_to_clear:
        monkeypatch.delenv(key, raising=False)

    for key, value in env_overrides.items():
        monkeypatch.setenv(key, value)

    monkeypatch.setenv("RSLOGIC_LABEL_DB_ROOT", str(root))
    monkeypatch.setenv("RSLOGIC_S3_ENDPOINT_URL", "https://s3.example.invalid")

    importlib.invalidate_caches()
    import config as cfg_module

    reloaded = importlib.reload(cfg_module)
    cfg = reloaded.load_config()

    assert cfg.s3.region == "eu-west-1"
    assert cfg.s3.processed_bucket_name == "processed-bucket"
    assert cfg.label_db.database_url == "postgresql+psycopg://test:test@db:5432/testdb"
    assert cfg.label_db.migration_root == str(root)
    assert cfg.label_db.alembic_ini == str(root / "alembic.ini")
    assert cfg.queue.backend == "redis"
    assert cfg.queue.redis_url == "redis://:s3cr3t@redis-host:6380/7"
    assert cfg.control.command_queue_key == "rslogic:unit:commands"
    assert cfg.api.base_url == "http://127.0.0.1:9000"
    assert cfg.s3.endpoint_url == "https://s3.example.invalid"


def test_load_config_uses_explicit_redis_url(monkeypatch):
    monkeypatch.setenv("RSLOGIC_REDIS_URL", "redis://:x@y:7777/3")
    for key in ["RSLOGIC_REDIS_HOST", "RSLOGIC_REDIS_PORT", "RSLOGIC_REDIS_DB"]:
        monkeypatch.delenv(key, raising=False)

    import config as cfg_module
    cfg = importlib.reload(cfg_module).load_config()

    assert cfg.queue.redis_url == "redis://:x@y:7777/3"


def test_load_config_parses_integers_and_booleans(monkeypatch):
    monkeypatch.setenv("RSLOGIC_QUEUE_POLL_SECONDS", "0")
    monkeypatch.setenv("RSLOGIC_WORKER_COUNT", "-7")
    monkeypatch.setenv("RSLOGIC_S3_MULTIPART_CONCURRENCY", "0")
    monkeypatch.setenv("RSLOGIC_S3_RESUME_UPLOADS", "false")

    import config as cfg_module
    cfg = importlib.reload(cfg_module).load_config()

    assert cfg.queue.poll_interval_seconds == 1
    assert cfg.queue.worker_count == 1
    assert cfg.s3.multipart_concurrency == 1
    assert cfg.s3.resume_uploads is False


def test_load_config_defaults_are_locked_when_bucket_env_missing(monkeypatch):
    monkeypatch.delenv("RSLOGIC_S3_BUCKET_NAME", raising=False)
    monkeypatch.delenv("LOCKED_WAITING_BUCKET_NAME", raising=False)

    import config as cfg_module
    cfg = importlib.reload(cfg_module).load_config()

    assert cfg.s3.bucket_name == "drone-imagery-waiting"


def test_load_config_applies_bucket_prefix_defaults(monkeypatch):
    monkeypatch.setenv("RSLOGIC_S3_REGION", "eu-north-1")
    monkeypatch.setenv("RSLOGIC_S3_PROCESSED_BUCKET_NAME", "processed-custom")
    import config as cfg_module
    cfg = importlib.reload(cfg_module).load_config()

    assert cfg.s3.region == "eu-north-1"
    assert cfg.s3.processed_bucket_name == "processed-custom"
    assert cfg.s3.scratchpad_prefix == "scratchpad"
