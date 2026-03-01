from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import config as cfg_module
import rslogic.db.migrate as migrate


def _config():
    return cfg_module.AppConfig(
        app_name="x",
        default_group_name="g",
        log=cfg_module.LogConfig(level="INFO", format="%(message)s"),
        s3=cfg_module.S3Config(
            region="us-east-1",
            bucket_name="bucket",
            processed_bucket_name="processed",
            scratchpad_prefix="scratch",
            endpoint_url=None,
            multipart_part_size=1,
            multipart_concurrency=1,
            resume_uploads=True,
            manifest_dir=".",
        ),
        queue=cfg_module.QueueConfig(
            worker_count=1,
            poll_interval_seconds=1,
            backend="redis",
            redis_url="redis://localhost:6379/0",
            redis_queue_key="jobs",
            redis_block_timeout_seconds=1,
            start_local_workers=True,
        ),
        control=cfg_module.ControlConfig(
            command_queue_key="commands",
            result_queue_key="results",
            block_timeout_seconds=1,
            result_ttl_seconds=120,
            request_timeout_seconds=30,
        ),
        rstools=cfg_module.RsToolsConfig(
            executable_path=None,
            working_root=".",
            mode="stub",
            sdk_base_url=None,
            sdk_client_id=None,
            sdk_app_token=None,
            sdk_auth_token=None,
        ),
        label_db=cfg_module.LabelDbConfig(
            migration_root="migration",
            alembic_ini="alembic.ini",
            database_url="postgresql://user:pass@localhost/db",
        ),
        api=cfg_module.ApiConfig(base_url="http://127.0.0.1:8000"),
    )


def test_load_db_env_replaces_percent_signs(monkeypatch):
    cfg = _config()
    monkeypatch.setattr(migrate, "load_config", lambda: cfg)
    env = migrate._load_db_env()
    assert env["DATABASE_URL"] == "postgresql://user:pass@localhost/db"


def test_run_alembic_command_forwarding(monkeypatch):
    calls = {}

    class _Completed:
        returncode = 0

    def fake_run(cmd, cwd=None, env=None, check=None, text=None):
        calls["cmd"] = cmd
        calls["cwd"] = str(cwd)
        calls["env_sample"] = dict(env or {})
        calls["check"] = check
        calls["text"] = text
        return _Completed()

    monkeypatch.setattr(migrate, "subprocess", SimpleNamespace(run=fake_run))
    exit_code = migrate._run_alembic(root="/tmp/root", ini_file="/tmp/alembic.ini", args=["current"])
    assert exit_code == 0
    assert calls["cmd"][0] == "alembic"
    assert calls["cmd"][1] == "-c"
    assert calls["cmd"][2] == "/tmp/alembic.ini"


def test_main_reports_missing_root(tmp_path, monkeypatch):
    cfg = _config()
    cfg = replace(cfg, label_db=replace(cfg.label_db, migration_root=str(tmp_path / "missing")))
    monkeypatch.setattr(migrate, "load_config", lambda: cfg)
    try:
        migrate.main(["current"])
        raise AssertionError("Expected FileNotFoundError")
    except FileNotFoundError as exc:
        assert "Alembic root does not exist" in str(exc)
