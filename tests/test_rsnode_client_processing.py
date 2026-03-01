from __future__ import annotations
from dataclasses import replace
from pathlib import Path
from typing import Any

from config import AppConfig
from rslogic.client import rsnode_client
from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
    ProcessingCommand,
)
from tests.conftest import build_test_app_config


class _FakeRedisCommandBus:
    def __init__(self, *_, **__) -> None:
        pass

    def set_presence(self, *_, **__) -> None:
        pass

    def delete(self, *_, **__) -> None:
        pass

    def push(self, *_, **__) -> None:
        pass

    def pop(self, *_, **__) -> None:
        return None

    def ping(self) -> None:
        return None

    def close(self) -> None:
        pass


def _mk_client(monkeypatch, config: AppConfig, queue_prefix: str = "rslogic:client:test") -> rsnode_client.RsNodeClient:
    monkeypatch.setattr(rsnode_client, "RedisCommandBus", _FakeRedisCommandBus)
    monkeypatch.setattr(rsnode_client, "load_config", lambda: config)
    return rsnode_client.RsNodeClient(
        command_queue_key=f"{queue_prefix}:commands",
        result_queue_key=f"{queue_prefix}:results",
        redis_url=config.queue.redis_url,
        rs_base_url=config.rstools.sdk_base_url,
        rs_client_id="client-id",
        rs_app_token="app-token",
        rs_auth_token="auth-token",
        worker_count=1,
    )


def test_prepare_filters_default_pull_is_true_and_uses_staging_root(monkeypatch, tmp_path):
    cfg = build_test_app_config()
    cfg = replace(cfg, s3=replace(cfg.s3, bucket_name="cfg-bucket"))
    client = _mk_client(monkeypatch, cfg)

    staging_root = tmp_path / "staging"
    old_image = staging_root / "Imagery" / "old.txt"
    old_image.parent.mkdir(parents=True, exist_ok=True)
    old_image.write_text("old", encoding="utf-8")
    assert old_image.exists()

    payload = {
        "job_id": "job-1",
        "working_directory": str(tmp_path / "work"),
        "image_keys": ["prefix/img1.jpg"],
        "filters": {
            "s3_staging_root": str(staging_root),
            "sdk_imagery_folder": "Imagery",
            "pull_s3_images": True,
        },
        "s3_bucket": "payload-bucket",
    }

    class _FakeS3Client:
        def __init__(self):
            self.calls: list[tuple[str, str, str]] = []

        def download_file(self, bucket: str, key: str, path: str) -> None:
            self.calls.append((bucket, key, path))
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(f"{bucket}:{key}", encoding="utf-8")

    fake_s3 = _FakeS3Client()
    client._get_s3_client = lambda region=None, endpoint_url=None: fake_s3

    filters = client._prepare_processing_filters(
        working_directory=Path(payload["working_directory"]),
        job_id="job-1",
        payload=payload,
        image_keys=["prefix/img1.jpg"],
    )

    assert filters["sdk_imagery_folder"] == str(staging_root / "Imagery")
    assert filters["_rslogic_staging"]["s3_pull_enabled"] is True
    assert filters["_rslogic_staging"]["downloaded"] == 1
    assert fake_s3.calls == [("payload-bucket", "prefix/img1.jpg", str(staging_root / "Imagery" / "prefix" / "img1.jpg"))]
    assert not old_image.exists()


def test_prepare_filters_can_skip_s3_pull_with_filters_payload_false(monkeypatch, tmp_path):
    cfg = build_test_app_config()
    cfg = replace(cfg, s3=replace(cfg.s3, bucket_name="cfg-bucket"))
    client = _mk_client(monkeypatch, cfg)

    staging_root = tmp_path / "staging"
    existing_file = staging_root / "Imagery" / "old.txt"
    existing_file.parent.mkdir(parents=True, exist_ok=True)
    existing_file.write_text("old", encoding="utf-8")

    payload = {
        "job_id": "job-2",
        "working_directory": str(tmp_path / "work"),
        "image_keys": [],
        "filters": {
            "s3_staging_root": str(staging_root),
            "sdk_imagery_folder": "Imagery",
            "pull_s3_images": "false",
        },
    }

    filters = client._prepare_processing_filters(
        working_directory=Path(payload["working_directory"]),
        job_id="job-2",
        payload=payload,
        image_keys=[],
    )

    assert filters["_rslogic_staging"]["s3_pull_enabled"] is False
    assert filters["_rslogic_staging"]["imagery_folder"] == str(staging_root / "Imagery")
    assert not existing_file.exists()


def test_prepare_filters_pulls_selected_s3_images_with_prefix_and_extension(monkeypatch, tmp_path):
    cfg = build_test_app_config()
    cfg = replace(cfg, s3=replace(cfg.s3, bucket_name="cfg-bucket"))
    client = _mk_client(monkeypatch, cfg)
    working_directory = tmp_path / "work"

    class _FakeS3Client:
        def __init__(self):
            self.calls: list[tuple[str, str, str]] = []

        def download_file(self, bucket: str, key: str, target_path: str) -> None:
            self.calls.append((bucket, key, target_path))
            path = Path(target_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"payload:{bucket}:{key}", encoding="utf-8")

    fake_s3 = _FakeS3Client()
    client._get_s3_client = lambda region=None, endpoint_url=None: fake_s3

    filters = client._prepare_processing_filters(
        working_directory=working_directory,
        job_id="job-3",
        payload={
            "job_id": "job-3",
            "filters": {
                "s3_staging_root": str(tmp_path / "staging"),
                "s3_prefix": "prefix/",
                "s3_extensions": [".jpg", ".png"],
                "s3_max_files": 1,
            },
            "s3_bucket": "payload-bucket",
        },
        image_keys=[
            "prefix/A.jpg",
            "prefix/B.png",
            "prefix/C.txt",
            "ignore.txt",
        ],
    )

    staging_summary = filters["_rslogic_staging"]
    assert filters["_rslogic_staging"]["s3_pull_enabled"] is True
    assert staging_summary["s3_bucket"] == "payload-bucket"
    assert staging_summary["requested"] == 4
    assert staging_summary["downloaded"] == 1
    assert staging_summary["filtered"] == 3
    assert fake_s3.calls and fake_s3.calls[0][0] == "payload-bucket"
    assert fake_s3.calls[0][1] == "prefix/A.jpg"
    assert len(staging_summary["files"]) == 1


def test_handle_processing_command_uses_prepared_filters(monkeypatch, tmp_path):
    cfg = build_test_app_config()
    client = _mk_client(monkeypatch, cfg)

    prepared_filters = {
        "sdk_imagery_folder": str(tmp_path / "job"),
        "_rslogic_staging": {
            "s3_pull_enabled": True,
            "imagery_folder": str(tmp_path / "job"),
            "job_id": "job-4",
        },
    }

    class _FakeRunner:
        def __init__(self) -> None:
            self.captured: dict[str, Any] | None = None

        def run(self, working_directory, image_keys, filters, *, job_id=None, progress_callback=None):
            self.captured = {
                "working_directory": str(working_directory),
                "image_keys": list(image_keys),
                "filters": filters,
                "job_id": job_id,
            }
            return {"status": "ok", "job_id": job_id}

    runner = _FakeRunner()

    def _publish_capture(**kwargs: Any) -> None:
        events.append(kwargs)

    events: list[dict[str, Any]] = []
    monkeypatch.setattr(client, "_publish", _publish_capture)
    monkeypatch.setattr(client, "_refresh_node_connection", lambda payload=None: (True, "ok"))
    monkeypatch.setattr(client, "_build_sdk_runner", lambda payload=None: runner)
    monkeypatch.setattr(
        client,
        "_prepare_processing_filters",
        lambda **_: prepared_filters,
    )

    command = ProcessingCommand(
        command_id="cmd-1",
        command_type=COMMAND_TYPE_PROCESSING_JOB,
        payload={
            "job_id": "job-4",
            "working_directory": str(tmp_path / "client-work"),
            "image_keys": ["a.jpg"],
            "filters": {},
        },
    )

    client._handle_processing_command(command)

    assert runner.captured is not None
    assert runner.captured["filters"] is prepared_filters
    assert any(event["status"] == "ok" for event in events)
    assert any(event["status"] == "accepted" for event in events)
