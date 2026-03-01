from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import importlib
import threading
import types
import sys

from rslogic.jobs.command_channel import ProcessingCommandResult

recon = importlib.import_module("scripts.reconstruct_via_redis")
import pytest


class _FakeRedisControl:
    def __init__(self, _url: str) -> None:
        self.url = _url
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_safe_exts_and_defaults():
    args = recon._parse_args([])

    assert args.imagery_subdir == "Imagery"
    assert args.timeout == 15
    assert args.request_timeout == 7200
    assert args.base_url == "http://192.168.193.59:8000"
    assert recon._safe_exts("jpg, png,,tif") == ["jpg", "png", "tif"]


def test_pick_session_and_expect_ok():
    assert recon._pick_session({"result": "  abc123  "}) == "abc123"
    assert recon._pick_session({"data": {"session": "s1"}}) == "s1"
    assert recon._pick_session({"data": {"result": "s2"}}) == "s2"
    assert recon._pick_session({}) == ""

    ok = ProcessingCommandResult(
        command_id="a",
        command_type="t",
        status="ok",
        data={"x": 1},
    )
    assert recon._expect_ok(ok) == {"x": 1}

    progress = ProcessingCommandResult(
        command_id="a",
        command_type="t",
        status="progress",
        data={"y": 2},
    )
    assert recon._expect_ok(progress) == {"y": 2}

    err = ProcessingCommandResult(
        command_id="a",
        command_type="t",
        status="error",
        error="bad",
        message="boom",
    )
    try:
        recon._expect_ok(err)
    except RuntimeError as exc:
        assert "boom: bad" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_emit_reconstruction_without_discovery_sends_command_sequence(monkeypatch, tmp_path):
    commands = []

    def _fake_send_command(
        redis_client,
        command_queue,
        reply_queue,
        command_type,
        payload,
        timeout_seconds,
        request_timeout,
        *,
        stop_event,
    ):
        del redis_client, command_queue, reply_queue, timeout_seconds, request_timeout, stop_event
        commands.append((command_type, payload))

        method = payload.get("method")
        data: dict[str, object] = {}
        if command_type == recon.COMMAND_TYPE_RSTOOL_COMMAND and method == "create":
            data = {"result": "session-id-1"}

        return ProcessingCommandResult(
            command_id="x",
            command_type=command_type,
            status="ok",
            data=data,
        )

    def _fake_download_images_from_s3(
        bucket,
        prefix,
        download_dir,
        max_files,
        allowed_extensions,
        **_kwargs,
    ):
        del bucket
        del prefix
        del max_files
        del allowed_extensions
        download_dir.mkdir(parents=True, exist_ok=True)
        target = download_dir / "img.jpg"
        target.write_text("dummy", encoding="utf-8")
        return [target]

    monkeypatch.setattr(recon, "RedisControlClient", _FakeRedisControl)
    monkeypatch.setattr(recon, "_send_command", _fake_send_command)
    monkeypatch.setattr(recon, "_download_images_from_s3", _fake_download_images_from_s3)

    cfg = recon.ReconConfig(
        redis_url="redis://localhost:6379/0",
        command_queue="rslogic:control:commands",
        timeout=1,
        request_timeout=2,
        base_url="http://node-host:8000",
        client_id="cid",
        app_token="app",
        auth_token="auth",
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_download_dir=str(tmp_path / "download"),
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_max_files=1,
        s3_extensions=("jpg", "png"),
        pull_s3=True,
        imagery_dir="",
        imagery_subdir="Imagery",
        save_path="test_auto.rspj",
        skip_discover=True,
        stage_only=False,
    )

    result = recon._emit_reconstruction(cfg)

    assert result == 0
    assert commands[0][0] == recon.COMMAND_TYPE_RSTOOL_COMMAND
    assert commands[0][1]["method"] == "connection"
    methods = [payload["method"] for _, payload in commands]
    assert "connect_user" in methods
    assert "create" in methods
    assert "new_scene" in methods
    assert any(payload["method"] == "add_folder" for _, payload in commands)
    assert any(payload["method"] == "save" for _, payload in commands)


def test_emit_reconstruction_stage_only_downloads_and_exits(monkeypatch, tmp_path):
    calls = {"download": 0, "command": 0}

    def _fake_download_images(
        bucket,
        prefix,
        download_dir,
        max_files,
        allowed_extensions,
        **_kwargs,
    ):
        del bucket, prefix, max_files, allowed_extensions
        calls["download"] += 1
        imagery = download_dir / "image.jpg"
        download_dir.mkdir(parents=True, exist_ok=True)
        imagery.write_text("fake-bytes", encoding="utf-8")
        return [imagery]

    def _fake_send_command(*_args, **_kwargs):
        calls["command"] += 1
        return ProcessingCommandResult(
            command_id="x",
            command_type=recon.COMMAND_TYPE_RSTOOL_COMMAND,
            status="ok",
            data={"result": "session-id"},
        )

    monkeypatch.setattr(recon, "_download_images_from_s3", _fake_download_images)
    monkeypatch.setattr(recon, "_send_command", _fake_send_command)
    monkeypatch.setattr(recon, "RedisControlClient", _FakeRedisControl)

    cfg = recon.ReconConfig(
        redis_url="redis://localhost:6379/0",
        command_queue="rslogic:control:commands",
        timeout=1,
        request_timeout=2,
        base_url="http://node-host:8000",
        client_id="cid",
        app_token="app",
        auth_token="auth",
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_download_dir=str(tmp_path / "download"),
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_max_files=1,
        s3_extensions=("jpg",),
        pull_s3=True,
        imagery_dir="",
        imagery_subdir="Imagery",
        save_path="test_auto.rspj",
        skip_discover=False,
        stage_only=True,
    )

    result = recon._emit_reconstruction(cfg)

    assert result == 0
    assert calls["download"] == 1
    assert calls["command"] == 0
    assert (tmp_path / "download" / "Imagery" / "image.jpg").exists()


def test_call_discover_forwards_target(monkeypatch):
    seen: dict[str, object] = {}

    def _fake_send_command(
        redis_client,
        command_queue,
        reply_queue,
        command_type,
        payload,
        timeout_seconds,
        request_timeout,
        *,
        stop_event,
    ):
        del redis_client, command_queue, reply_queue, timeout_seconds, request_timeout, stop_event
        seen.update(
            {
                "command_type": command_type,
                "payload": payload,
            }
        )
        return ProcessingCommandResult(
            command_id="id",
            command_type=command_type,
            status="ok",
            data={"available": {"node": {"connect_user": {}}, "project": {"create": {}}}},
        )

    redis = SimpleNamespace(close=lambda: None)
    cfg = recon.ReconConfig(
        redis_url="redis://localhost:6379/0",
        command_queue="rslogic:control:commands",
        timeout=1,
        request_timeout=2,
        base_url="",
        client_id="",
        app_token="",
        auth_token="",
        s3_bucket="",
        s3_prefix="",
        s3_download_dir="",
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_max_files=0,
        s3_extensions=(),
        pull_s3=False,
        imagery_dir="",
        imagery_subdir="Imagery",
        save_path="test_auto.rspj",
        skip_discover=False,
        stage_only=False,
    )

    monkeypatch.setattr(recon, "RedisControlClient", lambda _: redis)
    monkeypatch.setattr(recon, "_send_command", _fake_send_command)

    payload = recon._call_discover(
        redis_client=redis,
        cfg=cfg,
        reply_queue="reply",
        target="node",
        stop_event=threading.Event(),
    )

    assert payload["available"]["node"]["connect_user"] == {}
    assert seen["command_type"] == recon.COMMAND_TYPE_RSTOOL_DISCOVER
    assert seen["payload"]["target"] == "node"


def test_build_control_payload_is_roundtrip():
    payload = recon._build_control_payload(
        command_type=recon.COMMAND_TYPE_RSTOOL_COMMAND,
        payload={"target": "node", "method": "status"},
        reply_to="reply-queue",
    ).to_payload()

    parsed = recon.ProcessingCommand.parse(payload)
    assert parsed.command_type == recon.COMMAND_TYPE_RSTOOL_COMMAND
    assert parsed.payload["target"] == "node"
    assert parsed.reply_to == "reply-queue"


def test_wait_for_result_skips_unrelated_then_accepts_final():
    command_id = "cmd-1"
    queue = [
        {"command_id": "other", "command_type": "x", "status": "ok", "data": {}},
        {"command_id": command_id, "command_type": "x", "status": "accepted", "data": {"p": 1}},
        {"command_id": command_id, "command_type": "x", "status": "ok", "data": {"p": 2}},
    ]

    class _FakeClient:
        def __init__(self):
            self.calls = 0

        def pop(self, _queue: str, timeout_seconds: int):
            del timeout_seconds
            if self.calls < len(queue):
                payload = queue[self.calls]
                self.calls += 1
                return payload
            self.calls += 1
            return None

    client = _FakeClient()
    result = recon._wait_for_result(
        redis_client=client,
        reply_queue="reply",
        command_id=command_id,
        timeout_seconds=1,
        overall_deadline=recon.time.time() + 5.0,
        stop_event=threading.Event(),
    )

    assert result.command_id == command_id
    assert result.data == {"p": 2}


def test_wait_for_result_times_out(monkeypatch):
    class _IdleClient:
        def pop(self, _queue: str, timeout_seconds: int):
            del timeout_seconds
            return None

    clock = [0.0]

    def _fake_time() -> float:
        value = clock[0]
        clock[0] += 1.0
        return value

    monkeypatch.setattr(recon.time, "time", _fake_time)
    with pytest.raises(TimeoutError) as timeout_exc:
        recon._wait_for_result(
            redis_client=_IdleClient(),
            reply_queue="reply",
            command_id="missing",
            timeout_seconds=1,
            overall_deadline=5.0,
            stop_event=threading.Event(),
        )
    assert "Timed out waiting for reply to command_id=missing" in str(timeout_exc.value)


def test_send_command_pushes_and_waits_for_reply(monkeypatch):
    seen: dict[str, object] = {}

    class _FakeClient:
        def __init__(self) -> None:
            self.payload = None

        def push(self, queue_key: str, payload: dict, expire_seconds: int | None = None) -> None:
            seen["queue_key"] = queue_key
            seen["expire_seconds"] = expire_seconds
            seen["payload"] = payload

    fake_client = _FakeClient()

    def _fake_wait_for_result(
        redis_client,
        reply_queue,
        command_id,
        timeout_seconds,
        overall_deadline,
        *,
        stop_event,
    ):
        del redis_client, reply_queue, command_id, timeout_seconds, overall_deadline, stop_event
        parsed = recon.ProcessingCommand.parse(seen["payload"])
        return ProcessingCommandResult(
            command_id=parsed.command_id,
            command_type=parsed.command_type,
            status="ok",
            data={"ok": True},
        )

    monkeypatch.setattr(recon, "_wait_for_result", _fake_wait_for_result)

    result = recon._send_command(
        redis_client=fake_client,
        command_queue="cmd-queue",
        reply_queue="reply",
        command_type=recon.COMMAND_TYPE_RSTOOL_COMMAND,
        payload={"target": "node", "method": "status"},
        timeout_seconds=1,
        request_timeout=2,
        stop_event=threading.Event(),
    )

    assert result.status == "ok"
    assert result.data == {"ok": True}
    assert seen["queue_key"] == "cmd-queue"
    parsed = recon.ProcessingCommand.parse(seen["payload"])
    assert parsed.command_type == recon.COMMAND_TYPE_RSTOOL_COMMAND
    assert parsed.payload["target"] == "node"


def test_emit_reconstruction_requires_session(monkeypatch, tmp_path):
    call_count = {"commands": 0}

    def _fake_send_command(
        redis_client,
        command_queue,
        reply_queue,
        command_type,
        payload,
        timeout_seconds,
        request_timeout,
        *,
        stop_event,
    ):
        del redis_client, command_queue, reply_queue, timeout_seconds, request_timeout, stop_event
        call_count["commands"] += 1
        method = payload.get("method")
        if command_type == recon.COMMAND_TYPE_RSTOOL_COMMAND and method == "create":
            return ProcessingCommandResult(command_id="id", command_type=command_type, status="ok", data={})
        return ProcessingCommandResult(command_id="id", command_type=command_type, status="ok", data={})

    def _noop_download_images(*_args, **_kwargs):
        return []

    monkeypatch.setattr(recon, "RedisControlClient", _FakeRedisControl)
    monkeypatch.setattr(recon, "_send_command", _fake_send_command)
    monkeypatch.setattr(recon, "_download_images_from_s3", _noop_download_images)

    cfg = recon.ReconConfig(
        redis_url="redis://localhost:6379/0",
        command_queue="rslogic:control:commands",
        timeout=1,
        request_timeout=2,
        base_url="http://node-host:8000",
        client_id="cid",
        app_token="app",
        auth_token="auth",
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_download_dir=str(tmp_path / "download"),
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_max_files=0,
        s3_extensions=("jpg",),
        pull_s3=False,
        imagery_dir=str(tmp_path / "images"),
        imagery_subdir="Imagery",
        save_path="test_auto.rspj",
        skip_discover=True,
        stage_only=False,
    )

    result = recon._emit_reconstruction(cfg)
    assert result == 1
    assert call_count["commands"] >= 3


def test_emit_reconstruction_skips_discovery(monkeypatch, tmp_path):
    def _boom(*_args, **_kwargs):
        raise AssertionError("discover should not run with --skip-discover")

    def _fake_send_command(
        redis_client,
        command_queue,
        reply_queue,
        command_type,
        payload,
        timeout_seconds,
        request_timeout,
        *,
        stop_event,
    ):
        del redis_client, command_queue, reply_queue, timeout_seconds, request_timeout, stop_event
        method = payload.get("method")
        if payload.get("method") == "create":
            data = {"result": "session-id-1"}
        else:
            data = {}
        return ProcessingCommandResult(command_id="id", command_type=command_type, status="ok", data=data)

    def _fake_download(*_args, **_kwargs):
        return []

    cfg = recon.ReconConfig(
        redis_url="redis://localhost:6379/0",
        command_queue="rslogic:control:commands",
        timeout=1,
        request_timeout=2,
        base_url="http://node-host:8000",
        client_id="cid",
        app_token="app",
        auth_token="auth",
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_download_dir=str(tmp_path / "download"),
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_max_files=0,
        s3_extensions=("jpg",),
        pull_s3=False,
        imagery_dir=str(tmp_path / "images"),
        imagery_subdir="Imagery",
        save_path="test_auto.rspj",
        skip_discover=True,
        stage_only=False,
    )
    monkeypatch.setattr(recon, "RedisControlClient", _FakeRedisControl)
    monkeypatch.setattr(recon, "_send_command", _fake_send_command)
    monkeypatch.setattr(recon, "_download_images_from_s3", _fake_download)
    monkeypatch.setattr(recon, "_call_discover", _boom)

    assert recon._emit_reconstruction(cfg) == 0


def test_emit_reconstruction_downloads_images_with_stale_cleanup_and_removes_old_files(
    monkeypatch, tmp_path
):
    stale = tmp_path / "stage" / "Imagery"
    stale.mkdir(parents=True)
    stale_file = stale / "old.png"
    stale_file.write_text("stale", encoding="utf-8")
    called = {"download": 0}

    def _fake_send_command(
        redis_client,
        command_queue,
        reply_queue,
        command_type,
        payload,
        timeout_seconds,
        request_timeout,
        *,
        stop_event,
    ):
        del redis_client, command_queue, reply_queue, timeout_seconds, request_timeout, stop_event
        method = payload.get("method")
        if method == "create":
            data = {"result": "session"}
        else:
            data = {}
        return ProcessingCommandResult(
            command_id="id",
            command_type=command_type,
            status="ok",
            data=data,
        )

    def _fake_download_images(
        bucket,
        prefix,
        download_dir,
        max_files,
        allowed_extensions,
        **_kwargs,
    ):
        del bucket
        del prefix
        del max_files
        del allowed_extensions
        called["download"] += 1
        assert not (download_dir / "old.png").exists()
        return [download_dir / "new.png"]

    cfg = recon.ReconConfig(
        redis_url="redis://localhost:6379/0",
        command_queue="rslogic:control:commands",
        timeout=1,
        request_timeout=2,
        base_url="http://node-host:8000",
        client_id="cid",
        app_token="app",
        auth_token="auth",
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_download_dir=str(tmp_path / "stage"),
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_max_files=0,
        s3_extensions=("jpg",),
        pull_s3=True,
        imagery_dir="",
        imagery_subdir="Imagery",
        save_path="test_auto.rspj",
        skip_discover=True,
        stage_only=False,
    )
    monkeypatch.setattr(recon, "RedisControlClient", _FakeRedisControl)
    monkeypatch.setattr(recon, "_send_command", _fake_send_command)
    monkeypatch.setattr(recon, "_download_images_from_s3", _fake_download_images)

    result = recon._emit_reconstruction(cfg)
    assert result == 0
    assert called["download"] == 1


def test_download_images_from_s3_respects_extensions_and_limit(monkeypatch, tmp_path):
    downloaded: list[str] = []

    class _FakePaginator:
        def paginate(self, Bucket, Prefix):
            del Bucket
            del Prefix
            return [
                {"Contents": [{"Key": "one/JPG"}, {"Key": "two/photo.txt"}, {"Key": "three/IMG.tif"}]},
            ]

    class _FakeS3:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return _FakePaginator()

        def download_file(self, _bucket, key, filename):
            downloaded.append(key)
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            Path(filename).write_text("ok", encoding="utf-8")

    fake_boto3 = types.ModuleType("boto3")

    def _client(service, config=None, **_kwargs):
        del service
        del config
        return _FakeS3()

    fake_boto3.client = _client

    fake_botocore = types.ModuleType("botocore")
    fake_config = types.ModuleType("botocore.config")

    def _Config(*_args, **_kwargs):
        del _args
        del _kwargs
        return object()

    fake_config.Config = _Config
    fake_botocore.config = fake_config

    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setitem(sys.modules, "botocore", fake_botocore)
    monkeypatch.setitem(sys.modules, "botocore.config", fake_config)

    result = recon._download_images_from_s3(
        bucket="bucket",
        prefix="pref",
        download_dir=tmp_path,
        max_files=1,
        allowed_extensions=("jpg", "tif", "png"),
        region="us-east-1",
        endpoint_url="",
    )

    assert len(result) == 1
    assert downloaded == ["one/JPG"]
    assert result[0].exists()


def test_main_builds_config_and_invokes_runner(monkeypatch):
    called: dict[str, object] = {}

    def _fake_emit(cfg: recon.ReconConfig) -> int:
        called["cfg"] = cfg
        return 7

    monkeypatch.setattr(recon, "_emit_reconstruction", _fake_emit)

    exit_code = recon.main(
        [
            "--redis-url",
            "redis://redis-host:9002/9",
            "--command-queue",
            "commands",
            "--base-url",
            "http://node:8000",
            "--s3-download-dir",
            "/tmp/staging",
            "--s3-extensions",
            "jpeg,png",
            "--skip-discover",
        ]
    )
    assert exit_code == 7
    cfg = called["cfg"]
    assert isinstance(cfg, recon.ReconConfig)
    assert cfg.redis_url == "redis://redis-host:9002/9"
    assert cfg.command_queue == "commands"
    assert cfg.base_url == "http://node:8000"
    assert cfg.timeout >= 1
    assert cfg.request_timeout >= 1
    assert cfg.skip_discover is True
