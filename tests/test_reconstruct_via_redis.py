from __future__ import annotations

import json
from types import SimpleNamespace
import importlib
import threading
import time

import pytest

from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
    ProcessingCommand,
    ProcessingCommandResult,
)

recon = importlib.import_module("scripts.reconstruct_via_redis")


def test_parse_set_filters_from_kv_and_json():
    args = recon._parse_args(
        [
            "--set-filter",
            "max_images=4",
            "--set-filter",
            "sdk_run_align=false",
            "--filter-json",
            "{\"drone_type\": \"mavic\"}",
        ]
    )

    parsed = recon._parse_set_filters(args.set_filter, args.filter_json)
    assert parsed["max_images"] == 4
    assert parsed["sdk_run_align"] is False
    assert parsed["drone_type"] == "mavic"


def test_to_payload_includes_processing_contract():
    cfg = recon.ProcessingJobConfig(
        redis_url="redis://localhost:9002/0",
        command_queue="rslogic:control:commands",
        reply_queue="",
        timeout=15,
        request_timeout=1000,
        wait_for_result=True,
        job_id="job-123",
        working_directory="/tmp/job",
        base_url="http://node:8000",
        client_id="client-id",
        app_token="app-token",
        auth_token="auth-token",
        stage_only=False,
        pull_s3_images=True,
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_staging_root="/tmp/stage",
        s3_max_files=10,
        s3_extensions=("jpg", "png"),
        image_keys=["k1", "k2"],
        extra_filters={"sdk_task_timeout_seconds": 2000},
    )

    payload = recon._to_payload(cfg)
    assert payload["job_id"] == "job-123"
    assert payload["working_directory"] == "/tmp/job"
    assert payload["base_url"] == "http://node:8000"
    assert payload["image_keys"] == ["k1", "k2"]
    assert payload["s3_bucket"] == "bucket"
    assert payload["s3_prefix"] == "prefix"
    assert payload["s3_staging_root"] == "/tmp/stage"
    assert payload["s3_max_files"] == 10
    assert payload["s3_extensions"] == ["jpg", "png"]
    assert payload["pull_s3_images"] is True
    assert payload["filters"]["pull_s3_images"] is True
    assert payload["filters"]["s3_bucket"] == "bucket"
    assert payload["filters"]["s3_region"] == "us-east-1"
    assert payload["filters"]["sdk_task_timeout_seconds"] == 2000


def test_send_processing_command_pushes_processing_job_without_wait(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.pushed = []

        def push(self, queue, payload, expire_seconds=None):
            self.pushed.append((queue, payload, expire_seconds))

        def close(self):
            self.closed = True

        def pop(self, queue, timeout_seconds):
            raise AssertionError("pop should not be called for no-wait mode")

    fake = FakeRedis()
    monkeypatch.setattr(recon, "RedisControlClient", lambda _url: fake)
    monkeypatch.setattr(recon.time, "sleep", lambda *_: None)

    cfg = recon.ProcessingJobConfig(
        redis_url="redis://localhost:9002/0",
        command_queue="rslogic:control:commands",
        reply_queue="",
        timeout=5,
        request_timeout=2,
        wait_for_result=False,
        job_id="job-123",
        working_directory="/tmp/job",
        base_url="http://node:8000",
        client_id="client-id",
        app_token="app-token",
        auth_token="auth-token",
        stage_only=False,
        pull_s3_images=True,
        s3_bucket="bucket",
        s3_prefix="prefix",
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_staging_root="",
        s3_max_files=10,
        s3_extensions=("jpg", "png"),
        image_keys=["k1"],
        extra_filters={},
    )

    exit_code = recon._send_processing_command(cfg)
    assert exit_code == 0
    assert len(fake.pushed) == 1
    queue, body, ttl = fake.pushed[0]
    assert queue == "rslogic:control:commands"
    assert ttl == 86400
    message = ProcessingCommand.parse(body)
    assert message.command_type == COMMAND_TYPE_PROCESSING_JOB
    assert message.payload["job_id"] == "job-123"
    assert message.reply_to is None


def test_wait_for_result_skips_unrelated_then_completes(monkeypatch):
    command_id = "command-1"
    messages = [
        {"command_id": "other", "command_type": COMMAND_TYPE_PROCESSING_JOB, "status": "ok", "data": {}},
        {
            "command_id": command_id,
            "command_type": COMMAND_TYPE_PROCESSING_JOB,
            "status": "accepted",
            "data": {"message": "started"},
        },
        {
            "command_id": command_id,
            "command_type": COMMAND_TYPE_PROCESSING_JOB,
            "status": "ok",
            "data": {"done": True},
        },
    ]

    class FakeClient:
        def __init__(self):
            self.calls = 0

        def pop(self, _queue, timeout_seconds):
            del timeout_seconds
            if self.calls >= len(messages):
                return None
            item = messages[self.calls]
            self.calls += 1
            return item

    client = FakeClient()
    result = recon._wait_for_result(
        redis_client=client,
        result_queue="reply",
        command_id=command_id,
        timeout_seconds=1,
        overall_deadline=recon.time.time() + 10,
        stop_event=threading.Event(),
    )
    assert result.command_id == command_id
    assert result.data == {"done": True}


def test_wait_for_result_times_out(monkeypatch):
    class IdleClient:
        def pop(self, _queue, timeout_seconds):
            del timeout_seconds
            return None

    clock = [0.0]

    def _fake_time() -> float:
        value = clock[0]
        clock[0] += 1.0
        return value

    monkeypatch.setattr(recon.time, "time", _fake_time)
    with pytest.raises(TimeoutError):
        recon._wait_for_result(
            redis_client=IdleClient(),
            result_queue="reply",
            command_id="missing",
            timeout_seconds=1,
            overall_deadline=5.0,
            stop_event=threading.Event(),
        )


def test_send_processing_command_waited_result_error(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.last_command_id = ""

        def push(self, queue, payload, expire_seconds=None):
            del queue
            del expire_seconds
            parsed = ProcessingCommand.parse(payload)
            self.last_command_id = parsed.command_id

        def pop(self, queue, timeout_seconds):
            del queue
            del timeout_seconds
            return {
                "command_id": self.last_command_id,
                "command_type": COMMAND_TYPE_PROCESSING_JOB,
                "status": "error",
                "message": "failed",
                "error": "boom",
            }

        def close(self):
            self.closed = True

    fake = FakeRedis()
    monkeypatch.setattr(recon, "RedisControlClient", lambda _url: fake)

    cfg = recon.ProcessingJobConfig(
        redis_url="redis://localhost:9002/0",
        command_queue="rslogic:control:commands",
        reply_queue="reply",
        timeout=1,
        request_timeout=2,
        wait_for_result=True,
        job_id="job-err",
        working_directory="/tmp/job",
        base_url="http://node:8000",
        client_id="client-id",
        app_token="app-token",
        auth_token="auth-token",
        stage_only=False,
        pull_s3_images=True,
        s3_bucket="bucket",
        s3_prefix="",
        s3_region="us-east-1",
        s3_endpoint_url="",
        s3_staging_root="",
        s3_max_files=0,
        s3_extensions=(),
        image_keys=[],
        extra_filters={},
    )
    exit_code = recon._send_processing_command(cfg)
    assert exit_code == 1


def test_main_builds_config_and_dispatches(monkeypatch):
    captured: dict[str, recon.ProcessingJobConfig] = {}

    def _fake_send(cfg: recon.ProcessingJobConfig) -> int:
        captured["cfg"] = cfg
        return 11

    monkeypatch.setattr(recon, "_send_processing_command", _fake_send)

    exit_code = recon.main(
        [
            "--redis-url",
            "redis://redis-host:9002/9",
            "--command-queue",
            "commands",
            "--job-id",
            "job-x",
            "--no-wait",
            "--set-filter",
            "sdk_run_align=false",
        ]
    )
    assert exit_code == 11
    cfg = captured["cfg"]
    assert cfg.redis_url == "redis://redis-host:9002/9"
    assert cfg.command_queue == "commands"
    assert cfg.job_id == "job-x"
    assert cfg.wait_for_result is False
    assert cfg.extra_filters["sdk_run_align"] is False
