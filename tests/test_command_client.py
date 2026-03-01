from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.request import Request

import config as cfg_module
from rslogic.client import command_client as cc


class _FakeResponse:
    def __init__(self, body: bytes, content_type: str = "application/json") -> None:
        self._body = body
        self.headers = {"Content-Type": content_type}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class _FakeURLOpener:
    def __init__(self):
        self.calls = []
        self.responses = []

    def add_json_response(self, payload, content_type="application/json") -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.responses.append(_FakeResponse(raw, content_type=content_type))

    def add_text_response(self, body: str, content_type: str = "text/plain") -> None:
        self.responses.append(_FakeResponse(body.encode("utf-8"), content_type=content_type))

    def opener(self, request: Request, timeout=None):
        self.calls.append(request)
        del timeout
        return self.responses.pop(0)


def test_api_executor_builds_expected_payload_paths():
    opener = _FakeURLOpener()
    opener.add_json_response({"status": "ok"})

    exc = cc.ApiCommandExecutor(base_url="http://example", timeout_seconds=2)
    original_urlopen = cc.urllib_request.urlopen
    try:
        cc.urllib_request.urlopen = opener.opener
        payload = exc.execute("health", {})
    finally:
        cc.urllib_request.urlopen = original_urlopen

    assert payload == {"status": "ok"}
    assert isinstance(opener.calls[0], Request)
    assert opener.calls[0].full_url == "http://example/health"
    assert opener.calls[0].method == "GET"


def test_api_executor_rejects_unsupported_command_type():
    exc = cc.ApiCommandExecutor(base_url="http://example", timeout_seconds=2)
    try:
        exc.execute("unknown", {})
    except ValueError as exc_info:
        assert "unsupported command type" in str(exc_info)
    else:
        raise AssertionError("Expected ValueError")


def test_api_executor_requires_required_field():
    exc = cc.ApiCommandExecutor(base_url="http://example", timeout_seconds=2)
    try:
        exc.execute("get_job", {})
    except ValueError as exc_info:
        assert "job_id is required" in str(exc_info)
    else:
        raise AssertionError("Expected ValueError")


def test_api_executor_http_error_is_wrapped():
    def _raise_error(*_args, **_kwargs):
        raise HTTPError(
            url="http://example/jobs",
            code=500,
            msg="server error",
            hdrs=None,
            fp=None,
        )

    exc = cc.ApiCommandExecutor(base_url="http://example", timeout_seconds=2)
    original_urlopen = cc.urllib_request.urlopen
    try:
        cc.urllib_request.urlopen = _raise_error
        try:
            exc.execute("create_job", {"k": "v"})
        except RuntimeError as exc_info:
            assert "API POST /jobs failed" in str(exc_info)
        else:
            raise AssertionError("Expected RuntimeError")
    finally:
        cc.urllib_request.urlopen = original_urlopen


class _FakeRedisBus:
    def __init__(self, payloads=None):
        self._queued = list(payloads or [])
        self.pushed = []
        self.closed = False

    def pop(self, queue_key: str, timeout_seconds: int):
        del queue_key
        del timeout_seconds
        if not self._queued:
            return None
        raw = self._queued.pop(0)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            return json.loads(raw)
        return raw

    def push(self, queue_key: str, payload, expire_seconds=None):
        del expire_seconds
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self.pushed.append((queue_key, encoded))

    def close(self) -> None:
        self.closed = True


def _minimal_config():
    return cfg_module.AppConfig(
        app_name="x",
        default_group_name="g",
        log=cfg_module.LogConfig(level="INFO", format="%(asctime)s"),
        s3=cfg_module.S3Config(
            region="us-east-1",
            bucket_name="bucket",
            processed_bucket_name="proc",
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
            block_timeout_seconds=2,
            result_ttl_seconds=3600,
            request_timeout_seconds=120,
        ),
        rstools=cfg_module.RsToolsConfig(
            executable_path=None,
            working_root="/tmp",
            mode="stub",
            sdk_base_url=None,
            sdk_client_id=None,
            sdk_app_token=None,
            sdk_auth_token=None,
        ),
        label_db=cfg_module.LabelDbConfig(migration_root=".", alembic_ini="alembic.ini", database_url="postgres://x"),
        api=cfg_module.ApiConfig(base_url="http://api"),
    )


class _FakeExecutor:
    def execute(self, command_type, payload):
        if command_type == "fail":
            raise RuntimeError("boom")
        return {"ok": True}

    def close(self) -> None:
        return None


def test_control_worker_processes_valid_command():
    bus = _FakeRedisBus([
        json.dumps(
            {
                "command_id": "cmd-1",
                "type": "health",
                "payload": {},
            }
        )
    ])
    worker = cc.ControlWorker(config=_minimal_config(), bus=bus, executor=_FakeExecutor())
    processed = worker.process_once(timeout_seconds=1)
    assert processed is True
    assert bus.pushed
    _, raw = bus.pushed[0]
    payload = json.loads(raw.decode("utf-8"))
    assert payload["status"] == "ok"
    assert payload["command_id"] == "cmd-1"


def test_control_worker_returns_error_for_invalid_payload():
    bus = _FakeRedisBus([
        {
            "command_id": "cmd-3",
            "payload": {},
        }
    ])
    worker = cc.ControlWorker(config=_minimal_config(), bus=bus, executor=_FakeExecutor())
    processed = worker.process_once(timeout_seconds=1)
    assert processed is True
    assert len(bus.pushed) >= 1
    _, raw = bus.pushed[0]
    payload = json.loads(raw.decode("utf-8"))
    assert payload["status"] == "error"
    assert payload["error"].startswith("invalid command payload")


def test_control_worker_marks_failures_in_result_payload():
    bus = _FakeRedisBus([
        json.dumps(
            {
                "command_id": "cmd-2",
                "type": "fail",
                "payload": {},
            }
        )
    ])
    worker = cc.ControlWorker(config=_minimal_config(), bus=bus, executor=_FakeExecutor())
    processed = worker.process_once(timeout_seconds=1)
    assert processed is True
    _, raw = bus.pushed[0]
    payload = json.loads(raw.decode("utf-8"))
    assert payload["status"] == "error"
    assert payload["error"] == "boom"


def test_command_sender_send_without_wait():
    bus = _FakeRedisBus([])
    cfg = _minimal_config()
    sender = cc.CommandSender(config=cfg, bus=bus)
    result = sender.send(
        command_type="ingest_waiting",
        payload={"bucket": "x"},
        command_id="abc",
        wait=False,
    )
    assert result["submitted"] is True
    assert result["command_id"] == "abc"
    assert result["type"] == "ingest_waiting"
    assert bus.pushed[0][0] == "commands"
    envelope = json.loads(bus.pushed[0][1].decode("utf-8"))
    assert envelope["command_id"] == "abc"


def test_command_sender_send_with_wait(monkeypatch):
    cfg = _minimal_config()
    response = {
        "command_id": "cmd-wait",
        "type": "rstool_sdk.command",
        "status": "ok",
        "command_type": "rstool_sdk.command",
        "created_at": "x",
    }
    bus = _FakeRedisBus([])

    def _pop(queue_key, timeout_seconds):
        del queue_key, timeout_seconds
        return response

    monkeypatch.setattr(bus, "pop", _pop)
    sender = cc.CommandSender(config=cfg, bus=bus)
    result = sender.send(
        command_type="health",
        payload={},
        command_id="cmd-wait",
        wait=True,
        timeout_seconds=1,
    )
    assert result["status"] == "ok"
    assert result["command_id"] == "cmd-wait"


def test_parse_payload_handles_json_and_file(monkeypatch, tmp_path):
    payload = cc._parse_payload(payload_json='{"a":1}', payload_file=None)
    assert payload == {"a": 1}

    json_file = tmp_path / "payload.json"
    json_file.write_text('{"k":2}', encoding="utf-8")
    payload = cc._parse_payload(payload_json=None, payload_file=str(json_file))
    assert payload == {"k": 2}

    try:
        cc._parse_payload(payload_json='{"a":1}', payload_file=str(json_file))
    except ValueError as exc:
        assert "either" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_parse_payload_rejects_non_object():
    try:
        cc._parse_payload(payload_json='[1]', payload_file=None)
    except ValueError as exc:
        assert "payload must be a JSON object" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
