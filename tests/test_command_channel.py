from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from rslogic.jobs import command_channel as cc


class _FakeRedisClient:
    def __init__(self) -> None:
        self.queues: Dict[str, List[bytes]] = {}
        self.values: Dict[str, Any] = {}
        self.closed = False
        self.expirations: Dict[str, int | None] = {}

    def lpush(self, queue_key: str, value: bytes) -> None:
        self.queues.setdefault(queue_key, []).append(value)

    def brpop(self, queue_key: str, timeout: int):
        del timeout
        values = self.queues.get(queue_key)
        if not values:
            return None
        value = values.pop(0)
        return queue_key, value

    def set(self, name: str, value: str, ex: int | None = None) -> None:
        self.values[name] = value
        self.expirations[name] = ex

    def delete(self, name: str) -> None:
        self.values.pop(name, None)
        self.expirations.pop(name, None)

    def expire(self, key: str, seconds: int) -> None:
        self.expirations[key] = seconds

    def close(self) -> None:
        self.closed = True

    def ping(self) -> None:
        return None


class _FakeRedisModule:
    def __init__(self) -> None:
        self.client = _FakeRedisClient()
        self.__class__._singleton = self.client

    _singleton: _FakeRedisClient | None = None

    @classmethod
    def from_url(cls, *_args, **_kwargs):
        del _args
        del _kwargs
        if cls._singleton is None:
            cls._singleton = _FakeRedisClient()
        return cls._singleton


def test_processing_command_build_parse_roundtrip():
    command = cc.ProcessingCommand.build(
        command_type="rstool_sdk.command",
        payload={"target": "project"},
        reply_to="reply-queue",
    )
    payload = command.to_payload()
    restored = cc.ProcessingCommand.parse(payload)

    assert restored.command_id == command.command_id
    assert restored.command_type == "rstool_sdk.command"
    assert restored.payload == {"target": "project"}
    assert restored.reply_to == "reply-queue"


def test_processing_command_parse_uses_alias_type_key():
    parsed = cc.ProcessingCommand.parse({"type": "rstool_sdk.command", "id": "abc", "payload": {"a": 1}})
    assert parsed.command_id == "abc"
    assert parsed.command_type == "rstool_sdk.command"
    assert parsed.payload == {"a": 1}


def test_processing_command_parse_requires_payload_dict():
    try:
        cc.ProcessingCommand.parse({"command_type": "x", "payload": []})
    except ValueError as exc:
        assert "payload must be a JSON object" in str(exc)
    else:
        raise AssertionError("Expected ValueError for non-dict payload")


def test_processing_command_result_roundtrip_and_progress_coercion():
    result = cc.ProcessingCommandResult(
        command_id="cmd-1",
        command_type="rstool_sdk.command",
        status=cc.RESULT_STATUS_PROGRESS,
        message="in progress",
        progress=42.3,
        data={"phase": "run"},
        started_at="a",
        finished_at="b",
    )
    payload = result.to_payload()
    restored = cc.ProcessingCommandResult.parse(payload)
    assert restored.command_id == "cmd-1"
    assert restored.status == cc.RESULT_STATUS_PROGRESS
    assert restored.progress == 42.3
    assert restored.data == {"phase": "run"}


def test_processing_command_result_parse_requires_command_id():
    try:
        cc.ProcessingCommandResult.parse({"command_type": "x", "status": "ok"})
    except ValueError as exc:
        assert "command_id is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing command_id")


def test_redis_command_bus_push_pop_roundtrip(monkeypatch):
    fake = _FakeRedisModule()
    monkeypatch.setattr(cc, "Redis", fake)
    bus = cc.RedisCommandBus("redis://localhost:6379/0")

    bus.push("commands", {"command_type": "x", "payload": {"a": 1}})
    popped = bus.pop("commands", timeout_seconds=1)
    assert popped["command_type"] == "x"
    assert popped["payload"] == {"a": 1}


def test_redis_command_bus_pop_invalid_payload_returns_value_error(monkeypatch):
    fake = _FakeRedisModule()
    monkeypatch.setattr(cc, "Redis", fake)
    bus = cc.RedisCommandBus("redis://localhost:6379/0")
    raw_client = fake.client
    raw_client.lpush("commands", b"not-json")

    try:
        bus.pop("commands", timeout_seconds=1)
    except ValueError as exc:
        assert "Invalid JSON payload" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid payload payload")


def test_redis_command_bus_set_presence_and_ping(monkeypatch):
    fake = _FakeRedisModule()
    monkeypatch.setattr(cc, "Redis", fake)
    bus = cc.RedisCommandBus("redis://localhost:6379/0")
    bus.set_presence("presence:key", {"status": "online"}, ttl_seconds=5)
    assert fake.client.values["presence:key"] == json.dumps(
        {"status": "online"}, separators=(",", ":"), ensure_ascii=True
    )
    assert fake.client.expirations["presence:key"] == 5
    bus.delete("presence:key")
    assert "presence:key" not in fake.client.values
