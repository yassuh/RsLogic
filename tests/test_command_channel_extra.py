from __future__ import annotations

import json

from rslogic.jobs import command_channel as cc


def test_coerce_float_accepts_none_and_bad_inputs():
    assert cc._coerce_float("1.25") == 1.25
    assert cc._coerce_float(3) == 3.0
    assert cc._coerce_float("bad") is None


def test_processing_command_result_handles_invalid_types_and_coercion():
    result = cc.ProcessingCommandResult(
        command_id="cmd-1",
        command_type="rstool_sdk.command",
        status="progress",
        message="running",
        progress="42.5",
        data={"ok": True},
        error=None,
    )
    payload = result.to_payload()
    assert payload["progress"] == 42.5

    restored = cc.ProcessingCommandResult.parse(
        {
            "command_id": "cmd-1",
            "command_type": "rstool_sdk.command",
            "status": "ok",
            "progress": "12.5",
            "data": {"x": "y"},
            "error": "",
            "created_at": "2026-02-27T00:00:00",
        }
    )
    assert restored.error is None
    assert restored.progress == 12.5
    assert restored.created_at == "2026-02-27T00:00:00"


def test_redis_command_bus_push_sets_ttl_and_pop_parses_payload():
    class _FakeRedisClient:
        def __init__(self):
            self.push_args = []
            self.expire_args = []
            self.payload = None

        def lpush(self, queue_key, body):
            self.push_args.append((queue_key, body))

        def expire(self, key, seconds):
            self.expire_args.append((key, seconds))

        def brpop(self, queue_key, timeout=1):
            del timeout
            if self.payload is None:
                return None
            return queue_key, self.payload

        def close(self):
            return None

    class _FakeRedis:
        client = _FakeRedisClient()

        @staticmethod
        def from_url(*_args, **_kwargs):
            del _args
            del _kwargs
            return _FakeRedis.client

    import rslogic.jobs.command_channel as command_channel
    original = command_channel.Redis
    command_channel.Redis = _FakeRedis
    try:
        bus = command_channel.RedisCommandBus("redis://localhost:6379/0")
        bus.push("commands", {"type": "x", "payload": {"a": 1}}, expire_seconds=3)
        payload = {"type": "x", "payload": {"a": 1}}
        assert _FakeRedis.client.push_args[0][0] == "commands"
        pushed = json.loads(_FakeRedis.client.push_args[0][1].decode("utf-8"))
        assert pushed == payload
        assert _FakeRedis.client.expire_args == [("commands", 3)]
    finally:
        command_channel.Redis = original
