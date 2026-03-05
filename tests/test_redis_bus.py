from __future__ import annotations

import json

import redis

from rslogic.common.redis_bus import RedisBus


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.lengths: dict[str, int] = {}

    def keys(self, pattern: str) -> list[str]:
        if pattern == "rslogic:clients:*:heartbeat":
            return sorted(key for key in self.values if key.startswith("rslogic:clients:") and key.endswith(":heartbeat"))
        return []

    def llen(self, key: str) -> int:
        return self.lengths[key]

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def delete(self, *keys: str) -> int:
        return len(keys)

    def lpush(self, key: str, value: str) -> None:
        self.values[key] = value

    def brpop(self, keys, timeout=0):
        return None

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value


def test_get_client_heartbeat_parses_json(monkeypatch) -> None:
    fake = _FakeRedis()
    fake.values["rslogic:clients:client-a:heartbeat"] = json.dumps({"status": "alive"})
    monkeypatch.setattr(redis, "from_url", lambda *args, **kwargs: fake)

    bus = RedisBus("redis://unused", "commands", "results")

    assert bus.get_client_heartbeat("client-a") == {"status": "alive"}


def test_command_queue_depth_reads_queue_length(monkeypatch) -> None:
    fake = _FakeRedis()
    fake.lengths["commands:client-a:jobs"] = 7
    monkeypatch.setattr(redis, "from_url", lambda *args, **kwargs: fake)

    bus = RedisBus("redis://unused", "commands", "results")

    assert bus.command_queue_depth("client-a") == 7
