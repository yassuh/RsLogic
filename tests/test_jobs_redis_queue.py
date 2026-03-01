from __future__ import annotations


from rslogic.jobs import redis_queue as rq


class _FakeRedisClient:
    def __init__(self):
        self.pushes: list[tuple[tuple[str, object], dict]] = []
        self.values: list[tuple[str, bytes]] = []
        self.closed = False
        self.ping_called = False

    def lpush(self, queue_key: str, body: bytes) -> None:
        self.values.append((queue_key, body))

    def brpop(self, queue_key: str, timeout: int):
        del timeout
        if self.values:
            return self.values.pop(0)
        return None

    def ping(self) -> None:
        self.ping_called = True

    def close(self) -> None:
        self.closed = True


class _FakeRedisFactory:
    client = _FakeRedisClient()

    @staticmethod
    def from_url(*_args, **_kwargs):
        del _args
        del _kwargs
        return _FakeRedisFactory.client


def test_redis_job_queue_roundtrip_push_pop(monkeypatch):
    monkeypatch.setattr(rq, "Redis", _FakeRedisFactory)
    queue = rq.RedisJobQueue(redis_url="redis://localhost:6379/0", queue_key="jobs", block_timeout_seconds=1)

    payload = {"type": "processing", "job_id": "job-1"}
    queue.enqueue(payload)
    queue.ping()

    popped = queue.dequeue()
    assert popped == payload
    assert queue._client.ping_called is True


def test_redis_job_queue_dequeue_invalid_payload_returns_none(monkeypatch):
    fake_client = _FakeRedisClient()
    monkeypatch.setattr(rq, "Redis", type("Redis", (), {"from_url": staticmethod(lambda *a, **k: fake_client)}))
    queue = rq.RedisJobQueue(redis_url="redis://localhost:6379/0", queue_key="jobs")

    fake_client.values.append(("jobs", b"{not-json]"))
    assert queue.dequeue() is None

    fake_client.values.append(("jobs", b"\"json-string\""))
    assert queue.dequeue() is None


def test_redis_job_queue_rejects_unsupported_redis_backend(monkeypatch):
    original = rq.Redis
    monkeypatch.setattr(rq, "Redis", None)
    try:
        try:
            rq.RedisJobQueue(redis_url="redis://localhost:6379/0", queue_key="jobs")
            raise AssertionError("Expected RuntimeError")
        except RuntimeError as exc:
            assert "redis package is required for RSLOGIC_QUEUE_BACKEND=redis" in str(exc)
    finally:
        monkeypatch.setattr(rq, "Redis", original)
