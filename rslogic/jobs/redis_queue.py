"""Redis-backed durable queue for RsLogic job orchestration."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

try:
    from redis import Redis
    from redis.exceptions import RedisError
except ModuleNotFoundError:  # pragma: no cover - optional dependency until installed
    Redis = None  # type: ignore[assignment]

    class RedisError(Exception):
        """Fallback RedisError when redis dependency is missing."""

logger = logging.getLogger("rslogic.jobs.redis")


class RedisJobQueue:
    """Small JSON queue wrapper around a Redis list."""

    def __init__(
        self,
        *,
        redis_url: str,
        queue_key: str,
        block_timeout_seconds: int = 1,
    ) -> None:
        if Redis is None:
            raise RuntimeError("redis package is required for RSLOGIC_QUEUE_BACKEND=redis")
        self._queue_key = queue_key
        self._block_timeout_seconds = max(block_timeout_seconds, 1)
        self._client = Redis.from_url(redis_url, decode_responses=False)

    def ping(self) -> None:
        self._client.ping()

    def enqueue(self, payload: Dict[str, Any]) -> None:
        try:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            self._client.lpush(self._queue_key, body)
        except (RedisError, TypeError, ValueError):
            logger.exception("Failed to enqueue redis job payload type=%s", payload.get("type"))
            raise

    def dequeue(self) -> Optional[Dict[str, Any]]:
        try:
            item = self._client.brpop(self._queue_key, timeout=self._block_timeout_seconds)
        except RedisError:
            logger.exception("Failed to dequeue redis job payload queue=%s", self._queue_key)
            raise
        if item is None:
            return None

        _, raw = item
        try:
            decoded = raw.decode("utf-8", errors="replace")
            payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.error("Discarding invalid redis queue payload queue=%s", self._queue_key)
            return None
        if not isinstance(payload, dict):
            logger.error("Discarding non-dict redis queue payload queue=%s", self._queue_key)
            return None
        return payload

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            logger.debug("Ignoring redis close error", exc_info=True)
