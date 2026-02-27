"""Redis message protocol for processing jobs between rslogic and RSNode clients."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from uuid import uuid4

try:
    from redis import Redis
except ModuleNotFoundError:  # pragma: no cover - optional dependency until installed
    Redis = None  # type: ignore[assignment]


COMMAND_TYPE_PROCESSING_JOB = "processing_job.execute"

RESULT_STATUS_ACCEPTED = "accepted"
RESULT_STATUS_PROGRESS = "progress"
RESULT_STATUS_OK = "ok"
RESULT_STATUS_ERROR = "error"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ProcessingCommand:
    command_id: str
    command_type: str
    payload: Dict[str, Any]
    reply_to: Optional[str] = None
    created_at: str = field(default_factory=_utc_now_iso)

    @staticmethod
    def parse(raw: Dict[str, Any]) -> "ProcessingCommand":
        command_id = str(raw.get("command_id") or raw.get("id") or uuid4())
        command_type = str(raw.get("command_type") or "").strip() or str(raw.get("type") or "").strip()
        if not command_type:
            raise ValueError("command_type is required")
        payload_raw = raw.get("payload")
        if not isinstance(payload_raw, dict):
            raise ValueError("payload must be a JSON object")
        reply_to_raw = raw.get("reply_to")
        reply_to = str(reply_to_raw).strip() if reply_to_raw is not None else None
        created_at = str(raw.get("created_at") or _utc_now_iso())
        return ProcessingCommand(
            command_id=command_id,
            command_type=command_type,
            payload=payload_raw,
            reply_to=reply_to or None,
            created_at=created_at,
        )

    @staticmethod
    def build(command_type: str, payload: Dict[str, Any], reply_to: Optional[str] = None) -> "ProcessingCommand":
        return ProcessingCommand(
            command_id=str(uuid4()),
            command_type=command_type,
            payload=payload,
            reply_to=reply_to,
        )

    def to_payload(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "command_type": self.command_type,
            "payload": self.payload,
            "reply_to": self.reply_to,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class ProcessingCommandResult:
    command_id: str
    command_type: str
    status: str
    message: Optional[str] = None
    progress: Optional[float] = None
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    created_at: str = field(default_factory=_utc_now_iso)

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "command_id": self.command_id,
            "command_type": self.command_type,
            "status": self.status,
            "created_at": self.created_at,
        }
        if self.message is not None:
            payload["message"] = self.message
        if self.progress is not None:
            normalized_progress = _coerce_float(self.progress)
            if normalized_progress is not None:
                payload["progress"] = normalized_progress
        if self.data is not None:
            payload["data"] = self.data
        if self.error is not None:
            payload["error"] = self.error
        if self.started_at is not None:
            payload["started_at"] = self.started_at
        if self.finished_at is not None:
            payload["finished_at"] = self.finished_at
        return payload

    @staticmethod
    def parse(raw: Dict[str, Any]) -> "ProcessingCommandResult":
        command_id = str(raw.get("command_id") or raw.get("id") or "").strip()
        if not command_id:
            raise ValueError("command_id is required")
        command_type = str(raw.get("command_type") or raw.get("type") or "").strip()
        if not command_type:
            raise ValueError("command_type is required")
        status = str(raw.get("status") or "").strip().lower()
        if not status:
            raise ValueError("status is required")
        message = raw.get("message")
        progress_raw = raw.get("progress")
        progress = _coerce_float(progress_raw)
        data_raw = raw.get("data")
        data = data_raw if isinstance(data_raw, dict) else None
        error = raw.get("error")
        if error is not None and str(error).strip() == "":
            error = None
        started_at = raw.get("started_at")
        if started_at is not None:
            started_at = str(started_at)
        finished_at = raw.get("finished_at")
        if finished_at is not None:
            finished_at = str(finished_at)
        created_at = raw.get("created_at")
        return ProcessingCommandResult(
            command_id=command_id,
            command_type=command_type,
            status=status,
            message=None if message is None else str(message),
            progress=progress,
            data=None if data is None else data,
            error=None if error is None else str(error),
            started_at=started_at,
            finished_at=finished_at,
            created_at=str(created_at or _utc_now_iso()),
        )


class RedisCommandBus:
    """JSON bus on Redis list structures used for command/result messaging."""

    def __init__(self, redis_url: str) -> None:
        if Redis is None:
            raise RuntimeError("redis package is required")
        self._client = Redis.from_url(redis_url, decode_responses=False)

    def push(self, queue_key: str, payload: Dict[str, Any], *, expire_seconds: Optional[int] = None) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self._client.lpush(queue_key, body)
        if expire_seconds is not None and expire_seconds > 0:
            self._client.expire(queue_key, int(expire_seconds))

    def pop(self, queue_key: str, *, timeout_seconds: int) -> Optional[Dict[str, Any]]:
        item = self._client.brpop(queue_key, timeout=max(int(timeout_seconds), 1))
        if item is None:
            return None
        _, raw = item
        try:
            decoded = raw.decode("utf-8", errors="replace")
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON payload in queue message: queue_key={queue_key}")
        if not isinstance(payload, dict):
            raise ValueError(f"Non-object payload in queue message: queue_key={queue_key}")
        return payload

    def close(self) -> None:
        self._client.close()


__all__ = [
    "COMMAND_TYPE_PROCESSING_JOB",
    "RESULT_STATUS_ACCEPTED",
    "RESULT_STATUS_PROGRESS",
    "RESULT_STATUS_OK",
    "RESULT_STATUS_ERROR",
    "ProcessingCommand",
    "ProcessingCommandResult",
    "RedisCommandBus",
]
