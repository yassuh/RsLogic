"""Redis-backed command client for driving RsLogic server workflows."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import socket
from typing import Any, Dict, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from uuid import uuid4

from config import AppConfig, load_config

try:
    from redis import Redis
    from redis.exceptions import RedisError
except ModuleNotFoundError:  # pragma: no cover - optional dependency until installed
    Redis = None  # type: ignore[assignment]

    class RedisError(Exception):
        """Fallback RedisError when redis dependency is missing."""


logger = logging.getLogger("rslogic.client")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ControlCommand:
    command_id: str
    command_type: str
    payload: Dict[str, Any]
    reply_to: Optional[str]
    created_at: str


class RedisJsonBus:
    """Small JSON queue wrapper for command/result queues."""

    def __init__(self, redis_url: str) -> None:
        if Redis is None:
            raise RuntimeError("redis package is required for rslogic-client")
        self._client = Redis.from_url(redis_url, decode_responses=False)

    def ping(self) -> None:
        self._client.ping()

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
            logger.error("Discarding invalid JSON payload queue=%s", queue_key)
            return None
        if not isinstance(payload, dict):
            logger.error("Discarding non-object payload queue=%s", queue_key)
            return None
        return payload

    def close(self) -> None:
        self._client.close()


class ApiCommandExecutor:
    """Dispatches high-level command types against RsLogic API endpoints."""

    def __init__(self, *, base_url: str, timeout_seconds: int) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = max(timeout_seconds, 1)

    def close(self) -> None:
        return None

    def _request(
        self,
        *,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self._base_url}{normalized_path}"
        if params:
            query = urllib_parse.urlencode(params, doseq=True)
            if query:
                url = f"{url}?{query}"

        body: Optional[bytes] = None
        headers: Dict[str, str] = {}
        if json_payload is not None:
            body = json.dumps(json_payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib_request.Request(
            url=url,
            data=body,
            headers=headers,
            method=method.upper(),
        )

        try:
            with urllib_request.urlopen(request, timeout=self._timeout_seconds) as response:
                raw_body = response.read().decode("utf-8", errors="replace").strip()
                content_type = (response.headers.get("Content-Type") or "").lower()
        except urllib_error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()A
            if detail:
                try:
                    parsed = json.loads(detail)
                    if isinstance(parsed, dict):
                        detail = str(parsed.get("detail") or parsed)
                except json.JSONDecodeError:
                    pass
            raise RuntimeError(
                f"API {method.upper()} {normalized_path} failed [{exc.code}]: {detail or 'unknown error'}"
            ) from exc

        if not raw_body:
            return {}
        if "application/json" in content_type:
            parsed = json.loads(raw_body)
            if isinstance(parsed, dict):
                return parsed
            return {"data": parsed}
        return {"text": raw_body}

    @staticmethod
    def _require_command_field(payload: Dict[str, Any], field_name: str) -> str:
        value = str(payload.get(field_name) or "").strip()
        if not value:
            raise ValueError(f"{field_name} is required")
        return value

    def execute(self, command_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if command_type == "health":
            return self._request(method="GET", path="/health")

        if command_type == "ingest_waiting":
            return self._request(
                method="POST",
                path="/images/ingest/waiting",
                json_payload=payload,
            )

        if command_type == "create_job":
            return self._request(
                method="POST",
                path="/jobs",
                json_payload=payload,
            )

        if command_type == "get_job":
            job_id = self._require_command_field(payload, "job_id")
            return self._request(method="GET", path=f"/jobs/{job_id}")

        if command_type == "cancel_job":
            job_id = self._require_command_field(payload, "job_id")
            return self._request(method="POST", path=f"/jobs/{job_id}/cancel")

        if command_type == "list_jobs":
            params: Dict[str, Any] = {}
            if "status" in payload and payload.get("status") is not None:
                params["status"] = payload.get("status")
            if "limit" in payload and payload.get("limit") is not None:
                params["limit"] = payload.get("limit")
            return self._request(method="GET", path="/jobs", params=params or None)

        if command_type == "list_groups":
            params: Dict[str, Any] = {}
            if "limit" in payload and payload.get("limit") is not None:
                params["limit"] = payload.get("limit")
            return self._request(method="GET", path="/groups", params=params or None)

        if command_type == "create_group":
            return self._request(method="POST", path="/groups", json_payload=payload)

        if command_type == "api_request":
            method = str(payload.get("method") or "GET").strip().upper()
            path = str(payload.get("path") or "").strip()
            if not path:
                raise ValueError("path is required for api_request")
            query = payload.get("params")
            params = query if isinstance(query, dict) else None
            json_part = payload.get("json")
            json_payload = json_part if isinstance(json_part, dict) else None
            return self._request(method=method, path=path, params=params, json_payload=json_payload)

        raise ValueError(
            "unsupported command type. "
            "Supported: health, ingest_waiting, create_job, get_job, cancel_job, "
            "list_jobs, list_groups, create_group, api_request"
        )


class ControlWorker:
    """Long-running command worker that pops work from Redis and executes API actions."""

    def __init__(
        self,
        *,
        config: AppConfig,
        bus: Optional[RedisJsonBus] = None,
        executor: Optional[ApiCommandExecutor] = None,
    ) -> None:
        self._config = config
        self._bus = bus or RedisJsonBus(config.queue.redis_url)
        self._executor = executor or ApiCommandExecutor(
            base_url=config.api.base_url,
            timeout_seconds=config.control.request_timeout_seconds,
        )
        self._host = socket.gethostname()
        self._pid = os.getpid()

    def close(self) -> None:
        self._executor.close()
        self._bus.close()

    def _parse_command(self, raw: Dict[str, Any]) -> ControlCommand:
        command_id = str(raw.get("command_id") or raw.get("id") or uuid4()).strip()
        command_type = str(raw.get("type") or "").strip()
        if not command_type:
            raise ValueError("type is required")
        payload_raw = raw.get("payload", {})
        if payload_raw is None:
            payload_raw = {}
        if not isinstance(payload_raw, dict):
            raise ValueError("payload must be a JSON object")
        reply_to_raw = raw.get("reply_to")
        reply_to = str(reply_to_raw).strip() if reply_to_raw is not None else None
        created_at = str(raw.get("created_at") or _utc_now_iso())
        return ControlCommand(
            command_id=command_id,
            command_type=command_type,
            payload=payload_raw,
            reply_to=reply_to or None,
            created_at=created_at,
        )

    def _build_result(
        self,
        *,
        command: ControlCommand,
        started_at: str,
        ok: bool,
        result_payload: Optional[Dict[str, Any]],
        error_message: Optional[str],
    ) -> Dict[str, Any]:
        return {
            "command_id": command.command_id,
            "type": command.command_type,
            "status": "ok" if ok else "error",
            "created_at": command.created_at,
            "started_at": started_at,
            "finished_at": _utc_now_iso(),
            "worker": {
                "host": self._host,
                "pid": self._pid,
            },
            "result": result_payload if ok else None,
            "error": error_message if not ok else None,
        }

    def _publish_result(self, command: ControlCommand, result_payload: Dict[str, Any]) -> None:
        result_queue = self._config.control.result_queue_key
        ttl = self._config.control.result_ttl_seconds
        self._bus.push(result_queue, result_payload, expire_seconds=ttl)
        if command.reply_to and command.reply_to != result_queue:
            self._bus.push(command.reply_to, result_payload, expire_seconds=ttl)

    def process_once(self, *, timeout_seconds: Optional[int] = None) -> bool:
        queue_key = self._config.control.command_queue_key
        timeout = (
            max(timeout_seconds, 1)
            if timeout_seconds is not None
            else max(self._config.control.block_timeout_seconds, 1)
        )
        raw = self._bus.pop(queue_key, timeout_seconds=timeout)
        if raw is None:
            return False

        try:
            command = self._parse_command(raw)
        except Exception as exc:
            fallback = ControlCommand(
                command_id=str(raw.get("command_id") or raw.get("id") or uuid4()),
                command_type=str(raw.get("type") or "invalid"),
                payload={},
                reply_to=str(raw.get("reply_to") or "").strip() or None,
                created_at=str(raw.get("created_at") or _utc_now_iso()),
            )
            started_at = _utc_now_iso()
            result = self._build_result(
                command=fallback,
                started_at=started_at,
                ok=False,
                result_payload=None,
                error_message=f"invalid command payload: {exc}",
            )
            self._publish_result(fallback, result)
            logger.warning("Rejected invalid command payload: %s", exc)
            return True

        started_at = _utc_now_iso()
        try:
            logger.info("Executing command command_id=%s type=%s", command.command_id, command.command_type)
            response_payload = self._executor.execute(command.command_type, command.payload)
            result = self._build_result(
                command=command,
                started_at=started_at,
                ok=True,
                result_payload=response_payload,
                error_message=None,
            )
            logger.info("Command complete command_id=%s type=%s", command.command_id, command.command_type)
        except Exception as exc:  # noqa: BLE001
            result = self._build_result(
                command=command,
                started_at=started_at,
                ok=False,
                result_payload=None,
                error_message=str(exc),
            )
            logger.exception("Command failed command_id=%s type=%s", command.command_id, command.command_type)
        self._publish_result(command, result)
        return True

    def run_forever(self) -> None:
        logger.info(
            "Control worker started command_queue=%s result_queue=%s api=%s",
            self._config.control.command_queue_key,
            self._config.control.result_queue_key,
            self._config.api.base_url,
        )
        while True:
            self.process_once()


class CommandSender:
    """Producer utility for enqueueing command objects and optionally waiting for result."""

    def __init__(self, *, config: AppConfig, bus: Optional[RedisJsonBus] = None) -> None:
        self._config = config
        self._bus = bus or RedisJsonBus(config.queue.redis_url)

    def close(self) -> None:
        self._bus.close()

    def send(
        self,
        *,
        command_type: str,
        payload: Dict[str, Any],
        command_id: Optional[str] = None,
        wait: bool = False,
        timeout_seconds: Optional[int] = None,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        resolved_id = (command_id or str(uuid4())).strip()
        if not resolved_id:
            resolved_id = str(uuid4())
        resolved_reply = (reply_to or "").strip() or None
        if wait and resolved_reply is None:
            resolved_reply = f"{self._config.control.result_queue_key}:reply:{resolved_id}"

        envelope = {
            "command_id": resolved_id,
            "type": command_type,
            "payload": payload,
            "reply_to": resolved_reply,
            "created_at": _utc_now_iso(),
        }
        self._bus.push(self._config.control.command_queue_key, envelope)

        if not wait:
            return {
                "submitted": True,
                "command_id": resolved_id,
                "type": command_type,
                "command_queue": self._config.control.command_queue_key,
                "reply_to": resolved_reply,
            }

        wait_timeout = timeout_seconds or self._config.control.request_timeout_seconds
        result = self._bus.pop(resolved_reply, timeout_seconds=max(wait_timeout, 1))
        if result is None:
            raise TimeoutError(f"timed out waiting for result command_id={resolved_id}")
        return result


def _parse_payload(*, payload_json: Optional[str], payload_file: Optional[str]) -> Dict[str, Any]:
    if payload_json and payload_file:
        raise ValueError("Provide either --payload-json or --payload-file, not both")
    if payload_file:
        raw = json.loads(Path(payload_file).read_text(encoding="utf-8"))
    elif payload_json:
        raw = json.loads(payload_json)
    else:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("payload must be a JSON object")
    return raw


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RsLogic Redis command client")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("worker", help="Run server-side command worker")
    worker_parser.add_argument("--once", action="store_true", help="Process one command then exit")

    send_parser = subparsers.add_parser("send", help="Enqueue a command")
    send_parser.add_argument("--type", dest="command_type", required=True, help="Command type")
    send_parser.add_argument("--payload-json", default=None, help='JSON object payload, e.g. \'{"limit":1000}\'')
    send_parser.add_argument("--payload-file", default=None, help="Path to payload JSON file")
    send_parser.add_argument("--command-id", default=None, help="Optional explicit command id")
    send_parser.add_argument("--reply-to", default=None, help="Optional explicit Redis reply queue key")
    send_parser.add_argument("--wait", action="store_true", help="Wait for command result")
    send_parser.add_argument("--timeout", type=int, default=None, help="Wait timeout seconds (when --wait)")

    listen_parser = subparsers.add_parser("listen", help="Listen for results from result queue")
    listen_parser.add_argument("--once", action="store_true", help="Read one result then exit")
    listen_parser.add_argument("--timeout", type=int, default=None, help="Block timeout seconds")
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = load_config()
    try:
        if args.command == "worker":
            worker = ControlWorker(config=config)
            try:
                if args.once:
                    processed = worker.process_once(timeout_seconds=config.control.block_timeout_seconds)
                    print(json.dumps({"processed": processed}, indent=2))
                    return
                worker.run_forever()
            finally:
                worker.close()
            return

        if args.command == "send":
            payload = _parse_payload(payload_json=args.payload_json, payload_file=args.payload_file)
            sender = CommandSender(config=config)
            try:
                result = sender.send(
                    command_type=args.command_type,
                    payload=payload,
                    command_id=args.command_id,
                    wait=args.wait,
                    timeout_seconds=args.timeout,
                    reply_to=args.reply_to,
                )
            finally:
                sender.close()
            print(json.dumps(result, indent=2))
            return

        if args.command == "listen":
            bus = RedisJsonBus(config.queue.redis_url)
            timeout = args.timeout or config.control.block_timeout_seconds
            try:
                if args.once:
                    item = bus.pop(config.control.result_queue_key, timeout_seconds=timeout)
                    print(json.dumps(item or {"result": None}, indent=2))
                    return
                while True:
                    item = bus.pop(config.control.result_queue_key, timeout_seconds=timeout)
                    if item is None:
                        continue
                    print(json.dumps(item, indent=2))
            finally:
                bus.close()
            return

        parser.error(f"unsupported command: {args.command}")
    except (ValueError, RuntimeError, TimeoutError, FileNotFoundError, json.JSONDecodeError, RedisError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
