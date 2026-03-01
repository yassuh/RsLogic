#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
    ProcessingCommand,
    ProcessingCommandResult,
)

LOGGER = logging.getLogger("rslogic.reconstruction.redis-runner")
load_dotenv(override=False)


def _build_logger(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _default_client_id() -> str:
    return _env("RSLOGIC_RSTOOLS_SDK_CLIENT_ID") or str(uuid.uuid4())


def _default_tokens() -> tuple[str, str]:
    app_token = _env("RSLOGIC_RSTOOLS_SDK_APP_TOKEN", "123") or "123"
    auth_token = (
        _env(
            "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN",
            "85DBDE55-3FFF-4228-9F06-CBED4003BBB8",
        )
        or "85DBDE55-3FFF-4228-9F06-CBED4003BBB8"
    )
    return app_token, auth_token


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a processing job over the RsLogic control bus."
    )
    parser.add_argument("--redis-url", default="redis://192.168.193.56:9002/0")
    parser.add_argument("--command-queue", default="rslogic:control:commands")
    parser.add_argument(
        "--reply-queue",
        default="",
        help="Optional response queue. If omitted while waiting, one is auto-generated.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="Seconds to wait for each blocking pop while waiting for results.",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=7200,
        help="Maximum seconds to wait for the final command result.",
    )
    parser.add_argument("--wait", dest="wait_for_result", action="store_true", default=True)
    parser.add_argument("--no-wait", dest="wait_for_result", action="store_false")

    parser.add_argument("--job-id", default="")
    parser.add_argument("--working-directory", default="")
    parser.add_argument("--image-key", action="append", default=[], dest="image_keys")
    parser.add_argument(
        "--set-filter",
        action="append",
        default=[],
        help="Repeatable KEY=VALUE filters attached to the processing job.",
    )
    parser.add_argument(
        "--filter-json",
        default="",
        help="JSON object merged into --set-filter values.",
    )

    parser.add_argument("--base-url", default=_env("RSTOOL_BASE_URL", "http://192.168.193.59:8000"))
    parser.add_argument("--client-id", default=_default_client_id())
    parser.add_argument("--app-token", default=_default_tokens()[0])
    parser.add_argument("--auth-token", default=_default_tokens()[1])

    parser.add_argument("--stage-only", action="store_true", help="Only stage/sync files; do not run SDK jobs.")
    parser.add_argument("--no-stage-only", dest="stage_only", action="store_false")

    parser.add_argument("--pull-s3-images", action="store_true", default=True, dest="pull_s3_images")
    parser.add_argument("--no-pull-s3-images", action="store_false", dest="pull_s3_images")
    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument(
        "--s3-region",
        default=(_env("RSLOGIC_S3_REGION") or _env("S3_REGION") or "us-east-1"),
    )
    parser.add_argument(
        "--s3-endpoint-url",
        default=(_env("RSLOGIC_S3_ENDPOINT_URL") or _env("S3_ENDPOINT_URL")),
    )
    parser.add_argument("--s3-staging-root", default="")
    parser.add_argument("--s3-max-files", type=int, default=0)
    parser.add_argument(
        "--s3-extensions",
        default="jpg,jpeg,png,tif,tiff,webp,heic,arw,nef,cr2,dng",
        help="Comma separated list for optional S3 extension filtering.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _coerce_filter_value(raw: str) -> Any:
    raw = raw.strip()
    lowered = raw.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _parse_set_filters(raw_filters: Sequence[str], filter_json: str) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    if filter_json:
        try:
            parsed = json.loads(filter_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid --filter-json value: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError("--filter-json must be a JSON object.")
        filters.update(parsed)

    for item in raw_filters:
        if "=" not in str(item):
            raise ValueError(f"Invalid --set-filter value {item!r}; expected KEY=VALUE.")
        key, value = str(item).split("=", 1)
        filters[key.strip()] = _coerce_filter_value(value.strip())
    return filters


@dataclass(frozen=True)
class ProcessingJobConfig:
    redis_url: str
    command_queue: str
    reply_queue: str
    timeout: int
    request_timeout: int
    wait_for_result: bool
    job_id: str
    working_directory: str
    base_url: str
    client_id: str
    app_token: str
    auth_token: str
    stage_only: bool
    pull_s3_images: bool
    s3_bucket: str
    s3_prefix: str
    s3_region: str
    s3_endpoint_url: str
    s3_staging_root: str
    s3_max_files: int
    s3_extensions: Sequence[str]
    image_keys: Sequence[str]
    extra_filters: Dict[str, Any]


class RedisControlClient:
    def __init__(self, redis_url: str) -> None:
        try:
            import redis
        except Exception as exc:
            raise RuntimeError("redis package is required to send commands via control bus.") from exc

        self._redis = redis.from_url(redis_url, decode_responses=False)

    def push(self, queue: str, payload: Dict[str, Any], *, expire_seconds: Optional[int] = None) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self._redis.lpush(queue, body)
        if expire_seconds and int(expire_seconds) > 0:
            self._redis.expire(queue, int(expire_seconds))

    def pop(self, queue: str, timeout_seconds: int) -> Optional[Dict[str, Any]]:
        item = self._redis.brpop(queue, timeout=max(1, int(timeout_seconds)))
        if item is None:
            return None
        _, raw = item
        decoded = raw.decode("utf-8", errors="replace")
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Non-object payload in result queue: {queue}")
        return payload

    def close(self) -> None:
        self._redis.close()


def _to_payload(cfg: ProcessingJobConfig) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "job_id": cfg.job_id,
        "base_url": cfg.base_url,
        "client_id": cfg.client_id,
        "app_token": cfg.app_token,
        "auth_token": cfg.auth_token,
        "working_directory": (cfg.working_directory.strip() if cfg.working_directory else ""),
        "pull_s3_images": bool(cfg.pull_s3_images),
        "stage_only": bool(cfg.stage_only),
        "image_keys": list(cfg.image_keys),
    }

    if cfg.s3_bucket:
        payload["s3_bucket"] = cfg.s3_bucket
    if cfg.s3_prefix:
        payload["s3_prefix"] = cfg.s3_prefix
    if cfg.s3_region:
        payload["s3_region"] = cfg.s3_region
    if cfg.s3_endpoint_url:
        payload["s3_endpoint_url"] = cfg.s3_endpoint_url
    if cfg.s3_staging_root:
        payload["s3_staging_root"] = cfg.s3_staging_root
    if cfg.s3_max_files > 0:
        payload["s3_max_files"] = int(cfg.s3_max_files)
    if cfg.s3_extensions:
        payload["s3_extensions"] = list(cfg.s3_extensions)

    filters = dict(cfg.extra_filters)
    if cfg.stage_only:
        filters.setdefault("stage_only", True)
    if cfg.s3_bucket:
        filters.setdefault("s3_bucket", cfg.s3_bucket)
    if cfg.s3_prefix:
        filters.setdefault("s3_prefix", cfg.s3_prefix)
    if cfg.s3_region:
        filters.setdefault("s3_region", cfg.s3_region)
    if cfg.s3_endpoint_url:
        filters.setdefault("s3_endpoint_url", cfg.s3_endpoint_url)
    if cfg.s3_staging_root:
        filters.setdefault("s3_staging_root", cfg.s3_staging_root)
    if cfg.s3_max_files > 0:
        filters.setdefault("s3_max_files", int(cfg.s3_max_files))
    if cfg.s3_extensions:
        filters.setdefault("s3_extensions", list(cfg.s3_extensions))
    filters.setdefault("pull_s3_images", bool(cfg.pull_s3_images))

    if filters:
        payload["filters"] = filters
    return payload


def _build_reply_queue(cfg_reply_queue: str) -> str:
    if cfg_reply_queue.strip():
        return cfg_reply_queue.strip()
    return f"rslogic:control:results:{uuid.uuid4()}"


def _wait_for_result(
    redis_client: RedisControlClient,
    result_queue: str,
    command_id: str,
    timeout_seconds: int,
    overall_deadline: float,
    *,
    stop_event: threading.Event,
) -> ProcessingCommandResult:
    while time.time() < overall_deadline and not stop_event.is_set():
        raw = redis_client.pop(result_queue, timeout_seconds=timeout_seconds)
        if raw is None:
            continue
        parsed = ProcessingCommandResult.parse(raw)
        if parsed.command_id != command_id:
            LOGGER.debug("Ignoring unrelated result for command_id=%s", parsed.command_id)
            continue
        if parsed.status in {"accepted", "progress"}:
            LOGGER.info("Command %s still running (%s%%).", parsed.command_id, parsed.progress)
            continue
        return parsed

    if stop_event.is_set():
        raise RuntimeError("operation interrupted by signal")
    raise TimeoutError(f"Timed out waiting for result for command_id={command_id} on queue={result_queue}")


def _expect_success(result: ProcessingCommandResult) -> Dict[str, Any]:
    if result.status == "error":
        message = result.message or "command failed"
        if result.error:
            raise RuntimeError(f"{message}: {result.error}")
        raise RuntimeError(message)
    if result.status not in {"ok", "accepted"}:
        raise RuntimeError(f"Unexpected status '{result.status}' for command_id={result.command_id}")
    return result.data or {}


def _safe_exts(raw: str) -> Sequence[str]:
    values = [item.strip().lower() for item in (raw or "").split(",") if item.strip()]
    return values


def _send_processing_command(cfg: ProcessingJobConfig) -> int:
    stop_event = threading.Event()

    def _handle_sig(_sig: int, _frame: Any) -> None:
        LOGGER.warning("Interrupted. Exiting.")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    control = RedisControlClient(cfg.redis_url)
    reply_queue = _build_reply_queue(cfg.reply_queue) if cfg.wait_for_result else ""
    payload = _to_payload(cfg)

    command = ProcessingCommand.build(
        command_type=COMMAND_TYPE_PROCESSING_JOB,
        payload=payload,
        reply_to=reply_queue or None,
    )
    message = command.to_payload()
    control.push(cfg.command_queue, message, expire_seconds=86400)

    LOGGER.info("Published command_id=%s to queue=%s", command.command_id, cfg.command_queue)
    if not cfg.wait_for_result:
        LOGGER.info("Fire-and-forget mode. Not waiting for Redis result.")
        control.close()
        return 0

    try:
        result = _wait_for_result(
            redis_client=control,
            result_queue=reply_queue,
            command_id=command.command_id,
            timeout_seconds=cfg.timeout,
            overall_deadline=time.time() + cfg.request_timeout,
            stop_event=stop_event,
        )
        data = _expect_success(result)
        LOGGER.info("Command completed: %s", data)
        return 0
    except TimeoutError as exc:
        LOGGER.error("Timeout waiting for result: %s", exc)
        return 1
    except Exception as exc:
        LOGGER.error("Processing job dispatch failed: %s", exc, exc_info=True)
        return 1
    finally:
        control.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _build_logger(args.verbose)
    filters = _parse_set_filters(args.set_filter, args.filter_json)

    cfg = ProcessingJobConfig(
        redis_url=args.redis_url.strip(),
        command_queue=args.command_queue.strip(),
        reply_queue=(args.reply_queue or "").strip(),
        timeout=max(1, int(args.timeout)),
        request_timeout=max(1, int(args.request_timeout)),
        wait_for_result=bool(args.wait_for_result),
        job_id=(args.job_id.strip() or str(uuid.uuid4())),
        working_directory=args.working_directory.strip(),
        base_url=args.base_url.strip(),
        client_id=args.client_id.strip(),
        app_token=args.app_token.strip(),
        auth_token=args.auth_token.strip(),
        stage_only=bool(args.stage_only),
        pull_s3_images=bool(args.pull_s3_images),
        s3_bucket=args.s3_bucket.strip(),
        s3_prefix=args.s3_prefix.strip(),
        s3_region=args.s3_region.strip(),
        s3_endpoint_url=args.s3_endpoint_url.strip(),
        s3_staging_root=args.s3_staging_root.strip(),
        s3_max_files=max(0, int(args.s3_max_files)),
        s3_extensions=_safe_exts(args.s3_extensions),
        image_keys=list(args.image_keys),
        extra_filters=filters,
    )
    return _send_processing_command(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
