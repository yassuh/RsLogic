#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
import time
import uuid
from dataclasses import dataclass
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from rslogic.jobs.command_channel import (
    COMMAND_TYPE_RSTOOL_COMMAND,
    COMMAND_TYPE_RSTOOL_DISCOVER,
    ProcessingCommand,
    ProcessingCommandResult,
)

LOGGER = logging.getLogger("rslogic.reconstruction.redis-runner")


def _build_logger(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _default_client_id() -> str:
    return os.getenv("RSLOGIC_RSTOOLS_SDK_CLIENT_ID", "").strip() or str(uuid.uuid4())


def _default_tokens() -> tuple[str, str]:
    app_token = os.getenv("RSLOGIC_RSTOOLS_SDK_APP_TOKEN", "123").strip() or "123"
    auth_token = os.getenv(
        "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN",
        "85DBDE55-3FFF-4228-9F06-CBED4003BBB8",
    ).strip() or "85DBDE55-3FFF-4228-9F06-CBED4003BBB8"
    return app_token, auth_token


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Drive a reconstruction workflow through rslogic rsnode Redis control bus."
    )
    parser.add_argument("--redis-url", default="redis://192.168.193.56:9002/0")
    parser.add_argument("--command-queue", default="rslogic:control:commands")
    parser.add_argument("--timeout", type=int, default=15, help="Per-queue wait timeout in seconds.")
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=7200,
        help="Max wall-clock seconds to wait for each command result.",
    )
    parser.add_argument(
        "--base-url",
        default="http://192.168.193.59:8000",
        help="RealityScan API base URL passed in payload for SDK commands.",
    )
    parser.add_argument("--client-id", default=_default_client_id())
    parser.add_argument("--app-token", default=_default_tokens()[0])
    parser.add_argument("--auth-token", default=_default_tokens()[1])

    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--s3-download-dir", default="")
    parser.add_argument("--s3-max-files", type=int, default=0)
    parser.add_argument(
        "--s3-extensions",
        default="jpg,jpeg,png,tif,tiff,webp,heic,arw,nef,cr2,dng",
        help="Comma-separated image extensions to download.",
    )
    parser.add_argument(
        "--no-pull-s3",
        action="store_true",
        help="Skip S3 image download step and keep local folder as-is.",
    )
    parser.add_argument(
        "--imagery-dir",
        default="",
        help="Directory that contains imagery for add_folder. Defaults to <cwd>/recon_staging/Imagery.",
    )
    parser.add_argument(
        "--imagery-subdir",
        default="Imagery",
        help="Subdirectory under --imagery-dir for downloaded images.",
    )
    parser.add_argument("--save-path", default="test_auto.rspj")
    parser.add_argument(
        "--skip-discover",
        action="store_true",
        help="Skip discover checks and trust command compatibility.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


@dataclass(frozen=True)
class ReconConfig:
    redis_url: str
    command_queue: str
    timeout: int
    request_timeout: int
    base_url: str
    client_id: str
    app_token: str
    auth_token: str
    s3_bucket: str
    s3_prefix: str
    s3_download_dir: str
    s3_max_files: int
    s3_extensions: Sequence[str]
    pull_s3: bool
    imagery_dir: str
    imagery_subdir: str
    save_path: str
    skip_discover: bool


class RedisControlClient:
    def __init__(self, redis_url: str) -> None:
        try:
            import redis
        except Exception as exc:
            raise RuntimeError("redis package is required to send commands via control bus.") from exc
        self._redis = redis.from_url(redis_url, decode_responses=False)

    def push(self, queue: str, payload: Dict[str, Any], expire_seconds: Optional[int] = None) -> None:
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
            raise RuntimeError(f"Non-object payload from queue {queue}")
        return payload

    def close(self) -> None:
        self._redis.close()


def _safe_exts(raw: str) -> Sequence[str]:
    values = [item.strip().lower() for item in (raw or "").split(",") if item.strip()]
    return values or ["jpg", "jpeg", "png", "tif", "tiff"]


def _download_images_from_s3(
    bucket: str,
    prefix: str,
    download_dir: Path,
    max_files: int,
    allowed_extensions: Sequence[str],
) -> List[Path]:
    if not bucket:
        raise RuntimeError("Cannot download from S3: --s3-bucket is required.")

    try:
        import boto3
        from botocore.config import Config
    except Exception as exc:
        raise RuntimeError("boto3 is required for S3 download mode.") from exc

    conf = Config(
        max_pool_connections=max(4, os.cpu_count() or 4),
        retries={"max_attempts": 10, "mode": "standard"},
    )
    s3 = boto3.client("s3", config=conf)
    paginator = s3.get_paginator("list_objects_v2")

    download_dir.mkdir(parents=True, exist_ok=True)
    target_exts = {f".{ext.lower().lstrip('.')}" for ext in allowed_extensions}
    downloaded: List[Path] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = str(obj.get("Key") or "").strip()
            if not key or key.endswith("/"):
                continue
            extension = Path(key.lower()).suffix
            if extension and extension not in target_exts:
                continue

            rel = key[len(prefix):].lstrip("/") if prefix else key
            target_path = download_dir / rel
            target_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(target_path))
            downloaded.append(target_path)
            LOGGER.info("Downloaded S3 object %s -> %s", key, target_path)
            if max_files > 0 and len(downloaded) >= max_files:
                return downloaded

    return downloaded


def _build_control_payload(
    *,
    command_type: str,
    payload: Dict[str, Any],
    reply_to: str,
) -> ProcessingCommand:
    return ProcessingCommand.build(command_type=command_type, payload=payload, reply_to=reply_to)


def _wait_for_result(
    redis_client: RedisControlClient,
    reply_queue: str,
    command_id: str,
    timeout_seconds: int,
    overall_deadline: float,
    *,
    stop_event: threading.Event,
) -> ProcessingCommandResult:
    while time.time() < overall_deadline and not stop_event.is_set():
        raw = redis_client.pop(reply_queue, timeout_seconds=timeout_seconds)
        if raw is None:
            continue
        parsed = ProcessingCommandResult.parse(raw)
        if parsed.command_id != command_id:
            LOGGER.debug("Ignoring unrelated result for command_id=%s", parsed.command_id)
            continue

        if parsed.status == "error":
            return parsed
        if parsed.status in {"accepted", "progress"}:
            LOGGER.debug(
                "Command %s still running (status=%s). Waiting for final status.",
                parsed.command_id,
                parsed.status,
            )
            continue
        LOGGER.info("Result for command_id=%s status=%s", parsed.command_id, parsed.status)
        return parsed

    if stop_event.is_set():
        raise RuntimeError("Operation interrupted by signal.")
    raise TimeoutError(f"Timed out waiting for reply to command_id={command_id} on queue={reply_queue}")


def _expect_ok(result: ProcessingCommandResult) -> Dict[str, Any]:
    if result.status == "error":
        message = result.message or "command failed"
        raise RuntimeError(f"{message}: {result.error}")
    if result.status not in {"ok", "accepted", "progress"}:
        raise RuntimeError(f"Unexpected status '{result.status}' for command result.")
    return result.data or {}


def _pick_session(command_result_data: Dict[str, Any]) -> str:
    value = command_result_data.get("result")
    if isinstance(value, str) and value.strip():
        return value.strip()
    nested = command_result_data.get("data")
    if isinstance(nested, dict):
        maybe = nested.get("session")
        if isinstance(maybe, str) and maybe.strip():
            return maybe.strip()
        maybe = nested.get("result")
        if isinstance(maybe, str) and maybe.strip():
            return maybe.strip()
    return ""


def _require_method(methods: Dict[str, Any], target: str, method: str) -> None:
    if method not in methods:
        raise RuntimeError(f"{target}.{method} is not available from discovered SDK methods.")


def _send_command(
    redis_client: RedisControlClient,
    command_queue: str,
    reply_queue: str,
    command_type: str,
    payload: Dict[str, Any],
    timeout_seconds: int,
    request_timeout: int,
    *,
    stop_event: threading.Event,
) -> ProcessingCommandResult:
    command = _build_control_payload(command_type=command_type, payload=payload, reply_to=reply_queue)
    redis_client.push(command_queue, command.to_payload(), expire_seconds=None)
    LOGGER.debug("Sent %s command %s", command_type, command.command_id)
    result = _wait_for_result(
        redis_client=redis_client,
        reply_queue=reply_queue,
        command_id=command.command_id,
        timeout_seconds=timeout_seconds,
        overall_deadline=time.time() + request_timeout,
        stop_event=stop_event,
    )
    return result


def _call_discover(
    redis_client: RedisControlClient,
    cfg: ReconConfig,
    reply_queue: str,
    target: str,
    *,
    stop_event: threading.Event,
) -> Dict[str, Any]:
    result = _send_command(
        redis_client=redis_client,
        command_queue=cfg.command_queue,
        reply_queue=reply_queue,
        command_type=COMMAND_TYPE_RSTOOL_DISCOVER,
        payload={"target": target},
        timeout_seconds=cfg.timeout,
        request_timeout=cfg.request_timeout,
        stop_event=stop_event,
    )
    return _expect_ok(result)


def _emit_reconstruction(cfg: ReconConfig) -> int:
    stop_event = threading.Event()

    def _handle_sigterm(_sig: int, _frame: Any) -> None:
        LOGGER.warning("Received shutdown signal. Exiting after current step.")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigterm)
    signal.signal(signal.SIGTERM, _handle_sigterm)

    imagery_root = Path(cfg.imagery_dir) if cfg.imagery_dir else Path.cwd() / "recon_staging"
    imagery_dir = imagery_root / cfg.imagery_subdir

    # Keep reply queue unique per run so command completions are deterministic.
    reply_queue = f"rslogic:control:recon:{uuid.uuid4()}"
    redis_client = RedisControlClient(cfg.redis_url)

    try:
        if cfg.pull_s3:
            LOGGER.info("Downloading imagery from s3://%s/%s", cfg.s3_bucket, cfg.s3_prefix)
            downloaded = _download_images_from_s3(
                bucket=cfg.s3_bucket,
                prefix=cfg.s3_prefix,
                download_dir=imagery_dir,
                max_files=cfg.s3_max_files,
                allowed_extensions=cfg.s3_extensions,
            )
            if not downloaded:
                LOGGER.warning("No image files were downloaded from s3://%s/%s", cfg.s3_bucket, cfg.s3_prefix)
            else:
                LOGGER.info("Downloaded %s image files to %s", len(downloaded), imagery_dir)
        else:
            LOGGER.info("Skipping S3 download (--no-pull-s3), using existing imagery dir: %s", imagery_dir)

        if not imagery_dir.exists():
            if cfg.pull_s3:
                raise RuntimeError(f"Imagery directory does not exist: {imagery_dir}")
            imagery_dir.mkdir(parents=True, exist_ok=True)
            LOGGER.warning("Imagery directory did not exist and was created: %s", imagery_dir)

        if not cfg.skip_discover:
            node_methods = _call_discover(
                redis_client=redis_client,
                cfg=cfg,
                reply_queue=reply_queue,
                target="node",
                stop_event=stop_event,
            ).get("available", {}).get("node", {})
            project_methods = _call_discover(
                redis_client=redis_client,
                cfg=cfg,
                reply_queue=reply_queue,
                target="project",
                stop_event=stop_event,
            ).get("available", {}).get("project", {})
            if not isinstance(node_methods, dict) or not isinstance(project_methods, dict):
                raise RuntimeError("Discover response missing target method maps.")

            LOGGER.info("Discovered node methods: %s", ", ".join(sorted(node_methods.keys())))
            LOGGER.info("Discovered project methods: %s", ", ".join(sorted(project_methods.keys())))
            for method in ("connect_user",):
                _require_method(node_methods, "node", method)
            for method in ("create", "new_scene", "add_folder", "command", "save"):
                _require_method(project_methods, "project", method)

        def _send_project_command(method: str, args: Sequence[Any], session_id: Optional[str]) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "target": "project",
                "method": method,
                "args": list(args),
                "kwargs": {},
                "base_url": cfg.base_url,
                "client_id": cfg.client_id,
                "app_token": cfg.app_token,
                "auth_token": cfg.auth_token,
            }
            if session_id:
                payload["session"] = session_id
            result = _send_command(
                redis_client=redis_client,
                command_queue=cfg.command_queue,
                reply_queue=reply_queue,
                command_type=COMMAND_TYPE_RSTOOL_COMMAND,
                payload=payload,
                timeout_seconds=cfg.timeout,
                request_timeout=cfg.request_timeout,
                stop_event=stop_event,
            )
            return _expect_ok(result)

        def _send_node_command(method: str, args: Sequence[Any], kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "target": "node",
                "method": method,
                "args": list(args),
                "kwargs": dict(kwargs or {}),
                "base_url": cfg.base_url,
                "client_id": cfg.client_id,
                "app_token": cfg.app_token,
                "auth_token": cfg.auth_token,
            }
            result = _send_command(
                redis_client=redis_client,
                command_queue=cfg.command_queue,
                reply_queue=reply_queue,
                command_type=COMMAND_TYPE_RSTOOL_COMMAND,
                payload=payload,
                timeout_seconds=cfg.timeout,
                request_timeout=cfg.request_timeout,
                stop_event=stop_event,
            )
            return _expect_ok(result)

        node_connection = _send_node_command("connection", args=[])
        LOGGER.info("Node connection response: %s", node_connection)

        connect_user = _send_node_command("connect_user", args=[])
        LOGGER.info("connect_user response: %s", connect_user)

        create_result = _send_project_command("create", args=[], session_id=None)
        session = _pick_session(create_result)
        if not session:
            raise RuntimeError("project.create returned no session to continue reconstruction.")
        LOGGER.info("Created RS session: %s", session)

        _send_project_command("new_scene", args=[], session_id=session)
        project_settings = [
            ["appIncSubdirs=true"],
            ["sfmCameraPriorAccuracyX=0.1"],
            ["sfmCameraPriorAccuracyY=0.1"],
            ["sfmCameraPriorAccuracyZ=0.1"],
            ["sfmCameraPriorAccuracyYaw=1"],
            ["sfmCameraPriorAccuracyPitch=1"],
            ["sfmDetectorSensitivity=Ultra"],
            ["sfmCameraPriorAccuracyRoll=1"],
        ]
        for params in project_settings:
            _send_project_command("command", ["set", params], session_id=session)

        imagery_path = str(imagery_dir.resolve())
        _send_project_command("add_folder", [imagery_path], session_id=session)
        _send_project_command("command", ["align"], session_id=session)
        _send_project_command("command", ["calculateNormalModel"], session_id=session)
        _send_project_command("command", ["calculateOrthoProjection"], session_id=session)
        _send_project_command("save", [cfg.save_path], session_id=session)

        LOGGER.info("Reconstruction command sequence completed successfully. session=%s", session)
        LOGGER.info("Project save target: %s", cfg.save_path)
        LOGGER.info("Imagery source path: %s", imagery_path)
        return 0

    except TimeoutError as exc:
        LOGGER.error("Timed out waiting for command result: %s", exc)
        return 1
    except Exception as exc:
        LOGGER.error("Reconstruction run failed: %s", exc, exc_info=True)
        return 1
    finally:
        redis_client.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _build_logger(args.verbose)
    cfg = ReconConfig(
        redis_url=args.redis_url,
        command_queue=args.command_queue,
        timeout=max(1, int(args.timeout)),
        request_timeout=max(1, int(args.request_timeout)),
        base_url=(args.base_url or "").strip(),
        client_id=(args.client_id or "").strip(),
        app_token=(args.app_token or "").strip(),
        auth_token=(args.auth_token or "").strip(),
        s3_bucket=(args.s3_bucket or "").strip(),
        s3_prefix=(args.s3_prefix or "").strip(),
        s3_download_dir=(args.s3_download_dir or "").strip(),
        s3_max_files=max(0, int(args.s3_max_files)),
        s3_extensions=_safe_exts(args.s3_extensions),
        pull_s3=not args.no_pull_s3,
        imagery_dir=(args.imagery_dir or "").strip(),
        imagery_subdir=(args.imagery_subdir or "Imagery").strip(),
        save_path=(args.save_path or "test_auto.rspj").strip(),
        skip_discover=bool(args.skip_discover),
    )
    return _emit_reconstruction(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
