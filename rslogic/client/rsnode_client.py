"""Redis worker that executes processing jobs against a remote RealityScan node."""

from __future__ import annotations

import argparse
import logging
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Optional

from config import load_config
from rslogic.jobs import RsToolsSdkRunner
from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
    ProcessingCommand,
    ProcessingCommandResult,
    RESULT_STATUS_ERROR,
    RESULT_STATUS_OK,
    RESULT_STATUS_PROGRESS,
    RedisCommandBus,
    RESULT_STATUS_ACCEPTED,
)

logger = logging.getLogger("rslogic.client.rsnode")


def _to_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RsNodeClient:
    """Runs processing commands pulled from Redis and updates RSNode via SDK."""

    def __init__(
        self,
        *,
        command_queue_key: str,
        result_queue_key: str,
        redis_url: str,
        rs_base_url: str,
        rs_client_id: str,
        rs_app_token: str,
        rs_auth_token: str,
        block_timeout_seconds: int = 1,
        result_ttl_seconds: int = 3600,
        worker_count: int = 1,
    ) -> None:
        self._bus = RedisCommandBus(redis_url)
        self._command_queue_key = command_queue_key
        self._result_queue_key = result_queue_key
        self._block_timeout_seconds = max(int(block_timeout_seconds), 1)
        self._result_ttl_seconds = max(int(result_ttl_seconds), 1)
        self._runner = RsToolsSdkRunner(
            base_url=rs_base_url,
            client_id=rs_client_id,
            app_token=rs_app_token,
            auth_token=rs_auth_token,
        )
        self._executor = ThreadPoolExecutor(max_workers=max(1, int(worker_count)))
        self._worker_count = max(1, int(worker_count))
        self._stop_event = threading.Event()

    def close(self) -> None:
        self._stop_event.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._bus.close()

    def _publish(
        self,
        *,
        command: ProcessingCommand,
        status: str,
        message: Optional[str],
        progress: Optional[float],
        data: Optional[Dict[str, Any]],
        error: Optional[str] = None,
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> None:
        payload = ProcessingCommandResult(
            command_id=command.command_id,
            command_type=command.command_type,
            status=status,
            message=message,
            progress=progress,
            data=data,
            error=error,
            started_at=started_at,
            finished_at=finished_at,
        ).to_payload()
        self._bus.push(self._result_queue_key, payload, expire_seconds=self._result_ttl_seconds)
        if command.reply_to:
            self._bus.push(command.reply_to, payload, expire_seconds=self._result_ttl_seconds)

    def _handle_processing_command(self, command: ProcessingCommand) -> None:
        started_at = _to_utc_iso()
        payload = command.payload
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="missing job_id in processing command payload",
                progress=0.0,
                data=None,
                error="missing job_id",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        raw_working_directory = str(payload.get("working_directory") or "").strip()
        if raw_working_directory:
            working_directory = Path(raw_working_directory)
        else:
            working_directory = Path("/tmp") / "rslogic-rsnode-jobs" / job_id
        working_directory.mkdir(parents=True, exist_ok=True)

        image_keys = payload.get("image_keys") if isinstance(payload.get("image_keys"), list) else []
        image_keys = [str(item) for item in image_keys]
        filters = payload.get("filters")
        if not isinstance(filters, dict):
            filters = {}

        def progress_cb(progress: float, message: str, details: Optional[Dict[str, Any]]) -> None:
            self._publish(
                command=command,
                status=RESULT_STATUS_PROGRESS,
                message=message,
                progress=progress,
                data={"job_id": job_id, "details": details or {}},
            )

        try:
            self._publish(
                command=command,
                status=RESULT_STATUS_ACCEPTED,
                message="processing job started",
                progress=10.0,
                data={"job_id": job_id},
                started_at=started_at,
            )
            result = self._runner.run(
                working_directory=working_directory,
                image_keys=image_keys,
                filters=filters,
                job_id=job_id,
                progress_callback=progress_cb,
            )
            self._publish(
                command=command,
                status=RESULT_STATUS_OK,
                message="processing completed",
                progress=100.0,
                data={"job_id": job_id, "result": result},
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
        except Exception as exc:
            error_message = str(exc)
            logger.exception(
                "RSNode client processing failed job_id=%s command_id=%s",
                job_id,
                command.command_id,
            )
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="processing failed",
                progress=100.0,
                data={"job_id": job_id, "error_class": exc.__class__.__name__},
                error=error_message,
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )

    def _dispatch(self, raw: Dict[str, Any]) -> None:
        try:
            command = ProcessingCommand.parse(raw)
        except Exception as exc:
            logger.error("Invalid command payload: %s", exc)
            return

        if command.command_type != COMMAND_TYPE_PROCESSING_JOB:
            logger.warning("Unsupported command type=%s command_id=%s", command.command_type, command.command_id)
            return

        self._executor.submit(self._handle_processing_command, command)

    def process_once(self, timeout_seconds: Optional[int] = None) -> bool:
        timeout = max(int(timeout_seconds or self._block_timeout_seconds), 1)
        raw = self._bus.pop(self._command_queue_key, timeout_seconds=timeout)
        if raw is None:
            return False
        self._dispatch(raw)
        return True

    def run_forever(self) -> None:
        logger.info(
            "RSNode client started command_queue=%s result_queue=%s workers=%s",
            self._command_queue_key,
            self._result_queue_key,
            self._worker_count,
        )
        while not self._stop_event.is_set():
            try:
                self.process_once(self._block_timeout_seconds)
            except Exception as exc:  # pragma: no cover - runtime loop guard
                logger.exception("RSNode client loop error: %s", exc)
                time.sleep(1.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RsLogic RSNode client worker")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    subparsers = parser.add_subparsers(dest="command", required=False)

    run_parser = subparsers.add_parser("run", help="Run RSNode command worker")
    run_parser.add_argument("--workers", type=int, default=1, help="Concurrent worker count")
    run_parser.add_argument("--once", action="store_true", help="Process one command and exit")
    run_parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout seconds for a single pop attempt",
    )

    run_alias = subparsers.add_parser("worker", help="Alias for run")
    run_alias.add_argument("--workers", type=int, default=1, help="Concurrent worker count")
    run_alias.add_argument("--once", action="store_true", help="Process one command and exit")
    run_alias.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout seconds for a single pop attempt",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command not in {None, "run", "worker"}:
        raise SystemExit(f"unsupported command: {args.command}")

    action = args.command or "run"
    workers = getattr(args, "workers", 1)
    timeout = getattr(args, "timeout", None)
    is_once = bool(getattr(args, "once", False))
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    config = load_config()
    client = RsNodeClient(
        command_queue_key=config.control.command_queue_key,
        result_queue_key=config.control.result_queue_key,
        redis_url=config.queue.redis_url,
        rs_base_url=(config.rstools.sdk_base_url or "").strip(),
        rs_client_id=(config.rstools.sdk_client_id or "").strip(),
        rs_app_token=(config.rstools.sdk_app_token or "").strip(),
        rs_auth_token=(config.rstools.sdk_auth_token or "").strip(),
        block_timeout_seconds=config.control.block_timeout_seconds,
        result_ttl_seconds=config.control.result_ttl_seconds,
        worker_count=max(1, int(workers)),
    )
    if action == "run" or action == "worker":
        try:
            if is_once:
                client.process_once(timeout_seconds=timeout)
            else:
                client.run_forever()
        finally:
            client.close()


if __name__ == "__main__":
    main()
