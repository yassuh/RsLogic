"""Redis worker that executes processing jobs against a remote RealityScan node."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import ast
import inspect
import logging
import json
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
from typing import Any, Dict, Optional

from config import load_config
from rslogic.jobs import RsToolsSdkRunner
from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
    COMMAND_TYPE_RSTOOL_COMMAND,
    COMMAND_TYPE_RSTOOL_DISCOVER,
    ProcessingCommand,
    ProcessingCommandResult,
    RESULT_STATUS_ACCEPTED,
    RESULT_STATUS_ERROR,
    RESULT_STATUS_OK,
    RESULT_STATUS_PROGRESS,
    RedisCommandBus,
)

ROOT = Path(__file__).resolve().parents[2]
_SDK_SOURCE = ROOT / "internal_tools" / "rstool-sdk" / "src"
_SDK_NODE_SOURCE = _SDK_SOURCE / "realityscan_sdk" / "resources" / "node.py"
_SDK_PROJECT_SOURCE = _SDK_SOURCE / "realityscan_sdk" / "resources" / "project.py"
if _SDK_SOURCE.exists() and str(_SDK_SOURCE) not in sys.path:
    sys.path.insert(0, str(_SDK_SOURCE))

_sdk_import_error: Optional[str] = None
try:
    from realityscan_sdk import RealityScanClient
    from realityscan_sdk.resources.node import NodeAPI
    from realityscan_sdk.resources.project import ProjectAPI
except Exception as exc:  # pragma: no cover - optional dependency until installed
    _sdk_import_error = f"{type(exc).__name__}: {exc}"
    RealityScanClient = None  # type: ignore[assignment]
    NodeAPI = None  # type: ignore[assignment]
    ProjectAPI = None  # type: ignore[assignment]


logger = logging.getLogger("rslogic.client.rsnode")


def _to_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mask_secret(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return "MISSING"
    if len(value) <= 8:
        return "********"
    return f"{value[:3]}...{value[-3:]}"


def _collect_missing_sdk_env(
    *,
    base_url: str,
    client_id: str,
    app_token: str,
    auth_token: str,
) -> list[str]:
    missing: list[str] = []
    if not base_url:
        missing.append("RSLOGIC_RSTOOLS_SDK_BASE_URL")
    if not client_id:
        missing.append("RSLOGIC_RSTOOLS_SDK_CLIENT_ID")
    if not app_token:
        missing.append("RSLOGIC_RSTOOLS_SDK_APP_TOKEN")
    if not auth_token:
        missing.append("RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN")
    return missing


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
        self._rs_base_url = rs_base_url
        self._rs_client_id = rs_client_id
        self._rs_app_token = rs_app_token
        self._rs_auth_token = rs_auth_token
        self._runner = RsToolsSdkRunner(
            base_url=rs_base_url,
            client_id=rs_client_id,
            app_token=rs_app_token,
            auth_token=rs_auth_token,
        )
        self._executor = ThreadPoolExecutor(max_workers=max(1, int(worker_count)))
        self._worker_count = max(1, int(worker_count))
        self._stop_event = threading.Event()
        self._sdk_target_methods = self._load_sdk_target_methods()

    @staticmethod
    def _normalize_command_payload(value: Any) -> Any:
        if dataclasses.is_dataclass(value):
            return dataclasses.asdict(value)
        if isinstance(value, bytes):
            return {
                "type": "bytes",
                "encoding": "base64",
                "value": base64.b64encode(value).decode("ascii"),
            }
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, (list, tuple)):
            return [RsNodeClient._normalize_command_payload(item) for item in value]
        if isinstance(value, dict):
            return {str(key): RsNodeClient._normalize_command_payload(item) for key, item in value.items()}
        try:
            return json.loads(json.dumps(value))
        except Exception:
            return str(value)

    @staticmethod
    def _resolve_sdk_attribute(target_obj: Any, path: str) -> Any:
        current: Any = target_obj
        parts = [part.strip() for part in str(path).split(".") if part.strip()]
        if not parts:
            raise ValueError("path is empty")
        for part in parts:
            if part.startswith("_"):
                raise ValueError(f"private attribute access is blocked: {part}")
            if not hasattr(current, part):
                raise ValueError(f"Path '{path}' was not found on resolved target")
            current = getattr(current, part)
        return current

    @staticmethod
    def _resolve_sdk_callable(target_obj: Any, method_path: str) -> Any:
        resolved = RsNodeClient._resolve_sdk_attribute(target_obj, method_path)
        if not callable(resolved):
            raise ValueError(f"Resolved method '{method_path}' is not callable")
        return resolved

    @staticmethod
    def _load_sdk_target_methods() -> Dict[str, Dict[str, Dict[str, str]]]:
        methods: Dict[str, Dict[str, Dict[str, str]]] = {"node": {}, "project": {}}
        target_map: Dict[str, Any] = {
            "node": NodeAPI,
            "project": ProjectAPI,
        }
        for target, api_cls in target_map.items():
            if api_cls is None:
                source_methods = RsNodeClient._load_sdk_methods_from_source(target)
                if source_methods:
                    methods[target] = source_methods
                continue
            for name, member in inspect.getmembers(api_cls, predicate=inspect.isfunction):
                if name.startswith("_"):
                    continue
                try:
                    signature = str(inspect.signature(member))
                except (TypeError, ValueError):
                    signature = "(...)"
                methods[target][name] = {
                    "signature": signature,
                    "doc": (inspect.getdoc(member) or "").strip(),
                }
            if not methods[target]:
                source_methods = RsNodeClient._load_sdk_methods_from_source(target)
                if source_methods:
                    methods[target] = source_methods
        return methods

    @staticmethod
    def _load_sdk_methods_from_source(target: str) -> Dict[str, Dict[str, str]]:
        if target == "node":
            source_file = _SDK_NODE_SOURCE
            class_name = "NodeAPI"
        elif target == "project":
            source_file = _SDK_PROJECT_SOURCE
            class_name = "ProjectAPI"
        else:
            return {}

        if not source_file.exists():
            return {}

        try:
            source_text = source_file.read_text(encoding="utf-8")
            source_tree = ast.parse(source_text)
        except Exception:
            return {}

        target_class: Optional[ast.ClassDef] = None
        for child in source_tree.body:
            if isinstance(child, ast.ClassDef) and child.name == class_name:
                target_class = child
                break
        if target_class is None:
            return {}

        method_signatures: Dict[str, str] = {}
        method_docs: Dict[str, str] = {}

        for node in target_class.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name.startswith("_"):
                    continue
                try:
                    method_signatures[node.name] = f"({ast.unparse(node.args)})"
                except Exception:
                    method_signatures[node.name] = "(...)"
                method_docs[node.name] = (ast.get_docstring(node) or "").strip()

            if isinstance(node, ast.Assign):
                if not isinstance(node.value, (ast.Name, ast.Attribute)):
                    continue
                alias_target = ast.unparse(node.value)
                for target_node in node.targets:
                    if not isinstance(target_node, ast.Name):
                        continue
                    alias_name = target_node.id
                    if alias_name.startswith("_"):
                        continue
                    source_signature = method_signatures.get(alias_target)
                    if source_signature is None and isinstance(node.value, ast.Name):
                        source_signature = method_signatures.get(node.value.id)
                    method_signatures[alias_name] = source_signature or "(...)"
                    method_docs[alias_name] = f"Alias for {alias_target}" if not method_docs.get(alias_name) else method_docs[alias_name]

        methods: Dict[str, Dict[str, str]] = {}
        for name in sorted(method_signatures):
            methods[name] = {
                "signature": method_signatures[name],
                "doc": method_docs.get(name, ""),
            }
        return methods

    @staticmethod
    def _ensure_sdk_client_config(*, base_url: str, client_id: str, app_token: str, auth_token: str) -> None:
        missing: list[str] = []
        if not base_url:
            missing.append("base_url")
        if not client_id:
            missing.append("client_id")
        if not app_token:
            missing.append("app_token")
        if not auth_token:
            missing.append("auth_token")
        if missing:
            raise ValueError(f"missing sdk configuration: {', '.join(missing)}")

    def _build_realityscan_client(self, payload: Dict[str, Any]) -> RealityScanClient:
        base_url = str(payload.get("base_url") or self._rs_base_url).strip()
        client_id = str(payload.get("client_id") or self._rs_client_id).strip()
        app_token = str(payload.get("app_token") or self._rs_app_token).strip()
        auth_token = str(payload.get("auth_token") or self._rs_auth_token).strip()
        self._ensure_sdk_client_config(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            auth_token=auth_token,
        )
        return RealityScanClient(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            auth_token=auth_token,
        )

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

    def _handle_sdk_discovery(self, command: ProcessingCommand) -> None:
        started_at = _to_utc_iso()
        payload = command.payload
        target_request = str(payload.get("target") or "").strip().lower()
        requested_targets = []
        if target_request:
            requested_targets.append(target_request)
        else:
            requested_targets.extend(["node", "project"])
        unsupported_targets = [t for t in requested_targets if t not in {"node", "project"}]
        if unsupported_targets:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid target for rstool_sdk.discover",
                progress=0.0,
                data={"requested_targets": requested_targets},
                error=f"target must be node or project; got {unsupported_targets}",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        data: Dict[str, Any] = {
            "available": {},
            "sdk_available": bool(_sdk_import_error is None),
        }
        if _sdk_import_error:
            data["import_error"] = _sdk_import_error
        for target in requested_targets:
            if target == "node":
                methods = dict(sorted(self._sdk_target_methods["node"].items()))
            else:
                methods = dict(sorted(self._sdk_target_methods["project"].items()))
            data["available"][target] = methods

        if data["sdk_available"] is False:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="SDK not available",
                progress=0.0,
                data=data,
                error="realityscan_sdk could not be imported",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        self._publish(
            command=command,
            status=RESULT_STATUS_OK,
            message="discovered rstool commands",
            progress=100.0,
            data=data,
            started_at=started_at,
            finished_at=_to_utc_iso(),
        )

    def _handle_rstool_command(self, command: ProcessingCommand) -> None:
        started_at = _to_utc_iso()
        payload = command.payload
        if RealityScanClient is None:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="RealityScan SDK not available",
                progress=0.0,
                data={"command_id": command.command_id},
                error="realityscan_sdk package is not importable",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        target = str(payload.get("target") or "node").strip().lower()
        target_object = str(payload.get("target_object") or "").strip() or None
        method_name = str(payload.get("method") or "").strip()
        if target not in {"node", "project", "client"}:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid target",
                progress=0.0,
                data={"command_id": command.command_id},
                error="target must be `node`, `project`, or `client`",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return
        if not method_name:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="missing method",
                progress=0.0,
                data={"command_id": command.command_id},
                error="payload requires `method`",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        args = payload.get("args")
        kwargs = payload.get("kwargs")
        if args is None:
            args = []
        if not isinstance(args, list):
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid args",
                progress=0.0,
                data={"method": method_name},
                error="`args` must be an array",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return
        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid kwargs",
                progress=0.0,
                data={"method": method_name},
                error="`kwargs` must be an object",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        session_action = str(payload.get("session_action") or "").strip().lower()
        session_id = payload.get("session") if payload.get("session") is not None else payload.get("session_id")
        try:
            with self._build_realityscan_client(payload) as client:
                if session_id is not None:
                    client.session = str(session_id)

                if target == "node":
                    command_root = client.node
                elif target == "project":
                    command_root = client.project
                else:
                    command_root = client

                if target == "project":
                    if session_action == "open":
                        guid = payload.get("project_guid") or payload.get("guid") or payload.get("session_key")
                        project_name = payload.get("project_name")
                        if not guid:
                            raise ValueError("`session_action=open` requires `project_guid`")
                        client.project.open(str(guid), name=project_name)
                    elif session_action == "create":
                        client.project.create()
                    elif session_action == "disconnect":
                        client.project.disconnect()

                if target_object:
                    base_target = RsNodeClient._resolve_sdk_attribute(command_root, target_object)
                else:
                    base_target = command_root

                method = self._resolve_sdk_callable(base_target, method_name)

                self._publish(
                    command=command,
                    status=RESULT_STATUS_ACCEPTED,
                    message="rstool sdk command started",
                    progress=15.0,
                    data={"target": target, "method": method_name},
                    started_at=started_at,
                )

                result = method(*args, **kwargs)
                normalized = self._normalize_command_payload(result)
                self._publish(
                    command=command,
                    status=RESULT_STATUS_OK,
                    message="rstool sdk command completed",
                    progress=100.0,
                    data={
                        "target": target,
                        "method": method_name,
                        "result": normalized,
                    },
                    started_at=started_at,
                    finished_at=_to_utc_iso(),
                )
        except Exception as exc:
            logger.exception(
                "RSNode SDK command failed command_id=%s target=%s method=%s",
                command.command_id,
                target,
                method_name,
            )
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="rstool sdk command failed",
                progress=100.0,
                data={
                    "target": target,
                    "method": method_name,
                    "target_object": target_object,
                    "error_class": exc.__class__.__name__,
                },
                error=str(exc),
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )

    def _dispatch(self, raw: Dict[str, Any]) -> None:
        try:
            command = ProcessingCommand.parse(raw)
        except Exception as exc:
            logger.error("Invalid command payload: %s", exc)
            return

        if command.command_type == COMMAND_TYPE_PROCESSING_JOB:
            self._executor.submit(self._handle_processing_command, command)
            return
        if command.command_type == COMMAND_TYPE_RSTOOL_DISCOVER:
            self._executor.submit(self._handle_sdk_discovery, command)
            return
        if command.command_type == COMMAND_TYPE_RSTOOL_COMMAND:
            self._executor.submit(self._handle_rstool_command, command)
            return

        logger.warning("Unsupported command type=%s command_id=%s", command.command_type, command.command_id)

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
    redis_url = config.queue.redis_url
    rs_base_url = (config.rstools.sdk_base_url or "").strip()
    rs_client_id = (config.rstools.sdk_client_id or "").strip()
    rs_app_token = (config.rstools.sdk_app_token or "").strip()
    rs_auth_token = (config.rstools.sdk_auth_token or "").strip()

    logger.info("RSNode client startup config redis_url=%s", redis_url)
    logger.info(
        "RSNode SDK env: base_url=%s client_id=%s app_token=%s auth_token=%s",
        _mask_secret(rs_base_url),
        _mask_secret(rs_client_id),
        _mask_secret(rs_app_token),
        _mask_secret(rs_auth_token),
    )

    missing_sdk_env = _collect_missing_sdk_env(
        base_url=rs_base_url,
        client_id=rs_client_id,
        app_token=rs_app_token,
        auth_token=rs_auth_token,
    )
    if missing_sdk_env:
        raise SystemExit(
            "Missing required SDK environment variables for rslogic rsnode client startup: "
            + ", ".join(missing_sdk_env)
        )

    logger.info("RSNode client startup: pinging Redis control queue")
    bus: Optional[RedisCommandBus] = None
    try:
        bus = RedisCommandBus(redis_url)
        bus.ping()
    except Exception as exc:
        raise SystemExit(f"Redis ping failed for {redis_url}: {exc}") from exc
    finally:
        if bus is not None:
            bus.close()

    client = RsNodeClient(
        command_queue_key=config.control.command_queue_key,
        result_queue_key=config.control.result_queue_key,
        redis_url=redis_url,
        rs_base_url=rs_base_url,
        rs_client_id=rs_client_id,
        rs_app_token=rs_app_token,
        rs_auth_token=rs_auth_token,
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
