from __future__ import annotations

import json
import logging
import os
import shlex
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from rslogic.client.config import ClientConfig, load_client_config
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


SDK_PATH = Path(__file__).resolve().parents[2] / "internal_tools" / "rstool-sdk" / "src"
if SDK_PATH.exists():
    source = str(SDK_PATH)
    if source not in sys.path:
        sys.path.insert(0, source)

try:
    from realityscan_sdk import RealityScanClient
except Exception:  # pragma: no cover - optional dependency
    RealityScanClient = None  # type: ignore[assignment]


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def _coerce_str(value: Any, *, default: str) -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed


def _coerce_args(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _coerce_kwargs(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(val) for key, val in value.items()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return [_to_jsonable(item) for item in value]
    if hasattr(value, "model_dump"):
        with suppress(Exception):
            return _to_jsonable(value.model_dump())
    if hasattr(value, "__dict__"):
        with suppress(Exception):
            return {key: _to_jsonable(val) for key, val in value.__dict__.items()}
    return str(value)


def _validate_public_name(name: str) -> str:
    clean = str(name).strip()
    if not clean or clean.startswith("_"):
        raise ValueError(f"invalid private method segment: {clean}")
    return clean


@dataclass
class _CommandPlanStep:
    target: str = "project"
    method: str = "command"
    command_name: str = ""
    params: list[Any] | None = None


class RsNodeSupervisor:
    def __init__(self, executable: str, args: list[str], restart_delay_seconds: float) -> None:
        self._executable = Path(executable)
        self._args = args
        self._restart_delay_seconds = restart_delay_seconds
        self._process: Optional[subprocess.Popen[bytes]] = None
        self._lock = threading.Lock()

    def is_running(self) -> bool:
        process = self._process
        return process is not None and process.poll() is None

    @property
    def pid(self) -> int | None:
        if self._process is None:
            return None
        return self._process.pid

    def start(self, *, restart: bool = False) -> None:
        with self._lock:
            if self.is_running():
                return
            if restart:
                self._cleanup()
            if not self._executable.exists():
                logging.error("RSNode executable missing: %s", self._executable)
                return
            command = [str(self._executable), *self._args]
            self._process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
            )
            logging.info("RSNode started pid=%s", self._process.pid)

    def ensure_running(self) -> None:
        if self.is_running():
            return
        if self._process is not None and self._process.poll() is not None:
            self._process = None
        if self._restart_delay_seconds > 0:
            time.sleep(self._restart_delay_seconds)
        self.start(restart=True)

    def stop(self) -> None:
        with self._lock:
            self._cleanup()

    def _cleanup(self) -> None:
        if self._process is None:
            return
        process = self._process
        self._process = None
        if process.poll() is None:
            with suppress(Exception):
                process.terminate()
            with suppress(Exception):
                process.wait(timeout=5)
            if process.poll() is None:
                with suppress(Exception):
                    process.kill()


class RsNodeClient:
    def __init__(self, config: ClientConfig) -> None:
        self._config = config
        self._bus = RedisCommandBus(config.redis_url)
        self._executor = ThreadPoolExecutor(max_workers=config.max_workers, thread_name_prefix="rslogic-client")
        self._rsnode = RsNodeSupervisor(
            executable=config.rsnode_executable,
            args=config.rsnode_args,
            restart_delay_seconds=config.rsnode_restart_delay_seconds,
        )
        self._active_commands: set[str] = set()
        self._command_lock = threading.Lock()
        self._running = True
        self._logger = logging.getLogger("rslogic.client")

    def run(self) -> None:
        self._logger.info("starting client_id=%s", self._config.client_id)
        self._bus.ping()
        self._rsnode.start(restart=True)
        heartbeat_deadline = 0.0
        while self._running:
            try:
                now = time.monotonic()
                if now >= heartbeat_deadline:
                    self._heartbeat()
                    heartbeat_deadline = now + self._config.heartbeat_interval_seconds
                self._rsnode.ensure_running()
                payload = self._bus.pop(self._config.command_queue_key, timeout_seconds=self._config.poll_timeout_seconds)
                if payload is None:
                    continue
                self._handle_raw_command(payload)
            except RuntimeError as exc:
                self._logger.error("redis error: %s", exc)
                time.sleep(1.0)
                self._bus = RedisCommandBus(self._config.redis_url)
            except KeyboardInterrupt:
                self._running = False
            except Exception:
                self._logger.exception("client loop failure")

        self._rsnode.stop()

    def stop(self) -> None:
        self._running = False
        self._rsnode.stop()
        self._bus.close()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _handle_raw_command(self, raw: Dict[str, Any]) -> None:
        try:
            command = ProcessingCommand.parse(raw)
        except Exception as exc:
            self._logger.warning("invalid command payload: %s", exc)
            return

        with self._command_lock:
            if command.command_id in self._active_commands:
                self._logger.debug("duplicate command skipped: %s", command.command_id)
                return
            if not self._is_for_this_client(command):
                self._logger.debug("command %s not for this client", command.command_id)
                return
            self._active_commands.add(command.command_id)

        self._publish_result(command, RESULT_STATUS_ACCEPTED, "accepted")
        self._executor.submit(self._run_command_async, command)

    def _run_command_async(self, command: ProcessingCommand) -> None:
        payload = dict(command.payload)
        reply_to = command.reply_to
        started_at = _utc_now_iso()
        try:
            if command.command_type == COMMAND_TYPE_RSTOOL_DISCOVER:
                status = self._handle_discover()
                self._publish_result(
                    command,
                    RESULT_STATUS_OK,
                    "discover complete",
                    data=status,
                    progress=100.0,
                    reply_to=reply_to,
                    started_at=started_at,
                )
                return

            if command.command_type == COMMAND_TYPE_RSTOOL_COMMAND:
                self._publish_result(command, RESULT_STATUS_PROGRESS, "executing rstool command", progress=20.0, reply_to=reply_to)
                result = self._handle_rstool_command(payload, publish_progress=self._mk_progress_publisher(command, reply_to))
                self._publish_result(
                    command,
                    RESULT_STATUS_OK,
                    "rstool command completed",
                    progress=100.0,
                    data={"result": result},
                    reply_to=reply_to,
                    started_at=started_at,
                )
                return

            if command.command_type == COMMAND_TYPE_PROCESSING_JOB:
                self._publish_result(command, RESULT_STATUS_PROGRESS, "starting processing job", progress=10.0, reply_to=reply_to)
                result = self._handle_processing_job(
                    payload,
                    publish_progress=self._mk_progress_publisher(command, reply_to),
                )
                self._publish_result(
                    command,
                    RESULT_STATUS_OK,
                    "processing job complete",
                    progress=100.0,
                    data=result,
                    reply_to=reply_to,
                    started_at=started_at,
                )
                return

            if self._config.allow_shell_fallback:
                command_payload = payload.get("command")
                if command_payload is None:
                    raise ValueError("shell command missing payload['command']")
                self._publish_result(command, RESULT_STATUS_PROGRESS, "running fallback command", progress=35.0, reply_to=reply_to)
                shell_result = self._execute_shell_command(command_payload)
                self._publish_result(
                    command,
                    RESULT_STATUS_OK,
                    "fallback command complete",
                    progress=100.0,
                    data=shell_result,
                    reply_to=reply_to,
                    started_at=started_at,
                )
                return

            raise ValueError(f"unsupported command_type={command.command_type}")
        except Exception as exc:
            self._publish_result(
                command,
                RESULT_STATUS_ERROR,
                "command execution failed",
                error=str(exc),
                reply_to=reply_to,
                progress=100.0,
            )
        finally:
            with self._command_lock:
                self._active_commands.discard(command.command_id)

    def _handle_discover(self) -> dict[str, Any]:
        status = {
            "client_id": self._config.client_id,
            "rsnode_running": self._rsnode.is_running(),
            "rsnode_pid": self._rsnode.pid,
            "sdk_available": RealityScanClient is not None,
            "hostname": socket.gethostname(),
        }
        if RealityScanClient is None:
            status["sdk_status"] = "missing_realityscan_sdk"
            return status
        try:
            with RealityScanClient(
                base_url=self._config.sdk_base_url or "http://localhost:8000",
                client_id=self._config.sdk_client_id or "",
                app_token=self._config.sdk_app_token or "",
                auth_token=self._config.sdk_auth_token or "",
            ) as client:
                status["sdk_node_status"] = _to_jsonable(self._safe_call(
                    lambda: client.node.status(),
                ))
                status["sdk_project_status"] = _to_jsonable(self._safe_call(
                    lambda: client.project.status(),
                ))
        except Exception as exc:
            status["sdk_status"] = f"runtime_error: {exc}"
        return status

    def _handle_processing_job(
        self,
        payload: Dict[str, Any],
        *,
        publish_progress: Optional[callable] = None,
    ) -> dict[str, Any]:
        filters_raw = payload.get("filters")
        filters = filters_raw if isinstance(filters_raw, dict) else {}
        working_directory = Path(_coerce_str(payload.get("working_directory"), default=str(Path.cwd())))
        if not working_directory.exists():
            working_directory.mkdir(parents=True, exist_ok=True)
        if not isinstance(payload.get("image_keys"), Iterable):
            image_count = 0
        else:
            image_count = len([item for item in payload.get("image_keys", []) if str(item).strip()])

        plan = self._build_processing_plan(filters, working_directory)
        custom_plan = payload.get("command_plan")
        if isinstance(custom_plan, list):
            plan = custom_plan or plan

        if RealityScanClient is None:
            raise RuntimeError("realityscan_sdk is not available on this client machine")

        with RealityScanClient(
            base_url=_coerce_str(self._config.sdk_base_url, default="http://localhost:8000"),
            client_id=self._config.sdk_client_id or "",
            app_token=self._config.sdk_app_token or "",
            auth_token=self._config.sdk_auth_token or "",
        ) as client:
            self._ensure_sdk_connected(client)
            executed: list[dict[str, Any]] = []
            total = max(len(plan), 1)
            for index, step in enumerate(plan, start=1):
                normalized = self._normalize_plan_step(step)
                if publish_progress is not None:
                    publish_progress(
                        10 + int(75 * index / total),
                        f"processing step {index}/{total}: {normalized.command_name or normalized.method}",
                        normalized.__dict__,
                    )
                command_response = self._execute_plan_step(client, normalized)
                executed.append(command_response)

            node_status = _to_jsonable(self._safe_call(client.node.status))
            project_status = _to_jsonable(self._safe_call(client.project.status))
            return {
                "status": "completed",
                "job_id": _coerce_str(payload.get("job_id"), default=None) or None,
                "working_directory": str(working_directory),
                "image_count": image_count,
                "filters": filters,
                "executed_commands": executed,
                "node_status": node_status,
                "project_status": project_status,
            }

    def _handle_rstool_command(
        self,
        payload: Dict[str, Any],
        *,
        publish_progress: Optional[callable] = None,
    ) -> Dict[str, Any]:
        if RealityScanClient is None:
            raise RuntimeError("realityscan_sdk is not available on this client machine")
        with RealityScanClient(
            base_url=_coerce_str(self._config.sdk_base_url, default="http://localhost:8000"),
            client_id=self._config.sdk_client_id or "",
            app_token=self._config.sdk_app_token or "",
            auth_token=self._config.sdk_auth_token or "",
        ) as client:
            self._ensure_sdk_connected(client)
            if payload.get("command_plan"):
                plan = payload["command_plan"]
                if not isinstance(plan, list):
                    raise ValueError("command_plan must be a list")
                responses: list[Any] = []
                for step in plan:
                    normalized = self._normalize_plan_step(step)
                    responses.append(self._execute_plan_step(client, normalized))
                return {"status": "processed", "results": responses}
            method = payload.get("method")
            if method is None:
                raise ValueError("rstool command requires payload['method']")
            response = self._call_sdk_target(
                client,
                target=_coerce_str(payload.get("target"), default="client"),
                target_object=payload.get("target_object"),
                method=method,
                args=_coerce_args(payload.get("args")),
                kwargs=_coerce_kwargs(payload.get("kwargs")),
            )
            if publish_progress is not None:
                publish_progress(90, "rstool command executed", {"method": method})
            return {"result": response}

    def _is_for_this_client(self, command: ProcessingCommand) -> bool:
        target_client = command.payload.get("target_client_id") or command.payload.get("client_id")
        target_tags = command.payload.get("target_client_tags")
        if target_client:
            return str(target_client) == self._config.client_id
        if target_tags is None:
            return True
        if isinstance(target_tags, str):
            target_tags = [part.strip() for part in target_tags.split(",") if part.strip()]
        if isinstance(target_tags, (list, tuple, set)):
            return bool(set(map(str, target_tags)) & set(self._config.client_tags))
        return True

    def _normalize_plan_step(self, raw: Any) -> _CommandPlanStep:
        if isinstance(raw, dict):
            if "target" in raw:
                target = _coerce_str(raw.get("target"), default="project")
            else:
                target = "project"
            if "method" in raw:
                method = _coerce_str(raw.get("method"), default="command")
            else:
                method = "command"
            if "command_name" in raw:
                command_name = _coerce_str(raw.get("command_name"), default="")
            elif "command" in raw:
                command_name = _coerce_str(raw.get("command"), default="")
            else:
                command_name = ""
            params = _coerce_args(raw.get("params"))
            return _CommandPlanStep(target=target, method=method, command_name=command_name, params=params)
        if isinstance(raw, (list, tuple)):
            items = list(raw)
            if not items:
                raise ValueError("empty plan step")
            command_name = _coerce_str(items[0], default="")
            params = _coerce_args(items[1] if len(items) > 1 else [])
            return _CommandPlanStep(command_name=command_name, params=params)
        if isinstance(raw, str):
            return _CommandPlanStep(command_name=raw)
        raise ValueError(f"unsupported plan step type: {type(raw)}")

    def _execute_plan_step(self, client: Any, step: _CommandPlanStep) -> Dict[str, Any]:
        args = _coerce_args(step.params)
        if step.command_name:
            response = self._call_sdk_target(
                client,
                target=step.target,
                target_object=None,
                method=step.method,
                args=[step.command_name],
                kwargs={"params": args} if step.method == "command" else {},
            )
        else:
            response = self._call_sdk_target(
                client,
                target=step.target,
                target_object=None,
                method=step.method,
                args=args,
            )
        return {
            "target": step.target,
            "method": step.method,
            "command_name": step.command_name,
            "params": args,
            "result": response,
        }

    @staticmethod
    def _build_processing_plan(filters: Dict[str, Any], working_directory: Path) -> list[dict[str, Any]]:
        imagery_folder = _coerce_str(filters.get("sdk_imagery_folder"), default="Imagery")
        project_path = _coerce_str(filters.get("sdk_project_path"), default=f"{working_directory.name}.rspj")
        include_subdirs = _coerce_bool(filters.get("sdk_include_subdirs"), default=True)
        detector_sensitivity = _coerce_str(filters.get("sdk_detector_sensitivity"), default="Ultra")
        camera_accuracy_xyz = _coerce_float(filters.get("sdk_camera_prior_accuracy_xyz"), default=0.1)
        camera_accuracy_ypr = _coerce_float(filters.get("sdk_camera_prior_accuracy_yaw_pitch_roll"), default=1.0)
        run_align = _coerce_bool(filters.get("sdk_run_align"), default=True)
        run_normal = _coerce_bool(filters.get("sdk_run_normal_model"), default=True)
        run_ortho = _coerce_bool(filters.get("sdk_run_ortho_projection"), default=True)
        stage_only = _coerce_bool(filters.get("stage_only"), default=False)

        steps: list[dict[str, Any]] = [
            {"target": "project", "method": "command", "command": "newScene"},
            {"target": "project", "method": "command", "command": "set", "params": [f"appIncSubdirs={'true' if include_subdirs else 'false'}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmCameraPriorAccuracyX={camera_accuracy_xyz}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmCameraPriorAccuracyY={camera_accuracy_xyz}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmCameraPriorAccuracyZ={camera_accuracy_xyz}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmCameraPriorAccuracyYaw={camera_accuracy_ypr}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmCameraPriorAccuracyPitch={camera_accuracy_ypr}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmCameraPriorAccuracyRoll={camera_accuracy_ypr}"]},
            {"target": "project", "method": "command", "command": "set", "params": [f"sfmDetectorSensitivity={detector_sensitivity}"]},
            {"target": "project", "method": "command", "command": "addFolder", "params": [imagery_folder]},
        ]
        if not stage_only:
            if run_align:
                steps.append({"target": "project", "method": "command", "command": "align"})
            if run_normal:
                steps.append({"target": "project", "method": "command", "command": "calculateNormalModel"})
            if run_ortho:
                steps.append({"target": "project", "method": "command", "command": "calculateOrthoProjection"})
            if project_path:
                steps.append({"target": "project", "method": "command", "command": "save", "params": [project_path]})
        return steps

    def _call_sdk_target(
        self,
        client: Any,
        *,
        target: str,
        target_object: Any,
        method: str,
        args: list[Any],
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> dict[str, Any]:
        target = target.strip().lower()
        if target == "node":
            root = client.node
        elif target == "project":
            root = client.project
        elif target == "client":
            root = client
        else:
            raise ValueError(f"unsupported target '{target}'")

        current = root
        if target_object not in (None, "", []):
            object_parts = str(target_object).split(".")
            for raw_part in object_parts:
                part = _validate_public_name(raw_part)
                current = getattr(current, part)
        if not method:
            raise ValueError("method is required")
        method_parts = str(method).split(".")
        for index, raw_part in enumerate(method_parts):
            part = _validate_public_name(raw_part)
            current = getattr(current, part)
            if index < len(method_parts) - 1:
                if current is None:
                    raise AttributeError(f"missing intermediate target: {part}")

        if not callable(current):
            return _to_jsonable(current)
        kwargs_payload = _coerce_kwargs(kwargs)
        result = current(*args, **kwargs_payload)
        return _to_jsonable(result)

    @staticmethod
    def _safe_call(func: Callable[[], Any]) -> Any:
        with suppress(Exception):
            return func()
        return {"error": "runtime_failed"}

    def _ensure_sdk_connected(self, client: Any) -> None:
        with suppress(Exception):
            client.node.connect_user()

    def _publish_result(
        self,
        command: ProcessingCommand,
        status: str,
        message: str,
        *,
        progress: float | None = None,
        data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        reply_to: Optional[str] = None,
        started_at: Optional[str] = None,
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
            finished_at=_utc_now_iso(),
        ).to_payload()
        if self._config.client_tags:
            payload["client_tags"] = list(self._config.client_tags)
        payload["source"] = "rslogic.client"
        self._bus.push(self._config.result_queue_key, payload, expire_seconds=self._config.result_ttl_seconds)
        if reply_to:
            self._bus.push(reply_to, payload, expire_seconds=self._config.result_ttl_seconds)

    def _mk_progress_publisher(self, command: ProcessingCommand, reply_to: Optional[str]):
        if reply_to is None:
            def _no_emit(progress: int, message: str, data: Optional[Dict[str, Any]]) -> None:
                self._publish_result(
                    command,
                    RESULT_STATUS_PROGRESS,
                    message,
                    progress=float(progress),
                    data=data,
                    reply_to=None,
                )

            return _no_emit

        def _emit(progress: int, message: str, data: Optional[Dict[str, Any]] = None) -> None:
            self._publish_result(
                command,
                RESULT_STATUS_PROGRESS,
                message,
                progress=float(progress),
                data=data,
                reply_to=reply_to,
            )

        return _emit

    def _execute_shell_command(self, raw_command: Any) -> Dict[str, Any]:
        if isinstance(raw_command, str):
            cmd = raw_command.strip()
            if not cmd:
                raise ValueError("empty command")
            shell = True
            args = cmd
        elif isinstance(raw_command, list):
            args = " ".join(shlex.quote(str(part)) for part in raw_command)
            shell = True
        else:
            raise TypeError("command payload must be string or list")

        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            shell=shell,
            timeout=3600,
        )
        return {
            "returncode": result.returncode,
            "stdout": (result.stdout or "")[-4000:],
            "stderr": (result.stderr or "")[-4000:],
        }

    def _heartbeat(self) -> None:
        payload = {
            "type": "client_status",
            "client_id": self._config.client_id,
            "client_tags": self._config.client_tags,
            "online": self._running,
            "rsnode_running": self._rsnode.is_running(),
            "rsnode_pid": self._rsnode.pid,
            "poll_seconds": self._config.poll_timeout_seconds,
            "max_workers": self._config.max_workers,
            "allow_shell_fallback": self._config.allow_shell_fallback,
        }
        self._bus.set_presence(
            self._config.status_queue_key,
            payload,
            ttl_seconds=self._config.heartbeat_ttl_seconds,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_client_config()
    client = RsNodeClient(config)
    try:
        client.run()
    finally:
        client.stop()


if __name__ == "__main__":
    main()
