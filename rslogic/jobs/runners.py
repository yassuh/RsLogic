"""Wrappers for different RealityScan execution strategies."""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, TypeAlias
from uuid import uuid4

from datetime import datetime, timezone

from config import RsToolsConfig, load_config
from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
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
if _SDK_SOURCE.exists() and str(_SDK_SOURCE) not in sys.path:
    sys.path.insert(0, str(_SDK_SOURCE))

try:
    from realityscan_sdk import RealityScanClient
except Exception:  # pragma: no cover - optional dependency
    RealityScanClient = None

logger = logging.getLogger("rslogic.jobs.runners")

ProgressCallback: TypeAlias = Callable[[float, str, Optional[Dict[str, Any]]], None]


class RsToolsRunner:
    """Adapter interface for launching RealityScan jobs."""

    def run(
        self,
        working_directory: Path,
        image_keys: Sequence[str],
        filters: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError


class StubRsToolsRunner(RsToolsRunner):
    def run(
        self,
        working_directory: Path,
        image_keys: Sequence[str],
        filters: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        return {
            "status": "simulated",
            "message": "RsTools runner not configured; this is a dry-run placeholder.",
            "job_id": job_id,
            "selected_images": len(image_keys),
            "filters": filters,
        }


class SubprocessRsToolsRunner(RsToolsRunner):
    """Run an external RsTools CLI if an executable is provided."""

    def __init__(self, executable: str) -> None:
        self._executable = executable

    def run(
        self,
        working_directory: Path,
        image_keys: Sequence[str],
        filters: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        args = [self._executable, "process", "--working-dir", str(working_directory)]
        for key in image_keys:
            args.extend(["--image", key])
        if filters:
            for key, value in sorted(filters.items()):
                if value is None:
                    continue
                args.extend([f"--filter-{key}", str(value)])

        process = subprocess.run(args, check=False, capture_output=True, text=True)
        if process.returncode != 0:
            raise RuntimeError(process.stderr or "RsTools CLI returned a non-zero exit code")

        return {
            "status": "completed",
            "message": process.stdout[:2000],
            "job_id": job_id,
            "selected_images": len(image_keys),
            "filters": filters,
        }


class RsToolsSdkRunner(RsToolsRunner):
    """Run via the official RealityScan Python SDK when installed."""

    _SUCCESS_STATES = {"finished", "completed", "success", "succeeded", "done", "ready"}
    _FAIL_STATES = {"failed", "error", "aborted", "cancelled", "canceled"}

    def __init__(
        self,
        base_url: str,
        *,
        client_id: str,
        app_token: str,
        auth_token: str,
    ) -> None:
        if RealityScanClient is None:
            raise RuntimeError(
                "RealityScan SDK is not importable. Install it or use CLI/stub runner mode."
            )
        self._base_url = base_url
        self._client_id = client_id
        self._app_token = app_token
        self._auth_token = auth_token

    @staticmethod
    def _as_bool(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        rendered = str(value).strip().lower()
        if rendered in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if rendered in {"0", "false", "f", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _as_float(value: Any, *, default: float) -> float:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_int(value: Any, *, default: int) -> int:
        if value is None:
            return default
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _as_str(value: Any, *, default: str) -> str:
        if value is None:
            return default
        rendered = str(value).strip()
        return rendered or default

    @staticmethod
    def _build_progress(progress: float) -> float:
        return max(0.0, min(100.0, float(progress)))

    def _emit_progress(
        self,
        callback: Optional[ProgressCallback],
        progress: float,
        message: str,
        *,
        stage: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        if callback is None:
            return
        callback(
            self._build_progress(progress),
            message,
            {
                "stage": stage,
                **(payload or {}),
            },
        )

    def _wait_for_task(self, client: Any, task_id: str, *, timeout_seconds: int) -> Dict[str, Any]:
        started = time.monotonic()
        last_state = "unknown"
        while True:
            statuses = client.project.tasks(task_ids=[task_id])
            if statuses:
                status = statuses[0]
                state = (status.state or "").strip().lower()
                error_code = self._as_int(status.errorCode, default=0)
                if state:
                    last_state = state
                if state in self._SUCCESS_STATES:
                    return {
                        "task_id": task_id,
                        "state": status.state,
                        "error_code": error_code,
                        "error_message": status.errorMessage,
                    }
                if state in self._FAIL_STATES or error_code != 0:
                    raise RuntimeError(
                        f"RealityScan task failed task_id={task_id} state={status.state} "
                        f"error_code={error_code} error_message={status.errorMessage}"
                    )
                if status.timeEnd is not None:
                    if error_code == 0:
                        return {
                            "task_id": task_id,
                            "state": status.state,
                            "error_code": error_code,
                            "error_message": status.errorMessage,
                        }
                    raise RuntimeError(
                        f"RealityScan task ended with error task_id={task_id} state={status.state} "
                        f"error_code={error_code} error_message={status.errorMessage}"
                    )

            if time.monotonic() - started >= timeout_seconds:
                raise RuntimeError(
                    f"RealityScan task timeout task_id={task_id} last_state={last_state} timeout_seconds={timeout_seconds}"
                )
            time.sleep(1.0)

    def _run_command(
        self,
        client: Any,
        *,
        command_name: str,
        params: Sequence[str] | None,
        timeout_seconds: int,
    ) -> Dict[str, Any]:
        task = client.project.command(command_name, params=params)
        task_result = self._wait_for_task(client, str(task.taskID), timeout_seconds=timeout_seconds)
        task_result["command"] = command_name
        task_result["params"] = list(params or [])
        return task_result

    @staticmethod
    def _coerce_str(value: Any, *, default: str) -> str:
        if value is None:
            return default
        rendered = str(value).strip()
        return rendered or default

    @staticmethod
    def _coerce_int(value: Any, *, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed >= 0 else default

    @staticmethod
    def _coerce_int_list(raw: Any, *, default: Sequence[str]) -> Sequence[str]:
        if raw is None:
            return default
        if isinstance(raw, str):
            parts = [part.strip() for part in raw.split(",") if part.strip()]
            return [part for part in parts]
        if isinstance(raw, Sequence):
            out: list[str] = []
            for item in raw:
                text = str(item).strip()
                if text:
                    out.append(text)
            return out
        return default

    def run(
        self,
        working_directory: Path,
        image_keys: Sequence[str],
        filters: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        imagery_folder = self._as_str(filters.get("sdk_imagery_folder"), default="Imagery")
        project_path = self._as_str(filters.get("sdk_project_path"), default=f"{working_directory.name}.rspj")
        include_subdirs = self._as_bool(filters.get("sdk_include_subdirs"), default=True)
        detector_sensitivity = self._as_str(filters.get("sdk_detector_sensitivity"), default="Ultra")
        camera_prior_xyz = self._as_float(filters.get("sdk_camera_prior_accuracy_xyz"), default=0.1)
        camera_prior_ypr = self._as_float(filters.get("sdk_camera_prior_accuracy_yaw_pitch_roll"), default=1.0)
        run_align = self._as_bool(filters.get("sdk_run_align"), default=True)
        run_normal_model = self._as_bool(filters.get("sdk_run_normal_model"), default=True)
        run_ortho_projection = self._as_bool(filters.get("sdk_run_ortho_projection"), default=True)
        task_timeout_seconds = self._as_int(filters.get("sdk_task_timeout_seconds"), default=7200)

        if not (run_align or run_normal_model or run_ortho_projection):
            raise RuntimeError("At least one SDK processing stage must be enabled (align/normal_model/ortho_projection).")

        command_plan = [
            ("newScene", None),
            ("set", [f"appIncSubdirs={'true' if include_subdirs else 'false'}"]),
            ("set", [f"sfmCameraPriorAccuracyX={camera_prior_xyz}"]),
            ("set", [f"sfmCameraPriorAccuracyY={camera_prior_xyz}"]),
            ("set", [f"sfmCameraPriorAccuracyZ={camera_prior_xyz}"]),
            ("set", [f"sfmCameraPriorAccuracyYaw={camera_prior_ypr}"]),
            ("set", [f"sfmCameraPriorAccuracyPitch={camera_prior_ypr}"]),
            ("set", [f"sfmCameraPriorAccuracyRoll={camera_prior_ypr}"]),
            ("set", [f"sfmDetectorSensitivity={detector_sensitivity}"]),
            ("addFolder", [imagery_folder]),
        ]
        if run_align:
            command_plan.append(("align", None))
        if run_normal_model:
            command_plan.append(("calculateNormalModel", None))
        if run_ortho_projection:
            command_plan.append(("calculateOrthoProjection", None))
        if project_path:
            command_plan.append(("save", [project_path]))

        step_count = max(len(command_plan), 1)
        progress_start = 10.0
        progress_end = 90.0
        step_size = (progress_end - progress_start) / step_count

        executed_commands: list[Dict[str, Any]] = []
        with RealityScanClient(  # type: ignore[misc]
            base_url=self._base_url,
            client_id=self._client_id,
            app_token=self._app_token,
            auth_token=self._auth_token,
        ) as client:
            self._emit_progress(
                progress_callback,
                5.0,
                "connected to RSNode API",
                stage="connect",
                payload={"job_id": job_id},
            )
            client.node.connect_user()
            session_id = client.project.create()
            self._emit_progress(
                progress_callback,
                8.0,
                "connected to RSNode and opened project session",
                stage="project_created",
                payload={"session_id": session_id, "job_id": job_id},
            )

            for index, (command_name, params) in enumerate(command_plan, start=0):
                current_progress = progress_start + (index * step_size)
                self._emit_progress(
                    progress_callback,
                    current_progress,
                    f"starting command: {command_name}",
                    stage="command_start",
                    payload={
                        "command": command_name,
                        "params": list(params or []),
                        "job_id": job_id,
                        "sequence_index": index + 1,
                        "sequence_total": step_count,
                    },
                )
                task_result = self._run_command(
                    client,
                    command_name=command_name,
                    params=params,
                    timeout_seconds=task_timeout_seconds,
                )
                executed_commands.append(task_result)
                self._emit_progress(
                    progress_callback,
                    current_progress + step_size,
                    f"completed command: {command_name}",
                    stage="command_complete",
                    payload={
                        "command": command_name,
                        "command_result": task_result,
                        "job_id": job_id,
                    },
                )

            self._emit_progress(
                progress_callback,
                93.0,
                "querying node and project status",
                stage="status_query",
                payload={"job_id": job_id},
            )
            project_status = client.project.status()
            node_status = client.node.status()
            self._emit_progress(
                progress_callback,
                96.0,
                "finalizing job",
                stage="finalizing",
                payload={"job_id": job_id},
            )
            return {
                "status": "completed",
                "selected_images": len(image_keys),
                "filters": filters,
                "job_id": job_id,
                "message": (
                    "RealityScan SDK command sequence completed. "
                    "Folder import uses addFolder and must point to a path visible to the RealityScan node."
                ),
                "working_directory": str(working_directory),
                "session_id": session_id,
                "imagery_folder": imagery_folder,
                "project_path": project_path,
                "executed_commands": executed_commands,
                "project_status": {
                    "restarted": project_status.restarted,
                    "progress": project_status.progress,
                    "time_total": project_status.timeTotal,
                    "time_estimation": project_status.timeEstimation,
                    "error_code": project_status.errorCode,
                    "change_counter": project_status.changeCounter,
                    "process_id": project_status.processID,
                },
                "node_status": {
                    "status": node_status.status,
                    "api_version": node_status.apiVersion,
                    "active_sessions": node_status.activeSessions,
                    "max_sessions": node_status.maxSessions,
                    "session_ids": node_status.sessionIds,
                },
            }


class RsToolsRemoteRunner(RsToolsRunner):
    """Dispatches jobs to a remote rsnode client over Redis."""

    def __init__(
        self,
        redis_url: str,
        command_queue_key: str,
        result_queue_key: str,
        request_timeout_seconds: int,
        result_ttl_seconds: int,
    ) -> None:
        self._bus = RedisCommandBus(redis_url)
        self._command_queue_key = command_queue_key
        self._result_queue_key = result_queue_key
        self._request_timeout_seconds = max(int(request_timeout_seconds), 1)
        self._result_ttl_seconds = max(int(result_ttl_seconds), 1)

    def close(self) -> None:
        self._bus.close()

    def _publish_result(
        self,
        command: ProcessingCommand,
        reply_to: Optional[str],
        *,
        status: str,
        message: Optional[str] = None,
        progress: Optional[float] = None,
        data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        started_at: Optional[str] = None,
    ) -> None:
        result = ProcessingCommandResult(
            command_id=command.command_id,
            command_type=command.command_type,
            status=status,
            message=message,
            progress=progress,
            data=data,
            error=error,
            started_at=started_at,
            finished_at=_utc_now_iso(),
        )
        payload = result.to_payload()
        self._bus.push(self._result_queue_key, payload, expire_seconds=self._result_ttl_seconds)
        if reply_to:
            self._bus.push(reply_to, payload, expire_seconds=self._result_ttl_seconds)

    def run(
        self,
        working_directory: Path,
        image_keys: Sequence[str],
        filters: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        if job_id is None:
            job_id = str(filters.get("job_id") or uuid4())

        command_id = str(uuid4())
        started_at = _utc_now_iso()
        reply_to = f"{self._result_queue_key}:reply:{command_id}"
        command_payload = {
            "job_id": job_id,
            "working_directory": str(working_directory),
            "image_keys": list(image_keys),
            "filters": filters,
        }
        command = ProcessingCommand(
            command_id=command_id,
            command_type=COMMAND_TYPE_PROCESSING_JOB,
            payload=command_payload,
            reply_to=reply_to,
        )
        self._bus.push(self._command_queue_key, command.to_payload())
        if progress_callback is not None:
            progress_callback(
                10.0,
                "processing request sent to rsnode worker",
                {"command_id": command_id, "job_id": job_id},
            )
        self._publish_result(
            command,
            reply_to=reply_to,
            status=RESULT_STATUS_ACCEPTED,
            message="job accepted by rslogic",
            progress=10.0,
            started_at=started_at,
            data={"job_id": job_id},
        )

        timeout_at = time.monotonic() + self._request_timeout_seconds
        while True:
            remaining = timeout_at - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for rsnode client result for job_id={job_id}")

            event_payload = self._bus.pop(reply_to, timeout_seconds=max(1, min(10, int(remaining))))
            if event_payload is None:
                continue
            try:
                event = ProcessingCommandResult.parse(event_payload)
            except Exception as exc:  # pragma: no cover - malformed payload guard
                logger.warning(
                    "Ignoring invalid rsnode command result for job_id=%s command_id=%s error=%s",
                    job_id,
                    command_id,
                    exc,
                )
                continue
            if event.command_id != command_id:
                logger.debug("Ignoring mismatched command result command_id=%s expected=%s", event.command_id, command_id)
                continue

            if event.status == RESULT_STATUS_ACCEPTED:
                if progress_callback is not None:
                    progress_callback(
                        event.progress or 10.0,
                        event.message or "processing accepted by rsnode client",
                        {
                            "command_id": command_id,
                            "job_id": job_id,
                            "data": event.data,
                        },
                    )
                continue

            if event.status == RESULT_STATUS_PROGRESS:
                if progress_callback is not None:
                    progress = event.progress or 0.0
                    progress_callback(
                        progress,
                        event.message or "processing in progress",
                        {
                            "command_id": command_id,
                            "job_id": job_id,
                            "data": event.data,
                        },
                    )
                continue

            if event.status == RESULT_STATUS_OK:
                if progress_callback is not None:
                    progress_callback(
                        event.progress or 100.0,
                        event.message or "processing completed",
                        event.data,
                    )
                return {
                    "status": "completed",
                    "job_id": job_id,
                    "command_id": command_id,
                    "result": event.data,
                }

            if event.status == RESULT_STATUS_ERROR:
                raise RuntimeError(event.error or event.message or "rsnode client reported error")

            raise RuntimeError(f"Unexpected rsnode status {event.status} for job_id={job_id}")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_sdk_runner(config: RsToolsConfig) -> RsToolsRunner:
    base_url = (config.sdk_base_url or "").strip()
    client_id = (config.sdk_client_id or "").strip()
    app_token = (config.sdk_app_token or "").strip()
    auth_token = (config.sdk_auth_token or "").strip()
    if not (base_url and client_id and app_token and auth_token):
        raise RuntimeError(
            "RealityScan SDK mode requires RSLOGIC_RSTOOLS_SDK_BASE_URL, "
            "RSLOGIC_RSTOOLS_SDK_CLIENT_ID, RSLOGIC_RSTOOLS_SDK_APP_TOKEN, "
            "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN."
        )
    return RsToolsSdkRunner(
        base_url=base_url,
        client_id=client_id,
        app_token=app_token,
        auth_token=auth_token,
    )


def _build_remote_runner(config: RsToolsConfig) -> RsToolsRunner:
    del config
    runtime = load_config()
    if not (runtime.rstools.sdk_base_url and runtime.rstools.sdk_client_id and runtime.rstools.sdk_app_token and runtime.rstools.sdk_auth_token):
        raise RuntimeError(
            "Remote RSTools runner requires RSLOGIC_RSTOOLS_SDK_BASE_URL, "
            "RSLOGIC_RSTOOLS_SDK_CLIENT_ID, RSLOGIC_RSTOOLS_SDK_APP_TOKEN, "
            "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN."
        )
    return RsToolsRemoteRunner(
        redis_url=runtime.queue.redis_url,
        command_queue_key=runtime.control.command_queue_key,
        result_queue_key=runtime.control.result_queue_key,
        request_timeout_seconds=runtime.control.request_timeout_seconds,
        result_ttl_seconds=runtime.control.result_ttl_seconds,
    )


def build_runner_from_config(config: Optional[RsToolsConfig] = None) -> RsToolsRunner:
    """Return the preferred runner for the current deployment."""
    cfg = config or load_config().rstools
    mode = (cfg.mode or "stub").strip().lower()
    if mode == "sdk":
        return _build_sdk_runner(cfg)
    if mode in {"remote", "rsnode_client", "client"}:
        return _build_remote_runner(cfg)
    if mode in {"cli", "subprocess"} and cfg.executable_path:
        return SubprocessRsToolsRunner(cfg.executable_path)
    return StubRsToolsRunner()
