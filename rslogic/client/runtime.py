"""Client runtime service."""

from __future__ import annotations

import contextlib
from dataclasses import asdict
import json
import logging
import os
import uuid
import signal
import threading
import time
from pathlib import Path
from typing import Any
from dotenv import dotenv_values
import socket

try:
    from realityscan_sdk.client import RealityScanClient
    _SDK_AVAILABLE = True
except ModuleNotFoundError:
    RealityScanClient = None  # type: ignore[assignment]
    _SDK_AVAILABLE = False


_REQUIRED_CLIENT_ENV_KEYS = {
    "RSLOGIC_CLIENT_ID",
    "RSLOGIC_CLIENT_LOG_LEVEL",
    "RSLOGIC_REDIS_HOST",
    "RSLOGIC_REDIS_PORT",
    "RSLOGIC_CONTROL_COMMAND_QUEUE",
    "RSLOGIC_CONTROL_RESULT_QUEUE",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "RSLOGIC_DATA_ROOT",
    "RSLOGIC_RSTOOLS_WORKING_ROOT",
    "RSLOGIC_RSTOOLS_EXECUTABLE",
    "RSLOGIC_RSTOOLS_EXECUTABLE_ARGS",
    "RSLOGIC_RSTOOLS_MODE",
    "RSLOGIC_RSTOOLS_SDK_BASE_URL",
    "RSLOGIC_RSTOOLS_SDK_CLIENT_ID",
    "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN",
    "RSLOGIC_RSTOOLS_SDK_APP_TOKEN",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_ENDPOINT_URL",
    "S3_REGION",
}

_CLIENT_ENV_FILE = Path(__file__).resolve().parents[2] / "client.env"
_SESSION_STATE_FILE = "client-sdk-session.json"


def _validate_client_env_contract() -> None:
    values = _read_client_env_values()
    missing = [name for name in sorted(_REQUIRED_CLIENT_ENV_KEYS) if not values.get(name)]
    if missing:
        raise RuntimeError(f"Client runtime missing required env values: {', '.join(missing)}")


def _read_client_env_values() -> dict[str, str]:
    raw = dotenv_values(_CLIENT_ENV_FILE)
    normalized: dict[str, str] = {}
    for key, value in raw.items():
        normalized_key = str(key).strip().lstrip("\ufeff")
        if not normalized_key:
            continue
        normalized[normalized_key] = str(value).strip() if isinstance(value, str) else ""
    return normalized


def _load_client_env() -> None:
    path = _CLIENT_ENV_FILE
    if not path.is_file():
        raise RuntimeError(f"Client env file not found: {path}")

    loaded_raw = _read_client_env_values()
    loaded = loaded_raw
    for key, value in loaded.items():
        os.environ[key] = str(value)
    _validate_client_env_contract()


_load_client_env()

from rslogic.config import CONFIG
from rslogic.common.db import LabelDbStore
from rslogic.common.redis_bus import RedisBus
from rslogic.common.schemas import Step
from rslogic.client.executor import StepExecutor
from rslogic.client.file_ops import FileExecutor
from rslogic.client.process_guard import RsNodeProcess


class ClientRuntime:
    def __init__(self) -> None:
        self._configure_logging()
        self._log = logging.getLogger("rslogic.client.runtime")
        self.stop_event = threading.Event()
        self._task_state: dict[str, dict[str, Any]] = {}
        self._task_state_lock = threading.Lock()
        self._active_job_id: str | None = None
        self._active_sdk_client: object | None = None
        self.client_id = os.getenv("RSLOGIC_CLIENT_ID", "").strip()
        if not self.client_id:
            raise RuntimeError("RSLOGIC_CLIENT_ID is required")
        self.sdk_client_id = self._normalize_sdk_client_id(
            os.getenv("RSLOGIC_RSTOOLS_SDK_CLIENT_ID", CONFIG.rstools.sdk_client_id or None),
            fallback=self.client_id,
        )
        self.redis_bus = RedisBus(
            CONFIG.queue.redis_url,
            CONFIG.control.command_queue_key,
            CONFIG.control.result_queue_key,
        )
        self.db = LabelDbStore(CONFIG.label_db.database_url, CONFIG.label_db.migration_root)
        self.node_guard = RsNodeProcess(
            os.environ["RSLOGIC_RSTOOLS_EXECUTABLE"],
            os.environ["RSLOGIC_RSTOOLS_EXECUTABLE_ARGS"],
        )
        self.data_root = Path(os.environ["RSLOGIC_DATA_ROOT"])
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.file_executor = FileExecutor(self.db, self.data_root)
        self._session_state_file = self.data_root / _SESSION_STATE_FILE
        self._active_session: str | None = self._load_active_session()
        self._job_lock = threading.Lock()
        self._task_heartbeat_seconds = max(
            int(os.getenv("RSLOGIC_CLIENT_TASK_POLL_SECONDS", "3")),
            1,
        )

    @staticmethod
    def _configure_logging() -> None:
        level_name = os.getenv("RSLOGIC_CLIENT_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logger = logging.getLogger("rslogic")
        logger.setLevel(level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s"
                )
            )
            handler.setLevel(level)
            logger.addHandler(handler)
        logger.propagate = False
        for child in (
            "rslogic.client.runtime",
            "rslogic.client.executor",
            "rslogic.client.file_ops",
            "rslogic.client.process_guard",
            "rslogic.common.redis_bus",
        ):
            child_logger = logging.getLogger(child)
            child_logger.setLevel(level)
            child_logger.propagate = True
        logging.captureWarnings(True)

    def _load_active_session(self) -> str | None:
        if not self._session_state_file.is_file():
            return None
        try:
            payload = json.loads(self._session_state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        session = payload.get("session")
        if isinstance(session, str) and session.strip():
            return session.strip()
        return None

    def _persist_active_session(self, session: str | None) -> None:
        self._active_session = session
        if not session:
            if self._session_state_file.is_file():
                with contextlib.suppress(OSError):
                    self._session_state_file.unlink()
            return
        payload = {"session": session}
        with self._session_state_file.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp)

    @staticmethod
    def _to_jsonable_dict(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            return {"value": value}
        if hasattr(value, "__dataclass_fields__"):
            try:
                return asdict(value)
            except Exception:
                pass
        task_id = getattr(value, "taskID", None)
        if task_id is None:
            task_id = getattr(value, "taskId", None)
        if task_id is None:
            return None
        return {
            "taskID": str(task_id),
            "state": str(getattr(value, "state", "")),
            "errorCode": getattr(value, "errorCode", 0),
            "errorMessage": str(getattr(value, "errorMessage", "")),
            "timeStart": getattr(value, "timeStart", None),
            "timeEnd": getattr(value, "timeEnd", None),
        }

    @staticmethod
    def _extract_task_ids(result: Any) -> list[str]:
        task_ids: list[str] = []

        def add(candidate: Any) -> None:
            if candidate is None:
                return
            task_id = candidate
            if isinstance(candidate, dict):
                task_id = candidate.get("taskID") or candidate.get("taskId") or candidate.get("id")
            else:
                if hasattr(candidate, "taskID"):
                    task_id = getattr(candidate, "taskID")
                elif hasattr(candidate, "taskId"):
                    task_id = getattr(candidate, "taskId")
                if task_id is None:
                    return
            if isinstance(task_id, str):
                task_id = task_id.strip()
            else:
                task_id = str(task_id).strip()
            task_id = str(task_id).strip()
            if task_id and task_id not in task_ids:
                task_ids.append(task_id)

        if isinstance(result, (list, tuple, set)):
            for item in result:
                add(item)
            return task_ids
        if isinstance(result, dict):
            add(result)
            return task_ids
        add(result)
        return task_ids

    @staticmethod
    def _normalize_task_state(value: Any) -> str:
        return "" if value is None else str(value).strip().lower()

    @staticmethod
    def _is_task_terminal(state: Any) -> bool:
        return ClientRuntime._normalize_task_state(state) in {
            "finished",
            "done",
            "canceled",
            "cancelled",
            "failed",
            "error",
            "aborted",
        }

    @staticmethod
    def _is_task_started(state: Any) -> bool:
        return ClientRuntime._normalize_task_state(state) == "started"

    @staticmethod
    def _task_ids_terminal(task_updates: list[dict[str, Any]], wanted_task_ids: list[str]) -> bool:
        if not wanted_task_ids:
            return False
        by_id = {}
        for task in task_updates:
            task_id = str(task.get("taskID", "")).strip() or str(task.get("task_id", "")).strip()
            if task_id:
                by_id[task_id] = task
        for task_id in wanted_task_ids:
            task_state = by_id.get(task_id)
            if task_state is None:
                return False
            if not ClientRuntime._is_task_terminal(task_state.get("state")):
                return False
        return True

    @staticmethod
    def _project_is_running(project_status: dict[str, Any] | None) -> bool:
        if not project_status:
            return False
        process_id = project_status.get("processID", project_status.get("processId", 0))
        try:
            if int(process_id or 0) > 0:
                return True
        except Exception:
            pass
        progress = project_status.get("progress")
        try:
            progress_f = float(progress)
        except Exception:
            return False
        return 0.0 < progress_f < 1.0

    def _init_job_task_state(self, job_id: str, session: str | None) -> None:
        with self._task_state_lock:
            self._task_state[job_id] = {
                "session": session,
                "tasks": {},
            }

    def _register_step_tasks(
        self,
        job_id: str,
        *,
        step_index: int,
        step_action: str,
        step_kind: str,
        task_ids: list[str],
        session: str | None,
    ) -> None:
        now = time.time()
        with self._task_state_lock:
            job_state = self._task_state.setdefault(
                job_id,
                {
                    "session": session,
                    "tasks": {},
                },
            )
            job_state["session"] = session
            tasks = job_state["tasks"]
            if not isinstance(tasks, dict):
                tasks = {}
                job_state["tasks"] = tasks
            for task_id in task_ids:
                tasks[task_id] = {
                    "task_id": task_id,
                    "taskID": task_id,
                    "session": session,
                    "step_index": step_index,
                    "step_action": step_action,
                    "step_kind": step_kind,
                    "state": "started",
                    "status": "started",
                    "errorCode": 0,
                    "errorMessage": "",
                    "timeStart": None,
                    "timeEnd": None,
                    "created_at": now,
                    "updated_at": now,
                }

    def _query_task_status(
        self,
        sdk_client: object | None,
        job_id: str,
        *,
        task_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if sdk_client is None:
            return []
        with self._task_state_lock:
            job_state = self._task_state.get(job_id)
            if not job_state:
                job_state = {"session": None, "tasks": {}}
                self._task_state[job_id] = job_state
        tasks = job_state.get("tasks")
        if not isinstance(tasks, dict):
            tasks = None
        if task_ids is None:
            task_ids = list(tasks.keys()) if isinstance(tasks, dict) else []
        if not task_ids:
            return []
        try:
            statuses = sdk_client.project.tasks(task_ids=task_ids)  # type: ignore[attr-defined]
        except Exception as exc:
            self._log.debug(
                "job=%s project.tasks(query task_ids=%s) failed: %s",
                job_id,
                task_ids,
                exc,
            )
            return []
        if not isinstance(statuses, (list, tuple)):
            self._log.debug(
                "job=%s project.tasks returned non-sequence response=%s",
                job_id,
                self._safe_preview(statuses),
            )
            return []
        if tasks is None:
            tasks = {}
            with self._task_state_lock:
                self._task_state[job_id] = {"session": job_state.get("session"), "tasks": tasks}

        now = time.time()
        with self._task_state_lock:
            for raw_status in statuses:
                payload = self._to_jsonable_dict(raw_status)
                if not payload:
                    continue
                task_id = str(payload.get("taskID", "")).strip()
                if not task_id:
                    continue
                task = tasks.get(task_id)
                if task is None:
                    task = {
                        "taskID": task_id,
                        "created_at": now,
                        "state": "",
                        "status": "",
                        "errorCode": 0,
                        "errorMessage": "",
                        "timeStart": None,
                        "timeEnd": None,
                    }
                    tasks[task_id] = task
                task.update(
                    {
                        "state": payload.get("state", ""),
                        "status": payload.get("state", ""),
                        "errorCode": payload.get("errorCode", 0),
                        "errorMessage": payload.get("errorMessage", ""),
                        "timeStart": payload.get("timeStart", None),
                        "timeEnd": payload.get("timeEnd", None),
                        "updated_at": now,
                    }
                )

                if "created_at" not in task:
                    task["created_at"] = now

        result = []
        with self._task_state_lock:
            for task in tasks.values():
                task_state = dict(task)
                state = task_state.get("state")
                task_state["is_terminal"] = self._is_task_terminal(state)
                result.append(task_state)
        return result

    def _query_project_status(self, sdk_client: object | None) -> dict[str, Any] | None:
        if sdk_client is None:
            return None
        try:
            status = sdk_client.project.status()  # type: ignore[attr-defined]
        except Exception as exc:
            self._log.debug("project status poll failed: %s", exc)
            return None
        return self._to_jsonable_dict(status)

    def _wait_for_step_tasks(
        self,
        *,
        sdk_client: object | None,
        job_id: str,
        group_id: str | None,
        step_index: int,
        total_steps: int,
        step_action: str,
        step_kind: str,
        task_ids: list[str],
        timeout_s: int,
    ) -> list[dict[str, Any]]:
        if not task_ids or sdk_client is None:
            return []
        timeout = max(0, int(timeout_s))
        deadline = time.monotonic() + timeout if timeout > 0 else None
        started_at = time.monotonic()
        wanted = [task_id for task_id in task_ids if task_id]
        monitor_state: dict[str, list[dict[str, Any]] | dict[str, Any] | None | bool] = {
            "task_updates": [],
            "project_status": None,
            "running_tasks": [],
            "completed_tasks": [],
            "completed": False,
        }
        state_lock = threading.Lock()
        monitor_done = threading.Event()
        monitor_stop = threading.Event()
        failed: list[Exception] = []

        def _is_finished(task_updates: list[dict[str, Any]]) -> bool:
            if not task_updates:
                return False
            return self._task_ids_terminal(task_updates, wanted)

        def monitor_loop() -> None:
            while not monitor_stop.is_set():
                if self.stop_event.is_set():
                    break
                task_updates = self._query_task_status(sdk_client, job_id, task_ids=wanted)
                by_id: dict[str, dict[str, Any]] = {}
                for task_update in task_updates:
                    task_id = str(task_update.get("taskID", "")).strip()
                    if task_id:
                        by_id[task_id] = task_update

                running_updates = [task_update for task_update in task_updates if self._is_task_started(task_update.get("state"))]
                terminal_updates = [task_update for task_update in task_updates if self._is_task_terminal(task_update.get("state"))]
                project_status = self._query_project_status(sdk_client)
                missing = [task_id for task_id in wanted if task_id not in by_id]
                elapsed = round(time.monotonic() - started_at, 2)

                with state_lock:
                    monitor_state["task_updates"] = task_updates
                    monitor_state["project_status"] = project_status
                    monitor_state["running_tasks"] = running_updates
                    monitor_state["completed_tasks"] = terminal_updates

                if missing:
                    self._log.debug(
                        "job=%s step=%s/%s waiting for task status; missing=%s",
                        job_id,
                        step_index,
                        total_steps,
                        self._safe_preview(missing, max_len=300),
                    )
                elif task_updates:
                    self._log.debug(
                        "job=%s step=%s/%s task wave: running=%s terminal=%s project_running=%s",
                        job_id,
                        step_index,
                        total_steps,
                        len(running_updates),
                        len(terminal_updates),
                        self._project_is_running(project_status),
                    )

                if terminal_updates and _is_finished(task_updates):
                    if any(
                        int(task_update.get("errorCode", 0) or 0) != 0
                        or str(task_update.get("state", "")).strip().lower()
                        in {"failed", "error", "aborted", "canceled", "cancelled"}
                        for task_update in task_updates
                    ):
                        error_codes = {
                            t.get("taskID"): t.get("errorCode")
                            for t in task_updates
                            if int(t.get("errorCode", 0) or 0) != 0
                        }
                        failed.append(RuntimeError(f"step task failed: {error_codes}"))
                    with state_lock:
                        monitor_state["completed"] = True
                    monitor_done.set()
                    break

                if task_updates:
                    self._report_progress(
                        job_id=job_id,
                        group_id=group_id,
                        progress=((step_index - 1) / max(1, total_steps)) * 100.0,
                        status="running",
                        message=f"step {step_index}/{total_steps} waiting: {step_action}",
                        task_state={"tasks": task_updates},
                        project_status=project_status,
                        result_summary={
                            "phase": "step_task_wait",
                            "step_index": step_index,
                            "step_kind": step_kind,
                            "step_action": step_action,
                            "elapsed_seconds": elapsed,
                            "task_count": len(task_updates),
                            "running_tasks": running_updates,
                            "completed_tasks": terminal_updates,
                            "task_state": {"tasks": task_updates},
                            "project_status": project_status,
                        },
                    )

                if monitor_done.wait(self._task_heartbeat_seconds):
                    break

        monitor_thread = threading.Thread(
            target=monitor_loop,
            name=f"task-monitor-{job_id}-{step_index}",
            daemon=True,
        )
        monitor_thread.start()

        try:
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                if monitor_done.wait(min(self._task_heartbeat_seconds, max(0.1, (deadline - time.monotonic()) if deadline else self._task_heartbeat_seconds))):
                    break

                if failed:
                    raise failed[0]
                with state_lock:
                    task_updates = list(monitor_state["task_updates"])  # type: ignore[arg-type]
                    project_status = monitor_state["project_status"]
                    running_updates = list(monitor_state["running_tasks"])  # type: ignore[arg-type]
                    completed_updates = list(monitor_state["completed_tasks"])  # type: ignore[arg-type]
                    completed = bool(monitor_state["completed"])
                if completed and _is_finished(task_updates):
                    if any(
                        int(task_update.get("errorCode", 0) or 0) != 0
                        or str(task_update.get("state", "")).strip().lower()
                        in {"failed", "error", "aborted", "canceled", "cancelled"}
                        for task_update in task_updates
                    ):
                        error_codes = {
                            t.get("taskID"): t.get("errorCode")
                            for t in task_updates
                            if int(t.get("errorCode", 0) or 0) != 0
                        }
                        raise RuntimeError(f"step task failed: {error_codes}")
                    self._report_progress(
                        job_id=job_id,
                        group_id=group_id,
                        progress=((step_index - 1) / max(1, total_steps)) * 100.0,
                        status="running",
                        message=f"step {step_index}/{total_steps} complete: {step_action}",
                        task_state={"tasks": task_updates},
                        project_status=project_status,
                        result_summary={
                            "phase": "step_task_wait",
                            "step_index": step_index,
                            "step_kind": step_kind,
                            "step_action": step_action,
                            "elapsed_seconds": round(time.monotonic() - started_at, 2),
                            "task_count": len(task_updates),
                            "running_tasks": running_updates,
                            "completed_tasks": completed_updates,
                            "task_state": {"tasks": task_updates},
                            "project_status": project_status,
                        },
                    )
                    return task_updates

            if failed:
                raise failed[0]
            if deadline is not None and time.monotonic() >= deadline and not monitor_done.is_set():
                with state_lock:
                    timeout_state = list(monitor_state["task_updates"])  # type: ignore[arg-type]
                if timeout_state:
                    raise TimeoutError(
                        f"step {step_index}/{total_steps} task wait timed out after {timeout}s: "
                        f"{self._safe_preview(timeout_state, max_len=300)}"
                    )
                raise TimeoutError(f"step {step_index}/{total_steps} task wait timed out after {timeout}s with no task status")

            with state_lock:
                final_updates = list(monitor_state["task_updates"])  # type: ignore[arg-type]
            if not final_updates:
                with state_lock:
                    final_updates = list(monitor_state["task_updates"])  # type: ignore[arg-type]
            return final_updates
        finally:
            monitor_stop.set()
            monitor_thread.join(timeout=1.0)

    @staticmethod
    def _is_unlimited_step_timeout(step: Step) -> bool:
        if step.action in {"sdk_new_scene", "sdk_project_new_scene"}:
            return True
        if step.action == "sdk_project_command":
            command = str(step.params.get("name", "")).strip().lower()
            return command == "align"
        return False

    def _sdk_client(self) -> object:
        if not _SDK_AVAILABLE:
            raise RuntimeError(
                "realityscan_sdk is not installed. Install it in this environment to run sdk commands."
            )
        client = RealityScanClient(
            base_url=os.environ["RSLOGIC_RSTOOLS_SDK_BASE_URL"],
            client_id=self.sdk_client_id,
            auth_token=os.environ["RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN"],
            app_token=os.environ["RSLOGIC_RSTOOLS_SDK_APP_TOKEN"],
            verify_tls=False,
        )
        if self._active_session:
            client.session = self._active_session
        return client

    @staticmethod
    def _normalize_sdk_client_id(raw_client_id: str | None, *, fallback: str) -> str:
        raw = (raw_client_id or "").strip()
        if not raw:
            raw = (fallback or "").strip()
        if raw:
            try:
                uuid.UUID(raw)
                return raw
            except ValueError:
                return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"rslogic:{raw}"))
        return str(uuid.uuid4())

    @staticmethod
    def _looks_like_uuid(value: str | None) -> bool:
        if not value:
            return False
        try:
            uuid.UUID(str(value).strip())
            return True
        except ValueError:
            return False

    def _ensure_group_id(self, raw_group_id: str | None) -> str | None:
        if not raw_group_id:
            return None
        if self._looks_like_uuid(raw_group_id):
            return raw_group_id
        group, _ = self.db.get_or_create_group(raw_group_id)
        return group.id

    def run(self) -> None:
        self._log.info("client runtime starting client_id=%s sdk_client_id=%s data_root=%s", self.client_id, self.sdk_client_id, self.data_root)
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()

        while not self.stop_event.is_set():
            self._log.debug("polling queue: client_id=%s", self.client_id)
            with contextlib.suppress(Exception):
                self.node_guard.ensure_running()
            payload = self.redis_bus.pop_command(self.client_id, CONFIG.queue.poll_interval_seconds)
            if not payload:
                self._log.debug("no command received in this poll window")
                continue
            self._log.info("received command payload: job_id=%s type=%s", payload.get("job_id"), payload.get("type"))
            if payload.get("type") != "job":
                self._log.debug("ignoring non-job payload=%s", self._safe_preview(payload, max_len=300))
                continue
            if not self._job_lock.acquire(blocking=False):
                job_id = str(payload.get("job_id"))
                group_id = self._ensure_group_id(payload.get("group_id"))
                self.redis_bus.publish_result(
                    self.client_id,
                    {"job_id": job_id, "status": "rejected", "progress": 0, "message": "client is already busy"},
                )
                self.db.upsert_processing_job(
                    job_id=job_id,
                    image_group_id=group_id,
                    status="rejected",
                    progress=0.0,
                    message="client is already busy",
                )
                self._log.warning("client busy; rejected job_id=%s group_id=%s", job_id, group_id)
                continue
            self._log.debug("acquired lock for job_id=%s", payload.get("job_id"))
            self._run_job(payload)

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            self._log.debug("publishing heartbeat for client_id=%s", self.client_id)
            task_state: dict[str, Any] | None = None
            project_status: dict[str, Any] | None = None
            running_tasks: list[dict[str, Any]] = []
            completed_tasks: list[dict[str, Any]] = []
            if self._active_job_id is not None and self._active_sdk_client is not None:
                task_updates = self._query_task_status(self._active_sdk_client, self._active_job_id)
                project_status = self._query_project_status(self._active_sdk_client)
                if task_updates:
                    task_state = {"tasks": task_updates}
                    running_tasks = [t for t in task_updates if self._is_task_started(t.get("state"))]
                    completed_tasks = [t for t in task_updates if t.get("is_terminal")]
                    self._log.debug(
                        "client=%s active_job=%s task_count=%s running=%s completed=%s",
                        self.client_id,
                        self._active_job_id,
                        len(task_updates),
                        len(running_tasks),
                        len(completed_tasks),
                    )
            self.redis_bus.heartbeat(
                self.client_id,
                {
                    "status": "alive",
                    "service": "rslogic-client",
                    "pid": os.getpid(),
                    "host": socket.gethostname(),
                    "active_job_id": self._active_job_id,
                    "task_state": task_state,
                    "project_status": project_status,
                    "task_count": len(running_tasks) + len(completed_tasks),
                    "running_tasks": running_tasks,
                    "completed_tasks": completed_tasks,
                },
            )
            with contextlib.suppress(Exception):
                self.node_guard.ensure_running()
            time.sleep(5)

    def _safe_preview(self, value: Any, *, max_len: int = 1400) -> str:
        text = repr(value)
        if len(text) <= max_len:
            return text
        return f"{text[:max_len]}…(+{len(text)-max_len} chars)"

    @staticmethod
    def _result_preview(value: Any, *, max_len: int = 1800) -> dict[str, Any]:
        text = repr(value)
        preview = text if len(text) <= max_len else f"{text[:max_len]}…(+{len(text)-max_len} chars)"
        return {
            "result_type": type(value).__name__,
            "result": text,
            "result_preview": preview,
        }

    def _start_step_heartbeat(
        self,
        *,
        job_id: str,
        group_id: str | None,
        step_index: int,
        total_steps: int,
        step_action: str,
        step_kind: str,
        sdk_client: object | None = None,
    ) -> tuple[threading.Event, threading.Thread, float]:
        started_at = time.monotonic()
        stop_event = threading.Event()

        def report_loop() -> None:
            while not stop_event.wait(self._task_heartbeat_seconds):
                elapsed = round(time.monotonic() - started_at, 2)
                progress = ((step_index - 1) / max(1, total_steps)) * 100.0
                task_updates = self._query_task_status(sdk_client, job_id)
                project_status = self._query_project_status(sdk_client)
                session = None
                with self._task_state_lock:
                    job_state = self._task_state.get(job_id, {})
                if isinstance(job_state, dict):
                    raw_session = job_state.get("session")
                    if isinstance(raw_session, str) and raw_session.strip():
                        session = raw_session.strip()
                running_tasks = [t for t in task_updates if self._is_task_started(t.get("state"))]
                completed_tasks = [t for t in task_updates if t.get("is_terminal")]
                self._log.debug(
                    "job=%s step=%s/%s heartbeat task_count=%s running=%s completed=%s session=%s",
                    job_id,
                    step_index,
                    total_steps,
                    len(task_updates),
                    len(running_tasks),
                    len(completed_tasks),
                    session,
                )
                if project_status:
                    self._log.debug(
                        "job=%s project_status=%s",
                        job_id,
                        self._safe_preview(project_status),
                    )
                if task_updates:
                    sample = task_updates[:3]
                    self._log.debug(
                        "job=%s task_updates=%s",
                        job_id,
                        self._safe_preview(sample, max_len=500),
                    )
                self._report_progress(
                    job_id=job_id,
                    group_id=group_id,
                    progress=progress,
                    status="running",
                    message=f"step {step_index}/{total_steps} in_progress: {step_action}",
                    task_state={"tasks": task_updates},
                    project_status=project_status,
                    result_summary={
                        "phase": "step_heartbeat",
                        "step_index": step_index,
                        "step_kind": step_kind,
                        "step_action": step_action,
                        "session": session,
                        "elapsed_seconds": elapsed,
                        "task_count": len(task_updates),
                        "running_tasks": running_tasks,
                        "completed_tasks": completed_tasks,
                        "task_state": {"tasks": task_updates},
                        "project_status": project_status,
                    },
                )

        heartbeat = threading.Thread(target=report_loop, name=f"step-heartbeat-{job_id}", daemon=True)
        heartbeat.start()
        return stop_event, heartbeat, started_at

    def _report_progress(
        self,
        *,
        job_id: str,
        group_id: str | None,
        progress: float,
        message: str,
        status: str = "running",
        result_summary: dict[str, Any] | None = None,
        task_state: dict[str, Any] | None = None,
        project_status: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "job_id": job_id,
            "group_id": group_id,
            "status": status,
            "progress": progress,
            "message": message,
        }
        if task_state is not None:
            payload["task_state"] = task_state
        if project_status is not None:
            payload["project_status"] = project_status
        if result_summary is not None:
            payload["result_summary"] = result_summary
        self.redis_bus.publish_result(self.client_id, payload)

    def _run_job(self, payload: dict) -> None:
        job_id = str(payload.get("job_id"))
        group_id = self._ensure_group_id(payload.get("group_id"))
        steps = payload.get("steps", [])
        last_step_result: dict[str, Any] | None = None
        last_step_duration: float | None = None
        self._active_job_id = job_id
        self._log.info("job_start job_id=%s group_id=%s step_count=%s", job_id, group_id, len(steps))
        step_objects = [Step.model_validate(raw_step) for raw_step in steps]
        sdk_needed = any(step.kind == "sdk" for step in step_objects)
        sdk_client = self._sdk_client() if sdk_needed else None
        if sdk_client is not None:
            self._log.debug("sdk client instantiated for job_id=%s", job_id)
        self._active_sdk_client = sdk_client
        executor = StepExecutor(
            sdk_client=sdk_client,
            file_executor=self.file_executor,
            initial_session=self._active_session,
            on_session_update=self._persist_active_session,
        )
        try:
            executor.begin_job(job_id, group_id=group_id)
            self._init_job_task_state(job_id, executor.current_session())
            self.node_guard.start()
            self._log.debug("node guard started for job_id=%s", job_id)
            self.db.upsert_processing_job(
                job_id=job_id,
                image_group_id=group_id,
                status="running",
                progress=0.0,
                message=f"started by {self.client_id}",
                filters={"steps": steps},
            )
            self._report_progress(
                job_id=job_id,
                group_id=group_id,
                progress=0.0,
                message="started",
                result_summary={
                    "phase": "job_start",
                    "session": executor.current_session(),
                },
            )
            for idx, raw_step in enumerate(steps, start=1):
                self._log.info(
                    "job=%s client=%s step=%s/%s action=%s kind=%s params=%s",
                    job_id,
                    self.client_id,
                    idx,
                    len(steps),
                    raw_step.get("action"),
                    raw_step.get("kind"),
                    self._safe_preview(raw_step.get("params", {})),
                )
                step = step_objects[idx - 1]
                heartbeat_stop = None
                heartbeat_thread = None
                heartbeat_started = None
                step_error = None
                try:
                    self._log.info(
                        "job=%s step_start step=%s/%s action=%s kind=%s",
                        job_id,
                        idx,
                        len(steps),
                        step.action,
                        step.kind,
                    )
                    heartbeat_stop, heartbeat_thread, heartbeat_started = self._start_step_heartbeat(
                        job_id=job_id,
                        group_id=group_id,
                        step_index=idx,
                        total_steps=len(steps),
                        step_action=step.action,
                        step_kind=step.kind,
                        sdk_client=sdk_client,
                    )
                    self._report_progress(
                        job_id=job_id,
                        group_id=group_id,
                        progress=((idx - 1) / max(1, len(steps))) * 100.0,
                        message=f"step {idx}/{len(steps)} start: {step.action}",
                        result_summary={
                            "phase": "step_start",
                            "step_index": idx,
                            "step_kind": step.kind,
                            "step_action": step.action,
                            "step_params": raw_step.get("params"),
                        },
                    )
                    step_started = time.monotonic()
                    res = executor.execute(step, job_id=job_id, group_id=group_id)
                    step_task_ids = self._extract_task_ids(res)
                    step_snapshot = None

                    if not step_task_ids and step.action in {"sdk_project_create", "sdk_project_open"}:
                        step_session = executor.current_session()
                        if not step_session and isinstance(res, str):
                            step_session = res.strip()
                        if not step_session:
                            raise RuntimeError(f"{step.action} must return a session for completion tracking")

                    if step_task_ids:
                        step_timeout = 0 if self._is_unlimited_step_timeout(step) else step.timeout_s
                        self._register_step_tasks(
                            job_id,
                            step_index=idx,
                            step_action=step.action,
                            step_kind=step.kind,
                            task_ids=step_task_ids,
                            session=executor.current_session(),
                        )
                        step_snapshot = self._wait_for_step_tasks(
                            sdk_client=sdk_client,
                            job_id=job_id,
                            group_id=group_id,
                            step_index=idx,
                            total_steps=len(steps),
                            step_action=step.action,
                            step_kind=step.kind,
                            task_ids=step_task_ids,
                            timeout_s=step_timeout,
                        )
                    else:
                        step_snapshot = []
                    step_time = round(time.monotonic() - step_started, 3)
                    last_step_result = self._result_preview(res)
                    last_step_result["duration_seconds"] = step_time
                    last_step_result["step_index"] = idx
                    last_step_result["step_kind"] = step.kind
                    last_step_result["step_action"] = step.action
                    last_step_duration = step_time
                except Exception as exc:
                    step_error = exc
                    self._log.exception(
                        "job=%s step_fail step=%s/%s action=%s kind=%s error=%s",
                        job_id,
                        idx,
                        len(steps),
                        step.action,
                        step.kind,
                        exc,
                    )
                    raise
                finally:
                    if heartbeat_stop is not None:
                        heartbeat_stop.set()
                    if heartbeat_thread is not None:
                        heartbeat_thread.join(timeout=0.5)
                    if heartbeat_started is not None and step_error is None:
                        heartbeat_seconds = round(time.monotonic() - heartbeat_started, 3)
                    else:
                        heartbeat_seconds = None

                progress = (idx / max(1, len(steps))) * 100.0
                self._log.info(
                    "job=%s step=%s done in %.2fs result=%s",
                    job_id,
                    idx,
                    step_time,
                    self._safe_preview(res, max_len=500),
                )
                task_snapshot = step_snapshot
                if task_snapshot is None:
                    task_snapshot = self._query_task_status(sdk_client, job_id, task_ids=step_task_ids)
                step_project_status = self._query_project_status(sdk_client)
                self.db.upsert_processing_job(
                    job_id=job_id,
                    image_group_id=group_id,
                    status="running",
                    progress=progress,
                    message=f"step {idx}/{len(steps)} ok: {step.action} ({step_time}s)",
                    result_summary={
                        "last_result": last_step_result["result"] if last_step_result else None,
                        "last_result_type": last_step_result["result_type"] if last_step_result else type(res).__name__,
                        "last_result_preview": last_step_result,
                        "last_step": {
                            "index": idx,
                            "kind": step.kind,
                            "action": step.action,
                            "duration_seconds": step_time,
                            "result_type": type(res).__name__,
                            "heartbeat_seconds": heartbeat_seconds,
                            "session": executor.current_session(),
                        },
                        "task_state": {"tasks": task_snapshot},
                        "project_status": step_project_status,
                    },
                )
                self._report_progress(
                    job_id=job_id,
                    group_id=group_id,
                    progress=progress,
                    message=f"step {idx}/{len(steps)} ok: {step.action} ({step_time}s)",
                    task_state={"tasks": task_snapshot},
                    project_status=step_project_status,
                    result_summary={
                        "phase": "step_complete",
                        "step_index": idx,
                        "step_kind": step.kind,
                        "step_action": step.action,
                        "duration_seconds": step_time,
                        "result_type": type(res).__name__,
                        "result": self._safe_preview(res),
                        "session": executor.current_session(),
                        "task_state": {"tasks": task_snapshot},
                        "project_status": step_project_status,
                    },
                )

            completion_payload = {
                "job_id": job_id,
                "status": "completed",
                "progress": 100,
                "message": "completed",
            }
            if last_step_result is not None:
                completion_result = dict(last_step_result)
                completion_result["duration_seconds"] = last_step_duration
                final_task_state = self._query_task_status(sdk_client, job_id)
                project_status = self._query_project_status(sdk_client)
                completion_payload["result_summary"] = {
                    "phase": "job_complete",
                    "last_step_result": completion_result,
                    "session": executor.current_session(),
                    "final_task_state": {"tasks": final_task_state},
                    "final_project_status": project_status,
                }
                completion_payload["task_state"] = {"tasks": final_task_state}
                completion_payload["project_status"] = project_status
            self.db.upsert_processing_job(
                job_id=job_id,
                image_group_id=group_id,
                status="completed",
                progress=100,
                message="completed",
                result_summary=completion_payload.get("result_summary"),
            )
            self.redis_bus.publish_result(self.client_id, completion_payload)
        except Exception as exc:
            failure_tasks = self._query_task_status(sdk_client, job_id)
            failure_project_status = self._query_project_status(sdk_client)
            self.db.upsert_processing_job(
                job_id=job_id,
                image_group_id=group_id,
                status="failed",
                progress=0,
                message=str(exc),
            )
            self._report_progress(
                job_id=job_id,
                group_id=group_id,
                progress=0,
                message=str(exc),
                task_state={"tasks": failure_tasks},
                project_status=failure_project_status,
                result_summary={
                    "phase": "job_failed",
                    "error": str(exc),
                    "task_state": {"tasks": failure_tasks},
                    "project_status": failure_project_status,
                    "session": executor.current_session(),
                },
            )
            self.redis_bus.publish_result(self.client_id, {"job_id": job_id, "status": "failed", "progress": 0, "message": str(exc)})
            self._log.exception("job_failed job_id=%s group_id=%s", job_id, group_id)
        finally:
            executor.end_job(job_id)
            if sdk_client is not None:
                with contextlib.suppress(Exception):
                    sdk_client.close()
            with self._task_state_lock:
                self._task_state.pop(job_id, None)
            self._active_job_id = None
            self._active_sdk_client = None
            self._job_lock.release()

    def _shutdown(self, *_args) -> None:
        self.stop_event.set()
        self.node_guard.stop()


def run_forever() -> None:
    ClientRuntime().run()


def main() -> None:
    run_forever()
