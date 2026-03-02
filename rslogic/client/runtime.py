"""Client runtime service."""

from __future__ import annotations

import contextlib
import logging
import uuid
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

try:
    from realityscan_sdk.client import RealityScanClient
    _SDK_AVAILABLE = True
except ModuleNotFoundError as exc:
    RealityScanClient = None  # type: ignore[assignment]
    _SDK_AVAILABLE = False
    _SDK_IMPORT_ERROR = str(exc)

def _load_client_env() -> None:
    env_candidates = [
        os.getenv("RSLOGIC_CLIENT_ENV_FILE", "").strip(),
        str(Path(__file__).resolve().parent.parent / "client.env"),
        str(Path(__file__).resolve().parent.parent.parent / "client.env"),
    ]
    for env_file in env_candidates:
        if not env_file:
            continue
        path = Path(env_file)
        if path.is_file():
            load_dotenv(path, override=False)
            break


_load_client_env()

from config import CONFIG
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
        self.client_id = os.getenv("RSLOGIC_CLIENT_ID", os.getenv("CLIENT_ID", "default-client"))
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
            os.getenv("RSLOGIC_RSTOOLS_EXECUTABLE", CONFIG.rstools.executable_path or ""),
            os.getenv("RSLOGIC_RSTOOLS_EXECUTABLE_ARGS", CONFIG.rstools.executable_args),
        )
        self.data_root = Path(os.getenv("RSLOGIC_DATA_ROOT", os.getenv("RSLOGIC_RSTOOLS_WORKING_ROOT", CONFIG.rstools.working_root)))
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.file_executor = FileExecutor(self.db, self.data_root)
        self._job_lock = threading.Lock()
        self._step_heartbeat_seconds = max(
            int(os.getenv("RSLOGIC_CLIENT_STEP_HEARTBEAT_SECONDS", "3")),
            1,
        )

    @staticmethod
    def _configure_logging() -> None:
        level_name = os.getenv("RSLOGIC_CLIENT_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logger = logging.getLogger("rslogic.client.runtime")
        logger.setLevel(level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s"
                )
            )
            logger.addHandler(handler)
        logger.propagate = False

    def _sdk_client(self) -> object:
        if not _SDK_AVAILABLE:
            raise RuntimeError(
                "realityscan_sdk is not installed. Install it in this environment to run sdk commands."
            )
        return RealityScanClient(
            base_url=os.getenv("RSLOGIC_RSTOOLS_SDK_BASE_URL", CONFIG.rstools.sdk_base_url or "http://127.0.0.1:8000"),
            client_id=self.sdk_client_id,
            auth_token=os.getenv("RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN", CONFIG.rstools.sdk_auth_token or ""),
            app_token=os.getenv("RSLOGIC_RSTOOLS_SDK_APP_TOKEN", CONFIG.rstools.sdk_app_token or "123"),
            verify_tls=False,
        )

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
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()

        while not self.stop_event.is_set():
            with contextlib.suppress(Exception):
                self.node_guard.ensure_running()
            payload = self.redis_bus.pop_command(self.client_id, CONFIG.queue.poll_interval_seconds)
            if not payload:
                continue
            if payload.get("type") != "job":
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
                continue
            self._run_job(payload)

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            self.redis_bus.heartbeat(self.client_id, {"status": "alive", "service": "rslogic-client"})
            with contextlib.suppress(Exception):
                self.node_guard.ensure_running()
            time.sleep(5)

    def _safe_preview(self, value: Any, *, max_len: int = 1400) -> str:
        text = repr(value)
        if len(text) <= max_len:
            return text
        return f"{text[:max_len]}…(+{len(text)-max_len} chars)"

    def _start_step_heartbeat(
        self,
        *,
        job_id: str,
        group_id: str | None,
        step_index: int,
        total_steps: int,
        step_action: str,
        step_kind: str,
    ) -> tuple[threading.Event, threading.Thread, float]:
        started_at = time.monotonic()
        stop_event = threading.Event()

        def report_loop() -> None:
            while not stop_event.wait(self._step_heartbeat_seconds):
                elapsed = round(time.monotonic() - started_at, 2)
                progress = ((step_index - 1) / max(1, total_steps)) * 100.0
                self._report_progress(
                    job_id=job_id,
                    group_id=group_id,
                    progress=progress,
                    status="running",
                    message=f"step {step_index}/{total_steps} in_progress: {step_action}",
                    result_summary={
                        "phase": "step_heartbeat",
                        "step_index": step_index,
                        "step_kind": step_kind,
                        "step_action": step_action,
                        "elapsed_seconds": elapsed,
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
    ) -> None:
        payload = {
            "job_id": job_id,
            "group_id": group_id,
            "status": status,
            "progress": progress,
            "message": message,
        }
        if result_summary is not None:
            payload["result_summary"] = result_summary
        self.redis_bus.publish_result(self.client_id, payload)

    def _run_job(self, payload: dict) -> None:
        job_id = str(payload.get("job_id"))
        group_id = self._ensure_group_id(payload.get("group_id"))
        steps = payload.get("steps", [])
        step_objects = [Step.model_validate(raw_step) for raw_step in steps]
        sdk_needed = any(step.kind == "sdk" for step in step_objects)
        sdk_client = self._sdk_client() if sdk_needed else None
        executor = StepExecutor(sdk_client=sdk_client, file_executor=self.file_executor)
        try:
            executor.begin_job(job_id, group_id=group_id)
            self.node_guard.start()
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
                result_summary={"phase": "job_start"},
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
                    heartbeat_stop, heartbeat_thread, heartbeat_started = self._start_step_heartbeat(
                        job_id=job_id,
                        group_id=group_id,
                        step_index=idx,
                        total_steps=len(steps),
                        step_action=step.action,
                        step_kind=step.kind,
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
                    step_time = round(time.monotonic() - step_started, 3)
                except Exception as exc:
                    step_error = exc
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
                self.db.upsert_processing_job(
                    job_id=job_id,
                    image_group_id=group_id,
                    status="running",
                    progress=progress,
                    message=f"step {idx}/{len(steps)} ok: {step.action} ({step_time}s)",
                    result_summary={
                        "last_result": res,
                        "last_step": {
                            "index": idx,
                            "kind": step.kind,
                            "action": step.action,
                            "duration_seconds": step_time,
                            "result_type": type(res).__name__,
                            "heartbeat_seconds": heartbeat_seconds,
                        },
                    },
                )
                self._report_progress(
                    job_id=job_id,
                    group_id=group_id,
                    progress=progress,
                    message=f"step {idx}/{len(steps)} ok: {step.action} ({step_time}s)",
                    result_summary={
                        "phase": "step_complete",
                        "step_index": idx,
                        "step_kind": step.kind,
                        "step_action": step.action,
                        "duration_seconds": step_time,
                        "result_type": type(res).__name__,
                    },
                )

            self.db.upsert_processing_job(
                job_id=job_id,
                image_group_id=group_id,
                status="completed",
                progress=100,
                message="completed",
            )
            self.redis_bus.publish_result(self.client_id, {"job_id": job_id, "status": "completed", "progress": 100, "message": "completed"})
        except Exception as exc:
            self.db.upsert_processing_job(
                job_id=job_id,
                image_group_id=group_id,
                status="failed",
                progress=0,
                message=str(exc),
            )
            self._report_progress(job_id=job_id, group_id=group_id, progress=0, message=str(exc))
            self.redis_bus.publish_result(self.client_id, {"job_id": job_id, "status": "failed", "progress": 0, "message": str(exc)})
        finally:
            executor.end_job(job_id)
            if sdk_client is not None:
                with contextlib.suppress(Exception):
                    sdk_client.close()
            self._job_lock.release()

    def _shutdown(self, *_args) -> None:
        self.stop_event.set()
        self.node_guard.stop()


def run_forever() -> None:
    ClientRuntime().run()


def main() -> None:
    run_forever()
