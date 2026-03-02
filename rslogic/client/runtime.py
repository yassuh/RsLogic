"""Client runtime service."""

from __future__ import annotations

import contextlib
import os
import signal
import threading
import time
from pathlib import Path
from dotenv import load_dotenv

from realityscan_sdk.client import RealityScanClient

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
        self.stop_event = threading.Event()
        self.client_id = os.getenv("RSLOGIC_CLIENT_ID", os.getenv("CLIENT_ID", "default-client"))
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

    def _sdk_client(self) -> RealityScanClient:
        return RealityScanClient(
            base_url=os.getenv("RSLOGIC_RSTOOLS_SDK_BASE_URL", CONFIG.rstools.sdk_base_url or "http://127.0.0.1:8000"),
            client_id=os.getenv("RSLOGIC_RSTOOLS_SDK_CLIENT_ID", CONFIG.rstools.sdk_client_id or self.client_id),
            auth_token=os.getenv("RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN", CONFIG.rstools.sdk_auth_token or ""),
            app_token=os.getenv("RSLOGIC_RSTOOLS_SDK_APP_TOKEN", CONFIG.rstools.sdk_app_token or "123"),
            verify_tls=False,
        )

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
                self.redis_bus.publish_result(
                    self.client_id,
                    {"job_id": job_id, "status": "rejected", "progress": 0, "message": "client is already busy"},
                )
                self.db.upsert_processing_job(
                    job_id=job_id,
                    image_group_id=payload.get("group_id"),
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

    def _report_progress(self, *, job_id: str, group_id: str | None, progress: float, message: str) -> None:
        self.redis_bus.publish_result(
            self.client_id,
            {
                "job_id": job_id,
                "group_id": group_id,
                "status": "running",
                "progress": progress,
                "message": message,
            },
        )

    def _run_job(self, payload: dict) -> None:
        job_id = str(payload.get("job_id"))
        group_id = payload.get("group_id")
        steps = payload.get("steps", [])
        sdk_client = self._sdk_client()
        executor = StepExecutor(sdk_client=sdk_client, file_executor=self.file_executor)
        try:
            executor.begin_job(job_id)
            self.node_guard.start()
            self.db.upsert_processing_job(
                job_id=job_id,
                image_group_id=group_id,
                status="running",
                progress=0.0,
                message=f"started by {self.client_id}",
                filters={"steps": steps},
            )
            self._report_progress(job_id=job_id, group_id=group_id, progress=0.0, message="started")
            for idx, raw_step in enumerate(steps, start=1):
                step = Step.model_validate(raw_step)
                res = executor.execute(step, job_id=job_id, group_id=group_id)
                progress = (idx / max(1, len(steps))) * 100.0
                self.db.upsert_processing_job(
                    job_id=job_id,
                    image_group_id=group_id,
                    status="running",
                    progress=progress,
                    message=f"step {idx}/{len(steps)} ok: {step.action}",
                    result_summary={"last_result": res},
                )
                self._report_progress(
                    job_id=job_id,
                    group_id=group_id,
                    progress=progress,
                    message=f"step {idx}/{len(steps)} ok: {step.action}",
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
            sdk_client.close()
            self._job_lock.release()

    def _shutdown(self, *_args) -> None:
        self.stop_event.set()
        self.node_guard.stop()


def run_forever() -> None:
    ClientRuntime().run()


def main() -> None:
    run_forever()
