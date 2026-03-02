"""Orchestrator API and job dispatcher."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException
import uvicorn

from rslogic.config import CONFIG

from rslogic.common.db import LabelDbStore
from rslogic.common.redis_bus import RedisBus
from rslogic.common.schemas import JobRequest


app = FastAPI(title="rslogic-orchestrator", version="0.2.0")

_bus = RedisBus(CONFIG.queue.redis_url, CONFIG.control.command_queue_key, CONFIG.control.result_queue_key)
_db = LabelDbStore(CONFIG.label_db.database_url, CONFIG.label_db.migration_root)
_result_threads: dict[str, threading.Thread] = {}


def _extract_task_status(result_summary: dict[str, object] | None) -> dict[str, object] | None:
    if not result_summary:
        return None
    for key in ("task_state", "final_task_state", "task_status"):
        value = result_summary.get(key)
        if isinstance(value, dict) and isinstance(value.get("tasks"), list):
            return {"source": key, "tasks": value.get("tasks")}
        if isinstance(value, list):
            return {"source": key, "tasks": value}
    running = result_summary.get("running_tasks")
    completed = result_summary.get("completed_tasks")
    if isinstance(running, list) or isinstance(completed, list):
        return {
            "source": "step_heartbeat",
            "task_count": result_summary.get("task_count"),
            "running_tasks": running,
            "completed_tasks": completed,
        }
    return None


def _extract_project_status(result_summary: dict[str, object] | None) -> dict[str, object] | None:
    if not result_summary:
        return None
    for key in ("project_status", "final_project_status"):
        value = result_summary.get(key)
        if isinstance(value, dict):
            return value
    return None


def _resolve_client(request: JobRequest) -> str:
    if request.requested_client:
        active = _bus.list_active_clients()
        if active and request.requested_client not in active:
            raise RuntimeError(f"requested client is not active: {request.requested_client}")
        return request.requested_client
    if request.auto_assign:
        active = _bus.list_active_clients()
        if not active:
            raise RuntimeError("no active clients are currently available")
        return active[0]
    raise RuntimeError("requested client is required unless auto_assign is true")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/clients")
def active_clients() -> dict[str, list[str]]:
    return {"clients": _bus.list_active_clients()}


@app.post("/jobs")
def create_job(payload: JobRequest) -> dict[str, Any]:
    try:
        client_id = _resolve_client(payload)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    job_id = str(uuid.uuid4())
    if payload.group_name and not payload.group_id:
        group, _ = _db.get_or_create_group(payload.group_name)
        payload.group_id = group.id

    _db.upsert_processing_job(
        job_id=job_id,
        image_group_id=payload.group_id,
        status="queued",
        progress=0.0,
        message="queued",
        filters={"steps": [s.model_dump() for s in payload.steps], "metadata": payload.metadata},
    )

    envelope = {
        "type": "job",
        "job_id": job_id,
        "client_id": client_id,
        "group_id": payload.group_id,
        "steps": [s.model_dump() for s in payload.steps],
        "created_at": time.time(),
        "metadata": payload.metadata,
    }
    _bus.publish_command(client_id, envelope)
    _db.upsert_processing_job(
        job_id=job_id,
        image_group_id=payload.group_id,
        status="dispatched",
        progress=1.0,
        message=f"dispatched to {client_id}",
    )
    return {"job_id": job_id, "client_id": client_id, "status": "dispatched"}


@app.get("/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    with _db.session() as session:
        job = session.get(_db.ProcessingJob, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"job {job_id} not found")
        result_summary = job.result_summary if isinstance(job.result_summary, dict) else None
        return {
            "job_id": job.id,
            "status": job.status,
            "progress": job.progress,
            "message": job.message,
            "filters": job.filters,
            "result_summary": job.result_summary,
            "task_status": _extract_task_status(result_summary),
            "project_status": _extract_project_status(result_summary),
        }


@app.get("/jobs")
def list_jobs() -> list[dict[str, Any]]:
    with _db.session() as session:
        jobs = session.query(_db.ProcessingJob).order_by(_db.ProcessingJob.created_at.desc()).limit(100).all()
        return [
            {
                "job_id": job.id,
                "status": job.status,
                "progress": job.progress,
                "message": job.message,
                "updated_at": str(job.updated_at),
                "task_status": _extract_task_status(
                    job.result_summary if isinstance(job.result_summary, dict) else None
                ),
                "project_status": _extract_project_status(
                    job.result_summary if isinstance(job.result_summary, dict) else None
                ),
            }
            for job in jobs
        ]


def _consume_results() -> None:
    while True:
        payload = _bus.pop_result(timeout_s=2)
        if not payload:
            continue
        job_id = payload.get("job_id")
        if not job_id:
            continue
        status = str(payload.get("status", "unknown"))
        progress_raw = payload.get("progress", 0.0)
        try:
            progress = float(progress_raw)
        except Exception:
            progress = 0.0
        message = str(payload.get("message", ""))
        result_summary = payload.get("result_summary")
        if not isinstance(result_summary, dict):
            result_summary = payload.get("result")
        if not isinstance(result_summary, dict):
            result_summary = None
        if isinstance(payload.get("task_state"), dict):
            if result_summary is None:
                result_summary = {}
            result_summary["task_state"] = payload["task_state"]
        if isinstance(payload.get("project_status"), dict):
            if result_summary is None:
                result_summary = {}
            result_summary["project_status"] = payload["project_status"]
        _db.upsert_processing_job(
            job_id=job_id,
            image_group_id=payload.get("group_id"),
            status=status,
            progress=progress,
            message=message,
            result_summary=result_summary,
            filters=None,
        )


@app.on_event("startup")
def _startup() -> None:
    th = threading.Thread(target=_consume_results, name="orchestrator-result-consumer", daemon=True)
    th.start()
    _result_threads["consumer"] = th


def main() -> None:
    uvicorn.run("rslogic.api.server:app", host="0.0.0.0", port=8000, log_level="info")
