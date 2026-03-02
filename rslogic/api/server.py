"""Orchestrator API and job dispatcher."""

from __future__ import annotations

import contextlib
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


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            return float(value)
    return None


def _coerce_task_id(task: dict[str, Any]) -> str | None:
    task_id = task.get("taskID") or task.get("taskId") or task.get("id")
    if task_id is None:
        return None
    text = str(task_id).strip()
    return text or None


def _coerce_task_items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        if isinstance(value.get("tasks"), list):
            value = value["tasks"]
        elif _coerce_task_id(value) is not None:
            value = [value]
        else:
            return []
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        if _coerce_task_id(item) is None:
            continue
        tasks.append(item)
    return tasks


def _extract_task_status(result_summary: dict[str, object] | None) -> dict[str, object] | None:
    if not result_summary:
        return None
    by_id = result_summary.get("_task_state_by_id")
    if isinstance(by_id, dict) and by_id:
        tasks = [task for task in by_id.values() if isinstance(task, dict)]
        tasks.sort(key=lambda item: str(item.get("taskID", "")))
        return {
            "source": "merged_by_id",
            "task_count": len(tasks),
            "tasks": tasks,
        }

    for key in ("task_state", "final_task_state", "task_status"):
        tasks = _coerce_task_items(result_summary.get(key))
        if tasks:
            return {"source": key, "task_count": len(tasks), "tasks": tasks}
    for key in ("task_state", "task_status"):
        value = result_summary.get(key)
        if isinstance(value, dict) and isinstance(value.get("tasks"), list):
            return {"source": key, "tasks": value.get("tasks")}

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


def _merge_task_states(
    existing: dict[str, Any],
    incoming: list[dict[str, Any]],
    *, now: float | None,
) -> dict[str, Any]:
    by_id = {}
    raw_by_id = existing.get("_task_state_by_id")
    if isinstance(raw_by_id, dict):
        for task_id, task_value in raw_by_id.items():
            if not isinstance(task_value, dict):
                continue
            by_id[str(task_id)] = dict(task_value)

    if not by_id:
        for key in ("task_state", "final_task_state", "task_status"):
            for task in _coerce_task_items(existing.get(key)):
                task_id = _coerce_task_id(task)
                if task_id is None:
                    continue
                task = dict(task)
                task.setdefault("task_last_seen", now)
                by_id[task_id] = task

    now_ts = now or _as_float(time.time())
    for task in incoming:
        task_id = _coerce_task_id(task)
        if task_id is None:
            continue
        task_seen = _as_float(task.get("task_last_seen"))
        if task_seen is None:
            task_seen = now_ts
        task = dict(task)
        task["task_last_seen"] = task_seen
        task.setdefault("created_at", task_seen)
        task["updated_at"] = task_seen

        previous = by_id.get(task_id)
        if previous:
            prev_seen = _as_float(previous.get("task_last_seen"))
            if prev_seen is not None and prev_seen > task_seen:
                continue
            merged = dict(previous)
            merged.update(task)
            by_id[task_id] = merged
        else:
            by_id[task_id] = task

    tasks = list(by_id.values())
    tasks.sort(key=lambda item: str(item.get("taskID", "")))
    merged = dict(existing)
    merged["_task_state_by_id"] = by_id
    merged["task_state"] = {"tasks": tasks}
    merged["task_state_last_seen"] = now_ts
    return merged


def _merge_project_status(
    existing: dict[str, Any],
    incoming: dict[str, Any],
    *, now: float | None,
) -> dict[str, Any]:
    now_ts = now or _as_float(time.time())
    merged = dict(existing)
    incoming = dict(incoming)
    existing_seen = _as_float(existing.get("project_status_last_seen"))
    incoming_seen = _as_float(incoming.get("project_last_seen")) or now_ts
    if existing_seen is not None and existing_seen > incoming_seen:
        return existing
    payload_status = merged.get("project_status")
    if isinstance(payload_status, dict):
        payload_status = dict(payload_status)
        payload_status.update(incoming)
        payload_status.setdefault("project_last_seen", incoming_seen)
        merged["project_status"] = payload_status
    else:
        incoming.setdefault("project_last_seen", incoming_seen)
        merged["project_status"] = incoming
    merged["project_status_last_seen"] = incoming_seen
    return merged


def _read_existing_result_summary(job_id: str) -> dict[str, Any] | None:
    with _db.session() as session:
        job = session.get(_db.ProcessingJob, job_id)
        if job is None:
            return None
        if isinstance(job.result_summary, dict):
            return dict(job.result_summary)
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
        merged_result_summary = result_summary
        timestamp = _as_float(payload.get("timestamp"))
        incoming_task_state = payload.get("task_state")
        incoming_project_status = payload.get("project_status")

        if merged_result_summary is not None and not isinstance(merged_result_summary, dict):
            merged_result_summary = None
        if merged_result_summary is None:
            merged_result_summary = {}

        if isinstance(result_summary, dict):
            merged_result_summary.update(result_summary)

        existing = _read_existing_result_summary(job_id)
        if existing:
            merged_result_summary = dict(existing | merged_result_summary)

        if isinstance(incoming_task_state, dict):
            tasks = _coerce_task_items(incoming_task_state)
            merged_result_summary = _merge_task_states(
                merged_result_summary,
                tasks,
                now=timestamp,
            )
        elif isinstance(result_summary, dict):
            tasks = _coerce_task_items(result_summary.get("task_state"))
            if tasks:
                merged_result_summary = _merge_task_states(
                    merged_result_summary,
                    tasks,
                    now=timestamp,
                )

        if isinstance(incoming_project_status, dict):
            merged_result_summary = _merge_project_status(
                merged_result_summary,
                dict(incoming_project_status),
                now=timestamp,
            )
        elif isinstance(result_summary, dict):
            old_project_status = result_summary.get("project_status")
            if isinstance(old_project_status, dict):
                merged_result_summary = _merge_project_status(
                    merged_result_summary,
                    dict(old_project_status),
                    now=timestamp,
                )
        _db.upsert_processing_job(
            job_id=job_id,
            image_group_id=payload.get("group_id"),
            status=status,
            progress=progress,
            message=message,
            result_summary=merged_result_summary,
            filters=None,
        )


@app.on_event("startup")
def _startup() -> None:
    th = threading.Thread(target=_consume_results, name="orchestrator-result-consumer", daemon=True)
    th.start()
    _result_threads["consumer"] = th


def main() -> None:
    uvicorn.run("rslogic.api.server:app", host="0.0.0.0", port=8000, log_level="info")
