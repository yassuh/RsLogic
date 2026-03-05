"""Orchestrator API and job dispatcher."""

from __future__ import annotations

import argparse
import contextlib
from pathlib import Path
import threading
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from rslogic.config import CONFIG

from rslogic.api.web_models import IngestStartRequest, UploadStartRequest, WorkflowImportRequest
from rslogic.api.web_ops import OperationRegistry
from rslogic.common.db import LabelDbStore
from rslogic.common.redis_bus import RedisBus
from rslogic.common.schemas import JobRequest
from rslogic.tui.job_builder import action_catalog, fragment_catalog, read_workflow_path_or_inline


app = FastAPI(title="rslogic-orchestrator", version="0.2.0")

_bus = RedisBus(CONFIG.queue.redis_url, CONFIG.control.command_queue_key, CONFIG.control.result_queue_key)
_db = LabelDbStore(CONFIG.label_db.database_url, CONFIG.label_db.migration_root)
_operations = OperationRegistry()
_result_threads: dict[str, threading.Thread] = {}
_WEB_ROOT = Path(__file__).resolve().parent / "web"
_WEB_INDEX = _WEB_ROOT / "index.html"
_WEB_STATIC = _WEB_ROOT / "static"

app.mount("/static", StaticFiles(directory=str(_WEB_STATIC)), name="static")


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
        job = session.get(_db.RealityScanJob, job_id)
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


def _client_status_payload(client_id: str) -> dict[str, Any]:
    heartbeat = _bus.get_client_heartbeat(client_id)
    heartbeat_age = None
    if isinstance(heartbeat, dict):
        heartbeat_ts = _as_float(heartbeat.get("ts"))
        heartbeat_age = (_as_float(time.time()) - heartbeat_ts) if heartbeat_ts is not None else None
    return {
        "client_id": client_id,
        "queue_depth": _bus.command_queue_depth(client_id),
        "heartbeat": heartbeat,
        "heartbeat_age": round(heartbeat_age, 2) if heartbeat_age is not None else None,
        "task_status": _extract_task_status(heartbeat if isinstance(heartbeat, dict) else None),
        "project_status": _extract_project_status(heartbeat if isinstance(heartbeat, dict) else None),
    }


def _has_subdirectories(path: Path) -> bool:
    try:
        return any(child.is_dir() for child in path.iterdir())
    except OSError:
        return False


def _directory_listing(path_value: str | None) -> dict[str, Any]:
    path = Path(path_value).expanduser() if path_value else Path.cwd()
    path = path.resolve()
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"path not found: {path}")
    if not path.is_dir():
        raise HTTPException(status_code=400, detail=f"path is not a directory: {path}")

    try:
        entries = sorted((child for child in path.iterdir() if child.is_dir()), key=lambda item: item.name.lower())
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"failed to list directory: {exc}")

    return {
        "path": str(path),
        "parent": str(path.parent) if path.parent != path else None,
        "directories": [
            {
                "name": child.name,
                "path": str(child),
                "has_children": _has_subdirectories(child),
            }
            for child in entries
        ],
    }


def _raise_service_unavailable(exc: Exception) -> None:
    raise HTTPException(status_code=503, detail=str(exc))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui")


@app.get("/ui", include_in_schema=False)
def ui_index() -> FileResponse:
    return FileResponse(_WEB_INDEX)


@app.get("/ui/api/job-builder/metadata")
def web_job_builder_metadata() -> dict[str, Any]:
    return {
        "fragments": fragment_catalog(),
        "actions": action_catalog(),
    }


@app.post("/ui/api/job-builder/import")
def web_job_builder_import(payload: WorkflowImportRequest) -> dict[str, Any]:
    try:
        steps = read_workflow_path_or_inline(payload.source)
    except (OSError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"failed to load workflow: {exc}")
    return {"step_count": len(steps), "steps": steps}


@app.get("/ui/api/upload/directories")
def web_upload_directories(path: str | None = Query(default=None)) -> dict[str, Any]:
    return _directory_listing(path)


@app.get("/ui/api/operations")
def web_operation_list() -> dict[str, Any]:
    return {"operations": _operations.list_recent()}


@app.get("/ui/api/operations/{operation_id}")
def web_operation_status(operation_id: str) -> dict[str, Any]:
    operation = _operations.get(operation_id)
    if operation is None:
        raise HTTPException(status_code=404, detail=f"operation {operation_id} not found")
    return operation


@app.post("/ui/api/upload")
def web_start_upload(payload: UploadStartRequest) -> dict[str, Any]:
    operation = _operations.start_upload(payload.path)
    return operation.snapshot()


@app.post("/ui/api/ingest")
def web_start_ingest(payload: IngestStartRequest) -> dict[str, Any]:
    operation = _operations.start_ingest(group_name=payload.group_name, limit=payload.limit)
    return operation.snapshot()


@app.get("/ui/api/clients")
def web_clients() -> dict[str, Any]:
    try:
        client_ids = _bus.list_active_clients()
        return {
            "clients": [_client_status_payload(client_id) for client_id in client_ids],
        }
    except Exception as exc:
        _raise_service_unavailable(exc)


@app.get("/ui/api/clients/{client_id}")
def web_client_status(client_id: str) -> dict[str, Any]:
    try:
        return _client_status_payload(client_id)
    except Exception as exc:
        _raise_service_unavailable(exc)


@app.post("/ui/api/clients/{client_id}/clear-queues")
def web_client_clear_queues(client_id: str) -> dict[str, Any]:
    try:
        deleted = _bus.clear_client_queues(client_id)
        return {
            "client_id": client_id,
            "deleted_keys": deleted,
        }
    except Exception as exc:
        _raise_service_unavailable(exc)


@app.get("/clients")
def active_clients() -> dict[str, list[str]]:
    try:
        return {"clients": _bus.list_active_clients()}
    except Exception as exc:
        _raise_service_unavailable(exc)


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
    job_definition = payload.model_dump(exclude_none=True)

    _db.upsert_processing_job(
        job_id=job_id,
        job_name=payload.job_name,
        image_group_id=payload.group_id,
        status="queued",
        progress=0.0,
        message="queued",
        job_definition=job_definition,
    )

    envelope = {
        "type": "job",
        "job_id": job_id,
        "job_name": payload.job_name,
        "client_id": client_id,
        "group_id": payload.group_id,
        "group_name": payload.group_name,
        "target_client": payload.target_client,
        "auto_assign": payload.auto_assign,
        "steps": [s.model_dump() for s in payload.steps],
        "created_at": time.time(),
        "metadata": payload.metadata,
    }
    _bus.publish_command(client_id, envelope)
    _db.upsert_processing_job(
        job_id=job_id,
        job_name=payload.job_name,
        image_group_id=payload.group_id,
        status="dispatched",
        progress=1.0,
        message=f"dispatched to {client_id}",
        job_definition=job_definition,
    )
    return {"job_id": job_id, "job_name": payload.job_name, "client_id": client_id, "status": "dispatched"}


@app.get("/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    with _db.session() as session:
        job = session.get(_db.RealityScanJob, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"job {job_id} not found")
        result_summary = job.result_summary if isinstance(job.result_summary, dict) else None
        return {
            "job_id": job.id,
            "job_name": job.job_name,
            "status": job.status,
            "progress": job.progress,
            "message": job.message,
            "job_definition": job.job_definition,
            "result_summary": job.result_summary,
            "task_status": _extract_task_status(result_summary),
            "project_status": _extract_project_status(result_summary),
        }


@app.get("/jobs")
def list_jobs() -> list[dict[str, Any]]:
    with _db.session() as session:
        jobs = session.query(_db.RealityScanJob).order_by(_db.RealityScanJob.created_at.desc()).limit(100).all()
        return [
            {
                "job_id": job.id,
                "job_name": job.job_name,
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
            job_name=str(payload.get("job_name", "")).strip() or None,
            image_group_id=payload.get("group_id"),
            status=status,
            progress=progress,
            message=message,
            result_summary=merged_result_summary,
            job_definition=None,
        )


@app.on_event("startup")
def _startup() -> None:
    th = threading.Thread(target=_consume_results, name="orchestrator-result-consumer", daemon=True)
    th.start()
    _result_threads["consumer"] = th


def main(argv: list[str] | None = None) -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description="RsLogic operator web server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)
    display_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    print(f"rslogic web ui: http://{display_host}:{args.port}/ui", flush=True)
    uvicorn.run(
        "rslogic.api.server:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        reload=args.reload,
    )
