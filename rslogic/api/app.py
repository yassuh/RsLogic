"""FastAPI endpoints for RsLogic job and data management."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import List, Optional
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from config import load_config
from rslogic.jobs.service import ImageFilter, ImageUploadOrchestrator, JobOrchestrator, JobStatus
from rslogic.services.ingestion import ImageIngestionService
from rslogic.storage import StorageRepository
from rslogic.api import schemas

app = FastAPI(title="RsLogic API")
_config = load_config()
_default_group_name = _config.default_group_name
logger = logging.getLogger("rslogic.api")

_cors_origins = os.getenv("RSLOGIC_CORS_ORIGINS", "*").strip()
if _cors_origins == "*" or not _cors_origins:
    _allowed_origins = ["*"]
else:
    _allowed_origins = [value.strip() for value in _cors_origins.split(",") if value.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

_repository = StorageRepository()
_job_service = JobOrchestrator(
    repository=_repository,
    max_workers=_config.queue.worker_count,
)
_upload_job_service = ImageUploadOrchestrator(
    repository=_repository,
    max_workers=_config.queue.worker_count,
)
_ingest_service = ImageIngestionService(repository=_repository)
_terminal_job_states = {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value}


@app.on_event("startup")
def startup_event() -> None:
    logger.info(
        "API startup processing_backend=%s upload_backend=%s local_workers=%s redis_queue_base=%s",
        _job_service.backend,
        _upload_job_service.backend,
        _config.queue.start_local_workers,
        _config.queue.redis_queue_key,
    )


@app.on_event("shutdown")
def shutdown_event() -> None:
    logger.info("API shutdown starting")
    _job_service.close()
    _upload_job_service.close()
    logger.info("API shutdown complete")


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    request_id = uuid4().hex[:12]
    started = time.perf_counter()
    logger.info(
        "HTTP start request_id=%s method=%s path=%s query=%s client=%s",
        request_id,
        request.method,
        request.url.path,
        request.url.query,
        request.client.host if request.client else "-",
    )
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.exception(
            "HTTP error request_id=%s method=%s path=%s elapsed_ms=%.2f",
            request_id,
            request.method,
            request.url.path,
            elapsed_ms,
        )
        raise

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    logger.info(
        "HTTP done request_id=%s method=%s path=%s status=%s elapsed_ms=%.2f",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    response.headers["x-request-id"] = request_id
    return response


def _resolve_group_name(group_name: Optional[str]) -> str:
    value = (group_name or "").strip()
    return value or _default_group_name


async def _persist_upload_to_temp(upload_file: UploadFile) -> Path:
    suffix = Path(upload_file.filename or "").suffix

    def _copy() -> Path:
        started = time.perf_counter()
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            upload_file.file.seek(0)
            shutil.copyfileobj(upload_file.file, tmp, length=8 * 1024 * 1024)
            tmp_path = Path(tmp.name)
        size = tmp_path.stat().st_size
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.debug(
            "Upload temp persisted filename=%s temp_path=%s size_bytes=%s elapsed_ms=%.2f",
            upload_file.filename,
            str(tmp_path),
            size,
            elapsed_ms,
        )
        return tmp_path

    return await asyncio.to_thread(_copy)


def _job_to_payload(job) -> dict:
    return {
        "id": job.id,
        "status": job.status,
        "progress": job.progress,
        "message": job.message,
        "filters": job.filters,
        "result_summary": getattr(job, "result_summary", None),
        "created_at": job.created_at.isoformat() if getattr(job, "created_at", None) else None,
        "updated_at": job.updated_at.isoformat() if getattr(job, "updated_at", None) else None,
    }


def _cleanup_temp_paths(paths: list[Path]) -> None:
    for tmp_file_path in paths:
        tmp_file_path.unlink(missing_ok=True)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs", response_model=schemas.JobModel)
def create_job(payload: schemas.JobCreateRequest):
    resolved_group_name = _resolve_group_name(payload.group_name)
    logger.info("Create processing job requested group_name=%s", resolved_group_name)
    filters = ImageFilter(
        group_name=resolved_group_name,
        drone_type=payload.drone_type,
        start_time=payload.start_time,
        end_time=payload.end_time,
        min_latitude=payload.min_latitude,
        max_latitude=payload.max_latitude,
        min_longitude=payload.min_longitude,
        max_longitude=payload.max_longitude,
        max_images=payload.max_images,
        sdk_imagery_folder=payload.sdk_imagery_folder,
        sdk_project_path=payload.sdk_project_path,
        sdk_include_subdirs=payload.sdk_include_subdirs,
        sdk_detector_sensitivity=payload.sdk_detector_sensitivity,
        sdk_camera_prior_accuracy_xyz=payload.sdk_camera_prior_accuracy_xyz,
        sdk_camera_prior_accuracy_yaw_pitch_roll=payload.sdk_camera_prior_accuracy_yaw_pitch_roll,
        sdk_run_align=payload.sdk_run_align,
        sdk_run_normal_model=payload.sdk_run_normal_model,
        sdk_run_ortho_projection=payload.sdk_run_ortho_projection,
        sdk_task_timeout_seconds=payload.sdk_task_timeout_seconds,
    )
    job_id = _job_service.submit_job(resolved_group_name, filters)
    job = _job_service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=500, detail="failed to create job")
    return job


@app.get("/jobs", response_model=List[schemas.JobModel])
def list_jobs(
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, gt=0),
):
    logger.debug("List jobs requested status=%s limit=%s", status, limit)
    parsed_status = None
    if status is not None:
        try:
            parsed_status = JobStatus(status)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid status") from exc
    return _job_service.list_jobs(status=parsed_status, limit=limit)


@app.get("/jobs/{job_id}", response_model=schemas.JobModel)
def get_job(job_id: str):
    logger.debug("Get job requested job_id=%s", job_id)
    job = _job_service.get_job(job_id)
    if job is None:
        job = _upload_job_service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    logger.info("Cancel job requested job_id=%s", job_id)
    job = _repository.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    is_upload_batch = isinstance(job.filters, dict) and job.filters.get("type") == "upload_batch"
    if is_upload_batch:
        _upload_job_service.cancel_job(job_id)
    else:
        _job_service.cancel_job(job_id)
    return {"status": "cancellation requested", "job_id": job_id}


@app.get("/images", response_model=List[schemas.ImageModel])
def list_images(
    group_name: Optional[str] = Query(default=None),
    drone_type: Optional[str] = Query(default=None),
    min_latitude: Optional[float] = Query(default=None),
    max_latitude: Optional[float] = Query(default=None),
    min_longitude: Optional[float] = Query(default=None),
    max_longitude: Optional[float] = Query(default=None),
    limit: int = Query(default=500, gt=0),
):
    logger.debug(
        "List images requested group_name=%s drone_type=%s limit=%s",
        group_name,
        drone_type,
        limit,
    )
    return _repository.list_images(
        group_name=group_name,
        drone_type=drone_type,
        min_lat=min_latitude,
        max_lat=max_latitude,
        min_lon=min_longitude,
        max_lon=max_longitude,
        limit=limit,
    )


@app.post("/images/ingest", response_model=schemas.ImageModel)
def ingest_from_s3(payload: schemas.IngestRequest):
    resolved_group_name = _resolve_group_name(payload.group_name)
    logger.info(
        "Ingest from S3 requested group_name=%s object_key=%s",
        resolved_group_name,
        payload.object_key,
    )
    image_id = _ingest_service.ingest_from_s3(
        group_name=resolved_group_name,
        object_key=payload.object_key,
        extra=payload.extra,
    )
    image = _repository.get_image(image_id)
    if image is None:
        raise HTTPException(status_code=500, detail="ingested image not found")
    return image


@app.post("/images/ingest/waiting", response_model=schemas.WaitingIngestResponse)
def ingest_waiting_bucket(payload: schemas.WaitingIngestRequest):
    group_override = (payload.group_name or "").strip() or None
    logger.info(
        "Waiting ingest requested group_override=%s prefix=%s limit=%s concurrency=%s override_existing=%s",
        group_override or "-",
        payload.prefix,
        payload.limit,
        payload.concurrency,
        payload.override_existing,
    )
    try:
        summary = _ingest_service.ingest_waiting_bucket_metadata(
            group_name=group_override,
            prefix=payload.prefix,
            limit=payload.limit,
            concurrency=payload.concurrency,
            override_existing=payload.override_existing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("Waiting ingest failed")
        raise HTTPException(status_code=500, detail="failed to ingest waiting bucket metadata")
    return summary


@app.post("/images/upload", response_model=schemas.JobModel)
async def upload_and_ingest_image(
    group_name: Optional[str] = Form(default=None),
    batch_id: Optional[str] = Form(default=None),
    files: Optional[list[UploadFile]] = File(default=None),
    file: Optional[UploadFile] = File(default=None),
    prefix: str | None = Form(default=None),
    extra: str | None = Form(default=None),
    resume: bool = Form(default=True),
    upload_concurrency: int = Form(default=24),
):
    resolved_group_name = _resolve_group_name(group_name)
    logger.info(
        "Upload request received group_name=%s batch_id=%s prefix=%s upload_concurrency=%s",
        resolved_group_name,
        batch_id,
        prefix,
        upload_concurrency,
    )
    if files and file is not None:
        raise HTTPException(status_code=400, detail="use either file or files, not both")

    uploaded_files = list(files or [])
    if file is not None:
        uploaded_files.append(file)

    if not uploaded_files:
        raise HTTPException(status_code=400, detail="missing file")
    if any(not upload_file.filename for upload_file in uploaded_files):
        raise HTTPException(status_code=400, detail="all uploaded files must include a filename")
    if any(upload_file.filename.endswith("/") for upload_file in uploaded_files if upload_file.filename):
        raise HTTPException(status_code=400, detail="invalid filename")
    if upload_concurrency < 1:
        raise HTTPException(status_code=400, detail="upload_concurrency must be at least 1")

    parsed_extra: dict[str, str] | None = None
    if extra:
        try:
            parsed = json.loads(extra)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="invalid extra JSON payload") from exc
        if not isinstance(parsed, dict):
            raise HTTPException(status_code=400, detail="extra must be a JSON object")
        parsed_extra = {str(k): str(v) for k, v in parsed.items()}

    tmp_file_paths: list[Path] = []
    try:
        logger.info(
            "Persisting upload payload to temp files batch_id=%s file_count=%s",
            batch_id or "-",
            len(uploaded_files),
        )
        persisted_paths = await asyncio.gather(
            *(_persist_upload_to_temp(upload_file) for upload_file in uploaded_files)
        )
        tmp_file_paths.extend(persisted_paths)
        logger.info(
            "Temp persistence complete batch_id=%s temp_file_count=%s",
            batch_id or "-",
            len(tmp_file_paths),
        )

        job_id = _upload_job_service.submit_batch(
            group_name=resolved_group_name,
            local_paths=tmp_file_paths,
            prefix=prefix,
            extra=parsed_extra,
            resume=resume,
            concurrency=upload_concurrency,
            batch_id=batch_id,
        )
        logger.info(
            "Upload batch queued job_id=%s group_name=%s file_count=%s",
            job_id,
            resolved_group_name,
            len(tmp_file_paths),
        )
        job = _upload_job_service.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=500, detail="failed to queue upload batch")
        return job
    except ValueError as exc:
        logger.warning("Upload request rejected batch_id=%s reason=%s", batch_id or "-", str(exc))
        _cleanup_temp_paths(tmp_file_paths)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("Upload request failed batch_id=%s", batch_id or "-")
        _cleanup_temp_paths(tmp_file_paths)
        raise HTTPException(status_code=500, detail="failed to queue upload batch")
    finally:
        for upload_file in uploaded_files:
            await upload_file.close()


@app.get("/images/upload/{batch_id}", response_model=schemas.JobModel)
def get_upload_batch(batch_id: str):
    logger.debug("Get upload batch requested batch_id=%s", batch_id)
    job = _upload_job_service.get_job(batch_id)
    if job is None:
        raise HTTPException(status_code=404, detail="upload batch not found")
    return job


@app.post("/images/upload/prepare", response_model=schemas.JobModel)
def prepare_upload_job(payload: schemas.UploadPrepareRequest):
    resolved_group_name = _resolve_group_name(payload.group_name)
    logger.info(
        "Prepare upload batch requested group_name=%s prefix=%s concurrency=%s resume=%s",
        resolved_group_name,
        payload.prefix,
        payload.upload_concurrency,
        payload.resume,
    )
    try:
        job_id = _upload_job_service.prepare_batch(
            group_name=resolved_group_name,
            prefix=payload.prefix,
            resume=payload.resume,
            concurrency=payload.upload_concurrency,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    job = _upload_job_service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=500, detail="failed to prepare upload batch")
    logger.info("Prepare upload batch created job_id=%s group_name=%s", job_id, resolved_group_name)
    return job


@app.websocket("/ws/upload")
async def upload_files_via_websocket(websocket: WebSocket):
    await websocket.accept()
    logger.info("Upload websocket connected client=%s", websocket.client.host if websocket.client else "-")

    batch_id: Optional[str] = None
    group_name: Optional[str] = None
    prefix: Optional[str] = None
    resume: bool = True
    upload_concurrency: int = 24
    extra: Optional[dict[str, str]] = None
    temp_paths: list[Path] = []
    current_file_handle = None
    current_file_path: Optional[Path] = None
    current_file_name: Optional[str] = None
    current_file_size: int = 0
    current_last_progress: int = 0
    started = False

    async def _send_error(message: str):
        await websocket.send_json({"type": "error", "message": message})

    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                logger.info("Upload websocket disconnected batch_id=%s", batch_id or "-")
                break

            raw_text = message.get("text")
            raw_bytes = message.get("bytes")

            if raw_text is not None:
                try:
                    payload = json.loads(raw_text)
                except json.JSONDecodeError:
                    await _send_error("invalid JSON control message")
                    continue

                message_type = payload.get("type")
                if message_type == "start":
                    if started:
                        await _send_error("start already received")
                        continue
                    started = True
                    batch_id = str(payload.get("batch_id") or "").strip() or None
                    group_name = _resolve_group_name(payload.get("group_name"))
                    prefix = payload.get("prefix")
                    resume = bool(payload.get("resume", True))
                    upload_concurrency = int(payload.get("upload_concurrency", 24))
                    if upload_concurrency < 1:
                        upload_concurrency = 1
                    raw_extra = payload.get("extra")
                    if isinstance(raw_extra, dict):
                        extra = {str(k): str(v) for k, v in raw_extra.items()}
                    else:
                        extra = None
                    logger.info(
                        "Upload websocket start batch_id=%s group_name=%s prefix=%s concurrency=%s",
                        batch_id or "-",
                        group_name,
                        prefix,
                        upload_concurrency,
                    )
                    await websocket.send_json(
                        {
                            "type": "ack_start",
                            "batch_id": batch_id,
                            "group_name": group_name,
                            "upload_concurrency": upload_concurrency,
                        }
                    )
                    continue

                if not started:
                    await _send_error("start must be sent before other messages")
                    continue

                if message_type == "file_start":
                    if current_file_handle is not None:
                        await _send_error("file_start received while previous file is open")
                        continue
                    incoming_name = str(payload.get("name") or "").strip()
                    if not incoming_name:
                        await _send_error("file_start requires name")
                        continue
                    suffix = Path(incoming_name).suffix
                    current_file_name = incoming_name
                    current_file_size = 0
                    current_last_progress = 0
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                    current_file_path = Path(tmp.name)
                    current_file_handle = tmp
                    logger.info(
                        "Upload websocket file start batch_id=%s file=%s temp=%s",
                        batch_id or "-",
                        current_file_name,
                        str(current_file_path),
                    )
                    await websocket.send_json({"type": "ack_file_start", "name": current_file_name})
                    continue

                if message_type == "file_end":
                    if current_file_handle is None or current_file_path is None:
                        await _send_error("file_end received without open file")
                        continue
                    current_file_handle.close()
                    current_file_handle = None
                    temp_paths.append(current_file_path)
                    logger.info(
                        "Upload websocket file end batch_id=%s file=%s size_bytes=%s",
                        batch_id or "-",
                        current_file_name or "-",
                        current_file_size,
                    )
                    await websocket.send_json(
                        {
                            "type": "file_received",
                            "name": current_file_name,
                            "size_bytes": current_file_size,
                            "file_count": len(temp_paths),
                        }
                    )
                    current_file_path = None
                    current_file_name = None
                    current_file_size = 0
                    current_last_progress = 0
                    continue

                if message_type == "complete":
                    if current_file_handle is not None:
                        await _send_error("cannot complete while file is still open")
                        continue
                    if group_name is None:
                        await _send_error("upload start context missing group_name")
                        continue
                    if not temp_paths:
                        await _send_error("no files uploaded")
                        continue
                    logger.info(
                        "Upload websocket complete received batch_id=%s file_count=%s",
                        batch_id or "-",
                        len(temp_paths),
                    )
                    queued_job_id = _upload_job_service.submit_batch(
                        group_name=group_name,
                        local_paths=temp_paths,
                        prefix=prefix,
                        extra=extra,
                        resume=resume,
                        concurrency=upload_concurrency,
                        batch_id=batch_id,
                    )
                    temp_paths = []
                    await websocket.send_json({"type": "queued", "job_id": queued_job_id})
                    await websocket.close(code=1000)
                    logger.info("Upload websocket queued job_id=%s", queued_job_id)
                    return

                if message_type == "abort":
                    logger.warning("Upload websocket abort received batch_id=%s", batch_id or "-")
                    await websocket.send_json({"type": "aborted"})
                    await websocket.close(code=1000)
                    return

                await _send_error(f"unsupported message type: {message_type}")
                continue

            if raw_bytes is not None:
                if current_file_handle is None:
                    await _send_error("binary frame received without file_start")
                    continue
                current_file_handle.write(raw_bytes)
                current_file_size += len(raw_bytes)
                if current_file_size - current_last_progress >= 16 * 1024 * 1024:
                    current_last_progress = current_file_size
                    await websocket.send_json(
                        {
                            "type": "file_progress",
                            "name": current_file_name,
                            "size_bytes": current_file_size,
                        }
                    )
                continue

            await _send_error("unsupported websocket frame")
    except WebSocketDisconnect:
        logger.info("Upload websocket disconnected unexpectedly batch_id=%s", batch_id or "-")
    except Exception:
        logger.exception("Upload websocket failure batch_id=%s", batch_id or "-")
        try:
            await _send_error("upload websocket failed")
        except Exception:
            pass
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
    finally:
        if current_file_handle is not None:
            current_file_handle.close()
        if temp_paths:
            _cleanup_temp_paths(temp_paths)


@app.websocket("/ws/jobs/{job_id}")
async def stream_job_status(websocket: WebSocket, job_id: str):
    await websocket.accept()
    logger.info("WebSocket connected job_id=%s client=%s", job_id, websocket.client.host if websocket.client else "-")
    last_status: Optional[str] = None
    job_wait_deadline = time.perf_counter() + 45.0
    try:
        while True:
            job = _repository.get_job(job_id)
            if job is None:
                if time.perf_counter() < job_wait_deadline:
                    await websocket.send_json(
                        {"id": job_id, "status": "waiting_for_job_creation", "progress": 0.0}
                    )
                    await asyncio.sleep(0.5)
                    continue
                await websocket.send_json({"error": "job not found", "job_id": job_id})
                await websocket.close(code=4404)
                logger.warning("WebSocket job not found after wait job_id=%s", job_id)
                return

            await websocket.send_json(_job_to_payload(job))
            if job.status != last_status:
                logger.info("WebSocket job status update job_id=%s status=%s progress=%.2f", job_id, job.status, job.progress)
                last_status = job.status
            if job.status in _terminal_job_states:
                await websocket.close(code=1000)
                logger.info("WebSocket closing on terminal job state job_id=%s status=%s", job_id, job.status)
                return
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected job_id=%s", job_id)
        return


@app.post("/groups", response_model=schemas.GroupModel)
def create_group(payload: schemas.GroupCreateRequest):
    try:
        return _repository.create_image_group(
            name=payload.name,
            description=payload.description,
            extra=payload.extra,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/groups", response_model=List[schemas.GroupModel])
def list_groups(limit: int = Query(default=100, gt=0)):
    return _repository.list_image_groups(limit=limit)
