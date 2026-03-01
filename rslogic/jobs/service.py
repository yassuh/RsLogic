"""Processing job orchestration and filtering utilities."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional

from config import AppConfig, load_config
from rslogic.jobs.redis_queue import RedisJobQueue
from rslogic.jobs.runners import RsToolsRunner, build_runner_from_config
from rslogic.services.ingestion import ImageIngestionService
from rslogic.storage import StorageRepository

logger = logging.getLogger("rslogic.jobs")

_QUEUE_BACKEND_MEMORY = "memory"
_QUEUE_BACKEND_REDIS = "redis"
_QUEUE_TYPE_PROCESSING = "processing_job"
_QUEUE_TYPE_UPLOAD_BATCH = "upload_batch"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class ImageFilter:
    group_name: Optional[str] = None
    drone_type: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    min_latitude: Optional[float] = None
    max_latitude: Optional[float] = None
    min_longitude: Optional[float] = None
    max_longitude: Optional[float] = None
    max_images: Optional[int] = None
    sdk_imagery_folder: Optional[str] = None
    sdk_project_path: Optional[str] = None
    sdk_include_subdirs: bool = True
    sdk_detector_sensitivity: Optional[str] = "Ultra"
    sdk_camera_prior_accuracy_xyz: Optional[float] = 0.1
    sdk_camera_prior_accuracy_yaw_pitch_roll: Optional[float] = 1.0
    sdk_run_align: bool = True
    sdk_run_normal_model: bool = True
    sdk_run_ortho_projection: bool = True
    sdk_task_timeout_seconds: Optional[int] = 7200
    session_code: Optional[str] = None
    pull_s3_images: bool = True
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    s3_region: Optional[str] = None
    s3_endpoint_url: Optional[str] = None
    s3_max_files: Optional[int] = None
    s3_extensions: Optional[list[str]] = None
    s3_staging_root: Optional[str] = None


@dataclass(frozen=True)
class _UploadBatchState:
    batch_id: str
    group_name: str
    local_paths: list[Path]
    prefix: Optional[str]
    extra: Optional[Dict[str, str]]
    resume: bool
    concurrency: int


_TERMINAL_JOB_STATES = {
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.CANCELLED.value,
}


def _resolve_job_workdir(*, working_root: Path, filters: ImageFilter, job_id: str) -> Path:
    base_root = Path(working_root).expanduser()
    if filters.s3_staging_root:
        staged_root = Path(filters.s3_staging_root).expanduser()
        if staged_root.is_absolute():
            return staged_root
        return (base_root / staged_root).resolve()
    if filters.session_code:
        return (base_root / filters.session_code).resolve()
    return (base_root / job_id).resolve()


class JobOrchestrator:
    """Queue and execute photogrammetry jobs."""

    def __init__(
        self,
        repository: Optional[StorageRepository] = None,
        runner: Optional[RsToolsRunner] = None,
        max_workers: int = 2,
        config: Optional[AppConfig] = None,
        start_workers: Optional[bool] = None,
    ) -> None:
        self._config = config or load_config()
        self._repo = repository or StorageRepository()
        self._runner = runner or build_runner_from_config(self._config.rstools)
        self._max_workers = max(max_workers, 1)
        self._backend = self._normalize_backend(self._config.queue.backend)
        self._local_workers_enabled = (
            self._config.queue.start_local_workers if start_workers is None else bool(start_workers)
        )
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._running: Dict[str, Future] = {}
        self._cancelled: set[str] = set()
        self._stop_event = threading.Event()
        self._consumer_threads: list[threading.Thread] = []
        self._redis_queue: Optional[RedisJobQueue] = None

        if self._backend == _QUEUE_BACKEND_REDIS:
            queue_key = f"{self._config.queue.redis_queue_key}:processing"
            self._redis_queue = RedisJobQueue(
                redis_url=self._config.queue.redis_url,
                queue_key=queue_key,
                block_timeout_seconds=self._config.queue.redis_block_timeout_seconds,
            )
            self._redis_queue.ping()
            logger.info("Processing queue backend=redis queue=%s", queue_key)
            if self._local_workers_enabled:
                self.start_workers()
        else:
            logger.info("Processing queue backend=memory workers=%s", self._max_workers)

    @staticmethod
    def _normalize_backend(raw_backend: str) -> str:
        backend = (raw_backend or "").strip().lower()
        if backend not in {_QUEUE_BACKEND_MEMORY, _QUEUE_BACKEND_REDIS}:
            raise ValueError(
                f"Unsupported RSLOGIC_QUEUE_BACKEND='{raw_backend}'. "
                f"Expected one of: '{_QUEUE_BACKEND_MEMORY}', '{_QUEUE_BACKEND_REDIS}'."
            )
        return backend

    @property
    def backend(self) -> str:
        return self._backend

    def start_workers(self) -> None:
        if self._backend != _QUEUE_BACKEND_REDIS or self._redis_queue is None:
            return
        if self._consumer_threads:
            return

        self._stop_event.clear()
        for index in range(self._max_workers):
            thread = threading.Thread(
                target=self._redis_worker_loop,
                name=f"rslogic-processing-worker-{index + 1}",
                daemon=True,
            )
            thread.start()
            self._consumer_threads.append(thread)
        logger.info("Started processing redis workers count=%s", len(self._consumer_threads))

    def stop_workers(self, *, close_queue: bool = True) -> None:
        self._stop_event.set()
        for thread in self._consumer_threads:
            thread.join(timeout=2.0)
        self._consumer_threads.clear()
        if close_queue and self._redis_queue is not None:
            self._redis_queue.close()
            self._redis_queue = None

    def close(self) -> None:
        self.stop_workers()
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=False)

    def submit_job(self, group_name: str, filters: ImageFilter) -> str:
        filters_dict = _encode_filter_dict(filters)
        job = self._repo.create_job(group_name=group_name, status=JobStatus.QUEUED.value, filters=filters_dict)

        if self._backend == _QUEUE_BACKEND_REDIS:
            assert self._redis_queue is not None
            self._redis_queue.enqueue(
                {
                    "type": _QUEUE_TYPE_PROCESSING,
                    "job_id": job.id,
                    "filters": filters_dict,
                }
            )
        else:
            future = self._executor.submit(self._execute, job.id, filters)
            self._running[job.id] = future
            future.add_done_callback(lambda _f, jid=job.id: self._running.pop(jid, None))

        logger.info("Processing job queued job_id=%s group_name=%s backend=%s", job.id, group_name, self._backend)
        return job.id

    def _redis_worker_loop(self) -> None:
        assert self._redis_queue is not None
        while not self._stop_event.is_set():
            try:
                payload = self._redis_queue.dequeue()
            except Exception:
                time.sleep(0.5)
                continue
            if payload is None:
                continue
            if str(payload.get("type", "")) != _QUEUE_TYPE_PROCESSING:
                logger.warning("Ignoring unsupported processing queue payload type=%s", payload.get("type"))
                continue
            job_id = str(payload.get("job_id") or "").strip()
            if not job_id:
                logger.error("Dropping processing queue payload missing job_id")
                continue
            filters = _decode_filter_dict(payload.get("filters"))
            self._execute(job_id, filters)

    def _is_cancelled(self, job_id: str) -> bool:
        if job_id in self._cancelled:
            return True
        job = self._repo.get_job(job_id)
        if job is None:
            return True
        return str(job.status) == JobStatus.CANCELLED.value

    def _execute(self, job_id: str, filters: ImageFilter) -> str:
        existing = self._repo.get_job(job_id)
        if existing is None:
            logger.warning("Processing job not found at execution start job_id=%s", job_id)
            return job_id
        if str(existing.status) in _TERMINAL_JOB_STATES:
            logger.info("Processing job already terminal job_id=%s status=%s", job_id, existing.status)
            return job_id

        if self._is_cancelled(job_id):
            self._repo.update_job(job_id, status=JobStatus.CANCELLED.value, message="job cancelled before start")
            logger.info("Processing job cancelled before start job_id=%s", job_id)
            return job_id

        logger.info("Processing job starting job_id=%s", job_id)
        self._repo.update_job(job_id, status=JobStatus.RUNNING.value, progress=5.0, message="collecting candidate images")

        images = self._repo.list_images(
            group_name=filters.group_name,
            drone_type=filters.drone_type,
            min_lat=filters.min_latitude,
            max_lat=filters.max_latitude,
            min_lon=filters.min_longitude,
            max_lon=filters.max_longitude,
            start_time=filters.start_time,
            end_time=filters.end_time,
            limit=filters.max_images,
        )

        if self._is_cancelled(job_id):
            self._repo.update_job(
                job_id,
                status=JobStatus.CANCELLED.value,
                progress=10.0,
                message="cancelled while collecting images",
            )
            logger.info("Processing job cancelled while collecting images job_id=%s", job_id)
            return job_id

        if not images:
            self._repo.update_job(
                job_id,
                status=JobStatus.FAILED.value,
                progress=0.0,
                message="no images matched the selected metadata filters",
            )
            logger.warning("Processing job failed: no matched images job_id=%s", job_id)
            return job_id

        working_dir = _resolve_job_workdir(
            working_root=Path(self._config.rstools.working_root),
            filters=filters,
            job_id=job_id,
        )
        working_dir.mkdir(parents=True, exist_ok=True)
        image_keys = [image.object_key for image in images if image.object_key]
        if not image_keys:
            self._repo.update_job(
                job_id,
                status=JobStatus.FAILED.value,
                progress=0.0,
                message="matched images do not contain S3 object keys",
            )
            logger.warning("Processing job failed: matched images missing object keys job_id=%s", job_id)
            return job_id

        self._repo.update_job(job_id, status=JobStatus.RUNNING.value, progress=25.0, message="starting processing")
        filter_payload = _encode_filter_dict(filters)
        try:
            summary = self._runner.run(
                working_directory=working_dir,
                image_keys=image_keys,
                filters=filter_payload,
                job_id=job_id,
                progress_callback=lambda progress, message, _data: self._update_processing_progress(
                    job_id=job_id,
                    progress=progress,
                    message=message,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self._repo.update_job(
                job_id,
                status=JobStatus.FAILED.value,
                progress=100.0,
                message=str(exc),
            )
            logger.exception("Processing job failed job_id=%s", job_id)
            return job_id

        if self._is_cancelled(job_id):
            self._repo.update_job(
                job_id,
                status=JobStatus.CANCELLED.value,
                progress=100.0,
                message="cancelled after processing started",
                result_summary=summary,
            )
            logger.info("Processing job cancelled after run started job_id=%s", job_id)
            return job_id

        self._repo.update_job(
            job_id,
            status=JobStatus.COMPLETED.value,
            progress=100.0,
            message="processing complete",
            result_summary=summary,
        )
        logger.info("Processing job completed job_id=%s image_key_count=%s", job_id, len(image_keys))
        return job_id

    def _update_processing_progress(self, *, job_id: str, progress: float, message: str) -> None:
        if self._is_cancelled(job_id):
            return
        clamped = max(0.0, min(100.0, float(progress)))
        self._repo.update_job(
            job_id,
            status=JobStatus.RUNNING.value,
            progress=clamped,
            message=message,
        )

    def get_job(self, job_id: str):
        return self._repo.get_job(job_id)

    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100):
        return self._repo.list_jobs(status=status.value if status else None, limit=limit)

    def cancel_job(self, job_id: str) -> bool:
        job = self._repo.get_job(job_id)
        if job is None:
            return False

        self._cancelled.add(job_id)
        if str(job.status) in _TERMINAL_JOB_STATES:
            return True

        self._repo.update_job(job_id, status=JobStatus.CANCELLED.value, message="cancellation requested")
        logger.info("Processing job cancellation requested job_id=%s", job_id)
        return True


class ImageUploadOrchestrator:
    """Queue and execute image upload+ingest jobs."""

    def __init__(
        self,
        repository: Optional[StorageRepository] = None,
        ingestion_service: Optional[ImageIngestionService] = None,
        max_workers: int = 2,
        config: Optional[AppConfig] = None,
        start_workers: Optional[bool] = None,
    ) -> None:
        self._config = config or load_config()
        self._repo = repository or StorageRepository()
        self._ingestion_service = ingestion_service or ImageIngestionService(repository=self._repo)
        self._max_workers = max(max_workers, 1)
        self._backend = JobOrchestrator._normalize_backend(self._config.queue.backend)
        self._local_workers_enabled = (
            self._config.queue.start_local_workers if start_workers is None else bool(start_workers)
        )
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._running: Dict[str, Future] = {}
        self._states: Dict[str, _UploadBatchState] = {}
        self._cancelled: set[str] = set()
        self._stop_event = threading.Event()
        self._consumer_threads: list[threading.Thread] = []
        self._redis_queue: Optional[RedisJobQueue] = None

        if self._backend == _QUEUE_BACKEND_REDIS:
            queue_key = f"{self._config.queue.redis_queue_key}:upload"
            self._redis_queue = RedisJobQueue(
                redis_url=self._config.queue.redis_url,
                queue_key=queue_key,
                block_timeout_seconds=self._config.queue.redis_block_timeout_seconds,
            )
            self._redis_queue.ping()
            logger.info("Upload queue backend=redis queue=%s", queue_key)
            if self._local_workers_enabled:
                self.start_workers()
        else:
            logger.info("Upload queue backend=memory workers=%s", self._max_workers)

    @property
    def backend(self) -> str:
        return self._backend

    def start_workers(self) -> None:
        if self._backend != _QUEUE_BACKEND_REDIS or self._redis_queue is None:
            return
        if self._consumer_threads:
            return

        self._stop_event.clear()
        for index in range(self._max_workers):
            thread = threading.Thread(
                target=self._redis_worker_loop,
                name=f"rslogic-upload-worker-{index + 1}",
                daemon=True,
            )
            thread.start()
            self._consumer_threads.append(thread)
        logger.info("Started upload redis workers count=%s", len(self._consumer_threads))

    def stop_workers(self, *, close_queue: bool = True) -> None:
        self._stop_event.set()
        for thread in self._consumer_threads:
            thread.join(timeout=2.0)
        self._consumer_threads.clear()
        if close_queue and self._redis_queue is not None:
            self._redis_queue.close()
            self._redis_queue = None

    def close(self) -> None:
        self.stop_workers()
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=False)

    def submit_batch(
        self,
        *,
        group_name: str,
        local_paths: list[Path],
        prefix: Optional[str] = None,
        extra: Optional[Dict[str, str]] = None,
        resume: bool = True,
        concurrency: int = 24,
        batch_id: Optional[str] = None,
    ) -> str:
        if not local_paths:
            raise ValueError("local_paths cannot be empty")
        if concurrency < 1:
            raise ValueError("upload concurrency must be at least 1")

        metadata = {
            "type": "upload_batch",
            "file_count": len(local_paths),
            "resume": resume,
            "concurrency": concurrency,
        }
        if prefix:
            metadata["prefix"] = prefix

        if batch_id is None:
            job = self._repo.create_job(
                group_name=group_name,
                status=JobStatus.QUEUED.value,
                filters=metadata,
                message="queued for server-side upload+ingest",
            )
            resolved_batch_id = job.id
            logger.info(
                "Upload batch created and queued job_id=%s group_name=%s file_count=%s",
                resolved_batch_id,
                group_name,
                len(local_paths),
            )
        else:
            if batch_id in self._running or batch_id in self._states:
                raise ValueError("upload batch is already running")
            job = self._repo.get_job(batch_id)
            if job is None:
                created = self._repo.create_job(
                    job_id=batch_id,
                    group_name=group_name,
                    status=JobStatus.QUEUED.value,
                    filters=metadata,
                    message="queued for server-side upload+ingest",
                )
                resolved_batch_id = created.id
                logger.info(
                    "Upload batch accepted client batch id and queued job_id=%s group_name=%s file_count=%s",
                    resolved_batch_id,
                    group_name,
                    len(local_paths),
                )
            else:
                if str(job.status) in {JobStatus.RUNNING.value, JobStatus.COMPLETED.value, JobStatus.FAILED.value}:
                    raise ValueError(f"upload batch is already {job.status}")
                self._repo.update_job(
                    batch_id,
                    status=JobStatus.QUEUED.value,
                    progress=0.0,
                    message="queued for server-side upload+ingest",
                    filters=metadata,
                )
                resolved_batch_id = batch_id
                logger.info(
                    "Upload batch reused and queued job_id=%s group_name=%s file_count=%s",
                    resolved_batch_id,
                    group_name,
                    len(local_paths),
                )

        paths = [Path(path) for path in local_paths]
        payload = {
            "type": _QUEUE_TYPE_UPLOAD_BATCH,
            "batch_id": resolved_batch_id,
            "group_name": group_name,
            "local_paths": [str(path) for path in paths],
            "prefix": prefix,
            "extra": extra,
            "resume": resume,
            "concurrency": concurrency,
        }

        if self._backend == _QUEUE_BACKEND_REDIS:
            assert self._redis_queue is not None
            self._redis_queue.enqueue(payload)
        else:
            state = _UploadBatchState(
                batch_id=resolved_batch_id,
                group_name=group_name,
                local_paths=paths,
                prefix=prefix,
                extra=extra,
                resume=resume,
                concurrency=concurrency,
            )
            self._states[resolved_batch_id] = state
            future = self._executor.submit(self._execute_batch, state)
            self._running[resolved_batch_id] = future
            future.add_done_callback(lambda _f, jid=resolved_batch_id: self._running.pop(jid, None))

        return resolved_batch_id

    def prepare_batch(
        self,
        *,
        group_name: str,
        prefix: Optional[str] = None,
        resume: bool = True,
        concurrency: int = 24,
    ) -> str:
        if concurrency < 1:
            raise ValueError("upload concurrency must be at least 1")

        metadata = {
            "type": "upload_batch",
            "phase": "awaiting_upload",
            "resume": resume,
            "concurrency": concurrency,
        }
        if prefix:
            metadata["prefix"] = prefix

        job = self._repo.create_job(
            group_name=group_name,
            status=JobStatus.QUEUED.value,
            filters=metadata,
            message="awaiting client upload",
        )
        logger.info("Upload batch prepared job_id=%s group_name=%s", job.id, group_name)
        return job.id

    def _redis_worker_loop(self) -> None:
        assert self._redis_queue is not None
        while not self._stop_event.is_set():
            try:
                payload = self._redis_queue.dequeue()
            except Exception:
                time.sleep(0.5)
                continue
            if payload is None:
                continue
            if str(payload.get("type", "")) != _QUEUE_TYPE_UPLOAD_BATCH:
                logger.warning("Ignoring unsupported upload queue payload type=%s", payload.get("type"))
                continue
            self._execute_batch_payload(payload)

    def _is_cancelled(self, batch_id: str) -> bool:
        if batch_id in self._cancelled:
            return True
        job = self._repo.get_job(batch_id)
        if job is None:
            return True
        return str(job.status) == JobStatus.CANCELLED.value

    def _execute_batch(self, state: _UploadBatchState) -> str:
        payload = {
            "batch_id": state.batch_id,
            "group_name": state.group_name,
            "local_paths": [str(path) for path in state.local_paths],
            "prefix": state.prefix,
            "extra": state.extra,
            "resume": state.resume,
            "concurrency": state.concurrency,
        }
        return self._execute_batch_payload(payload)

    def _execute_batch_payload(self, payload: Dict[str, Any]) -> str:
        batch_id = str(payload.get("batch_id") or "").strip()
        group_name = str(payload.get("group_name") or "").strip()
        local_paths = [Path(path) for path in payload.get("local_paths", []) if str(path).strip()]
        prefix = payload.get("prefix")
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else None
        resume = bool(payload.get("resume", True))
        concurrency_raw = payload.get("concurrency", 24)
        try:
            concurrency = max(int(concurrency_raw), 1)
        except (TypeError, ValueError):
            concurrency = 1

        if not batch_id:
            logger.error("Dropping upload payload missing batch_id")
            return ""
        if not group_name:
            logger.error("Dropping upload payload missing group_name batch_id=%s", batch_id)
            return batch_id
        if not local_paths:
            self._repo.update_job(
                batch_id,
                status=JobStatus.FAILED.value,
                progress=100.0,
                message="upload batch has no local paths",
                result_summary={"error": "empty local_paths", "image_ids": []},
            )
            logger.error("Upload batch missing local paths batch_id=%s", batch_id)
            return batch_id

        if self._is_cancelled(batch_id):
            self._repo.update_job(
                batch_id,
                status=JobStatus.CANCELLED.value,
                message="upload batch cancelled before start",
            )
            self._cleanup_state(batch_id, local_paths)
            logger.info("Upload batch cancelled before execution job_id=%s", batch_id)
            return batch_id

        logger.info(
            "Upload batch execution started job_id=%s group_name=%s file_count=%s",
            batch_id,
            group_name,
            len(local_paths),
        )
        self._repo.update_job(
            batch_id,
            status=JobStatus.RUNNING.value,
            progress=5.0,
            message="uploading and ingesting images",
        )

        try:
            image_ids = self._ingestion_service.upload_and_ingest_files(
                group_name=group_name,
                local_paths=local_paths,
                prefix=prefix,
                extra=extra,
                resume=resume,
                concurrency=concurrency,
            )
            self._repo.update_job(
                batch_id,
                status=JobStatus.COMPLETED.value,
                progress=100.0,
                message=f"ingested {len(image_ids)} image(s)",
                result_summary={
                    "image_ids": image_ids,
                    "image_count": len(image_ids),
                },
            )
            logger.info("Upload batch completed job_id=%s image_count=%s", batch_id, len(image_ids))
            return batch_id
        except Exception as exc:  # noqa: BLE001
            if self._is_cancelled(batch_id):
                self._repo.update_job(
                    batch_id,
                    status=JobStatus.CANCELLED.value,
                    progress=100.0,
                    message="upload batch cancelled",
                    result_summary={"error": str(exc), "image_ids": []},
                )
                logger.info("Upload batch cancelled during execution job_id=%s", batch_id)
                return batch_id
            self._repo.update_job(
                batch_id,
                status=JobStatus.FAILED.value,
                progress=100.0,
                message=str(exc),
                result_summary={"error": str(exc), "image_ids": []},
            )
            logger.exception("Upload batch failed job_id=%s", batch_id)
            return batch_id
        finally:
            if batch_id in self._cancelled:
                self._cancelled.discard(batch_id)
            self._cleanup_state(batch_id, local_paths)

    def _cleanup_state(self, batch_id: str, paths: list[Path]) -> None:
        logger.debug("Cleaning upload batch state job_id=%s temp_file_count=%s", batch_id, len(paths))
        for local_path in paths:
            local_path.unlink(missing_ok=True)
        self._states.pop(batch_id, None)
        self._running.pop(batch_id, None)

    def get_job(self, job_id: str):
        return self._repo.get_job(job_id)

    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100):
        return self._repo.list_jobs(status=status.value if status else None, limit=limit)

    def cancel_job(self, job_id: str) -> bool:
        job = self._repo.get_job(job_id)
        if job is None:
            return False

        self._cancelled.add(job_id)
        if str(job.status) in _TERMINAL_JOB_STATES:
            return True

        self._repo.update_job(job_id, status=JobStatus.CANCELLED.value, message="cancellation requested")
        logger.info("Upload batch cancellation requested job_id=%s", job_id)
        return True


def _encode_filter_dict(filters: ImageFilter) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {}
    for key, value in filters.__dict__.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            encoded[key] = value.isoformat()
        else:
            encoded[key] = value
    return encoded


def _decode_filter_dict(payload: Any) -> ImageFilter:
    if not isinstance(payload, dict):
        return ImageFilter()
    return ImageFilter(
        group_name=_as_optional_str(payload.get("group_name")),
        drone_type=_as_optional_str(payload.get("drone_type")),
        start_time=_as_datetime(payload.get("start_time")),
        end_time=_as_datetime(payload.get("end_time")),
        min_latitude=_as_optional_float(payload.get("min_latitude")),
        max_latitude=_as_optional_float(payload.get("max_latitude")),
        min_longitude=_as_optional_float(payload.get("min_longitude")),
        max_longitude=_as_optional_float(payload.get("max_longitude")),
        max_images=_as_optional_int(payload.get("max_images")),
        sdk_imagery_folder=_as_optional_str(payload.get("sdk_imagery_folder")),
        sdk_project_path=_as_optional_str(payload.get("sdk_project_path")),
        sdk_include_subdirs=_as_optional_bool(payload.get("sdk_include_subdirs"), default=True),
        sdk_detector_sensitivity=_as_optional_str(payload.get("sdk_detector_sensitivity")) or "Ultra",
        sdk_camera_prior_accuracy_xyz=_as_optional_float(payload.get("sdk_camera_prior_accuracy_xyz")),
        sdk_camera_prior_accuracy_yaw_pitch_roll=_as_optional_float(payload.get("sdk_camera_prior_accuracy_yaw_pitch_roll")),
        sdk_run_align=_as_optional_bool(payload.get("sdk_run_align"), default=True),
        sdk_run_normal_model=_as_optional_bool(payload.get("sdk_run_normal_model"), default=True),
        sdk_run_ortho_projection=_as_optional_bool(payload.get("sdk_run_ortho_projection"), default=True),
        sdk_task_timeout_seconds=_as_optional_int(payload.get("sdk_task_timeout_seconds")) or 7200,
        session_code=_as_optional_str(payload.get("session_code")),
        pull_s3_images=_as_optional_bool(payload.get("pull_s3_images"), default=True),
        s3_bucket=_as_optional_str(payload.get("s3_bucket")),
        s3_prefix=_as_optional_str(payload.get("s3_prefix")),
        s3_region=_as_optional_str(payload.get("s3_region")),
        s3_endpoint_url=_as_optional_str(payload.get("s3_endpoint_url")),
        s3_max_files=_as_optional_int(payload.get("s3_max_files")),
        s3_extensions=(payload.get("s3_extensions") if isinstance(payload.get("s3_extensions"), list) else None),
        s3_staging_root=_as_optional_str(payload.get("s3_staging_root")),
    )


def _as_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _as_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_optional_bool(value: Any, *, default: bool) -> bool:
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


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    rendered = str(value).strip()
    if not rendered:
        return None
    try:
        return datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError:
        return None
