"""Background operation tracking for upload and ingest web actions."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
import threading
import time
import uuid
from typing import Any

from rslogic.ingest import IngestService
from rslogic.upload_service import FolderUploader


@dataclass
class OperationState:
    operation_id: str
    kind: str
    status: str = "queued"
    message: str = "queued"
    progress_done: int = 0
    progress_total: int = 0
    logs: list[str] = field(default_factory=list)
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    finished_at: float | None = None

    def append_log(self, message: str) -> None:
        self.logs.append(message)
        self.logs = self.logs[-40:]
        self.updated_at = time.time()

    def snapshot(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "kind": self.kind,
            "status": self.status,
            "message": self.message,
            "progress_done": self.progress_done,
            "progress_total": self.progress_total,
            "logs": list(self.logs),
            "result": dict(self.result) if isinstance(self.result, dict) else self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
        }


class OperationRegistry:
    def __init__(self, *, max_items: int = 100) -> None:
        self._max_items = max_items
        self._lock = threading.Lock()
        self._items: OrderedDict[str, OperationState] = OrderedDict()

    def start_upload(self, path: str) -> OperationState:
        operation = self._create("upload", f"queued upload for {path}")
        self._start_thread(operation, self._run_upload, path)
        return operation

    def start_ingest(self, *, group_name: str | None, limit: int | None) -> OperationState:
        description = f"queued ingest group={group_name or '-'} limit={limit or '-'}"
        operation = self._create("ingest", description)
        self._start_thread(operation, self._run_ingest, group_name, limit)
        return operation

    def get(self, operation_id: str) -> dict[str, Any] | None:
        with self._lock:
            operation = self._items.get(operation_id)
            return None if operation is None else operation.snapshot()

    def list_recent(self) -> list[dict[str, Any]]:
        with self._lock:
            return [operation.snapshot() for operation in reversed(self._items.values())]

    def _create(self, kind: str, message: str) -> OperationState:
        operation = OperationState(operation_id=str(uuid.uuid4()), kind=kind, message=message)
        with self._lock:
            self._items[operation.operation_id] = operation
            self._trim()
        return operation

    def _trim(self) -> None:
        while len(self._items) > self._max_items:
            self._items.popitem(last=False)

    def _start_thread(self, operation: OperationState, target: Any, *args: Any) -> None:
        thread = threading.Thread(
            target=self._run_guarded,
            args=(operation.operation_id, target, *args),
            name=f"web-op-{operation.kind}-{operation.operation_id[:8]}",
            daemon=True,
        )
        thread.start()

    def _run_guarded(self, operation_id: str, target: Any, *args: Any) -> None:
        self._update(operation_id, status="running", message="running")
        try:
            result = target(operation_id, *args)
        except Exception as exc:
            self._update(
                operation_id,
                status="error",
                message=f"{type(exc).__name__}: {exc}",
                error=f"{type(exc).__name__}: {exc}",
                finished=True,
            )
            return
        self._update(operation_id, status="done", message="done", result=result, finished=True)

    def _update(
        self,
        operation_id: str,
        *,
        status: str | None = None,
        message: str | None = None,
        progress_done: int | None = None,
        progress_total: int | None = None,
        log: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        finished: bool = False,
    ) -> None:
        with self._lock:
            operation = self._items[operation_id]
            if status is not None:
                operation.status = status
            if message is not None:
                operation.message = message
            if progress_done is not None:
                operation.progress_done = progress_done
            if progress_total is not None:
                operation.progress_total = progress_total
            if log is not None:
                operation.append_log(log)
            else:
                operation.updated_at = time.time()
            if result is not None:
                operation.result = result
            if error is not None:
                operation.error = error
            if finished:
                operation.finished_at = time.time()
                operation.updated_at = operation.finished_at

    def _run_upload(self, operation_id: str, path: str) -> dict[str, Any]:
        folder = Path(path).expanduser().resolve()
        self._update(operation_id, message=f"uploading {folder}", log=f"uploading {folder}")
        uploader = FolderUploader()
        manifest = uploader.run(
            folder,
            on_progress=lambda done, total: self._update(
                operation_id,
                progress_done=done,
                progress_total=total,
                message=f"uploading {done}/{total}",
            ),
        )
        self._update(operation_id, log=f"uploaded {len(manifest)} object(s)")
        return {
            "path": str(folder),
            "uploaded_objects": len(manifest),
        }

    def _run_ingest(self, operation_id: str, group_name: str | None, limit: int | None) -> dict[str, Any]:
        service = IngestService()
        results = service.run(
            group_name=group_name,
            limit=limit,
            on_progress=lambda done, total: self._update(
                operation_id,
                progress_done=done,
                progress_total=total,
                message=f"ingesting {done}/{total}",
            ),
            on_result=lambda item, result: self._update(
                operation_id,
                log=f"{item.image_key} -> {result['bucket']}/{result['key']}",
            ),
            on_status=lambda message: self._update(
                operation_id,
                message=message,
                log=message,
            ),
        )
        return {
            "group_name": group_name,
            "limit": limit,
            "ingested_images": len(results),
        }
