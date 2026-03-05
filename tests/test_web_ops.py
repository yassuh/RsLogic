from __future__ import annotations

from rslogic.api.web_ops import OperationRegistry


def test_run_guarded_marks_operation_done() -> None:
    registry = OperationRegistry()
    operation = registry._create("upload", "queued")

    def target(operation_id: str, path: str) -> dict[str, object]:
        registry._update(
            operation_id,
            progress_done=1,
            progress_total=2,
            log=f"uploading {path}",
        )
        return {"path": path}

    registry._run_guarded(operation.operation_id, target, "/tmp/captures")
    snapshot = registry.get(operation.operation_id)

    assert snapshot is not None
    assert snapshot["status"] == "done"
    assert snapshot["result"] == {"path": "/tmp/captures"}
    assert snapshot["logs"] == ["uploading /tmp/captures"]
    assert snapshot["finished_at"] is not None


def test_run_guarded_marks_operation_error() -> None:
    registry = OperationRegistry()
    operation = registry._create("ingest", "queued")

    def target(operation_id: str) -> dict[str, object]:
        raise RuntimeError(f"failed {operation_id}")

    registry._run_guarded(operation.operation_id, target)
    snapshot = registry.get(operation.operation_id)

    assert snapshot is not None
    assert snapshot["status"] == "error"
    assert snapshot["error"] == f"RuntimeError: failed {operation.operation_id}"
    assert snapshot["finished_at"] is not None
