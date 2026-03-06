from __future__ import annotations

from pathlib import Path

import pytest

from rslogic.client.executor import StepExecutor
from rslogic.common.schemas import Step


class FakeFileExecutor:
    def __init__(self, working_root: Path) -> None:
        self.working_root = working_root
        self.staging_root = working_root / "staging"
        self.staging_root.mkdir(parents=True, exist_ok=True)
        (self.staging_root / "image.jpg").write_text("data", encoding="utf-8")
        self.calls: list[tuple[str, Path, Path, str]] = []

    def copy_staging_to_session(self, job_id: str, staging_dir: Path, session_dir: Path, group_id: str) -> Path:
        self.calls.append((job_id, staging_dir, session_dir, group_id))
        return session_dir


def test_file_copy_staging_to_session_requires_session_context(tmp_path: Path) -> None:
    executor = StepExecutor(sdk_client=None, file_executor=FakeFileExecutor(tmp_path))
    executor.begin_job("job-1")
    executor._staging_dir = tmp_path / "staging"  # noqa: SLF001

    step = Step(kind="file", action="file_copy_staging_to_session", params={})

    with pytest.raises(RuntimeError, match="session data directory not known"):
        executor.execute(step, job_id="job-1", group_id="group-1")


def test_file_copy_staging_to_session_targets_session_data_root(tmp_path: Path) -> None:
    file_executor = FakeFileExecutor(tmp_path)
    executor = StepExecutor(sdk_client=None, file_executor=file_executor, initial_session="session-123")
    executor.begin_job("job-1")
    executor._staging_dir = tmp_path / "staging"  # noqa: SLF001

    step = Step(kind="file", action="file_copy_staging_to_session", params={"relative_dir": "Imagery/raw"})
    result = executor.execute(step, job_id="job-1", group_id="group-1")

    assert result.value == str(tmp_path / "sessions" / "session-123" / "_data" / "Imagery" / "raw")
    assert file_executor.calls == [
        (
            "job-1",
            tmp_path / "staging",
            tmp_path / "sessions" / "session-123" / "_data" / "Imagery" / "raw",
            "group-1",
        )
    ]


def test_file_copy_staging_to_session_requires_stage_first(tmp_path: Path) -> None:
    executor = StepExecutor(sdk_client=None, file_executor=FakeFileExecutor(tmp_path), initial_session="session-123")
    executor.begin_job("job-1")

    step = Step(kind="file", action="file_copy_staging_to_session", params={})

    with pytest.raises(RuntimeError, match="run stage before file_copy_staging_to_session"):
        executor.execute(step, job_id="job-1", group_id="group-1")
