from __future__ import annotations

import pytest
pytest.importorskip("pydantic")
from pydantic import ValidationError

from rslogic.api import schemas


def test_job_create_request_defaults_and_validation():
    payload = schemas.JobCreateRequest()
    assert payload.group_name is None
    assert payload.sdk_include_subdirs is True
    assert payload.sdk_task_timeout_seconds == 7200
    with pytest.raises(ValidationError):
        schemas.JobCreateRequest(max_images=0)


def test_waiting_ingest_request_validates_limit_and_concurrency():
    payload = schemas.WaitingIngestRequest(limit=5, concurrency=2)
    assert payload.limit == 5
    assert payload.concurrency == 2
    with pytest.raises(ValidationError):
        schemas.WaitingIngestRequest(limit=0)


def test_job_model_roundtrip_and_aliases():
    model = schemas.JobModel(
        id="job-1",
        status="running",
        progress=0.5,
        group_name="g1",
    )
    dumped = model.model_dump()
    assert dumped["id"] == "job-1"
    assert dumped["status"] == "running"
