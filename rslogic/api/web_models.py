"""Request/response models for the lightweight web UI."""

from __future__ import annotations

from pydantic import BaseModel, Field


class UploadStartRequest(BaseModel):
    path: str = Field(min_length=1)


class IngestStartRequest(BaseModel):
    group_name: str | None = None
    limit: int | None = Field(default=None, ge=1)


class WorkflowImportRequest(BaseModel):
    source: str = Field(min_length=1)
