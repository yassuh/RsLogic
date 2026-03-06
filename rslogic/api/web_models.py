"""Request/response models for the lightweight web UI."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class UploadStartRequest(BaseModel):
    path: str = Field(min_length=1)


class IngestStartRequest(BaseModel):
    group_name: str | None = None
    limit: int | None = Field(default=None, ge=1)


class ImageGroupCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None
    image_ids: list[str] = Field(default_factory=list)


class ImageGroupMembershipRequest(BaseModel):
    mode: Literal["replace", "add", "remove"]
    image_ids: list[str] = Field(default_factory=list)
