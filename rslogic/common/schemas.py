"""Shared data contracts for jobs, commands, and runtime state."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Step(BaseModel):
    """One executable unit inside a workflow."""

    step_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    kind: str = "sdk"
    action: str
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_s: int = 600
    display_name: str | None = None

    @field_validator("kind")
    @classmethod
    def normalize_kind(cls, value: str) -> str:
        return str(value).strip().lower()

    @field_validator("action")
    @classmethod
    def normalize_action(cls, value: str) -> str:
        return str(value).strip().lower()


class JobRequest(BaseModel):
    """Input contract accepted by orchestrator API."""

    client_id: str | None = None
    target_client: str | None = None
    auto_assign: bool = False
    group_id: str | None = None
    group_name: str | None = None
    steps: list[Step] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def requested_client(self) -> str | None:
        if self.client_id:
            return self.client_id
        return self.target_client


@dataclass
class JobProgress:
    job_id: str
    client_id: str
    current_step: int = 0
    total_steps: int = 0
    status: str = "queued"
    message: str = ""
    result: dict[str, Any] = field(default_factory=dict)
