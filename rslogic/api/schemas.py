"""API request and response models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobCreateRequest(BaseModel):
    group_name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    drone_type: Optional[str] = Field(default=None, max_length=255)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    min_latitude: Optional[float] = None
    max_latitude: Optional[float] = None
    min_longitude: Optional[float] = None
    max_longitude: Optional[float] = None
    max_images: Optional[int] = Field(default=None, gt=0)
    sdk_imagery_folder: Optional[str] = Field(default=None, max_length=1024)
    sdk_project_path: Optional[str] = Field(default=None, max_length=1024)
    sdk_include_subdirs: bool = True
    sdk_detector_sensitivity: Optional[str] = Field(default="Ultra", max_length=64)
    sdk_camera_prior_accuracy_xyz: Optional[float] = 0.1
    sdk_camera_prior_accuracy_yaw_pitch_roll: Optional[float] = 1.0
    sdk_run_align: bool = True
    sdk_run_normal_model: bool = True
    sdk_run_ortho_projection: bool = True
    sdk_task_timeout_seconds: Optional[int] = Field(default=7200, gt=0)
    session_code: Optional[str] = Field(default=None, max_length=128)
    pull_s3_images: bool = True
    s3_bucket: Optional[str] = Field(default=None, max_length=255)
    s3_prefix: Optional[str] = Field(default=None, max_length=512)
    s3_region: Optional[str] = Field(default=None, max_length=255)
    s3_endpoint_url: Optional[str] = Field(default=None, max_length=1024)
    s3_max_files: Optional[int] = Field(default=None, gt=0)
    s3_extensions: Optional[List[str]] = None
    s3_staging_root: Optional[str] = Field(default=None, max_length=1024)


class IngestRequest(BaseModel):
    group_name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    object_key: str = Field(min_length=1)
    extra: Optional[Dict[str, str]] = None


class WaitingIngestRequest(BaseModel):
    group_name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    prefix: Optional[str] = None
    limit: int = Field(default=1000, gt=0)
    concurrency: int = Field(default=24, gt=0)
    override_existing: bool = False


class WaitingIngestItem(BaseModel):
    key: str
    image_id: Optional[str] = None
    group_name: Optional[str] = None
    status: str
    moved: bool = False
    source_uri: Optional[str] = None
    destination_uri: Optional[str] = None
    error: Optional[str] = None


class WaitingIngestResponse(BaseModel):
    bucket: str
    processed_bucket: Optional[str] = None
    prefix: Optional[str] = None
    group_name: Optional[str] = None
    scanned: int
    ingested: int
    skipped: int = 0
    failed: int
    moved: int = 0
    items: List[WaitingIngestItem] = Field(default_factory=list)


class UploadPrepareRequest(BaseModel):
    group_name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    prefix: Optional[str] = None
    resume: bool = True
    upload_concurrency: int = Field(default=24, gt=0)


class JobModel(BaseModel):
    id: str
    group_name: Optional[str] = None
    image_group_id: Optional[str] = None
    status: str
    progress: float
    message: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    result_summary: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ImageModel(BaseModel):
    id: str
    group_name: Optional[str] = None
    uri: str
    bucket_name: Optional[str] = None
    object_key: Optional[str] = None
    filename: Optional[str] = None
    captured_at: Optional[datetime] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None
    drone_model: Optional[str] = None
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None

    class Config:
        from_attributes = True


class GroupCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


class GroupModel(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
