"""Database persistence for image metadata and job state."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from importlib.util import module_from_spec, spec_from_file_location
from functools import lru_cache
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, cast
from uuid import uuid4

from geoalchemy2.elements import WKTElement
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import load_config

@dataclass(frozen=True)
class ImageRecordDTO:
    id: str
    group_name: Optional[str]
    uri: str
    bucket_name: Optional[str]
    object_key: Optional[str]
    filename: Optional[str]
    captured_at: Optional[datetime]
    latitude: Optional[float]
    longitude: Optional[float]
    altitude_m: Optional[float]
    drone_model: Optional[str]
    camera_make: Optional[str]
    camera_model: Optional[str]


class StorageRepository:
    """Database repository for images and jobs."""

    def __init__(self, database_url: Optional[str] = None) -> None:
        label_db = load_config().label_db
        self._label_db_root = Path(label_db.migration_root)
        self._engine = create_engine(database_url or label_db.database_url, future=True)
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            future=True,
        )

        label_models = _load_label_models(str(self._label_db_root))
        self.ImageAsset = cast(type, label_models["ImageAsset"])
        self.ImageGroup = cast(type, label_models["ImageGroup"])
        self.ImageGroupItem = cast(type, label_models["ImageGroupItem"])
        self.ProcessingJob = cast(type, label_models["ProcessingJob"])

    @contextmanager
    def session(self):
        session: Session = self._SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_image(
        self,
        *,
        group_name: Optional[str],
        bucket_name: Optional[str],
        object_key: Optional[str],
        filename: Optional[str],
        metadata: Dict[str, Any],
        metadata_json: Optional[Dict[str, Any]] = None,
        sha256: Optional[str],
        file_size: Optional[int],
        uri: Optional[str] = None,
        s3_url: Optional[str] = None,
    ) -> ImageAsset:
        return self._save_image(
            group_name=group_name,
            bucket_name=bucket_name,
            object_key=object_key,
            filename=filename,
            metadata=metadata,
            metadata_json=metadata_json,
            sha256=sha256,
            file_size=file_size,
            uri=uri,
            s3_url=s3_url,
            upsert=False,
        )

    def create_or_update_image(
        self,
        *,
        group_name: Optional[str],
        bucket_name: Optional[str],
        object_key: Optional[str],
        filename: Optional[str],
        metadata: Dict[str, Any],
        metadata_json: Optional[Dict[str, Any]] = None,
        sha256: Optional[str],
        file_size: Optional[int],
        uri: Optional[str] = None,
        s3_url: Optional[str] = None,
    ) -> ImageAsset:
        return self._save_image(
            group_name=group_name,
            bucket_name=bucket_name,
            object_key=object_key,
            filename=filename,
            metadata=metadata,
            metadata_json=metadata_json,
            sha256=sha256,
            file_size=file_size,
            uri=uri,
            s3_url=s3_url,
            upsert=True,
        )

    def _save_image(
        self,
        *,
        group_name: Optional[str],
        bucket_name: Optional[str],
        object_key: Optional[str],
        filename: Optional[str],
        metadata: Dict[str, Any],
        metadata_json: Optional[Dict[str, Any]],
        sha256: Optional[str],
        file_size: Optional[int],
        uri: Optional[str],
        s3_url: Optional[str],
        upsert: bool,
    ) -> ImageAsset:
        group_name = self._normalize_group_name(group_name)
        normalized_key = object_key.lstrip("/") if object_key else None
        resolved_uri = uri or s3_url
        if resolved_uri is None and bucket_name and normalized_key:
            resolved_uri = f"s3://{bucket_name}/{normalized_key}"
        if not resolved_uri:
            raise ValueError("uri is required to create image record")

        with self.session() as session:
            record = None
            if upsert:
                record = session.query(self.ImageAsset).filter(self.ImageAsset.uri == resolved_uri).one_or_none()
            if record is None:
                record = self.ImageAsset(
                    id=str(uuid4()),
                    uri=resolved_uri,
                )
                session.add(record)

            record.uri = resolved_uri
            record.bucket_name = bucket_name
            record.object_key = normalized_key
            record.filename = filename
            record.sha256 = sha256
            record.file_size = file_size
            record.captured_at = metadata.get("captured_at")
            latitude = metadata.get("latitude")
            longitude = metadata.get("longitude")
            record.latitude = latitude
            record.longitude = longitude
            record.location = self._build_location_geometry(latitude=latitude, longitude=longitude)
            record.altitude_m = metadata.get("altitude_m")
            record.drone_model = metadata.get("drone_model")
            record.camera_make = metadata.get("camera_make")
            record.camera_model = metadata.get("camera_model")
            record.focal_length_mm = metadata.get("focal_length_mm")
            record.image_width = metadata.get("image_width")
            record.image_height = metadata.get("image_height")
            record.software = metadata.get("software")

            metadata_payload = metadata_json if metadata_json is not None else metadata
            if hasattr(record, "metadata_json"):
                setattr(
                    record,
                    "metadata_json",
                    self._coerce_json_payload(metadata_payload),
                )
            record.extra = self._coerce_json_payload(metadata.get("extra", {}))
            session.flush()
            if group_name:
                group = self._get_or_create_image_group(session, group_name)
                existing = (
                    session.query(self.ImageGroupItem)
                    .filter(
                        self.ImageGroupItem.group_id == group.id,
                        self.ImageGroupItem.image_id == record.id,
                    )
                    .one_or_none()
                )
                if existing is None:
                    session.add(
                        self.ImageGroupItem(
                            group_id=group.id,
                            image_id=record.id,
                        )
                    )
                    session.flush()
            session.refresh(record)
            return record

    @staticmethod
    def _coerce_json_payload(value):
        if isinstance(value, dict):
            return {str(key): StorageRepository._coerce_json_payload(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [StorageRepository._coerce_json_payload(item) for item in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return value.hex()
        if value is None:
            return None
        return value

    def list_images(
        self,
        *,
        group_name: Optional[str] = None,
        drone_type: Optional[str] = None,
        min_lat: Optional[float] = None,
        max_lat: Optional[float] = None,
        min_lon: Optional[float] = None,
        max_lon: Optional[float] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[ImageAsset]:
        group_name = self._normalize_group_name(group_name)
        with self.session() as session:
            query = session.query(self.ImageAsset)
            if group_name:
                query = (
                    query.join(self.ImageGroupItem, self.ImageGroupItem.image_id == self.ImageAsset.id)
                    .join(self.ImageGroup, self.ImageGroup.id == self.ImageGroupItem.group_id)
                    .filter(self.ImageGroup.name == group_name)
                )
            if drone_type:
                query = query.filter(self.ImageAsset.drone_model == drone_type)
            if min_lat is not None:
                query = query.filter(self.ImageAsset.latitude >= min_lat)
            if max_lat is not None:
                query = query.filter(self.ImageAsset.latitude <= max_lat)
            if min_lon is not None:
                query = query.filter(self.ImageAsset.longitude >= min_lon)
            if max_lon is not None:
                query = query.filter(self.ImageAsset.longitude <= max_lon)
            if start_time:
                query = query.filter(self.ImageAsset.captured_at >= start_time)
            if end_time:
                query = query.filter(self.ImageAsset.captured_at <= end_time)

            if limit:
                query = query.limit(limit)

            return query.order_by(self.ImageAsset.created_at.desc()).all()

    def create_job(
        self,
        *,
        job_id: Optional[str] = None,
        group_name: Optional[str],
        status: str,
        filters: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
    ) -> ProcessingJob:
        group_name = self._normalize_group_name(group_name)
        with self.session() as session:
            group_id: Optional[str] = None
            if group_name:
                group = self._get_or_create_image_group(session, group_name)
                group_id = group.id
            job = self.ProcessingJob(
                id=job_id or str(uuid4()),
                image_group_id=group_id,
                status=status,
                filters=filters or {},
                progress=0.0,
                message=message,
            )
            session.add(job)
            session.flush()
            session.refresh(job)
            return job

    def update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        result_summary: Optional[Dict[str, Any]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Optional[ProcessingJob]:
        with self.session() as session:
            job = session.query(self.ProcessingJob).filter(self.ProcessingJob.id == job_id).one_or_none()
            if job is None:
                return None
            if status is not None:
                job.status = status
            if progress is not None:
                job.progress = progress
            if message is not None:
                job.message = message
            if result_summary is not None:
                job.result_summary = result_summary
            if filters is not None:
                job.filters = filters
            session.flush()
            session.refresh(job)
            return job

    def get_job(self, job_id: str) -> Optional[ProcessingJob]:
        with self.session() as session:
            return session.query(self.ProcessingJob).filter(self.ProcessingJob.id == job_id).one_or_none()

    def get_image(self, image_id: str) -> Optional[ImageAsset]:
        with self.session() as session:
            return session.query(self.ImageAsset).filter(self.ImageAsset.id == image_id).one_or_none()

    def get_image_by_uri(self, uri: str) -> Optional[ImageAsset]:
        with self.session() as session:
            return session.query(self.ImageAsset).filter(self.ImageAsset.uri == uri).one_or_none()

    def update_image_storage_location(
        self,
        image_id: str,
        *,
        bucket_name: Optional[str],
        object_key: Optional[str],
        uri: str,
    ) -> Optional[ImageAsset]:
        normalized_key = object_key.lstrip("/") if object_key else None
        with self.session() as session:
            record = session.query(self.ImageAsset).filter(self.ImageAsset.id == image_id).one_or_none()
            if record is None:
                return None
            record.bucket_name = bucket_name
            record.object_key = normalized_key
            record.uri = uri
            session.flush()
            session.refresh(record)
            return record

    def list_jobs(self, status: Optional[str] = None, limit: int = 100) -> List[ProcessingJob]:
        with self.session() as session:
            query = session.query(self.ProcessingJob).order_by(self.ProcessingJob.created_at.desc())
            if status:
                query = query.filter(self.ProcessingJob.status == status)
            return query.limit(limit).all()

    def create_image_group(
        self,
        *,
        name: str,
        description: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        normalized_name = self._normalize_group_name(name)
        if not normalized_name:
            raise ValueError("group name cannot be empty")
        with self.session() as session:
            group = self._get_or_create_image_group(
                session,
                normalized_name,
                description=description,
                extra=extra,
            )
            session.refresh(group)
            return group

    def list_image_groups(self, limit: int = 100):
        with self.session() as session:
            return (
                session.query(self.ImageGroup)
                .order_by(self.ImageGroup.created_at.desc())
                .limit(limit)
                .all()
            )

    def get_image_group_by_name(self, name: str):
        normalized_name = self._normalize_group_name(name)
        if not normalized_name:
            return None
        with self.session() as session:
            return session.query(self.ImageGroup).filter(self.ImageGroup.name == normalized_name).one_or_none()

    @staticmethod
    def _normalize_group_name(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @staticmethod
    def _build_location_geometry(*, latitude: Any, longitude: Any) -> Optional[WKTElement]:
        lat_value = StorageRepository._coerce_coordinate(latitude)
        lon_value = StorageRepository._coerce_coordinate(longitude)
        if lat_value is None or lon_value is None:
            return None
        if lat_value < -90 or lat_value > 90:
            return None
        if lon_value < -180 or lon_value > 180:
            return None
        return WKTElement(f"POINT({lon_value} {lat_value})", srid=4326)

    @staticmethod
    def _coerce_coordinate(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _get_or_create_image_group(
        self,
        session: Session,
        name: str,
        *,
        description: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        existing = session.query(self.ImageGroup).filter(self.ImageGroup.name == name).one_or_none()
        if existing is not None:
            if description and not existing.description:
                existing.description = description
                session.flush()
            return existing
        group = self.ImageGroup(
            id=str(uuid4()),
            name=name,
            description=description,
            extra=dict(extra or {}),
        )
        session.add(group)
        session.flush()
        return group


@lru_cache(maxsize=1)
def _load_label_models(root: str) -> dict[str, type]:
    """Load label-db SQLAlchemy models from the vendored submodule."""

    models_path = Path(root) / "models.py"
    if not models_path.exists():
        raise FileNotFoundError(f"Label-db models not found at: {models_path}")

    spec = spec_from_file_location("label_db_models", models_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load label-db models module from: {models_path}")

    module = module_from_spec(spec)
    # SQLAlchemy resolves string annotations using sys.modules lookup.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    required = ("ImageAsset", "ImageGroup", "ImageGroupItem", "ProcessingJob")
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise RuntimeError(f"Missing required label-db models: {', '.join(missing)}")

    return {
        "ImageAsset": getattr(module, "ImageAsset"),
        "ImageGroup": getattr(module, "ImageGroup"),
        "ImageGroupItem": getattr(module, "ImageGroupItem"),
        "ProcessingJob": getattr(module, "ProcessingJob"),
    }
