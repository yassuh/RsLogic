"""Label DB access abstraction used by orchestrator, ingest, and client."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from geoalchemy2.elements import WKTElement


def _load_models():
    try:
        import studio_db  # noqa: PLC0415
    except Exception as exc:
        raise RuntimeError(
            "studio-db dependency is not available. Install it with rslogic's "
            "editable dependency path before starting services."
        ) from exc

    required = ("RsLogicImageAsset", "ImageGroup", "ImageGroupItem", "RsLogicProcessingJob")
    missing = [name for name in required if not hasattr(studio_db, name)]
    if missing:
        raise RuntimeError(f"studio_db package missing required models: {', '.join(missing)}")

    return studio_db


@dataclass
class LabelDbStore:
    database_url: str
    migration_root: str

    def __post_init__(self) -> None:
        self.models = _load_models()
        module_path = getattr(self.models, "__file__", None)
        if module_path:
            self.migration_root = str(Path(module_path).resolve().parent)
        self.engine = create_engine(self.database_url, future=True)
        self.session_factory = sessionmaker(bind=self.engine, future=True, expire_on_commit=False)
        self.RsLogicImageAsset = self.models.RsLogicImageAsset
        self.ImageGroup = self.models.ImageGroup
        self.ImageGroupItem = self.models.ImageGroupItem
        self.ProcessingJob = self.models.RsLogicProcessingJob

    def session(self):
        return self.session_factory()

    def get_or_create_group(self, name: str) -> tuple[Any, bool]:
        if not name:
            raise ValueError("Group name required")
        with self.session() as session:
            q = select(self.ImageGroup).where(self.ImageGroup.name == name)
            group = session.execute(q).scalar_one_or_none()
            if group is not None:
                return group, False
            group = self.ImageGroup(id=str(uuid4()), name=name)
            session.add(group)
            session.commit()
            return group, True

    def create_image_asset(
        self,
        *,
        source_waiting_key: str,
        processed_bucket: str,
        processed_key: str,
        filename: str,
        metadata: dict[str, Any],
        sidecar_keys: list[str],
        source_bucket: str,
        latitude: float | None = None,
        longitude: float | None = None,
        altitude_m: float | None = None,
        location: object | None = None,
    ) -> Any:
        if location is None and latitude is not None and longitude is not None:
            location = WKTElement(f"POINT({longitude} {latitude})", srid=4326)
        uri = f"s3://{processed_bucket}/{processed_key}"
        with self.session() as session:
            asset = self.RsLogicImageAsset(
                id=str(uuid4()),
                uri=uri,
                bucket_name=processed_bucket,
                object_key=processed_key,
                filename=filename,
                latitude=latitude,
                longitude=longitude,
                altitude_m=altitude_m,
                location=location,
                metadata_json=metadata,
                extra={
                    "ingest_state": "pending_move",
                    "source_bucket": source_bucket,
                    "source_key": source_waiting_key,
                    "sidecar_keys": sidecar_keys,
                },
            )
            session.add(asset)
            session.commit()
            return asset

    def attach_asset_to_group(self, group_id: str, image_id: str, role: str | None = None) -> None:
        with self.session() as session:
            group = session.get(self.ImageGroup, group_id)
            if group is None:
                return
            q = (
                select(self.ImageGroupItem)
                .where(
                    self.ImageGroupItem.group_id == group_id,
                    self.ImageGroupItem.image_id == image_id,
                )
            )
            exists = session.execute(q).scalar_one_or_none()
            if exists is not None:
                return
            item = self.ImageGroupItem(group_id=group_id, image_id=image_id, role=role)
            session.add(item)
            session.commit()

    def update_asset_state(self, asset_id: str, updates: dict[str, Any]) -> None:
        with self.session() as session:
            asset = session.get(self.RsLogicImageAsset, asset_id)
            if asset is None:
                return
            current = dict(asset.extra or {})
            current.update(updates)
            asset.extra = current
            session.commit()

    def upsert_processing_job(
        self,
        *,
        job_id: str,
        image_group_id: str | None,
        status: str,
        progress: float = 0.0,
        message: str | None = None,
        filters: dict[str, Any] | None = None,
        result_summary: dict[str, Any] | None = None,
    ) -> None:
        with self.session() as session:
            job = session.get(self.ProcessingJob, job_id)
            if job is None:
                job = self.ProcessingJob(id=job_id, image_group_id=image_group_id, status=status, progress=progress)
                if filters is not None:
                    job.filters = filters
                if message is not None:
                    job.message = message
                if result_summary is not None:
                    job.result_summary = result_summary
                session.add(job)
            else:
                job.status = status
                job.progress = progress
                if filters is not None:
                    job.filters = filters
                if message is not None:
                    job.message = message
                if result_summary is not None:
                    job.result_summary = result_summary
            session.commit()

    def image_assets_for_group(self, group_id: str):
        with self.session() as session:
            q = (
                select(self.RsLogicImageAsset)
                .join(self.ImageGroupItem, self.ImageGroupItem.image_id == self.RsLogicImageAsset.id)
                .where(self.ImageGroupItem.group_id == group_id)
            )
            return session.execute(q).scalars().all()
