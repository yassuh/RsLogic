"""Label DB access abstraction used by orchestrator, ingest, and client."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, func, select
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

    required = ("RsLogicImageAsset", "ImageGroup", "ImageGroupItem", "RsLogicRealityScanJob")
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
        self.RealityScanJob = self.models.RsLogicRealityScanJob

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
        job_name: str | None,
        image_group_id: str | None,
        status: str,
        progress: float = 0.0,
        message: str | None = None,
        job_definition: dict[str, Any] | None = None,
        result_summary: dict[str, Any] | None = None,
    ) -> None:
        with self.session() as session:
            job = session.get(self.RealityScanJob, job_id)
            if job is None:
                job = self.RealityScanJob(
                    id=job_id,
                    job_name=job_name,
                    image_group_id=image_group_id,
                    status=status,
                    progress=progress,
                )
                if job_definition is not None:
                    job.job_definition = job_definition
                if message is not None:
                    job.message = message
                if result_summary is not None:
                    job.result_summary = result_summary
                session.add(job)
            else:
                if job_name is not None:
                    job.job_name = job_name
                job.status = status
                job.progress = progress
                if job_definition is not None:
                    job.job_definition = job_definition
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

    def list_image_groups(self) -> list[dict[str, Any]]:
        with self.session() as session:
            q = (
                select(self.ImageGroup, func.count(self.ImageGroupItem.image_id))
                .outerjoin(self.ImageGroupItem, self.ImageGroupItem.group_id == self.ImageGroup.id)
                .group_by(self.ImageGroup.id)
                .order_by(self.ImageGroup.name.asc())
            )
            rows = session.execute(q).all()
            return [
                {
                    "id": group.id,
                    "name": group.name,
                    "description": group.description,
                    "image_count": int(image_count or 0),
                    "created_at": group.created_at.isoformat() if group.created_at else None,
                    "updated_at": group.updated_at.isoformat() if group.updated_at else None,
                }
                for group, image_count in rows
            ]

    def get_image_group_detail(self, group_id: str) -> dict[str, Any] | None:
        with self.session() as session:
            group = session.get(self.ImageGroup, group_id)
            if group is None:
                return None
            image_ids = [item.image_id for item in group.image_items]
            image_ids.sort()
            return {
                "id": group.id,
                "name": group.name,
                "description": group.description,
                "image_ids": image_ids,
                "image_count": len(image_ids),
                "created_at": group.created_at.isoformat() if group.created_at else None,
                "updated_at": group.updated_at.isoformat() if group.updated_at else None,
            }

    def list_image_assets(self, *, require_coordinates: bool = False) -> list[dict[str, Any]]:
        with self.session() as session:
            latitude_expr = func.ST_Y(self.RsLogicImageAsset.location)
            longitude_expr = func.ST_X(self.RsLogicImageAsset.location)
            q = select(self.RsLogicImageAsset, latitude_expr, longitude_expr).order_by(
                self.RsLogicImageAsset.captured_at.asc().nullslast(),
                self.RsLogicImageAsset.created_at.asc(),
            )
            if require_coordinates:
                q = q.where(
                    (
                        (self.RsLogicImageAsset.latitude.is_not(None) & self.RsLogicImageAsset.longitude.is_not(None))
                        | self.RsLogicImageAsset.location.is_not(None)
                    )
                )
            rows = session.execute(q).all()
            payload = []
            for asset, location_latitude, location_longitude in rows:
                latitude = asset.latitude if asset.latitude is not None else float(location_latitude) if location_latitude is not None else None
                longitude = asset.longitude if asset.longitude is not None else float(location_longitude) if location_longitude is not None else None
                payload.append(
                    {
                        "id": asset.id,
                        "filename": asset.filename,
                        "uri": asset.uri,
                        "latitude": latitude,
                        "longitude": longitude,
                        "altitude_m": asset.altitude_m,
                        "captured_at": asset.captured_at.isoformat() if asset.captured_at else None,
                        "group_ids": sorted(item.group_id for item in asset.group_items),
                    }
                )
            return payload

    def create_image_group(
        self,
        *,
        name: str,
        description: str | None = None,
        image_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        cleaned_name = str(name).strip()
        if not cleaned_name:
            raise ValueError("group name is required")
        requested_ids = sorted(set(image_ids or []))
        with self.session() as session:
            existing = session.execute(select(self.ImageGroup).where(self.ImageGroup.name == cleaned_name)).scalar_one_or_none()
            if existing is not None:
                raise ValueError(f"group already exists: {cleaned_name}")
            group = self.ImageGroup(
                id=str(uuid4()),
                name=cleaned_name,
                description=str(description).strip() or None if description is not None else None,
            )
            session.add(group)
            if requested_ids:
                found_ids = set(
                    session.execute(
                        select(self.RsLogicImageAsset.id).where(self.RsLogicImageAsset.id.in_(requested_ids))
                    ).scalars().all()
                )
                missing_ids = [image_id for image_id in requested_ids if image_id not in found_ids]
                if missing_ids:
                    raise ValueError(f"unknown image ids: {', '.join(missing_ids[:5])}")
                for image_id in requested_ids:
                    session.add(self.ImageGroupItem(group_id=group.id, image_id=image_id))
            session.commit()
            return self.get_image_group_detail(group.id) or {
                "id": group.id,
                "name": group.name,
                "description": group.description,
                "image_ids": requested_ids,
                "image_count": len(requested_ids),
            }

    def delete_image_group(self, group_id: str) -> bool:
        with self.session() as session:
            group = session.get(self.ImageGroup, group_id)
            if group is None:
                return False
            session.delete(group)
            session.commit()
            return True

    def update_image_group_membership(
        self,
        *,
        group_id: str,
        image_ids: list[str],
        mode: str,
    ) -> dict[str, Any]:
        operation = str(mode).strip().lower()
        if operation not in {"replace", "add", "remove"}:
            raise ValueError("mode must be one of: replace, add, remove")
        requested_ids = sorted(set(image_ids))
        with self.session() as session:
            group = session.get(self.ImageGroup, group_id)
            if group is None:
                raise ValueError(f"group not found: {group_id}")
            if requested_ids:
                found_ids = set(
                    session.execute(
                        select(self.RsLogicImageAsset.id).where(self.RsLogicImageAsset.id.in_(requested_ids))
                    ).scalars().all()
                )
                missing_ids = [image_id for image_id in requested_ids if image_id not in found_ids]
                if missing_ids:
                    raise ValueError(f"unknown image ids: {', '.join(missing_ids[:5])}")
            existing_items = {item.image_id: item for item in group.image_items}
            existing_ids = set(existing_items)
            if operation == "replace":
                target_ids = set(requested_ids)
            elif operation == "add":
                target_ids = existing_ids | set(requested_ids)
            else:
                target_ids = existing_ids - set(requested_ids)

            for image_id, item in list(existing_items.items()):
                if image_id not in target_ids:
                    session.delete(item)
            for image_id in sorted(target_ids - existing_ids):
                session.add(self.ImageGroupItem(group_id=group.id, image_id=image_id))
            session.commit()
            return {
                "group_id": group.id,
                "mode": operation,
                "image_ids": sorted(target_ids),
                "image_count": len(target_ids),
            }
