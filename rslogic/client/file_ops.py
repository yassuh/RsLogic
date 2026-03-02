"""File-oriented workflow steps for the remote client."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from rslogic.common.db import LabelDbStore
from rslogic.common.s3 import make_client
from config import CONFIG


class FileExecutor:
    def __init__(self, db: LabelDbStore, working_root: Path) -> None:
        self.db = db
        self.s3 = make_client(endpoint_url=CONFIG.s3.endpoint_url, region_name=CONFIG.s3.region)
        self.working_root = working_root
        self.staging_root = self.working_root / "staging"
        self.working_projects_root = self.working_root / "working"

    @staticmethod
    def _coerce_storage_location(bucket_hint: str | None, object_hint: str | None, default_bucket: str) -> tuple[str, str]:
        if not object_hint:
            raise RuntimeError("asset missing object key")
        if object_hint.startswith("s3://"):
            remainder = object_hint.removeprefix("s3://")
            bucket, _, key = remainder.partition("/")
            if not bucket or not key:
                raise RuntimeError(f"invalid s3 uri: {object_hint}")
            return bucket, key
        if bucket_hint:
            return bucket_hint, object_hint
        return default_bucket, object_hint

    @staticmethod
    def _safe_name(value: str) -> str:
        return "".join(ch for ch in value if ch.isalnum() or ch in ("-", "_", ".", " "))

    def stage_group(self, group_id: str, job_id: str) -> Path:
        group_dir = self.staging_root / str(job_id)
        group_dir.mkdir(parents=True, exist_ok=True)
        assets = self.db.image_assets_for_group(group_id)
        manifests = []
        for asset in assets:
            if not (asset.object_key or asset.uri):
                continue
            bucket = asset.bucket_name or CONFIG.s3.processed_bucket_name
            image_key = asset.object_key or asset.uri or ""
            if not image_key:
                continue
            bucket, image_key = self._coerce_storage_location(bucket, image_key, CONFIG.s3.processed_bucket_name)
            image_name = self._safe_name(asset.id + "_" + (asset.filename or Path(image_key).name))
            local_path = group_dir / image_name
            self.s3.download_file(bucket, image_key, str(local_path))

            local_sidecars: list[str] = []
            for sidecar_key in (asset.extra or {}).get("sidecar_keys", []) or []:
                sc_bucket, sc_key = self._coerce_storage_location(asset.bucket_name, sidecar_key, CONFIG.s3.processed_bucket_name)
                safe_sidecar = self._safe_name(f"{asset.id}_{Path(sidecar_key).name}")
                local_sidecar = group_dir / safe_sidecar
                self.s3.download_file(sc_bucket, sc_key, str(local_sidecar))
                local_sidecars.append(str(local_sidecar))

            manifests.append({
                "asset_id": asset.id,
                "image": {
                    "bucket": bucket,
                    "key": image_key,
                    "local_path": str(local_path),
                    "filename": asset.filename,
                },
                "sidecars": local_sidecars,
            })

        manifest_path = group_dir / "stage-map.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "group_id": group_id,
                    "job_id": job_id,
                    "files": manifests,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return group_dir

    def write_manifest(self, job_id: str, staging_dir: Path, group_id: str) -> Path:
        manifest_path = staging_dir / f"{job_id}-manifest.json"
        payload = {
            "job_id": job_id,
            "group_id": group_id,
            "files": sorted(str(p) for p in staging_dir.rglob("*") if p.is_file()),
            "group_mapping": sorted(str(p) for p in staging_dir.rglob("stage-map.json")),
        }
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return manifest_path

    def move_staging_to_working(self, job_id: str, staging_dir: Path | None = None, working_dir: Path | None = None) -> Path:
        if staging_dir is None:
            staging_dir = self.staging_root / str(job_id)
        if working_dir is None:
            working_dir = self.working_projects_root / str(job_id)
        working_dir.mkdir(parents=True, exist_ok=True)

        for source in sorted(staging_dir.rglob("*")):
            if source.is_dir():
                continue
            rel = source.relative_to(staging_dir)
            target = working_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                target.unlink()
            shutil.move(str(source), str(target))
        return working_dir
