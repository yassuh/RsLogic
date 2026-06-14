from __future__ import annotations

import json
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from rslogic.common.db import LabelDbStore
from rslogic.common.s3 import make_client
from rslogic.config import CONFIG

_LOGGER = logging.getLogger("rslogic.client.file_ops")

class FileExecutor:
    def __init__(self, db: LabelDbStore, working_root: Path) -> None:
        self.db = db
        self.s3 = make_client(endpoint_url=CONFIG.s3.endpoint_url, region_name=CONFIG.s3.region)
        self.working_root = working_root
        self.staging_root = self.working_root / "staging"

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

    def _download_one(
        self,
        *,
        idx: int,
        total: int,
        asset: Any,
        image_key: str,
        bucket: str,
        local_path: Path,
    ) -> dict[str, object]:
        if not image_key:
            raise RuntimeError(f"asset {asset.id} has empty image key")
        if not (asset.object_key or asset.uri):
            raise RuntimeError(f"asset {getattr(asset, 'id', '<unknown>')} missing object locator")
        _LOGGER.info(
            "downloading [%s/%s] asset=%s bucket=%s key=%s -> %s",
            idx,
            total,
            asset.id,
            bucket,
            image_key,
            local_path,
        )
        self.s3.download_file(bucket, image_key, str(local_path))

    @staticmethod
    def _copy_one(
        *,
        source: Path,
        target: Path,
    ) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        shutil.copy2(str(source), str(target))

    @staticmethod
    def _stage_local_path(group_dir: Path, asset: Any, image_key: str) -> Path:
        image_name = f"{asset.id}_{Path(image_key).name}" if getattr(asset, "id", None) is not None else Path(image_key).name
        image_name = FileExecutor._safe_name(image_name)
        return group_dir / image_name

    def _resolve_object_locator(self, *, asset: Any, default_bucket: str) -> tuple[str, str]:
        if not (asset.object_key or asset.uri):
            raise RuntimeError(f"asset {getattr(asset, 'id', '<unknown>')} missing object locator")
        bucket = asset.bucket_name or default_bucket
        image_key = asset.object_key or asset.uri or ""
        if not image_key:
            raise RuntimeError(f"asset {asset.id} has empty image key")
        return self._coerce_storage_location(bucket, image_key, CONFIG.s3.processed_bucket_name)

    def _group_stage_files(self, group_id: str) -> list[tuple[Any, str, str, Path]]:
        assets = self.db.image_assets_for_group(group_id)
        assets_list = list(assets)
        stage_files: list[tuple[Any, str, str, Path]] = []
        seen_names: set[str] = set()
        for asset in assets_list:
            try:
                bucket, image_key = self._resolve_object_locator(
                    asset=asset,
                    default_bucket=CONFIG.s3.processed_bucket_name,
                )
            except RuntimeError as exc:
                _LOGGER.warning("%s, skipping", exc)
                continue
            local_path = self._stage_local_path(self.staging_root, asset, image_key)
            if local_path.name in seen_names:
                _LOGGER.debug(
                    "deduping duplicate staging target filename=%s asset=%s",
                    local_path.name,
                    getattr(asset, "id", "<unknown>"),
                )
                continue
            seen_names.add(local_path.name)
            stage_files.append((asset, bucket, image_key, local_path))
        return stage_files

    def stage_group(self, group_id: str, job_id: str) -> Path:
        _LOGGER.info("stage_group start group_id=%s job_id=%s", group_id, job_id)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        deduped_assets = self._group_stage_files(group_id)
        _LOGGER.debug("stage_group staged_assets=%s for group_id=%s", len(deduped_assets), group_id)
        worker_count = max(1, CONFIG.s3.multipart_concurrency)
        _LOGGER.debug("stage_group launching %s workers for file download", worker_count)
        pending: list[tuple[int, Any, str, str, Path]] = []
        staged_count = 0

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            for idx, (asset, bucket, image_key, local_path) in enumerate(deduped_assets, start=1):
                if local_path.is_file():
                    _LOGGER.debug(
                        "skip existing download [%s/%s] asset=%s local_path=%s",
                        idx,
                        len(deduped_assets),
                        asset.id,
                        local_path,
                    )
                    staged_count += 1
                    continue

                pending.append((idx, asset, bucket, image_key, local_path))

            futures = [
                pool.submit(
                    self._download_one,
                    idx=idx,
                    total=len(deduped_assets),
                    asset=asset,
                    image_key=image_key,
                    bucket=bucket,
                    local_path=local_path,
                )
                for idx, asset, bucket, image_key, local_path in pending
            ]
            for fut in as_completed(futures):
                fut.result()
                staged_count += 1

        _LOGGER.info("stage_group done group_id=%s file_count=%s", group_id, staged_count)
        return self.staging_root

    def write_manifest(self, job_id: str, staging_dir: Path, group_id: str) -> Path:
        _LOGGER.info("write_manifest start job_id=%s group_id=%s staging_dir=%s", job_id, group_id, staging_dir)
        manifest_path = staging_dir / f"{job_id}-manifest.json"
        staged_files = [str(local_path) for _, _, _, local_path in self._group_stage_files(group_id) if local_path.is_file()]
        payload = {
            "job_id": job_id,
            "group_id": group_id,
            "files": sorted(staged_files),
        }
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        _LOGGER.debug("write_manifest wrote %s", manifest_path)
        return manifest_path

    def copy_staging_to_session(
        self,
        job_id: str,
        staging_dir: Path,
        session_dir: Path,
        group_id: str,
    ) -> Path:
        """Copy staged assets into a RealityScan session data directory."""
        if session_dir.exists():
            shutil.rmtree(session_dir)
        session_dir.mkdir(parents=True, exist_ok=True)
        _LOGGER.info("copy_staging_to_session start job_id=%s staging_dir=%s session_dir=%s", job_id, staging_dir, session_dir)

        copy_jobs: list[tuple[Path, Path]] = []
        for _, _, _, source in self._group_stage_files(group_id):
            if not source.is_file():
                raise RuntimeError(f"staged file missing for group {group_id}: {source}")
            target = session_dir / source.name
            copy_jobs.append((source, target))
        if not copy_jobs:
            raise RuntimeError(f"no staged files found for group {group_id}")
        worker_count = max(1, min(CONFIG.s3.multipart_concurrency, len(copy_jobs)))
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [
                pool.submit(self._copy_one, source=source, target=target)
                for source, target in copy_jobs
            ]
            for fut in as_completed(futures):
                fut.result()
        _LOGGER.info("copy_staging_to_session done job_id=%s moved=%s target=%s", job_id, len(copy_jobs), session_dir)
        return session_dir
