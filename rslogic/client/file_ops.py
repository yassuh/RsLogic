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

    @staticmethod
    def _build_stage_record(
        *,
        asset: Any,
        bucket: str,
        image_key: str,
        local_path: Path,
        cached: bool = False,
    ) -> dict[str, object]:
        return {
            "asset_id": str(asset.id),
            "image": {
                "bucket": bucket,
                "key": image_key,
                "local_path": str(local_path),
                "filename": asset.filename or "",
                "cached": cached,
            },
        }

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
        return self._build_stage_record(
            asset=asset,
            bucket=bucket,
            image_key=image_key,
            local_path=local_path,
            cached=False,
        )

    def _manifested_staging_files(self, staging_dir: Path) -> list[Path]:
        manifest_path = staging_dir / "stage-map.json"
        if not manifest_path.is_file():
            return []
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _LOGGER.warning("unable to read stage manifest for filtering stage moves path=%s", manifest_path)
            return []
        files = payload.get("files")
        if not isinstance(files, list):
            return []
        paths: list[Path] = []
        for item in files:
            if not isinstance(item, dict):
                continue
            image = item.get("image")
            if not isinstance(image, dict):
                continue
            local_path = image.get("local_path")
            if not isinstance(local_path, str):
                continue
            path = Path(local_path)
            if path.exists() and path.is_file():
                paths.append(path)
        return paths

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

    def stage_group(self, group_id: str, job_id: str) -> Path:
        _LOGGER.info("stage_group start group_id=%s job_id=%s", group_id, job_id)
        group_dir = self.staging_root
        group_dir.mkdir(parents=True, exist_ok=True)
        existing_staging_files = {
            path.name for path in group_dir.iterdir() if path.is_file() and path.name != "stage-map.json"
        }
        assets = self.db.image_assets_for_group(group_id)
        assets_list = list(assets)
        _LOGGER.debug("stage_group existing files=%s", len(existing_staging_files))
        deduped_assets: list[Any] = []
        seen_names: set[str] = set(existing_staging_files)
        for asset in assets_list:
            try:
                bucket, image_key = self._resolve_object_locator(
                    asset=asset,
                    default_bucket=CONFIG.s3.processed_bucket_name,
                )
            except RuntimeError as exc:
                _LOGGER.warning("%s, skipping", exc)
                continue
            local_path = self._stage_local_path(group_dir, asset, image_key)
            if local_path.name in seen_names:
                _LOGGER.debug("deduping duplicate staging target filename=%s asset=%s", local_path.name, getattr(asset, "id", "<unknown>"))
                continue
            seen_names.add(local_path.name)
            deduped_assets.append(asset)
        _LOGGER.debug("stage_group total assets=%s deduped_assets=%s for group_id=%s", len(assets_list), len(deduped_assets), group_id)
        worker_count = max(1, CONFIG.s3.multipart_concurrency)
        _LOGGER.debug("stage_group launching %s workers for file download", worker_count)
        manifests: list[dict[str, object]] = []
        pending: list[tuple[int, Any, str, str, Path]] = []

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            for idx, asset in enumerate(deduped_assets, start=1):
                bucket, image_key = self._resolve_object_locator(
                    asset=asset,
                    default_bucket=CONFIG.s3.processed_bucket_name,
                )
                local_path = self._stage_local_path(group_dir, asset, image_key)

                if local_path.name in existing_staging_files:
                    _LOGGER.debug(
                        "skip existing download [%s/%s] asset=%s local_path=%s",
                        idx,
                        len(deduped_assets),
                        asset.id,
                        local_path,
                    )
                    manifests.append(
                        self._build_stage_record(
                            asset=asset,
                            bucket=bucket,
                            image_key=image_key,
                            local_path=local_path,
                            cached=True,
                        )
                    )
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
                result = fut.result()
                manifests.append(result)

        _LOGGER.info("stage_group done group_id=%s manifest_count=%s", group_id, len(manifests))

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
        _LOGGER.info("write_manifest start job_id=%s group_id=%s staging_dir=%s", job_id, group_id, staging_dir)
        manifest_path = staging_dir / f"{job_id}-manifest.json"
        payload = {
            "job_id": job_id,
            "group_id": group_id,
            "files": sorted(str(p) for p in staging_dir.rglob("*") if p.is_file()),
            "group_mapping": sorted(str(p) for p in staging_dir.rglob("stage-map.json")),
        }
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        _LOGGER.debug("write_manifest wrote %s", manifest_path)
        return manifest_path

    def copy_staging_to_working(
        self,
        job_id: str,
        staging_dir: Path | None = None,
        working_dir: Path | None = None,
    ) -> Path:
        """Copy staged assets into a working destination."""
        if staging_dir is None:
            staging_dir = self.staging_root
        if working_dir is None:
            working_dir = self.working_projects_root / str(job_id)
        working_dir.mkdir(parents=True, exist_ok=True)
        _LOGGER.info("copy_staging_to_working start job_id=%s staging_dir=%s working_dir=%s", job_id, staging_dir, working_dir)
        moved = 0

        manifest_files = self._manifested_staging_files(staging_dir)
        if manifest_files:
            sources = sorted(manifest_files)
        else:
            sources = sorted(path for path in staging_dir.iterdir() if path.is_file() and path.name != "stage-map.json")
        for source in sources:
            if source.is_dir():
                continue
            moved += 1
            rel = source.relative_to(staging_dir)
            target = working_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                target.unlink()
            shutil.copy2(str(source), str(target))
        _LOGGER.info("copy_staging_to_working done job_id=%s moved=%s target=%s", job_id, moved, working_dir)
        return working_dir

    def move_staging_to_working(self, job_id: str, staging_dir: Path | None = None, working_dir: Path | None = None) -> Path:
        """Backward-compatible alias for copy-based staging transfer."""
        return self.copy_staging_to_working(job_id, staging_dir=staging_dir, working_dir=working_dir)
