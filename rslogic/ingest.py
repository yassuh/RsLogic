"""Ingest flow: move waiting objects to processed bucket and persist to label-db."""

from __future__ import annotations

import argparse
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from config import CONFIG

from .common.db import LabelDbStore
from .common.s3 import make_client, s3_object_keys, move_object
from .sidecar_parser import extract_gps_from_exif, parse_exif, parse_sidecar
from .upload_service import IMAGE_SUFFIXES, SIDECAR_SUFFIXES


@dataclass
class IngestItem:
    image_key: str
    sidecar_keys: list[str]
    filename: str
    stem_key: str


class IngestService:
    def __init__(self, max_workers: int | None = None) -> None:
        self.s3 = make_client(endpoint_url=CONFIG.s3.endpoint_url, region_name=CONFIG.s3.region)
        self.waiting = CONFIG.s3.bucket_name
        self.processed = CONFIG.s3.processed_bucket_name
        self.db = LabelDbStore(CONFIG.label_db.database_url, CONFIG.label_db.migration_root)
        self.max_workers = max_workers or CONFIG.s3.multipart_concurrency

    def _pair_objects(self) -> tuple[list[IngestItem], list[dict[str, Any]]]:
        def _artifact_anchor(value: str) -> str:
            clean = Path(value).name
            return str(Path(clean).with_suffix("")).lower()

        sidecar_for_stem: dict[str, list[str]] = {}
        images: list[IngestItem] = []
        unmatched = []

        for obj in s3_object_keys(self.s3, self.waiting):
            key = obj["Key"]
            suffix = Path(key).suffix.lower()
            # Reconstruct stem and directory for robust pairing.
            clean_key = Path(key)
            stem = _artifact_anchor(str(clean_key))
            stem = stem.lower()
            if suffix in IMAGE_SUFFIXES:
                images.append(IngestItem(image_key=key, sidecar_keys=[], filename=clean_key.name, stem_key=stem))
            elif suffix in SIDECAR_SUFFIXES:
                sidecar_for_stem.setdefault(stem, []).append(key)
            else:
                unmatched.append(obj)

        for image in images:
            stem = image.stem_key
            image.sidecar_keys.extend(sidecar_for_stem.get(stem, []))
        return images, unmatched

    def _parse_payload(self, image_tmp: Path, sidecar_keys: list[str], image_key: str) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        payload["exif"] = parse_exif(image_tmp)
        payload["sidecars"] = []
        for sidecar_key in sidecar_keys:
            with tempfile.NamedTemporaryFile(suffix=Path(sidecar_key).suffix) as fh:
                self.s3.download_file(self.waiting, sidecar_key, fh.name)
                payload["sidecars"].append({
                    "key": sidecar_key,
                    "parsed": parse_sidecar(Path(fh.name)),
                })
        payload["source"] = {
            "waiting_bucket": self.waiting,
            "waiting_key": image_key,
            "s3_url": f"s3://{self.waiting}/{image_key}",
        }
        payload["geodata"] = extract_gps_from_exif(payload["exif"].get("exif", {}))
        return payload

    def _ingest_one(self, item: IngestItem, group_id: str | None, batch: str) -> dict[str, Any]:
        dest_key = Path(item.image_key).name
        sidecar_dest_keys: list[str] = []
        with tempfile.NamedTemporaryFile(suffix=item.filename) as image_tmp:
            self.s3.download_file(self.waiting, item.image_key, image_tmp.name)
            payload = self._parse_payload(Path(image_tmp.name), item.sidecar_keys, item.image_key)
            geodata = payload.get("geodata", {})
            asset = self.db.create_image_asset(
                source_waiting_key=item.image_key,
                processed_bucket=self.processed,
                processed_key=dest_key,
                filename=item.filename,
                metadata=payload,
                sidecar_keys=item.sidecar_keys,
                source_bucket=self.waiting,
                latitude=geodata.get("latitude"),
                longitude=geodata.get("longitude"),
                altitude_m=geodata.get("altitude_m"),
            )
            self.db.update_asset_state(
                asset.id,
                {"source_waiting_key": item.image_key, "ingest_batch": batch, "ingest_state": "moved"},
            )
            move_object(self.s3, self.waiting, item.image_key, self.processed, dest_key)
            for sidecar_key in item.sidecar_keys:
                dest_sidecar = Path(sidecar_key).name
                move_object(self.s3, self.waiting, sidecar_key, self.processed, dest_sidecar)
                sidecar_dest_keys.append(dest_sidecar)
            self.db.update_asset_state(
                asset.id,
                {"ingest_state": "completed", "processed_key": dest_key, "processed_sidecar_keys": sidecar_dest_keys},
            )
            if group_id:
                self.db.attach_asset_to_group(group_id, asset.id)
            return {
                "image_id": asset.id,
                "bucket": self.processed,
                "key": dest_key,
                "sidecars": sidecar_dest_keys,
            }

    def run(
        self,
        group_name: str | None = None,
        limit: int | None = None,
        *,
        on_progress: Callable[[int, int], None] | None = None,
        max_workers: int | None = None,
    ) -> list[dict[str, Any]]:
        images, _ = self._pair_objects()
        if limit is not None:
            images = images[:limit]

        batch = uuid4().hex
        group_id = None
        if group_name:
            group_id, _ = self.db.get_or_create_group(group_name)
            group_id = group_id.id

        total = len(images)
        workers = max_workers or self.max_workers
        results: list[dict[str, Any]] = []
        failures: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._ingest_one, item, group_id, batch): item for item in images
            }
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if on_progress is not None:
                    on_progress(completed, total)
                try:
                    results.append(future.result())
                except BaseException as exc:
                    failures.append(exc)

        if failures:
            raise RuntimeError(f"failed to ingest {len(failures)} image(s): {failures[0]}")
        return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Move waiting media to processed bucket and record in label-db")
    parser.add_argument("--group", default=None, help="Optional group name to attach images")
    parser.add_argument("--limit", type=int, default=None, help="Optional max images to ingest")
    args = parser.parse_args()
    service = IngestService()
    items = service.run(group_name=args.group, limit=args.limit)
    for item in items:
        print(f"ingested {item['image_id']} -> {item['bucket']}/{item['key']}")
