"""Ingest flow: move waiting objects to processed bucket and persist to label-db."""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from config import CONFIG

from .common.db import LabelDbStore
from .common.s3 import make_client, s3_object_keys, move_object
from .sidecar_parser import parse_exif, parse_sidecar
from .upload_service import IMAGE_SUFFIXES, SIDECAR_SUFFIXES


@dataclass
class IngestItem:
    image_key: str
    sidecar_keys: list[str]
    filename: str
    stem_key: str


class IngestService:
    def __init__(self) -> None:
        self.s3 = make_client(endpoint_url=CONFIG.s3.endpoint_url, region_name=CONFIG.s3.region)
        self.waiting = CONFIG.s3.bucket_name
        self.processed = CONFIG.s3.processed_bucket_name
        self.db = LabelDbStore(CONFIG.label_db.database_url, CONFIG.label_db.migration_root)

    def _pair_objects(self) -> tuple[list[IngestItem], list[dict[str, Any]]]:
        def _artifact_anchor(value: str) -> str:
            clean = Path(value)
            return str(clean.with_suffix("")).replace("\\", "/").lower()

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
        return payload

    def run(self, group_name: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        images, _ = self._pair_objects()
        if limit is not None:
            images = images[:limit]

        batch = uuid4().hex
        group_id = None
        if group_name:
            group_id, _ = self.db.get_or_create_group(group_name)
            group_id = group_id.id

        results = []
        for idx, item in enumerate(images, start=1):
            dest_key = f"{batch}/{idx:04d}_{item.filename}"
            sidecar_dest_keys: list[str] = []
            with tempfile.NamedTemporaryFile(suffix=item.filename) as image_tmp:
                self.s3.download_file(self.waiting, item.image_key, image_tmp.name)
                payload = self._parse_payload(Path(image_tmp.name), item.sidecar_keys, item.image_key)
                asset = self.db.create_image_asset(
                    source_waiting_key=item.image_key,
                    processed_bucket=self.processed,
                    processed_key=dest_key,
                    filename=item.filename,
                    metadata=payload,
                    sidecar_keys=item.sidecar_keys,
                    source_bucket=self.waiting,
                )
                self.db.update_asset_state(
                    asset.id,
                    {"source_waiting_key": item.image_key, "ingest_batch": batch, "ingest_state": "moved"},
                )
                move_object(self.s3, self.waiting, item.image_key, self.processed, dest_key)
                for sidecar_key in item.sidecar_keys:
                    sid = f"{Path(item.stem_key).name}_{Path(sidecar_key).name}"
                    dest_sidecar = f"{batch}/sidecars/{sid}"
                    move_object(self.s3, self.waiting, sidecar_key, self.processed, dest_sidecar)
                    sidecar_dest_keys.append(dest_sidecar)
                self.db.update_asset_state(
                    asset.id,
                    {"ingest_state": "completed", "processed_key": dest_key, "processed_sidecar_keys": sidecar_dest_keys},
                )
                if group_id:
                    self.db.attach_asset_to_group(group_id, asset.id)
                results.append({"image_id": asset.id, "bucket": self.processed, "key": dest_key, "sidecars": sidecar_dest_keys})
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
