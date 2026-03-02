"""Ingest flow: move waiting objects to processed bucket and persist to label-db."""

from __future__ import annotations

import argparse
from datetime import datetime
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


def _first_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        denominator = getattr(value, "denominator", 1)
        if denominator in (0, None):
            return None
        try:
            return float(value.numerator) / float(denominator)
        except Exception:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    v = _coerce_float(value)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _parse_capture_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y:%m:%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _extract_camera_payload(exif: dict[str, Any]) -> dict[str, Any]:
    captured_at = _parse_capture_time(_first_value(exif, "DateTimeOriginal", "DateTime", "DateTimeDigitized"))
    if captured_at is not None:
        captured_at = captured_at.isoformat()
    return {
        "drone_model": _first_value(exif, "Model", "ModelName", "LensModel", "ModelName2"),
        "camera_make": _first_value(exif, "Make", "Manufacturer", "Artist"),
        "camera_model": _first_value(exif, "Model", "CameraModel"),
        "focal_length_mm": _coerce_float(_first_value(exif, "FocalLength", "FocalLengthIn35mmFilm")),
        "image_width": _coerce_int(_first_value(exif, "ImageWidth", "ExifImageWidth")),
        "image_height": _coerce_int(_first_value(exif, "ImageLength", "ExifImageHeight")),
        "software": _first_value(exif, "Software", "SoftwareVersion"),
        "captured_at": captured_at,
    }


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

    def _pair_objects(
        self,
        *,
        on_status: Callable[[str], None] | None = None,
    ) -> tuple[list[IngestItem], list[dict[str, Any]], dict[str, int]]:
        def _artifact_anchor(value: str) -> str:
            clean = Path(value).name
            return str(Path(clean).with_suffix("")).lower()

        def _sidecar_anchor(value: str) -> list[str]:
            base = _artifact_anchor(value)
            image_suffix = Path(base).suffix.lower()
            if image_suffix in IMAGE_SUFFIXES:
                return [base, str(Path(base).with_suffix("")).lower()]
            return [base]

        sidecar_for_stem: dict[str, list[str]] = {}
        images: list[IngestItem] = []
        unmatched = []
        scanned = 0

        for obj in s3_object_keys(self.s3, self.waiting):
            scanned += 1
            if on_status is not None and scanned % 200 == 0:
                on_status(f"Scanning waiting bucket: {scanned} objects inspected...")
            key = obj["Key"]
            suffix = Path(key).suffix.lower()
            # Reconstruct stem and directory for robust pairing.
            clean_key = Path(key)
            stem = _artifact_anchor(str(clean_key))
            stem = stem.lower()
            if suffix in IMAGE_SUFFIXES:
                images.append(IngestItem(image_key=key, sidecar_keys=[], filename=clean_key.name, stem_key=stem))
            elif suffix in SIDECAR_SUFFIXES:
                for alt_stem in _sidecar_anchor(clean_key.as_posix()):
                    sidecar_for_stem.setdefault(alt_stem, []).append(key)
            else:
                unmatched.append(obj)

        for image in images:
            stem = image.stem_key
            image.sidecar_keys.extend(sidecar_for_stem.get(stem, []))
        sidecar_count = sum(len(v) for v in sidecar_for_stem.values())
        matched_with_sidecars = sum(1 for image in images if image.sidecar_keys)
        stats = {
            "scanned_objects": scanned,
            "images": len(images),
            "sidecars": sidecar_count,
            "images_with_sidecars": matched_with_sidecars,
            "unmatched_objects": len(unmatched),
        }
        if on_status is not None:
            on_status(
                "Pairing done: "
                f"{stats['images']} image(s), {stats['sidecars']} sidecar(s), "
                f"{stats['unmatched_objects']} unmatched object(s)"
            )
        return images, unmatched, stats

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
        if not sidecar_keys:
            payload["sidecars"].append({
                "key": f"{Path(image_key).name}.json",
                "parsed": {"json": {"embedded": True, "from_image_exif": True, "payload": payload["exif"]}},
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
            payload.setdefault("derived", {}).update(_extract_camera_payload(payload["exif"].get("exif", {})))
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
        on_result: Callable[[IngestItem, dict[str, Any]], None] | None = None,
        on_status: Callable[[str], None] | None = None,
        max_workers: int | None = None,
    ) -> list[dict[str, Any]]:
        if on_status is not None:
            on_status(f"Starting ingest preflight for bucket: {self.waiting}")
        images, _unmatched, pair_stats = self._pair_objects(on_status=on_status)
        if on_status is not None:
            on_status(
                "Ready-to-ingest: "
                f"{pair_stats['images']} images, "
                f"{pair_stats['unmatched_objects']} unmatched"
            )
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
        if on_status is not None:
            on_status(f"Starting ingest workers: {workers} threads")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._ingest_one, item, group_id, batch): item for item in images
            }
            completed = 0
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except BaseException as exc:
                    failures.append(exc)
                    continue
                completed += 1
                if on_progress is not None:
                    on_progress(completed, total)
                results.append(result)
                if on_result is not None:
                    on_result(item, result)

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
