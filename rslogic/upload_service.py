"""Upload flow utilities used by TUI and CLI."""

from __future__ import annotations

import json
import hashlib
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from .common.s3 import make_client
from config import CONFIG
from .sidecar_parser import parse_exif


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".gif", ".webp"}
SIDECAR_SUFFIXES = {".xmp", ".xml", ".json"}


def _artifact_anchor(item: Path, folder: Path) -> str:
    rel = item.relative_to(folder).with_suffix("")
    return rel.as_posix().lower()


def _sidecar_anchors(item: Path, folder: Path) -> set[str]:
    rel = item.relative_to(folder)
    base = rel.with_suffix("")
    anchors = {base.as_posix().lower()}
    image_stem = base.with_suffix("")
    if base.suffix.lower() in IMAGE_SUFFIXES:
        anchors.add(image_stem.as_posix().lower())
    return anchors


def _flatten_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    sanitized = "".join(ch if (ch.isalnum() or ch in "-._") else "_" for ch in prefix.strip())
    return sanitized.rstrip("_") + ("_" if sanitized else "")


def _hash_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class UploadRecord:
    image: Path
    sidecars: list[tuple[Path | dict[str, Any], str]]
    image_key: str
    sidecar_keys: list[str]
    bucket: str


def _scan_folder(
    folder: Path,
) -> tuple[list[Path], dict[str, list[Path]], dict[str, list[Path]]]:
    images = []
    by_stem = {}
    sidecars = {}
    for item in folder.rglob("*"):
        if not item.is_file():
            continue
        suffix = item.suffix.lower()
        if suffix in IMAGE_SUFFIXES:
            images.append(item)
            by_stem.setdefault(_artifact_anchor(item, folder), []).append(item)
            continue
        if suffix in SIDECAR_SUFFIXES:
            for anchor in _sidecar_anchors(item, folder):
                sidecars.setdefault(anchor, []).append(item)
    return images, by_stem, sidecars


def _upload_one(s3_client, bucket: str, source: Path, key: str) -> dict[str, str]:
    s3_client.upload_file(str(source), bucket, key)
    return {"local": str(source), "bucket": bucket, "key": key}


def _upload_embedded_sidecar(s3_client, bucket: str, payload: dict[str, Any], key: str) -> dict[str, str]:
    data = json.dumps(payload, indent=2, sort_keys=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as file_obj:
        file_obj.write(data)
        file_obj.flush()
        path = file_obj.name
    try:
        s3_client.upload_file(path, bucket, key)
    finally:
        os.remove(path)
    return {"local": f"embedded://{key}", "bucket": bucket, "key": key}


class FolderUploader:
    def __init__(self, bucket: str | None = None, manifest_dir: str | None = None, max_workers: int | None = None):
        self.bucket = bucket or CONFIG.s3.bucket_name
        self.max_workers = max_workers or CONFIG.s3.multipart_concurrency
        manifest_dir = manifest_dir or CONFIG.s3.manifest_dir
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.s3 = make_client(endpoint_url=CONFIG.s3.endpoint_url, region_name=CONFIG.s3.region)

    def run(
        self,
        folder: Path,
        root_prefix: str | None = None,
        *,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> list[dict[str, str]]:
        folder = folder.resolve()
        if not folder.exists():
            raise FileNotFoundError(folder)
        _, by_stem, by_sidecar = _scan_folder(folder)
        batch = uuid4().hex
        flat_prefix = _flatten_prefix(root_prefix)
        records: list[UploadRecord] = []

        for stem, image_paths in by_stem.items():
            for image in image_paths:
                file_hash = _hash_file(image)
                image_key = f"{flat_prefix}{file_hash}{image.suffix.lower()}"
                sidecar_paths = by_sidecar.get(stem, [])
                sidecar_specs: list[tuple[Path | dict[str, Any], str]] = []
                sidecar_keys = [f"{flat_prefix}{file_hash}{sp.suffix.lower()}" for sp in sidecar_paths]
                for sidecar_path, sidecar_key in zip(sidecar_paths, sidecar_keys):
                    sidecar_specs.append((sidecar_path, sidecar_key))
                if not sidecar_paths:
                    try:
                        exif_payload = parse_exif(image)
                    except Exception as exc:
                        exif_payload = {"exif": {}, "parse_error": str(exc)}
                    sidecar_key = f"{flat_prefix}{file_hash}.json"
                    sidecar_keys = [sidecar_key]
                    sidecar_specs.append(({"embedded": True, "image": image.name, "payload": exif_payload}, sidecar_key))
                else:
                    sidecar_keys = [f"{flat_prefix}{file_hash}{sp.suffix.lower()}" for sp in sidecar_paths]
                records.append(
                    UploadRecord(
                        image=image,
                        sidecars=sidecar_specs,
                        image_key=image_key,
                        sidecar_keys=sidecar_keys,
                        bucket=self.bucket,
                    )
                )

        manifest = []
        total = 0
        completed = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures: dict[Any, tuple[str, str]] = {}
            for rec in records:
                futures[pool.submit(_upload_one, self.s3, rec.bucket, rec.image, rec.image_key)] = (
                    "image",
                    str(rec.image),
                )
                for sidecar_source, sidecar_key in rec.sidecars:
                    if isinstance(sidecar_source, Path):
                        futures[pool.submit(_upload_one, self.s3, rec.bucket, sidecar_source, sidecar_key)] = (
                            "sidecar",
                            str(sidecar_source),
                        )
                    else:
                        futures[pool.submit(_upload_embedded_sidecar, self.s3, rec.bucket, sidecar_source, sidecar_key)] = (
                            "sidecar",
                            f"embedded://{sidecar_key}",
                        )
            total = len(futures)

            for future in as_completed(futures):
                kind, local = futures[future]
                result = future.result()
                completed += 1
                if on_progress is not None:
                    on_progress(completed, total)
                manifest.append({
                    "type": kind,
                    "local": local,
                    "s3_bucket": self.bucket,
                    "s3_key": result["key"],
                })

        manifest_id = root_prefix or batch
        manifest_path = self.manifest_dir / f"{os.getpid()}-{manifest_id}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest
