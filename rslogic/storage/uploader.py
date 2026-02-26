"""High-speed upload support for image files with resumable multipart uploads."""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from botocore.exceptions import BotoCoreError, ClientError

from config import S3Config, load_config
from rslogic.metadata import (
    PRIMARY_IMAGE_EXTENSIONS,
    DroneMetadataExtractor,
    DroneSidecarMetadataExtractor,
)
from rslogic.storage.s3 import S3ClientProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadResult:
    bucket: str
    key: str
    size: int
    etags: int
    resumed: bool
    skipped_existing: bool = False


@dataclass(frozen=True)
class _UploadState:
    upload_id: str
    bucket: str
    key: str
    part_size: int
    etags: Dict[int, str]
    completed: bool = False


class S3MultipartUploader:
    """Multipart uploader with a resumable local manifest.

    The manifest lets interrupted uploads skip already-uploaded parts when resumed.
    Uploads are locked to the configured waiting bucket.
    Object keys are always the SHA-256 digest of file bytes.
    User metadata is extracted from image metadata before upload.
    """

    def __init__(
        self,
        s3_config: Optional[S3Config] = None,
        client_provider: Optional[S3ClientProvider] = None,
        manifest_dir: Optional[Path] = None,
        part_size: Optional[int] = None,
    ) -> None:
        self._config = s3_config or load_config().s3
        self._client = (client_provider or S3ClientProvider(self._config)).get_client()
        self._state_dir = Path(manifest_dir or self._config.manifest_dir)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._part_size = part_size or self._config.multipart_part_size
        self._concurrency = self._config.multipart_concurrency
        self._part_upload_concurrency = max(1, min(8, self._concurrency))
        self._metadata_extractor = DroneMetadataExtractor()
        self._sidecar_extractor = DroneSidecarMetadataExtractor()
        self._metadata_json_key = "metadata_json"
        self._metadata_json_format_key = "metadata_json_format"
        self._metadata_json_format_value = "json-flat-v1"

    def _manifest_path(self, file_path: Path, bucket: str, key: str) -> Path:
        stat = file_path.stat()
        signature = f"{bucket}|{key}|{file_path}|{stat.st_size}|{int(stat.st_mtime_ns)}"
        digest = hashlib.sha256(signature.encode("utf-8")).hexdigest()
        return self._state_dir / f"{digest}.upload.json"

    def _load_state(self, path: Path) -> Optional[_UploadState]:
        if not path.exists():
            return None

        with path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)

        return _UploadState(
            upload_id=payload["upload_id"],
            bucket=payload["bucket"],
            key=payload["key"],
            part_size=payload["part_size"],
            etags={int(k): v for k, v in payload["etags"].items()},
            completed=payload.get("completed", False),
        )

    def _write_state(self, path: Path, state: _UploadState) -> None:
        payload = {
            "upload_id": state.upload_id,
            "bucket": state.bucket,
            "key": state.key,
            "part_size": state.part_size,
            "etags": state.etags,
            "completed": state.completed,
        }
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(payload), encoding="utf-8")
        temp.replace(path)

    def _normalize_prefix(self, prefix: Optional[str]) -> str:
        if not prefix:
            return ""
        return prefix.strip("/") + "/"

    def _hashed_key(self, file_path: Path, prefix: Optional[str] = None) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as fp:
            while chunk := fp.read(8 * 1024 * 1024):
                digest.update(chunk)
        return f"{self._normalize_prefix(prefix)}{digest.hexdigest()}"

    @staticmethod
    def _sanitize_metadata_key(key: str) -> str:
        normalized = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in key.lower())
        normalized = normalized.strip("_")
        return (normalized or "meta")[:64]

    @staticmethod
    def _to_metadata_value(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (dict, list, tuple, set)):
            return None
        if isinstance(value, datetime):
            rendered = value.isoformat()
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        else:
            rendered = str(value)
        rendered = rendered.strip()
        if not rendered:
            return None
        return rendered

    @staticmethod
    def _to_json_compatible(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(k): S3MultipartUploader._to_json_compatible(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [S3MultipartUploader._to_json_compatible(v) for v in value]
        return str(value)

    @staticmethod
    def _deep_merge_dicts(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(base)
        for key, value in incoming.items():
            existing = merged.get(key)
            if isinstance(existing, dict) and isinstance(value, dict):
                merged[key] = S3MultipartUploader._deep_merge_dicts(existing, value)
            else:
                merged[key] = value
        return merged

    def _normalize_user_metadata(self, metadata: Dict[str, Any]) -> Dict[str, str]:
        normalized: Dict[str, str] = {}
        for raw_key, raw_value in metadata.items():
            key = self._sanitize_metadata_key(str(raw_key))
            value = self._to_metadata_value(raw_value)
            if value is None:
                continue
            normalized[key] = value
        return normalized

    def _extract_upload_metadata(self, file_path: Path) -> tuple[Dict[str, Any], Dict[str, Any]]:
        # Build both compact scalar metadata and full nested metadata payload.
        extracted: Dict[str, Any] = {"source_file": file_path.name}
        full_payload: Dict[str, Any] = {"source_file": file_path.name}
        parsed: Dict[str, Any] = {}
        if file_path.suffix.lower() in PRIMARY_IMAGE_EXTENSIONS:
            try:
                parsed = self._metadata_extractor.extract_from_path(file_path).as_dict()
            except Exception as exc:
                logger.debug("Metadata extraction skipped path=%s reason=%s", str(file_path), str(exc))

        if parsed:
            full_payload = self._deep_merge_dicts(full_payload, self._to_json_compatible(parsed))
            for field_name, field_value in parsed.items():
                if field_name == "extra":
                    continue
                if field_value is not None:
                    extracted[field_name] = field_value

        sidecar = self._sidecar_extractor.extract_for_media(file_path, parse=True)
        full_payload = self._deep_merge_dicts(
            full_payload,
            {
                "sidecar": self._to_json_compatible(
                    {
                        "present": list(sidecar.present_sidecars),
                        "missing_expected": list(sidecar.missing_expected),
                        "metadata": sidecar.metadata,
                    }
                )
            },
        )
        sidecar_core_fields = (
            "captured_at",
            "latitude",
            "longitude",
            "altitude_m",
            "drone_model",
            "focal_length_mm",
        )
        for field_name in sidecar_core_fields:
            value = sidecar.metadata.get(field_name)
            if value is not None and extracted.get(field_name) is None:
                extracted[field_name] = value

        for key, value in sidecar.metadata.items():
            if key in sidecar_core_fields:
                continue
            if value is not None:
                extracted[key] = value

        if sidecar.missing_expected:
            extracted["sidecar_missing"] = ",".join(ext.lstrip(".") for ext in sidecar.missing_expected)

        # Include a compact EXIF subset for waiting-bucket ingest into DB metadata.
        exif_payload = parsed.get("extra", {}).get("exif", {})
        if isinstance(exif_payload, dict):
            exif_field_map = {
                "DateTimeOriginal": "exif_datetime_original",
                "DateTimeDigitized": "exif_datetime_digitized",
                "ExposureTime": "exif_exposure_time",
                "FNumber": "exif_f_number",
                "ISOSpeedRatings": "exif_iso_speed_ratings",
                "PhotographicSensitivity": "exif_photographic_sensitivity",
                "FocalLengthIn35mmFilm": "exif_focal_length_35mm",
                "Orientation": "exif_orientation",
                "LensMake": "exif_lens_make",
                "LensModel": "exif_lens_model",
                "ExposureProgram": "exif_exposure_program",
                "WhiteBalance": "exif_white_balance",
                "Flash": "exif_flash",
            }
            for exif_key, output_key in exif_field_map.items():
                value = exif_payload.get(exif_key)
                if value is not None:
                    extracted[output_key] = value

            # Include additional scalar EXIF tags that are not already mapped.
            for exif_key, value in exif_payload.items():
                if exif_key in exif_field_map:
                    continue
                if isinstance(value, dict):
                    continue
                scalar = isinstance(value, (str, int, float, bool, datetime))
                if not scalar:
                    continue
                dynamic_key = self._sanitize_metadata_key(f"exif_{exif_key}")
                if dynamic_key not in extracted:
                    extracted[dynamic_key] = value
        return extracted, full_payload

    def _flatten_metadata_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        flattened: Dict[str, Any] = {}

        def _walk(prefix: str, value: Any) -> None:
            if isinstance(value, dict):
                if not value and prefix:
                    flattened[self._sanitize_metadata_key(prefix)] = None
                    return
                for nested_key, nested_value in value.items():
                    key_part = str(nested_key).strip() or "field"
                    child_prefix = f"{prefix}_{key_part}" if prefix else key_part
                    _walk(child_prefix, nested_value)
                return
            if isinstance(value, (list, tuple, set)):
                sequence = list(value)
                if not sequence and prefix:
                    flattened[self._sanitize_metadata_key(prefix)] = []
                    return
                for idx, nested_value in enumerate(sequence):
                    child_prefix = f"{prefix}_{idx}" if prefix else str(idx)
                    _walk(child_prefix, nested_value)
                return
            if not prefix:
                return
            flattened[self._sanitize_metadata_key(prefix)] = self._to_json_compatible(value)

        _walk("", payload)
        return flattened

    def _build_metadata_json(self, payload: Dict[str, Any]) -> str:
        flattened = self._flatten_metadata_payload(payload)
        return json.dumps(
            flattened,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    def _parts_for_upload(self, total_size: int, part_size: int) -> int:
        part = max(part_size, 5 * 1024 * 1024)
        return (total_size + part - 1) // part

    def _part_offsets(self, total_size: int, part_size: int) -> Iterable[tuple[int, int, int]]:
        part_count = self._parts_for_upload(total_size, part_size)
        for part_number in range(1, part_count + 1):
            offset = (part_number - 1) * part_size
            part_length = min(part_size, total_size - offset)
            if part_length <= 0:
                continue
            yield part_number, offset, part_length

    def _upload_part(self, file_path: Path, upload_id: str, bucket: str, key: str, part_number: int, offset: int, size: int) -> tuple[int, str]:
        with file_path.open("rb") as fp:
            fp.seek(offset)
            data = fp.read(size)

        response = self._client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return part_number, response["ETag"]

    def _sync_with_remote_parts(self, upload_id: str, bucket: str, key: str) -> Dict[int, str]:
        # S3 reports already-uploaded parts from the multipart session.
        parts: Dict[int, str] = {}
        paginator = self._client.get_paginator("list_parts")
        for page in paginator.paginate(Bucket=bucket, Key=key, UploadId=upload_id):
            for part in page.get("Parts", []):
                parts[int(part["PartNumber"])] = part["ETag"]
        return parts

    def _object_exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def upload_file(
        self,
        local_path: Path,
        bucket: Optional[str] = None,
        s3_key: Optional[str] = None,
        prefix: Optional[str] = None,
        extra_metadata: Optional[Dict[str, str]] = None,
        resume: bool = True,
        override_existing: bool = False,
        bytes_progress_callback: Optional[Callable[[int], None]] = None,
    ) -> UploadResult:
        file_path = Path(local_path)
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"Upload source does not exist: {file_path}")

        locked_bucket = self._config.bucket_name
        if bucket is not None and bucket.lower() != locked_bucket:
            raise ValueError(f"bucket override is not allowed; uploads are locked to {locked_bucket}")
        bucket_name = locked_bucket
        prefix = prefix if prefix is not None else self._config.scratchpad_prefix
        if s3_key:
            logger.warning("Ignoring explicit s3_key override for %s; object key is locked to file hash", str(file_path))

        final_key = self._hashed_key(file_path, prefix=prefix)
        if not override_existing and self._object_exists(bucket_name, final_key):
            logger.info("Skipping existing object key=%s bucket=%s (override_existing=false)", final_key, bucket_name)
            return UploadResult(
                bucket=bucket_name,
                key=final_key,
                size=0,
                etags=0,
                resumed=False,
                skipped_existing=True,
            )

        metadata_payload, full_metadata_payload = self._extract_upload_metadata(file_path)
        if extra_metadata:
            metadata_payload.update(extra_metadata)
            full_metadata_payload = self._deep_merge_dicts(
                full_metadata_payload,
                {"upload_extra": self._to_json_compatible(extra_metadata)},
            )

        sha256_value = final_key.rsplit("/", 1)[-1]
        metadata_payload["sha256"] = sha256_value
        full_metadata_payload = self._deep_merge_dicts(
            full_metadata_payload,
            {
                "sha256": sha256_value,
            },
        )
        metadata_payload[self._metadata_json_key] = self._build_metadata_json(full_metadata_payload)
        metadata_payload[self._metadata_json_format_key] = self._metadata_json_format_value
        user_metadata = self._normalize_user_metadata(metadata_payload)
        file_size = file_path.stat().st_size

        # For small files, use simple put_object for reduced overhead.
        if file_size <= max(self._part_size, 8 * 1024 * 1024):
            body = file_path.read_bytes()
            self._client.put_object(
                Bucket=bucket_name,
                Key=final_key,
                Body=body,
                Metadata=user_metadata,
                ContentType="application/octet-stream",
            )
            if bytes_progress_callback is not None:
                bytes_progress_callback(file_size)
            return UploadResult(bucket=bucket_name, key=final_key, size=file_size, etags=1, resumed=False)

        manifest_file = self._manifest_path(file_path, bucket_name, final_key)
        state = self._load_state(manifest_file) if resume else None
        resumed = False

        if state is None or state.completed:
            # Completed manifests are stale for re-upload flows (for example after ingest moves objects).
            # Reset state and start a fresh multipart upload session.
            manifest_file.unlink(missing_ok=True)
            # Initialize a new multipart upload.
            response = self._client.create_multipart_upload(
                Bucket=bucket_name,
                Key=final_key,
                Metadata=user_metadata,
            )
            upload_id = response["UploadId"]
            state = _UploadState(
                upload_id=upload_id,
                bucket=bucket_name,
                key=final_key,
                part_size=max(self._part_size, 5 * 1024 * 1024),
                etags={},
                completed=False,
            )
            self._write_state(manifest_file, state)
        else:
            if not state.completed and state.upload_id:
                remote_parts = self._sync_with_remote_parts(state.upload_id, state.bucket, state.key)
                merged = dict(state.etags)
                merged.update(remote_parts)
                state = _UploadState(
                    upload_id=state.upload_id,
                    bucket=state.bucket,
                    key=state.key,
                    part_size=state.part_size,
                    etags=merged,
                    completed=False,
                )
                self._write_state(manifest_file, state)
                if merged:
                    resumed = True
            else:
                state = None

        assert state is not None
        part_size = state.part_size

        try:
            pending_parts = [
                (part_number, offset, length)
                for part_number, offset, length in self._part_offsets(file_size, part_size)
                if part_number not in state.etags
            ]

            if pending_parts:
                with ThreadPoolExecutor(max_workers=min(self._part_upload_concurrency, len(pending_parts))) as executor:
                    futures = {
                        executor.submit(
                            self._upload_part,
                            file_path,
                            state.upload_id,
                            bucket_name,
                            final_key,
                            part_number,
                            offset,
                            length,
                        ): (part_number, length)
                        for part_number, offset, length in pending_parts
                    }

                    for future in as_completed(futures):
                        part_number, length = futures[future]
                        _, etag = future.result()
                        state.etags[part_number] = etag
                        self._write_state(manifest_file, state)
                        if bytes_progress_callback is not None:
                            bytes_progress_callback(length)

            parts = [
                {"PartNumber": part_number, "ETag": etag}
                for part_number, etag in sorted(state.etags.items())
            ]

            self._client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=final_key,
                UploadId=state.upload_id,
                MultipartUpload={"Parts": parts},
            )
            manifest_file.unlink(missing_ok=True)
            return UploadResult(
                bucket=bucket_name,
                key=final_key,
                size=file_size,
                etags=len(parts),
                resumed=resumed,
            )
        except (BotoCoreError, ClientError, OSError):
            logger.exception("S3 upload failed and state was preserved for resume", extra={"key": final_key})
            raise

    def upload_many(
        self,
        local_paths: Sequence[Path],
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        extra_metadata: Optional[Dict[str, str]] = None,
        resume: bool = True,
        override_existing: bool = False,
        concurrency: int = 1,
        progress_callback: Optional[Callable[[UploadResult], None]] = None,
        bytes_progress_callback: Optional[Callable[[int], None]] = None,
        error_callback: Optional[Callable[[Path, Exception], None]] = None,
    ) -> List[UploadResult]:
        resolved_prefix = prefix if prefix is not None else self._config.scratchpad_prefix
        if concurrency <= 1:
            results: List[UploadResult] = []
            for path in local_paths:
                try:
                    result = self.upload_file(
                        local_path=path,
                        bucket=bucket,
                        prefix=resolved_prefix,
                        extra_metadata=extra_metadata,
                        resume=resume,
                        override_existing=override_existing,
                        bytes_progress_callback=bytes_progress_callback,
                    )
                except Exception as exc:
                    if error_callback is None:
                        raise
                    error_callback(path, exc)
                    continue
                results.append(result)
                if progress_callback is not None:
                    progress_callback(result)
            return results

        results: List[UploadResult] = []
        with ThreadPoolExecutor(max_workers=min(concurrency, len(local_paths))) as executor:
            futures = {
                executor.submit(
                    self.upload_file,
                    local_path=path,
                    bucket=bucket,
                    prefix=resolved_prefix,
                    extra_metadata=extra_metadata,
                    resume=resume,
                    override_existing=override_existing,
                    bytes_progress_callback=bytes_progress_callback,
                ): path
                for path in local_paths
            }
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    if error_callback is None:
                        raise
                    error_callback(file_path, exc)
                    continue
                results.append(result)
                if progress_callback is not None:
                    progress_callback(result)
        return results
