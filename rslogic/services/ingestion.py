"""Server-side ingestion path for uploaded images."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import json
import logging
from pathlib import Path
import warnings
from typing import Any, Dict, Optional

from config import load_config
from sqlalchemy.exc import SAWarning

from rslogic.metadata import PRIMARY_IMAGE_EXTENSIONS, DroneMetadataExtractor
from rslogic.storage import StorageRepository, S3MultipartUploader
from rslogic.storage.s3 import S3ClientProvider

import hashlib

logger = logging.getLogger("rslogic.ingestion")


@dataclass(frozen=True)
class _WaitingObjectSummary:
    key: str
    size: int


class ImageIngestionService:
    """Reads image bytes from S3 or local disk, extracts metadata, and stores records."""

    def __init__(
        self,
        repository: Optional[StorageRepository] = None,
        extractor: Optional[DroneMetadataExtractor] = None,
        client_provider: Optional[S3ClientProvider] = None,
    ) -> None:
        cfg = load_config()
        self._repo = repository or StorageRepository()
        self._extractor = extractor or DroneMetadataExtractor()
        self._client = (client_provider or S3ClientProvider()).get_client()
        self._uploader = S3MultipartUploader()
        self._waiting_bucket_name = cfg.s3.bucket_name
        self._processed_bucket_name = cfg.s3.processed_bucket_name
        self._metadata_json_key = "metadata_json"
        self._metadata_json_format_key = "metadata_json_format"
        self._metadata_json_format_value = "json-flat-v1"
        self._scratchpad_prefix = cfg.s3.scratchpad_prefix

    def upload_and_ingest_files(
        self,
        *,
        group_name: str,
        local_paths: list[Path],
        prefix: Optional[str] = None,
        extra: Optional[Dict[str, str]] = None,
        resume: bool = True,
        concurrency: int = 24,
    ) -> list[str]:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")

        supported_paths = [path for path in local_paths if path.suffix.lower() in PRIMARY_IMAGE_EXTENSIONS]
        skipped_paths = len(local_paths) - len(supported_paths)
        if skipped_paths:
            logger.info(
                "Upload+ingest skipping unsupported files group_name=%s skipped_count=%s supported_count=%s",
                group_name,
                skipped_paths,
                len(supported_paths),
            )
        if not supported_paths:
            logger.warning("Upload+ingest no supported image files group_name=%s", group_name)
            return []

        target_prefix = prefix or self._scratchpad_prefix
        logger.info(
            "Upload+ingest batch started group_name=%s file_count=%s concurrency=%s prefix=%s",
            group_name,
            len(supported_paths),
            concurrency,
            target_prefix,
        )

        def _upload_and_ingest_one(local_path: Path) -> str:
            logger.debug("Upload+ingest file start group_name=%s path=%s", group_name, str(local_path))
            upload = self._uploader.upload_file(
                local_path=local_path,
                s3_key=None,
                prefix=target_prefix,
                extra_metadata=extra,
                resume=resume,
            )
            image_id = self.ingest_local_file(
                group_name=group_name,
                local_path=local_path,
                object_key=upload.key,
                extra=extra,
            )
            logger.debug(
                "Upload+ingest file complete group_name=%s path=%s object_key=%s image_id=%s",
                group_name,
                str(local_path),
                upload.key,
                image_id,
            )
            return image_id

        image_ids: list[str] = ["" for _ in supported_paths]
        with ThreadPoolExecutor(max_workers=min(concurrency, len(supported_paths) or 1)) as executor:
            futures = {
                executor.submit(_upload_and_ingest_one, local_path): idx
                for idx, local_path in enumerate(supported_paths)
            }
            for future in as_completed(futures):
                index = futures[future]
                image_ids[index] = future.result()
        logger.info("Upload+ingest batch completed group_name=%s image_count=%s", group_name, len(image_ids))
        return image_ids

    def ingest_local_file(
        self,
        *,
        group_name: str,
        local_path: Path,
        object_key: Optional[str] = None,
        extra: Optional[Dict[str, str]] = None,
    ) -> str:
        image_id = self.ingest_local_bytes(
            group_name=group_name,
            bucket_name=self._waiting_bucket_name,
            object_key=object_key or local_path.name,
            data=local_path.read_bytes(),
            filename=local_path.name,
            extra=extra,
        )
        return image_id

    def upload_and_ingest_file(
        self,
        *,
        group_name: str,
        local_path: Path,
        prefix: Optional[str] = None,
        extra: Optional[Dict[str, str]] = None,
        resume: bool = True,
    ) -> str:
        upload = self._uploader.upload_file(
            local_path=local_path,
            s3_key=None,
            prefix=prefix or self._scratchpad_prefix,
            extra_metadata=extra,
            resume=resume,
        )
        return self.ingest_local_file(
            group_name=group_name,
            local_path=local_path,
            object_key=upload.key,
            extra=extra,
        )

    def ingest_local_bytes(
        self,
        *,
        group_name: str,
        bucket_name: str,
        object_key: str,
        data: bytes,
        filename: str,
        extra: Optional[Dict[str, str]] = None,
    ) -> str:
        logger.debug(
            "Ingest local bytes group_name=%s bucket=%s object_key=%s size_bytes=%s",
            group_name,
            bucket_name,
            object_key,
            len(data),
        )
        metadata = self._extractor.extract_from_bytes(data).as_dict()
        if extra:
            metadata.setdefault("extra", {}).update(extra)

        normalized_key = object_key.lstrip("/")
        sha = hashlib.sha256(data).hexdigest()
        record = self._repo.create_image(
            group_name=group_name,
            bucket_name=bucket_name,
            object_key=normalized_key,
            uri=f"s3://{bucket_name}/{normalized_key}",
            filename=filename,
            metadata=metadata,
            sha256=sha,
            file_size=len(data),
        )
        logger.debug("Image record created image_id=%s group_name=%s", record.id, group_name)
        return record.id

    def ingest_from_s3(
        self,
        *,
        group_name: str,
        object_key: str,
        extra: Optional[Dict[str, str]] = None,
    ) -> str:
        logger.info("Ingest from S3 start group_name=%s object_key=%s", group_name, object_key)
        normalized_key = object_key.lstrip("/")
        response = self._client.get_object(Bucket=self._waiting_bucket_name, Key=normalized_key)
        data = response["Body"].read()
        image_id = self.ingest_local_bytes(
            group_name=group_name,
            bucket_name=self._waiting_bucket_name,
            object_key=normalized_key,
            filename=normalized_key.rsplit("/", 1)[-1],
            data=data,
            extra=extra,
        )
        logger.info("Ingest from S3 complete group_name=%s object_key=%s image_id=%s", group_name, object_key, image_id)
        return image_id

    def ingest_waiting_bucket_metadata(
        self,
        *,
        group_name: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 1000,
        concurrency: int = 24,
        override_existing: bool = False,
    ) -> Dict[str, Any]:
        """Read S3 user metadata from waiting-bucket objects and persist to image_assets."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")

        normalized_prefix = (prefix or "").strip("/") or None
        objects = self._list_waiting_objects(prefix=normalized_prefix, limit=limit)
        logger.info(
            "Waiting ingest started source_bucket=%s destination_bucket=%s prefix=%s object_count=%s concurrency=%s group_override=%s override_existing=%s",
            self._waiting_bucket_name,
            self._processed_bucket_name,
            normalized_prefix,
            len(objects),
            concurrency,
            group_name or "-",
            override_existing,
        )

        if not objects:
            return {
                "bucket": self._waiting_bucket_name,
                "processed_bucket": self._processed_bucket_name,
                "prefix": normalized_prefix,
                "group_name": group_name,
                "scanned": 0,
                "ingested": 0,
                "skipped": 0,
                "failed": 0,
                "moved": 0,
                "items": [],
            }

        items: list[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=min(concurrency, len(objects), 32)) as executor:
            futures = {
                executor.submit(self._ingest_waiting_object_metadata, obj, group_name, override_existing): obj.key
                for obj in objects
            }
            for future in as_completed(futures):
                items.append(future.result())

        ingested_count = sum(1 for item in items if item.get("status") in {"ingested", "updated"})
        skipped_count = sum(1 for item in items if item.get("status") == "skipped_existing")
        failed_count = sum(1 for item in items if item.get("status") == "failed")
        moved_count = sum(1 for item in items if item.get("moved") is True)
        logger.info(
            "Waiting ingest completed source_bucket=%s destination_bucket=%s prefix=%s scanned=%s ingested=%s skipped=%s failed=%s moved=%s",
            self._waiting_bucket_name,
            self._processed_bucket_name,
            normalized_prefix,
            len(objects),
            ingested_count,
            skipped_count,
            failed_count,
            moved_count,
        )
        return {
            "bucket": self._waiting_bucket_name,
            "processed_bucket": self._processed_bucket_name,
            "prefix": normalized_prefix,
            "group_name": group_name,
            "scanned": len(objects),
            "ingested": ingested_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "moved": moved_count,
            "items": items,
        }

    def _list_waiting_objects(self, *, prefix: Optional[str], limit: int) -> list[_WaitingObjectSummary]:
        paginator = self._client.get_paginator("list_objects_v2")
        pages = (
            paginator.paginate(Bucket=self._waiting_bucket_name, Prefix=f"{prefix}/")
            if prefix
            else paginator.paginate(Bucket=self._waiting_bucket_name)
        )

        objects: list[_WaitingObjectSummary] = []
        for page in pages:
            for entry in page.get("Contents", []):
                key = str(entry.get("Key", ""))
                if not key:
                    continue
                objects.append(
                    _WaitingObjectSummary(
                        key=key,
                        size=int(entry.get("Size", 0)),
                    )
                )
                if len(objects) >= limit:
                    return objects
        return objects

    def _ingest_waiting_object_metadata(
        self,
        summary: _WaitingObjectSummary,
        group_override: Optional[str],
        override_existing: bool,
    ) -> Dict[str, Any]:
        key = summary.key
        source_uri = f"s3://{self._waiting_bucket_name}/{key}"
        destination_uri = f"s3://{self._processed_bucket_name}/{key}"
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SAWarning)
                existing_destination = self._repo.get_image_by_uri(destination_uri)
                existing_source = self._repo.get_image_by_uri(source_uri)

            response = self._client.head_object(Bucket=self._waiting_bucket_name, Key=key)
            raw_metadata = {str(k): str(v) for k, v in (response.get("Metadata") or {}).items()}
            parsed_metadata = self._parse_s3_user_metadata(raw_metadata)
            parsed_metadata.pop(self._metadata_json_key, None)
            parsed_metadata.pop(self._metadata_json_key.replace("_", "-"), None)
            parsed_metadata.pop(self._metadata_json_format_key, None)
            parsed_metadata.pop(self._metadata_json_format_key.replace("_", "-"), None)
            full_metadata = self._load_metadata_json_from_headers(raw_metadata)
            combined_metadata = self._merge_metadata_payloads(parsed_metadata, full_metadata)
            normalized = self._normalize_metadata_keys(combined_metadata)

            if existing_destination is not None and not override_existing:
                self._move_waiting_object(key)
                return {
                    "key": key,
                    "image_id": existing_destination.id,
                    "group_name": group_override or self._default_group_name(),
                    "status": "skipped_existing",
                    "moved": True,
                    "source_uri": source_uri,
                    "destination_uri": destination_uri,
                }
            existing_status = "updated" if (existing_destination is not None or existing_source is not None) else "ingested"

            resolved_group = self._resolve_group_name_from_metadata(
                group_override=group_override,
                normalized_metadata=normalized,
            )
            metadata_fields = self._build_image_fields_from_metadata(normalized)
            metadata_fields.setdefault("extra", {})
            metadata_fields["extra"].update(
                {
                    "ingest_source": "s3_user_metadata",
                    "metadata_key_count": len(raw_metadata),
                    "metadata_json_loaded": bool(full_metadata),
                }
            )

            filename = str(normalized.get("source_file") or Path(key).name)
            sha256_value = normalized.get("sha256")
            if sha256_value is None:
                sha256 = Path(key).name
            else:
                sha256 = str(sha256_value)

            persist_to_destination = existing_destination is not None
            persisted_uri = destination_uri if persist_to_destination else source_uri
            persisted_bucket = self._processed_bucket_name if persist_to_destination else self._waiting_bucket_name

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SAWarning)
                record = self._repo.create_or_update_image(
                    group_name=resolved_group,
                    bucket_name=persisted_bucket,
                    object_key=key,
                    filename=filename,
                    metadata=metadata_fields,
                    metadata_json=combined_metadata,
                    sha256=sha256,
                    file_size=int(response.get("ContentLength", summary.size) or summary.size),
                    uri=persisted_uri,
                )
            self._move_waiting_object(key)
            if not persist_to_destination:
                updated_record = self._repo.update_image_storage_location(
                    record.id,
                    bucket_name=self._processed_bucket_name,
                    object_key=key,
                    uri=destination_uri,
                )
                if updated_record is not None:
                    record = updated_record
            logger.debug(
                "Waiting ingest object persisted and moved key=%s image_id=%s group_name=%s metadata_keys=%s",
                key,
                record.id,
                resolved_group,
                len(combined_metadata),
            )
            return {
                "key": key,
                "image_id": record.id,
                "group_name": resolved_group,
                "status": existing_status,
                "moved": True,
                "source_uri": source_uri,
                "destination_uri": destination_uri,
            }
        except Exception as exc:
            logger.error("Waiting ingest failed key=%s error=%s", key, str(exc))
            return {
                "key": key,
                "image_id": None,
                "group_name": group_override or self._default_group_name(),
                "status": "failed",
                "moved": False,
                "source_uri": source_uri,
                "destination_uri": destination_uri,
                "error": "failed to ingest object metadata",
            }

    def _move_waiting_object(self, key: str) -> None:
        normalized_key = key.lstrip("/")
        logger.debug(
            "Moving waiting object source_bucket=%s destination_bucket=%s key=%s",
            self._waiting_bucket_name,
            self._processed_bucket_name,
            normalized_key,
        )
        self._client.copy_object(
            Bucket=self._processed_bucket_name,
            Key=normalized_key,
            CopySource={
                "Bucket": self._waiting_bucket_name,
                "Key": normalized_key,
            },
            MetadataDirective="COPY",
        )
        self._client.delete_object(
            Bucket=self._waiting_bucket_name,
            Key=normalized_key,
        )

    def _load_metadata_json_from_headers(self, raw_metadata: Dict[str, str]) -> Dict[str, Any]:
        json_value: Optional[str] = None
        for key_name in (self._metadata_json_key, self._metadata_json_key.replace("_", "-")):
            candidate = raw_metadata.get(key_name)
            if candidate is None:
                continue
            rendered = str(candidate).strip()
            if rendered:
                json_value = rendered
                break
        if not json_value:
            return {}

        format_value = ""
        for key_name in (self._metadata_json_format_key, self._metadata_json_format_key.replace("_", "-")):
            candidate = raw_metadata.get(key_name)
            if candidate is None:
                continue
            format_value = str(candidate).strip()
            if format_value:
                break
        if format_value and format_value != self._metadata_json_format_value:
            logger.warning(
                "Unsupported metadata_json_format=%s expected=%s",
                format_value,
                self._metadata_json_format_value,
            )
            return {}
        try:
            payload = json.loads(json_value)
        except ValueError:
            logger.warning("Failed to parse metadata_json payload")
            return {}
        if not isinstance(payload, dict):
            logger.warning("metadata_json payload is not a JSON object")
            return {}
        return payload

    @staticmethod
    def _merge_metadata_payloads(raw_metadata: Dict[str, Any], full_metadata: Dict[str, Any]) -> Dict[str, Any]:
        if not full_metadata:
            return dict(raw_metadata)
        return ImageIngestionService._deep_merge_dicts(dict(raw_metadata), full_metadata)

    @staticmethod
    def _deep_merge_dicts(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(base)
        for key, value in incoming.items():
            existing = merged.get(key)
            if isinstance(existing, dict) and isinstance(value, dict):
                merged[key] = ImageIngestionService._deep_merge_dicts(existing, value)
            else:
                merged[key] = value
        return merged

    def _build_image_fields_from_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "captured_at": self._as_datetime(metadata.get("captured_at")),
            "latitude": self._as_float(metadata.get("latitude")),
            "longitude": self._as_float(metadata.get("longitude")),
            "altitude_m": self._as_float(metadata.get("altitude_m")),
            "drone_model": self._as_str(metadata.get("drone_model")),
            "camera_make": self._as_str(metadata.get("camera_make")),
            "camera_model": self._as_str(metadata.get("camera_model")),
            "focal_length_mm": self._as_float(metadata.get("focal_length_mm")),
            "image_width": self._as_int(metadata.get("image_width")),
            "image_height": self._as_int(metadata.get("image_height")),
            "software": self._as_str(metadata.get("software")),
            "extra": {},
        }

    def _resolve_group_name_from_metadata(
        self,
        *,
        group_override: Optional[str],
        normalized_metadata: Dict[str, Any],
    ) -> str:
        candidate = (group_override or "").strip()
        if candidate:
            return candidate
        from_metadata = normalized_metadata.get("group_name")
        if from_metadata:
            rendered = str(from_metadata).strip()
            if rendered:
                return rendered
        return self._default_group_name()

    @staticmethod
    def _parse_s3_user_metadata(raw_metadata: Dict[str, str]) -> Dict[str, Any]:
        return {str(key): ImageIngestionService._parse_s3_value(value) for key, value in raw_metadata.items()}

    @staticmethod
    def _normalize_metadata_keys(metadata: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in metadata.items():
            normalized[key.lower().replace("-", "_").strip()] = value
        return normalized

    @staticmethod
    def _parse_s3_value(value: str) -> Any:
        raw = value.strip()
        lowered = raw.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"

        if raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
            try:
                return int(raw)
            except ValueError:
                pass

        if "." in raw:
            try:
                return float(raw)
            except ValueError:
                pass

        iso_candidate = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso_candidate)
            return dt.isoformat()
        except ValueError:
            return raw

    @staticmethod
    def _as_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        rendered = str(value).strip()
        return rendered or None

    @staticmethod
    def _default_group_name() -> str:
        return load_config().default_group_name
