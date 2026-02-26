"""Preview ingest utility that reads object metadata from the waiting S3 bucket."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import Any, Dict, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from config import S3Config, load_config
from rslogic.storage.s3 import S3ClientProvider

logger = logging.getLogger("rslogic.services.ingest_preview")


@dataclass(frozen=True)
class IngestMetadataItem:
    """Metadata snapshot for one object in the waiting bucket."""

    bucket: str
    key: str
    size: int
    etag: Optional[str]
    last_modified: Optional[datetime]
    storage_class: Optional[str]
    metadata: Dict[str, str]
    parsed_metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bucket": self.bucket,
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "storage_class": self.storage_class,
            "metadata": self.metadata,
            "parsed_metadata": self.parsed_metadata,
        }


@dataclass(frozen=True)
class _ObjectSummary:
    key: str
    size: int
    etag: Optional[str]
    last_modified: Optional[datetime]
    storage_class: Optional[str]


class S3MetadataIngestPreviewService:
    """Read and parse S3 user-defined metadata for waiting-bucket objects."""

    def __init__(
        self,
        s3_config: Optional[S3Config] = None,
        client_provider: Optional[S3ClientProvider] = None,
    ) -> None:
        self._config = s3_config or load_config().s3
        self._bucket = self._config.bucket_name
        self._client = (client_provider or S3ClientProvider(self._config)).get_client()
        self._head_concurrency = max(1, min(self._config.multipart_concurrency, 32))
        self._metadata_json_key = "metadata_json"
        self._metadata_json_format_key = "metadata_json_format"
        self._metadata_json_format_value = "json-flat-v1"

    def list_metadata(self, *, prefix: Optional[str] = None, limit: int = 100) -> List[IngestMetadataItem]:
        """Return parsed metadata for objects in the waiting bucket."""
        if limit < 1:
            raise ValueError("limit must be at least 1")

        objects = self._list_objects(prefix=prefix, limit=limit)
        if not objects:
            return []

        metadata_by_key: Dict[str, Dict[str, str]] = {}
        with ThreadPoolExecutor(max_workers=min(self._head_concurrency, len(objects))) as executor:
            futures = {
                executor.submit(self._head_metadata, obj.key): obj.key
                for obj in objects
            }
            for future in as_completed(futures):
                key = futures[future]
                metadata_by_key[key] = future.result()

        results: List[IngestMetadataItem] = []
        for obj in objects:
            metadata = metadata_by_key.get(obj.key, {})
            results.append(
                IngestMetadataItem(
                    bucket=self._bucket,
                    key=obj.key,
                    size=obj.size,
                    etag=obj.etag,
                    last_modified=obj.last_modified,
                    storage_class=obj.storage_class,
                    metadata=metadata,
                    parsed_metadata=self._parse_metadata(metadata),
                )
            )
        return results

    def _list_objects(self, *, prefix: Optional[str], limit: int) -> List[_ObjectSummary]:
        normalized_prefix = prefix.strip("/") + "/" if prefix else None
        paginator = self._client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=normalized_prefix) if normalized_prefix else paginator.paginate(Bucket=self._bucket)

        objects: List[_ObjectSummary] = []
        for page in pages:
            for entry in page.get("Contents", []):
                key = str(entry["Key"])
                objects.append(
                    _ObjectSummary(
                        key=key,
                        size=int(entry.get("Size", 0)),
                        etag=str(entry.get("ETag")).strip("\"") if entry.get("ETag") else None,
                        last_modified=entry.get("LastModified"),
                        storage_class=str(entry.get("StorageClass")) if entry.get("StorageClass") else None,
                    )
                )
                if len(objects) >= limit:
                    return objects
        return objects

    def _head_metadata(self, key: str) -> Dict[str, str]:
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
        except (BotoCoreError, ClientError):
            logger.exception("Failed to fetch head metadata bucket=%s key=%s", self._bucket, key)
            return {}
        metadata = response.get("Metadata") or {}
        return {str(k): str(v) for k, v in metadata.items()}

    def _parse_metadata(self, metadata: Dict[str, str]) -> Dict[str, Any]:
        parsed: Dict[str, Any] = {}
        for key, value in metadata.items():
            parsed[key] = self._parse_value(value)
        parsed.pop(self._metadata_json_key, None)
        parsed.pop(self._metadata_json_key.replace("_", "-"), None)
        parsed.pop(self._metadata_json_format_key, None)
        parsed.pop(self._metadata_json_format_key.replace("_", "-"), None)
        metadata_json = self._decode_metadata_json(metadata)
        if metadata_json:
            parsed.update(metadata_json)
        return parsed

    def _decode_metadata_json(self, metadata: Dict[str, str]) -> Dict[str, Any]:
        raw_json: Optional[str] = None
        for key_name in (self._metadata_json_key, self._metadata_json_key.replace("_", "-")):
            value = metadata.get(key_name)
            if value is None:
                continue
            rendered = str(value).strip()
            if rendered:
                raw_json = rendered
                break
        if not raw_json:
            return {}

        format_value = ""
        for key_name in (self._metadata_json_format_key, self._metadata_json_format_key.replace("_", "-")):
            value = metadata.get(key_name)
            if value is None:
                continue
            format_value = str(value).strip()
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
            payload = json.loads(raw_json)
        except ValueError:
            logger.warning("Failed to decode metadata_json for preview")
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload

    @staticmethod
    def _parse_value(value: str) -> Any:
        raw = value.strip()
        lowered = raw.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"

        try:
            if raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
                return int(raw)
        except Exception:
            pass

        try:
            if "." in raw:
                return float(raw)
        except Exception:
            pass

        iso_candidate = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso_candidate)
            return dt.isoformat()
        except ValueError:
            return raw
