"""Redis worker that executes processing jobs against a remote RealityScan node."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import ast
import inspect
import logging
import json
import threading
import re
import time
import shutil
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import os
import socket
from typing import Any, Dict, Optional, Sequence, Tuple

from config import load_config
from rslogic.storage.s3 import S3ClientProvider
from rslogic.jobs import RsToolsSdkRunner
from rslogic.jobs.command_channel import (
    COMMAND_TYPE_PROCESSING_JOB,
    COMMAND_TYPE_RSTOOL_COMMAND,
    COMMAND_TYPE_RSTOOL_DISCOVER,
    ProcessingCommand,
    ProcessingCommandResult,
    RESULT_STATUS_ACCEPTED,
    RESULT_STATUS_ERROR,
    RESULT_STATUS_OK,
    RESULT_STATUS_PROGRESS,
    RedisCommandBus,
)

ROOT = Path(__file__).resolve().parents[2]
_SDK_SOURCE = ROOT / "internal_tools" / "rstool-sdk" / "src"
_SDK_NODE_SOURCE = _SDK_SOURCE / "realityscan_sdk" / "resources" / "node.py"
_SDK_PROJECT_SOURCE = _SDK_SOURCE / "realityscan_sdk" / "resources" / "project.py"
if _SDK_SOURCE.exists() and str(_SDK_SOURCE) not in sys.path:
    sys.path.insert(0, str(_SDK_SOURCE))

_sdk_import_error: Optional[str] = None
try:
    from realityscan_sdk import RealityScanClient
    from realityscan_sdk.resources.node import NodeAPI
    from realityscan_sdk.resources.project import ProjectAPI
except Exception as exc:  # pragma: no cover - optional dependency until installed
    _sdk_import_error = f"{type(exc).__name__}: {exc}"
    RealityScanClient = None  # type: ignore[assignment]
    NodeAPI = None  # type: ignore[assignment]
    ProjectAPI = None  # type: ignore[assignment]


logger = logging.getLogger("rslogic.client.rsnode")

_SAFE_WINDOWS_PATH_RE = re.compile(r"^(?:[A-Za-z]:)?[\\/]")


def _as_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    rendered = str(value).strip().lower()
    if rendered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if rendered in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _as_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, default)


def _as_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _to_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, 1)


def _mask_secret(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return "MISSING"
    if len(value) <= 8:
        return "********"
    return f"{value[:3]}...{value[-3:]}"


def _collect_missing_sdk_env(
    *,
    base_url: str,
    client_id: str,
    app_token: str,
    auth_token: str,
) -> list[str]:
    missing: list[str] = []
    if not base_url:
        missing.append("RSLOGIC_RSTOOLS_SDK_BASE_URL")
    if not client_id:
        missing.append("RSLOGIC_RSTOOLS_SDK_CLIENT_ID")
    if not app_token:
        missing.append("RSLOGIC_RSTOOLS_SDK_APP_TOKEN")
    if not auth_token:
        missing.append("RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN")
    return missing


def _first_non_empty(payload: Optional[Dict[str, Any]], keys: Sequence[str]) -> str:
    if not payload:
        return ""
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        rendered = str(value).strip()
        if rendered:
            return rendered
    return ""


def _coerce_connection_payload(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if dataclasses.is_dataclass(value):
        return value.__dict__ if hasattr(value, "__dict__") else {}
    payload: Dict[str, Any] = {}
    for attribute in (
        "protocol",
        "hostAddress",
        "port",
        "authToken",
        "pairingPage",
        "landingPage",
        "allAddresses",
    ):
        if hasattr(value, attribute):
            payload[attribute] = getattr(value, attribute)
    return payload


class RsNodeClient:
    """Runs processing commands pulled from Redis and updates RSNode via SDK."""

    def __init__(
        self,
        *,
        command_queue_key: str,
        result_queue_key: str,
        redis_url: str,
        rs_base_url: str,
        rs_client_id: str,
        rs_app_token: str,
        rs_auth_token: str,
        block_timeout_seconds: int = 1,
        result_ttl_seconds: int = 3600,
        worker_count: int = 1,
    ) -> None:
        self._bus = RedisCommandBus(redis_url)
        self._command_queue_key = command_queue_key
        self._result_queue_key = result_queue_key
        self._block_timeout_seconds = max(int(block_timeout_seconds), 1)
        self._result_ttl_seconds = max(int(result_ttl_seconds), 1)
        self._rs_base_url = rs_base_url
        self._rs_client_id = rs_client_id
        self._rs_app_token = rs_app_token
        self._rs_auth_token = rs_auth_token
        self._executor = ThreadPoolExecutor(max_workers=max(1, int(worker_count)))
        self._worker_count = max(1, int(worker_count))
        self._stop_event = threading.Event()
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._sdk_state_lock = threading.Lock()
        self._heartbeat_interval_seconds = _parse_positive_int(
            os.getenv("RSLOGIC_CLIENT_HEARTBEAT_INTERVAL_SECONDS"),
            default=5,
        )
        self._heartbeat_ttl_seconds = _parse_positive_int(
            os.getenv("RSLOGIC_CLIENT_HEARTBEAT_TTL_SECONDS"),
            default=max(self._heartbeat_interval_seconds * 3, 15),
        )
        if self._heartbeat_ttl_seconds < self._heartbeat_interval_seconds + 1:
            self._heartbeat_ttl_seconds = self._heartbeat_interval_seconds + 1
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._presence_key = f"{self._command_queue_key}:presence:{self._worker_id}"
        self._sdk_connection = {}
        self._sdk_target_methods = self._load_sdk_target_methods()
        self._s3_client = None

    @staticmethod
    def _normalize_s3_extensions(raw: Any) -> Optional[list[str]]:
        if raw is None:
            return None
        if isinstance(raw, str):
            values = [item.strip() for item in raw.split(",")]
        elif isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray)):
            values = [str(item).strip() for item in raw]
        else:
            return None
        normalized = []
        for item in values:
            if not item:
                continue
            normalized.append(item.lstrip(".").lower())
        return normalized or None

    @staticmethod
    def _safe_relative_key(raw_key: str) -> str:
        normalized = str(raw_key or "").strip()
        if not normalized:
            return ""
        normalized = normalized.replace("\\", "/")
        while normalized.startswith("/"):
            normalized = normalized[1:]
        return normalized

    def _resolve_staging_root(self, *, working_directory: Path, payload: Dict[str, Any], filters: Dict[str, Any]) -> Path:
        raw_root = str(payload.get("s3_staging_root") or filters.get("s3_staging_root") or str(working_directory)).strip()
        if not raw_root:
            return working_directory
        candidate = Path(raw_root)
        if candidate.is_absolute() or _SAFE_WINDOWS_PATH_RE.match(raw_root):
            return candidate
        return (working_directory / candidate).resolve()

    def _resolve_imagery_folder(self, *, working_directory: Path, filters: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> Path:
        imagery_raw = str(
            (payload or {}).get("imagery_folder")
            or filters.get("sdk_imagery_folder")
            or "Imagery"
        ).strip()
        if not imagery_raw:
            imagery_raw = "Imagery"

        candidate = Path(imagery_raw)
        if candidate.is_absolute():
            return candidate
        # if user provides a path starting with leading slash, Path treats it as absolute on POSIX
        if _SAFE_WINDOWS_PATH_RE.match(imagery_raw):
            return Path(imagery_raw)
        return (working_directory / candidate).resolve()

    def _get_s3_client(self, *, region: Optional[str], endpoint_url: Optional[str]) -> Any:
        if self._s3_client is not None:
            return self._s3_client

        provider = S3ClientProvider()
        if region or endpoint_url:
            # Keep all existing behavior from config while allowing payload overrides.
            base_config = load_config().s3
            base_config = base_config.__class__(
                region=region or base_config.region,
                bucket_name=base_config.bucket_name,
                processed_bucket_name=base_config.processed_bucket_name,
                scratchpad_prefix=base_config.scratchpad_prefix,
                endpoint_url=endpoint_url,
                multipart_part_size=base_config.multipart_part_size,
                multipart_concurrency=base_config.multipart_concurrency,
                resume_uploads=base_config.resume_uploads,
                manifest_dir=base_config.manifest_dir,
            )
            provider = S3ClientProvider(base_config)

        self._s3_client = provider.get_client()
        return self._s3_client

    def _download_s3_images(self, *, bucket: str, image_keys: Sequence[str], payload: Dict[str, Any], imagery_folder: Path) -> dict[str, Any]:
        if not bucket:
            raise ValueError("s3_bucket is required when pull_s3_images is enabled")
        if not image_keys:
            return {
                "requested": 0,
                "downloaded": 0,
                "filtered": 0,
                "files": [],
            }

        region = _as_optional_str(payload.get("s3_region") or payload.get("region"))
        endpoint_url = _as_optional_str(payload.get("s3_endpoint_url") or payload.get("endpoint_url"))
        s3_prefix = _as_optional_str(payload.get("s3_prefix")) or ""
        max_files = _as_int(payload.get("s3_max_files") or payload.get("max_files") or 0, default=0)
        max_files = max(max_files, 0)

        allowed_ext = self._normalize_s3_extensions(payload.get("s3_extensions"))
        if allowed_ext:
            allowed = {f".{ext.lstrip('.').lower()}" for ext in allowed_ext}
        else:
            allowed = None

        imagery_folder.mkdir(parents=True, exist_ok=True)
        selected_keys: list[str] = []
        if image_keys:
            for raw_key in image_keys:
                key = self._safe_relative_key(str(raw_key))
                if not key:
                    continue
                if s3_prefix and key.startswith(s3_prefix):
                    trimmed = key[len(s3_prefix):].lstrip("/")
                else:
                    trimmed = key
                candidate_ext = Path(trimmed).suffix.lower()
                if allowed is not None and candidate_ext and candidate_ext not in allowed:
                    continue
                selected_keys.append(key)

        if max_files > 0:
            selected_keys = selected_keys[:max_files]

        if not selected_keys:
            return {
                "requested": len(image_keys),
                "downloaded": 0,
                "filtered": max(0, len(image_keys) - len(selected_keys)),
                "files": [],
            }

        client = self._get_s3_client(region=region, endpoint_url=endpoint_url)
        downloaded: list[str] = []
        for key in selected_keys:
            target_path = imagery_folder / self._safe_relative_key(key)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                client.download_file(bucket, key, str(target_path))
            except Exception as exc:
                raise RuntimeError(f"failed to download s3 object {key}: {exc}") from exc
            downloaded.append(str(target_path))

        filtered_count = len(image_keys) - len(downloaded)
        if max_files > 0:
            filtered_count = max(filtered_count, 0)
        return {
            "requested": len(image_keys),
            "downloaded": len(downloaded),
            "filtered": max(filtered_count, 0),
            "files": downloaded,
            "s3_prefix": s3_prefix or None,
            "s3_bucket": bucket,
            "imagery_folder": str(imagery_folder),
            "max_files": max_files or None,
            "extensions": allowed_ext,
        }

    def _prepare_processing_filters(self, *, working_directory: Path, job_id: str, payload: Dict[str, Any], image_keys: Sequence[str]) -> dict[str, Any]:
        filters = payload.get("filters")
        if not isinstance(filters, dict):
            filters = {}
        filters = dict(filters)

        pull_s3 = _as_bool(payload.get("pull_s3_images"), default=_as_bool(filters.get("pull_s3_images"), default=True))

        staging_root = self._resolve_staging_root(
            working_directory=working_directory,
            payload=payload,
            filters=filters,
        )
        staging_root.mkdir(parents=True, exist_ok=True)

        imagery_folder = self._resolve_imagery_folder(
            working_directory=staging_root,
            filters=filters,
            payload=payload,
        )

        # Ensure a clean staging folder for deterministic behavior per run.
        if imagery_folder.exists():
            shutil.rmtree(imagery_folder)
        imagery_folder.mkdir(parents=True, exist_ok=True)

        if not pull_s3:
            filters["sdk_imagery_folder"] = str(imagery_folder)
            filters["_rslogic_staging"] = {
                "s3_pull_enabled": False,
                "imagery_folder": str(imagery_folder),
                "job_id": job_id,
            }
            return filters

        bucket = _as_optional_str(payload.get("s3_bucket") or filters.get("s3_bucket"))
        if not bucket:
            bucket = load_config().s3.bucket_name
            if not bucket:
                raise ValueError("s3_bucket is required for S3 pull")

        summary = self._download_s3_images(
            bucket=bucket,
            image_keys=image_keys,
            imagery_folder=imagery_folder,
            payload={
                **filters,
                **{
                    "s3_bucket": bucket,
                    "s3_prefix": _as_optional_str(payload.get("s3_prefix") or filters.get("s3_prefix")),
                    "s3_region": _as_optional_str(payload.get("s3_region") or filters.get("s3_region")),
                    "s3_endpoint_url": _as_optional_str(payload.get("s3_endpoint_url") or filters.get("s3_endpoint_url")),
                    "s3_max_files": payload.get("s3_max_files") or filters.get("s3_max_files"),
                    "s3_extensions": payload.get("s3_extensions") or filters.get("s3_extensions"),
                },
            },
        )

        filters["sdk_imagery_folder"] = str(imagery_folder)
        filters["_rslogic_staging"] = {
            "s3_pull_enabled": True,
            "imagery_folder": str(imagery_folder),
            "job_id": job_id,
            **summary,
        }
        return filters

    def _effective_sdk_config(self, *, payload: Optional[Dict[str, Any]] = None) -> Tuple[str, str, str, str]:
        base_url = _first_non_empty(
            payload,
            (
                "base_url",
                "rs_base_url",
                "sdk_base_url",
                "RSLOGIC_RSTOOLS_SDK_BASE_URL",
                "RSTOOL_BASE_URL",
                "rstools_sdk_base_url",
            ),
        ) or self._rs_base_url
        client_id = _first_non_empty(
            payload,
            (
                "client_id",
                "rs_client_id",
                "sdk_client_id",
                "RSLOGIC_RSTOOLS_SDK_CLIENT_ID",
                "rstools_sdk_client_id",
            ),
        ) or self._rs_client_id
        app_token = _first_non_empty(
            payload,
            (
                "app_token",
                "rs_app_token",
                "sdk_app_token",
                "RSLOGIC_RSTOOLS_SDK_APP_TOKEN",
                "rstools_sdk_app_token",
            ),
        ) or self._rs_app_token
        auth_token = _first_non_empty(
            payload,
            (
                "auth_token",
                "rs_auth_token",
                "sdk_auth_token",
                "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN",
                "rstools_sdk_auth_token",
            ),
        ) or self._rs_auth_token
        return base_url.strip(), client_id.strip(), app_token.strip(), auth_token.strip()

    def _apply_payload_sdk_overrides(self, payload: Optional[Dict[str, Any]]) -> bool:
        if not payload:
            return False

        base_url, client_id, app_token, auth_token = self._effective_sdk_config(payload=payload)
        updated = False
        with self._sdk_state_lock:
            if base_url and base_url != self._rs_base_url:
                logger.info("Updating in-memory SDK base URL from command payload.")
                self._rs_base_url = base_url
                updated = True
            if client_id and client_id != self._rs_client_id:
                logger.info("Updating in-memory SDK client_id from command payload.")
                self._rs_client_id = client_id
                updated = True
            if app_token and app_token != self._rs_app_token:
                logger.info("Updating in-memory SDK app_token from command payload.")
                self._rs_app_token = app_token
                updated = True
            if auth_token and auth_token != self._rs_auth_token:
                logger.info("Updating in-memory SDK auth_token from command payload.")
                self._rs_auth_token = auth_token
                updated = True
        return updated

    def _apply_connection_payload(self, payload: Optional[Dict[str, Any]]) -> bool:
        if not payload:
            return False
        normalized = _coerce_connection_payload(payload)
        if not normalized:
            return False

        updated = False
        derived_base_url = self._derive_base_url_from_connection(normalized)
        if derived_base_url and derived_base_url != self._rs_base_url:
            with self._sdk_state_lock:
                if derived_base_url != self._rs_base_url:
                    logger.info("Updating in-memory SDK base URL from node connection payload.")
                    self._rs_base_url = derived_base_url
                    updated = True
        auth_token = str(normalized.get("authToken", "") or "").strip()
        with self._sdk_state_lock:
            if auth_token and auth_token != self._rs_auth_token:
                logger.info("Updating in-memory SDK auth token from node connection payload.")
                self._rs_auth_token = auth_token
                updated = True
        return updated

    @staticmethod
    def _derive_base_url_from_connection(payload: Dict[str, Any]) -> str:
        protocol = str(payload.get("protocol", "")).strip()
        host = str(payload.get("hostAddress", "")).strip()
        if not protocol or not host:
            return ""
        port_raw = payload.get("port", "")
        try:
            port = int(port_raw)
        except (TypeError, ValueError):
            port = 0
        if port > 0:
            return f"{protocol}://{host}:{port}"
        return f"{protocol}://{host}"

    def _build_sdk_runner(self, *, payload: Optional[Dict[str, Any]] = None) -> RsToolsSdkRunner:
        base_url, client_id, app_token, auth_token = self._effective_sdk_config(payload=payload)
        self._ensure_sdk_client_config(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            auth_token=auth_token,
        )
        return RsToolsSdkRunner(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            auth_token=auth_token,
        )

    def _refresh_node_connection(self, *, payload: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        if RealityScanClient is None:
            return False, "SDK package unavailable"

        if payload is not None:
            try:
                _ = self._apply_payload_sdk_overrides(payload)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Failed applying payload SDK overrides: %s", exc)

        base_url, client_id, app_token, auth_token = self._effective_sdk_config(payload=payload)
        if not (base_url and client_id and app_token):
            missing = []
            if not base_url:
                missing.append("base_url")
            if not client_id:
                missing.append("client_id")
            if not app_token:
                missing.append("app_token")
            return False, "missing_sdk_values: " + ", ".join(missing)

        try:
            require_auth = bool(auth_token)
            if not require_auth:
                logger.debug(
                    "No auth token available; attempting RSNode bootstrap with empty token to discover refreshed auth info."
                )
            with self._build_realityscan_client(payload=payload, require_auth=require_auth) as client:
                try:
                    client.node.connect_user()
                except Exception:
                    logger.debug(
                        "connect_user on startup failed; continuing to attempt connection lookup.",
                        exc_info=True,
                    )
                connection = client.node.connection()
                normalized = _coerce_connection_payload(connection)
                if not normalized:
                    return False, "invalid_connection_payload"
                updated = self._apply_connection_payload(normalized)
                with self._sdk_state_lock:
                    self._sdk_connection = normalized
                if updated:
                    logger.info("SDK connection payload refreshed from RSNode connection response.")
                else:
                    logger.debug("SDK connection lookup returned existing connection state.")
                inferred = self._derive_base_url_from_connection(normalized)
                if inferred:
                    logger.debug("RSNode connection endpoint observed: %s", inferred)
                return True, "ok"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    @staticmethod
    def _normalize_command_payload(value: Any) -> Any:
        if dataclasses.is_dataclass(value):
            return dataclasses.asdict(value)
        if isinstance(value, bytes):
            return {
                "type": "bytes",
                "encoding": "base64",
                "value": base64.b64encode(value).decode("ascii"),
            }
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, (list, tuple)):
            return [RsNodeClient._normalize_command_payload(item) for item in value]
        if isinstance(value, dict):
            return {str(key): RsNodeClient._normalize_command_payload(item) for key, item in value.items()}
        try:
            return json.loads(json.dumps(value))
        except Exception:
            return str(value)

    @staticmethod
    def _resolve_sdk_attribute(target_obj: Any, path: str) -> Any:
        current: Any = target_obj
        parts = [part.strip() for part in str(path).split(".") if part.strip()]
        if not parts:
            raise ValueError("path is empty")
        for part in parts:
            if part.startswith("_"):
                raise ValueError(f"private attribute access is blocked: {part}")
            if not hasattr(current, part):
                raise ValueError(f"Path '{path}' was not found on resolved target")
            current = getattr(current, part)
        return current

    @staticmethod
    def _resolve_sdk_callable(target_obj: Any, method_path: str) -> Any:
        resolved = RsNodeClient._resolve_sdk_attribute(target_obj, method_path)
        if not callable(resolved):
            raise ValueError(f"Resolved method '{method_path}' is not callable")
        return resolved

    @staticmethod
    def _load_sdk_target_methods() -> Dict[str, Dict[str, Dict[str, str]]]:
        methods: Dict[str, Dict[str, Dict[str, str]]] = {"node": {}, "project": {}}
        target_map: Dict[str, Any] = {
            "node": NodeAPI,
            "project": ProjectAPI,
        }
        for target, api_cls in target_map.items():
            if api_cls is None:
                source_methods = RsNodeClient._load_sdk_methods_from_source(target)
                if source_methods:
                    methods[target] = source_methods
                continue
            for name, member in inspect.getmembers(api_cls, predicate=inspect.isfunction):
                if name.startswith("_"):
                    continue
                try:
                    signature = str(inspect.signature(member))
                except (TypeError, ValueError):
                    signature = "(...)"
                methods[target][name] = {
                    "signature": signature,
                    "doc": (inspect.getdoc(member) or "").strip(),
                }
            if not methods[target]:
                source_methods = RsNodeClient._load_sdk_methods_from_source(target)
                if source_methods:
                    methods[target] = source_methods
        return methods

    @staticmethod
    def _load_sdk_methods_from_source(target: str) -> Dict[str, Dict[str, str]]:
        if target == "node":
            source_file = _SDK_NODE_SOURCE
            class_name = "NodeAPI"
        elif target == "project":
            source_file = _SDK_PROJECT_SOURCE
            class_name = "ProjectAPI"
        else:
            return {}

        if not source_file.exists():
            return {}

        try:
            source_text = source_file.read_text(encoding="utf-8")
            source_tree = ast.parse(source_text)
        except Exception:
            return {}

        target_class: Optional[ast.ClassDef] = None
        for child in source_tree.body:
            if isinstance(child, ast.ClassDef) and child.name == class_name:
                target_class = child
                break
        if target_class is None:
            return {}

        method_signatures: Dict[str, str] = {}
        method_docs: Dict[str, str] = {}

        for node in target_class.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name.startswith("_"):
                    continue
                try:
                    method_signatures[node.name] = f"({ast.unparse(node.args)})"
                except Exception:
                    method_signatures[node.name] = "(...)"
                method_docs[node.name] = (ast.get_docstring(node) or "").strip()

            if isinstance(node, ast.Assign):
                if not isinstance(node.value, (ast.Name, ast.Attribute)):
                    continue
                alias_target = ast.unparse(node.value)
                for target_node in node.targets:
                    if not isinstance(target_node, ast.Name):
                        continue
                    alias_name = target_node.id
                    if alias_name.startswith("_"):
                        continue
                    source_signature = method_signatures.get(alias_target)
                    if source_signature is None and isinstance(node.value, ast.Name):
                        source_signature = method_signatures.get(node.value.id)
                    method_signatures[alias_name] = source_signature or "(...)"
                    method_docs[alias_name] = f"Alias for {alias_target}" if not method_docs.get(alias_name) else method_docs[alias_name]

        methods: Dict[str, Dict[str, str]] = {}
        for name in sorted(method_signatures):
            methods[name] = {
                "signature": method_signatures[name],
                "doc": method_docs.get(name, ""),
            }
        return methods

    @staticmethod
    def _ensure_sdk_client_config(
        *,
        base_url: str,
        client_id: str,
        app_token: str,
        auth_token: str,
        require_auth: bool = True,
    ) -> None:
        missing: list[str] = []
        if not base_url:
            missing.append("base_url")
        if not client_id:
            missing.append("client_id")
        if not app_token:
            missing.append("app_token")
        if require_auth and not auth_token:
            missing.append("auth_token")
        if missing:
            raise ValueError(f"missing sdk configuration: {', '.join(missing)}")

    def _build_realityscan_client(
        self,
        payload: Optional[Dict[str, Any]] = None,
        *,
        require_auth: bool = True,
    ) -> RealityScanClient:
        base_url, client_id, app_token, auth_token = self._effective_sdk_config(payload=payload)
        self._ensure_sdk_client_config(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            auth_token=auth_token,
            require_auth=require_auth,
        )
        return RealityScanClient(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            auth_token=auth_token,
        )

    def close(self) -> None:
        self._stop_heartbeat()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=max(self._heartbeat_interval_seconds, 1))
        try:
            self._publish_presence(status="stopped")
        except Exception:
            logger.exception("Failed to publish stopped presence for worker_id=%s", self._worker_id)
        try:
            self._bus.delete(self._presence_key)
        except Exception:
            logger.warning("Failed to delete presence key for worker_id=%s", self._worker_id)
        self._stop_event.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._bus.close()

    def _build_presence_payload(self, *, status: str) -> Dict[str, Any]:
        return {
            "worker_id": self._worker_id,
            "status": status,
            "last_seen": _to_utc_iso(),
            "command_queue": self._command_queue_key,
            "result_queue": self._result_queue_key,
            "workers": self._worker_count,
            "sdk_base_url": self._rs_base_url,
            "pid": os.getpid(),
        }

    def _publish_presence(self, *, status: str) -> None:
        payload = self._build_presence_payload(status=status)
        self._bus.set_presence(
            self._presence_key,
            payload,
            ttl_seconds=self._heartbeat_ttl_seconds,
        )

    def presence_info(self) -> Dict[str, Any]:
        return {
            "presence_key": self._presence_key,
            "heartbeat_interval_seconds": self._heartbeat_interval_seconds,
            "heartbeat_ttl_seconds": self._heartbeat_ttl_seconds,
        }

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop_event.is_set():
            try:
                self._publish_presence(status="online")
            except Exception as exc:  # pragma: no cover - runtime loop guard
                logger.warning("Failed to publish rsnode client presence: %s", exc)
            self._heartbeat_stop_event.wait(self._heartbeat_interval_seconds)

    def _start_heartbeat(self) -> None:
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"rsnode-presence-{self._worker_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop_event.set()

    def _publish(
        self,
        *,
        command: ProcessingCommand,
        status: str,
        message: Optional[str],
        progress: Optional[float],
        data: Optional[Dict[str, Any]],
        error: Optional[str] = None,
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> None:
        payload = ProcessingCommandResult(
            command_id=command.command_id,
            command_type=command.command_type,
            status=status,
            message=message,
            progress=progress,
            data=data,
            error=error,
            started_at=started_at,
            finished_at=finished_at,
        ).to_payload()
        self._bus.push(self._result_queue_key, payload, expire_seconds=self._result_ttl_seconds)
        if command.reply_to:
            self._bus.push(command.reply_to, payload, expire_seconds=self._result_ttl_seconds)

    def _handle_processing_command(self, command: ProcessingCommand) -> None:
        started_at = _to_utc_iso()
        payload = command.payload
        try:
            _ = self._apply_payload_sdk_overrides(payload)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed applying processing payload SDK overrides: %s", exc)
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="missing job_id in processing command payload",
                progress=0.0,
                data=None,
                error="missing job_id",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        raw_working_directory = str(payload.get("working_directory") or "").strip()
        if raw_working_directory:
            working_directory = Path(raw_working_directory)
        else:
            working_directory = Path("/tmp") / "rslogic-rsnode-jobs" / job_id
        working_directory.mkdir(parents=True, exist_ok=True)

        image_keys = payload.get("image_keys") if isinstance(payload.get("image_keys"), list) else []
        image_keys = [str(item) for item in image_keys]
        filters = self._prepare_processing_filters(
            working_directory=working_directory,
            job_id=job_id,
            payload=payload,
            image_keys=image_keys,
        )

        def progress_cb(progress: float, message: str, details: Optional[Dict[str, Any]]) -> None:
            self._publish(
                command=command,
                status=RESULT_STATUS_PROGRESS,
                message=message,
                progress=progress,
                data={"job_id": job_id, "details": details or {}},
            )

        try:
            self._publish(
                command=command,
                status=RESULT_STATUS_ACCEPTED,
                message="processing job started",
                progress=10.0,
                data={"job_id": job_id},
                started_at=started_at,
            )
            try:
                refreshed, refresh_reason = self._refresh_node_connection(payload=payload)
                if not refreshed:
                    logger.debug("SDK bootstrap not fully refreshed before processing command: %s", refresh_reason)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("SDK bootstrap refresh failed before processing command: %s", exc)
            runner = self._build_sdk_runner(payload=payload)
            result = runner.run(
                working_directory=working_directory,
                image_keys=image_keys,
                filters=filters,
                job_id=job_id,
                progress_callback=progress_cb,
            )
            self._publish(
                command=command,
                status=RESULT_STATUS_OK,
                message="processing completed",
                progress=100.0,
                data={"job_id": job_id, "result": result},
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
        except Exception as exc:
            error_message = str(exc)
            logger.exception(
                "RSNode client processing failed job_id=%s command_id=%s",
                job_id,
                command.command_id,
            )
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="processing failed",
                progress=100.0,
                data={"job_id": job_id, "error_class": exc.__class__.__name__},
                error=error_message,
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )

    def _handle_sdk_discovery(self, command: ProcessingCommand) -> None:
        started_at = _to_utc_iso()
        payload = command.payload
        target_request = str(payload.get("target") or "").strip().lower()
        requested_targets = []
        if target_request:
            requested_targets.append(target_request)
        else:
            requested_targets.extend(["node", "project"])
        unsupported_targets = [t for t in requested_targets if t not in {"node", "project"}]
        if unsupported_targets:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid target for rstool_sdk.discover",
                progress=0.0,
                data={"requested_targets": requested_targets},
                error=f"target must be node or project; got {unsupported_targets}",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        data: Dict[str, Any] = {
            "available": {},
            "sdk_available": bool(_sdk_import_error is None),
        }
        if _sdk_import_error:
            data["import_error"] = _sdk_import_error
        for target in requested_targets:
            if target == "node":
                methods = dict(sorted(self._sdk_target_methods["node"].items()))
            else:
                methods = dict(sorted(self._sdk_target_methods["project"].items()))
            data["available"][target] = methods

        if data["sdk_available"] is False:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="SDK not available",
                progress=0.0,
                data=data,
                error="realityscan_sdk could not be imported",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        self._publish(
            command=command,
            status=RESULT_STATUS_OK,
            message="discovered rstool commands",
            progress=100.0,
            data=data,
            started_at=started_at,
            finished_at=_to_utc_iso(),
        )

    def _handle_rstool_command(self, command: ProcessingCommand) -> None:
        started_at = _to_utc_iso()
        payload = command.payload
        if RealityScanClient is None:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="RealityScan SDK not available",
                progress=0.0,
                data={"command_id": command.command_id},
                error="realityscan_sdk package is not importable",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        target = str(payload.get("target") or "node").strip().lower()
        target_object = str(payload.get("target_object") or "").strip() or None
        method_name = str(payload.get("method") or "").strip()
        if target not in {"node", "project", "client"}:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid target",
                progress=0.0,
                data={"command_id": command.command_id},
                error="target must be `node`, `project`, or `client`",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return
        if not method_name:
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="missing method",
                progress=0.0,
                data={"command_id": command.command_id},
                error="payload requires `method`",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        args = payload.get("args")
        kwargs = payload.get("kwargs")
        if args is None:
            args = []
        if not isinstance(args, list):
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid args",
                progress=0.0,
                data={"method": method_name},
                error="`args` must be an array",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return
        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="invalid kwargs",
                progress=0.0,
                data={"method": method_name},
                error="`kwargs` must be an object",
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )
            return

        session_action = str(payload.get("session_action") or "").strip().lower()
        session_id = payload.get("session") if payload.get("session") is not None else payload.get("session_id")
        try:
            try:
                _ = self._apply_payload_sdk_overrides(payload)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Failed applying rstool payload SDK overrides: %s", exc)

            refreshed, refresh_reason = self._refresh_node_connection(payload=payload)
            if not refreshed:
                logger.debug("SDK bootstrap not refreshed before rstool command: %s", refresh_reason)

            with self._build_realityscan_client(payload=payload) as client:
                if session_id is not None:
                    client.session = str(session_id)

                if target == "node":
                    command_root = client.node
                elif target == "project":
                    command_root = client.project
                else:
                    command_root = client

                if target == "project":
                    if session_action == "open":
                        guid = payload.get("project_guid") or payload.get("guid") or payload.get("session_key")
                        project_name = payload.get("project_name")
                        if not guid:
                            raise ValueError("`session_action=open` requires `project_guid`")
                        client.project.open(str(guid), name=project_name)
                    elif session_action == "create":
                        client.project.create()
                    elif session_action == "disconnect":
                        client.project.disconnect()

                if target_object:
                    base_target = RsNodeClient._resolve_sdk_attribute(command_root, target_object)
                else:
                    base_target = command_root

                method = self._resolve_sdk_callable(base_target, method_name)

                self._publish(
                    command=command,
                    status=RESULT_STATUS_ACCEPTED,
                    message="rstool sdk command started",
                    progress=15.0,
                    data={"target": target, "method": method_name},
                    started_at=started_at,
                )

                result = method(*args, **kwargs)
                normalized = self._normalize_command_payload(result)
                updated_connection = self._apply_connection_payload(_coerce_connection_payload(normalized))
                if updated_connection:
                    logger.debug(
                        "RSTool command returned connection payload and updated in-memory SDK state."
                    )
                self._publish(
                    command=command,
                    status=RESULT_STATUS_OK,
                    message="rstool sdk command completed",
                    progress=100.0,
                    data={
                        "target": target,
                        "method": method_name,
                        "result": normalized,
                        "connection_refreshed": bool(updated_connection),
                    },
                    started_at=started_at,
                    finished_at=_to_utc_iso(),
                )
        except Exception as exc:
            logger.exception(
                "RSNode SDK command failed command_id=%s target=%s method=%s",
                command.command_id,
                target,
                method_name,
            )
            self._publish(
                command=command,
                status=RESULT_STATUS_ERROR,
                message="rstool sdk command failed",
                progress=100.0,
                data={
                    "target": target,
                    "method": method_name,
                    "target_object": target_object,
                    "error_class": exc.__class__.__name__,
                },
                error=str(exc),
                started_at=started_at,
                finished_at=_to_utc_iso(),
            )

    def _dispatch(self, raw: Dict[str, Any]) -> None:
        try:
            command = ProcessingCommand.parse(raw)
        except Exception as exc:
            logger.error("Invalid command payload: %s", exc)
            return

        if command.command_type == COMMAND_TYPE_PROCESSING_JOB:
            self._executor.submit(self._handle_processing_command, command)
            return
        if command.command_type == COMMAND_TYPE_RSTOOL_DISCOVER:
            self._executor.submit(self._handle_sdk_discovery, command)
            return
        if command.command_type == COMMAND_TYPE_RSTOOL_COMMAND:
            self._executor.submit(self._handle_rstool_command, command)
            return

        logger.warning("Unsupported command type=%s command_id=%s", command.command_type, command.command_id)

    def process_once(self, timeout_seconds: Optional[int] = None) -> bool:
        timeout = max(int(timeout_seconds or self._block_timeout_seconds), 1)
        raw = self._bus.pop(self._command_queue_key, timeout_seconds=timeout)
        if raw is None:
            return False
        self._dispatch(raw)
        return True

    def run_forever(self) -> None:
        logger.info(
            "RSNode client started command_queue=%s result_queue=%s workers=%s",
            self._command_queue_key,
            self._result_queue_key,
            self._worker_count,
        )
        self._start_heartbeat()
        self._publish_presence(status="online")
        try:
            refreshed, reason = self._refresh_node_connection()
            if refreshed:
                logger.info("SDK bootstrap completed during startup: %s", reason)
            else:
                logger.warning("SDK bootstrap incomplete during startup: %s", reason)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("SDK startup bootstrap raised an unexpected error: %s", exc)
        while not self._stop_event.is_set():
            try:
                self.process_once(self._block_timeout_seconds)
            except Exception as exc:  # pragma: no cover - runtime loop guard
                logger.exception("RSNode client loop error: %s", exc)
                time.sleep(1.0)
        self._stop_heartbeat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RsLogic RSNode client worker")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    subparsers = parser.add_subparsers(dest="command", required=False)

    run_parser = subparsers.add_parser("run", help="Run RSNode command worker")
    run_parser.add_argument("--workers", type=int, default=1, help="Concurrent worker count")
    run_parser.add_argument("--once", action="store_true", help="Process one command and exit")
    run_parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout seconds for a single pop attempt",
    )

    run_alias = subparsers.add_parser("worker", help="Alias for run")
    run_alias.add_argument("--workers", type=int, default=1, help="Concurrent worker count")
    run_alias.add_argument("--once", action="store_true", help="Process one command and exit")
    run_alias.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout seconds for a single pop attempt",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command not in {None, "run", "worker"}:
        raise SystemExit(f"unsupported command: {args.command}")

    action = args.command or "run"
    workers = getattr(args, "workers", 1)
    timeout = getattr(args, "timeout", None)
    is_once = bool(getattr(args, "once", False))
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    config = load_config()
    redis_url = config.queue.redis_url
    rs_base_url = (config.rstools.sdk_base_url or "").strip()
    rs_client_id = (config.rstools.sdk_client_id or "").strip()
    rs_app_token = (config.rstools.sdk_app_token or "").strip()
    rs_auth_token = (config.rstools.sdk_auth_token or "").strip()

    logger.info("RSNode client startup config redis_url=%s", redis_url)
    logger.info(
        "RSNode SDK env: base_url=%s client_id=%s app_token=%s auth_token=%s",
        _mask_secret(rs_base_url),
        _mask_secret(rs_client_id),
        _mask_secret(rs_app_token),
        _mask_secret(rs_auth_token),
    )

    missing_sdk_env = _collect_missing_sdk_env(
        base_url=rs_base_url,
        client_id=rs_client_id,
        app_token=rs_app_token,
        auth_token=rs_auth_token,
    )
    if missing_sdk_env:
        logger.warning(
            "Missing SDK env values at startup: %s. Client will start; SDK-authenticated commands may fail "
            "until credentials are provided.",
            ", ".join(missing_sdk_env),
        )

    logger.info("RSNode client startup: pinging Redis control queue")
    bus: Optional[RedisCommandBus] = None
    try:
        bus = RedisCommandBus(redis_url)
        bus.ping()
        logger.info("RSNode client startup: redis ping successful")
    except Exception as exc:
        raise SystemExit(f"Redis ping failed for {redis_url}: {exc}") from exc
    finally:
        if bus is not None:
            bus.close()

    client = RsNodeClient(
        command_queue_key=config.control.command_queue_key,
        result_queue_key=config.control.result_queue_key,
        redis_url=redis_url,
        rs_base_url=rs_base_url,
        rs_client_id=rs_client_id,
        rs_app_token=rs_app_token,
        rs_auth_token=rs_auth_token,
        block_timeout_seconds=config.control.block_timeout_seconds,
        result_ttl_seconds=config.control.result_ttl_seconds,
        worker_count=max(1, int(workers)),
    )
    presence_info = client.presence_info()
    logger.info(
        "RSNode presence heartbeat: interval=%ss ttl=%ss key=%s",
        presence_info["heartbeat_interval_seconds"],
        presence_info["heartbeat_ttl_seconds"],
        presence_info["presence_key"],
    )
    if action == "run" or action == "worker":
        try:
            if is_once:
                client.process_once(timeout_seconds=timeout)
            else:
                client.run_forever()
        finally:
            client.close()


if __name__ == "__main__":
    main()
