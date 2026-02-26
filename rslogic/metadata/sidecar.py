"""Drone sidecar metadata extraction helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional
from xml.etree import ElementTree as ET

PRIMARY_IMAGE_EXTENSIONS = frozenset(
    {
        ".jpg",
        ".jpeg",
        ".png",
        ".tif",
        ".tiff",
        ".dng",
        ".heic",
        ".heif",
    }
)
PRIMARY_VIDEO_EXTENSIONS = frozenset({".mp4", ".mov", ".m4v", ".avi", ".mkv"})
PRIMARY_MEDIA_EXTENSIONS = PRIMARY_IMAGE_EXTENSIONS | PRIMARY_VIDEO_EXTENSIONS
SIDECAR_EXTENSIONS = frozenset({".srt", ".lrf", ".xmp", ".mrk"})

_EXPECTED_SIDECARS_BY_MEDIA_EXTENSION: Mapping[str, tuple[str, ...]] = {
    ".mp4": (".srt", ".lrf"),
    ".mov": (".srt", ".lrf"),
    ".m4v": (".srt", ".lrf"),
    ".avi": (".srt", ".lrf"),
    ".mkv": (".srt", ".lrf"),
}

_SRT_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
_SRT_KEY_VALUE_RE = re.compile(
    r"([A-Za-z_][A-Za-z0-9_/-]*)\s*:\s*([^,\[]*?)(?=(?:\s|,\s*)+[A-Za-z_][A-Za-z0-9_/-]*\s*:|$)"
)
_SRT_FRAME_RE = re.compile(r"FrameCnt:\s*(\d+)", flags=re.IGNORECASE)
_SRT_DIFF_RE = re.compile(r"DiffTime:\s*(\d+)ms", flags=re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class SidecarExtractionResult:
    metadata: Dict[str, Any]
    present_sidecars: Dict[str, Path]
    missing_expected: List[str]


class DroneSidecarMetadataExtractor:
    """Parse sibling sidecar files that share the same stem as media files."""

    def __init__(self, sidecar_extensions: Optional[Iterable[str]] = None) -> None:
        normalized = sidecar_extensions or SIDECAR_EXTENSIONS
        self._sidecar_extensions = tuple(sorted({str(ext).lower() for ext in normalized}))

    @staticmethod
    def expected_sidecars_for_media(media_path: Path) -> tuple[str, ...]:
        return _EXPECTED_SIDECARS_BY_MEDIA_EXTENSION.get(media_path.suffix.lower(), ())

    def discover_sidecars(self, media_path: Path) -> Dict[str, Path]:
        detected: Dict[str, Path] = {}
        for extension in self._sidecar_extensions:
            candidate = self._find_sidecar_path(media_path, extension)
            if candidate is not None:
                detected[extension] = candidate
        return detected

    def extract_for_media(self, media_path: Path, *, parse: bool = True) -> SidecarExtractionResult:
        present = self.discover_sidecars(media_path)
        expected = self.expected_sidecars_for_media(media_path)
        missing = [extension for extension in expected if extension not in present]

        metadata: Dict[str, Any] = {}
        for extension in present:
            metadata[f"sidecar_{extension.lstrip('.')}_present"] = True

        if parse:
            srt_path = present.get(".srt")
            if srt_path is not None:
                metadata.update(self._parse_srt(srt_path))

            lrf_path = present.get(".lrf")
            if lrf_path is not None:
                metadata.update(self._parse_lrf(lrf_path))

            xmp_path = present.get(".xmp")
            if xmp_path is not None:
                metadata.update(self._parse_xmp_sidecar(xmp_path))

            mrk_path = present.get(".mrk")
            if mrk_path is not None:
                metadata.update(self._parse_mrk(mrk_path))

        return SidecarExtractionResult(
            metadata=metadata,
            present_sidecars=present,
            missing_expected=missing,
        )

    def _find_sidecar_path(self, media_path: Path, extension: str) -> Optional[Path]:
        lower_ext = extension.lower()
        direct = media_path.with_suffix(lower_ext)
        if direct.exists() and direct.is_file():
            return direct

        upper = media_path.with_suffix(lower_ext.upper())
        if upper.exists() and upper.is_file():
            return upper

        stem_lower = media_path.stem.lower()
        for candidate in media_path.parent.glob(f"{media_path.stem}.*"):
            if not candidate.is_file():
                continue
            if candidate.stem.lower() != stem_lower:
                continue
            if candidate.suffix.lower() == lower_ext:
                return candidate
        return None

    @staticmethod
    def _normalize_key(raw_key: str) -> str:
        return re.sub(r"[^a-z0-9_]+", "_", raw_key.strip().lower()).strip("_")

    @staticmethod
    def _coerce_scalar(raw_value: str) -> Any:
        value = raw_value.strip().strip(",").strip()
        if not value:
            return None
        lowered = value.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        if "/" in value:
            return value
        try:
            if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                return int(value)
            return float(value)
        except ValueError:
            return value

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_line_datetime(raw_line: str) -> Optional[datetime]:
        line = raw_line.strip()
        if not line:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(line, fmt)
            except ValueError:
                continue
        return None

    def _parse_srt(self, path: Path) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return output

        first_capture: Optional[datetime] = None
        last_capture: Optional[datetime] = None
        max_frame_count: Optional[int] = None
        diff_times: List[int] = []

        for raw_line in content.splitlines():
            line = _HTML_TAG_RE.sub("", raw_line).strip()
            if not line:
                continue

            frame_match = _SRT_FRAME_RE.search(line)
            if frame_match:
                frame_count = int(frame_match.group(1))
                max_frame_count = frame_count if max_frame_count is None else max(max_frame_count, frame_count)

            diff_match = _SRT_DIFF_RE.search(line)
            if diff_match:
                diff_times.append(int(diff_match.group(1)))

            timestamp = self._parse_line_datetime(line)
            if timestamp is not None:
                if first_capture is None:
                    first_capture = timestamp
                last_capture = timestamp

            for segment in _SRT_BRACKET_RE.findall(line):
                for key, value in _SRT_KEY_VALUE_RE.findall(segment):
                    normalized_key = self._normalize_key(key)
                    parsed_value = self._coerce_scalar(value)
                    if parsed_value is None:
                        continue
                    output[f"sidecar_srt_{normalized_key}"] = parsed_value

        if first_capture is not None:
            output["sidecar_srt_start_at"] = first_capture.isoformat()
            output.setdefault("captured_at", first_capture.isoformat())
        if last_capture is not None:
            output["sidecar_srt_end_at"] = last_capture.isoformat()
        if max_frame_count is not None:
            output["sidecar_srt_frame_count"] = max_frame_count
        if diff_times:
            output["sidecar_srt_avg_diff_ms"] = round(sum(diff_times) / len(diff_times), 3)

        latitude = self._as_float(output.get("sidecar_srt_latitude"))
        longitude = self._as_float(output.get("sidecar_srt_longitude"))
        altitude_abs = self._as_float(output.get("sidecar_srt_abs_alt"))
        altitude_rel = self._as_float(output.get("sidecar_srt_rel_alt"))
        focal_len = self._as_float(output.get("sidecar_srt_focal_len"))

        if latitude is not None:
            output.setdefault("latitude", latitude)
        if longitude is not None:
            output.setdefault("longitude", longitude)
        if altitude_abs is not None:
            output.setdefault("altitude_m", altitude_abs)
        elif altitude_rel is not None:
            output.setdefault("altitude_m", altitude_rel)
        if focal_len is not None:
            output.setdefault("focal_length_mm", focal_len)

        return output

    @staticmethod
    def _decode_xmp_bytes(raw_xmp: bytes) -> Dict[str, str]:
        uri_to_prefix: Dict[str, str] = {}
        try:
            for _, (prefix, uri) in ET.iterparse(
                BytesIO(raw_xmp),
                events=("start-ns",),
            ):
                if uri and uri not in uri_to_prefix:
                    uri_to_prefix[uri] = prefix or ""
        except ET.ParseError:
            return {}

        def resolve_name(name: str) -> str:
            if not name.startswith("{") or "}" not in name:
                return name
            uri, local = name[1:].split("}", 1)
            prefix = uri_to_prefix.get(uri, "")
            return f"{prefix}:{local}" if prefix else local

        try:
            root = ET.fromstring(raw_xmp)
        except ET.ParseError:
            return {}

        parsed: Dict[str, str] = {}
        for element in root.iter():
            tag_name = resolve_name(element.tag)
            if tag_name.endswith("Description"):
                for raw_key, value in element.attrib.items():
                    parsed[resolve_name(raw_key)] = value
                continue
            text = (element.text or "").strip()
            if text and tag_name not in parsed:
                parsed[tag_name] = text
        return parsed

    def _parse_xmp_sidecar(self, path: Path) -> Dict[str, Any]:
        try:
            raw = path.read_bytes()
        except OSError:
            return {}

        parsed = self._decode_xmp_bytes(raw)
        if not parsed:
            return {}

        output: Dict[str, Any] = {"sidecar_xmp_key_count": len(parsed)}
        field_map = {
            "drone-dji:GpsLatitude": ("latitude", "sidecar_xmp_gps_latitude"),
            "drone-dji:GpsLongitude": ("longitude", "sidecar_xmp_gps_longitude"),
            "drone-dji:AbsoluteAltitude": ("altitude_m", "sidecar_xmp_absolute_altitude"),
            "drone-dji:RelativeAltitude": (None, "sidecar_xmp_relative_altitude"),
            "drone-dji:ProductName": ("drone_model", "sidecar_xmp_product_name"),
            "xmp:CreateDate": ("captured_at", "sidecar_xmp_create_date"),
        }
        for source_key, (canonical_key, output_key) in field_map.items():
            value = parsed.get(source_key)
            if value is None:
                continue
            output[output_key] = value
            if canonical_key is None:
                continue
            if canonical_key in {"latitude", "longitude", "altitude_m"}:
                numeric = self._as_float(value)
                if numeric is not None:
                    output.setdefault(canonical_key, numeric)
                continue
            output.setdefault(canonical_key, value)
        return output

    @staticmethod
    def _parse_lrf(path: Path) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        try:
            stat = path.stat()
            header = path.read_bytes()[:64]
        except OSError:
            return output

        output["sidecar_lrf_file_size"] = int(stat.st_size)
        if len(header) >= 12 and header[4:8] == b"ftyp":
            output["sidecar_lrf_container"] = "isobmff"
            major_brand = header[8:12].decode("ascii", errors="ignore").strip()
            if major_brand:
                output["sidecar_lrf_major_brand"] = major_brand
        return output

    @staticmethod
    def _parse_mrk(path: Path) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                line_count = sum(1 for _ in handle)
        except OSError:
            return output
        output["sidecar_mrk_line_count"] = line_count
        return output
