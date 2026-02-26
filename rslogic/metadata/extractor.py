"""Drone image metadata extraction from EXIF tags."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from fractions import Fraction
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from xml.etree import ElementTree as ET

from PIL import Image
from PIL.ExifTags import GPSTAGS, TAGS


def _to_json_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_to_json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_json_value(v) for k, v in value.items()}
    return str(value)


@dataclass
class ParsedMetadata:
    captured_at: Optional[datetime] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None
    drone_model: Optional[str] = None
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    focal_length_mm: Optional[float] = None
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    software: Optional[str] = None
    extra: Dict[str, Any] = None

    def as_dict(self) -> Dict[str, Any]:
        if self.extra is None:
            self.extra = {}
        return {
            "captured_at": self.captured_at,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude_m": self.altitude_m,
            "drone_model": self.drone_model,
            "camera_make": self.camera_make,
            "camera_model": self.camera_model,
            "focal_length_mm": self.focal_length_mm,
            "image_width": self.image_width,
            "image_height": self.image_height,
            "software": self.software,
            "extra": {k: _to_json_value(v) for k, v in self.extra.items()},
        }


class DroneMetadataExtractor:
    """Extract key photogrammetry metadata from image bytes or disk files."""

    @staticmethod
    def _to_float(values: Any) -> Optional[float]:
        if values is None:
            return None
        try:
            if isinstance(values, Fraction):
                return float(values)
            if isinstance(values, tuple) and len(values) == 2:
                return float(values[0]) / float(values[1])
            if isinstance(values, (int, float)):
                return float(values)
        except Exception:
            return None
        return None

    @staticmethod
    def _parse_time(value: Any) -> Optional[datetime]:
        if not value:
            return None
        value = str(value).strip()
        for fmt in ("%Y:%m:%d %H:%M:%S", "%Y:%m:%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_iso_time(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        rendered = str(value).strip()
        if not rendered:
            return None
        try:
            return datetime.fromisoformat(rendered.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _parse_signed_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        rendered = str(value).strip()
        if not rendered:
            return None
        if rendered.startswith("+"):
            rendered = rendered[1:]
        try:
            return float(rendered)
        except ValueError:
            return None

    @staticmethod
    def _to_degrees(value: Any, ref: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            degrees = value[0]
            minutes = value[1]
            seconds = value[2]
            d = DroneMetadataExtractor._to_float(degrees)
            m = DroneMetadataExtractor._to_float(minutes)
            s = DroneMetadataExtractor._to_float(seconds)
            if d is None or m is None or s is None:
                return None
            sign = -1 if ref in {"S", "W"} else 1
            return sign * (d + (m / 60.0) + (s / 3600.0))
        except Exception:
            return None

    @staticmethod
    def _parse_gps(gps_data: Dict[Any, Any]) -> Dict[str, Optional[float]]:
        if not gps_data:
            return {"latitude": None, "longitude": None, "altitude_m": None}

        translated = {
            GPSTAGS.get(k, k): v
            for k, v in gps_data.items()
            if isinstance(k, int)
        }

        latitude = None
        longitude = None
        altitude_m = None

        if "GPSLatitude" in translated and "GPSLatitudeRef" in translated:
            latitude = DroneMetadataExtractor._to_degrees(
                translated["GPSLatitude"],
                translated.get("GPSLatitudeRef"),
            )

        if "GPSLongitude" in translated and "GPSLongitudeRef" in translated:
            longitude = DroneMetadataExtractor._to_degrees(
                translated["GPSLongitude"],
                translated.get("GPSLongitudeRef"),
            )

        if "GPSAltitude" in translated:
            altitude_m = DroneMetadataExtractor._to_float(translated["GPSAltitude"])

        return {"latitude": latitude, "longitude": longitude, "altitude_m": altitude_m}

    @staticmethod
    def _decode_exif(image: Image.Image) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        exif = image.getexif()
        if not exif:
            return {}, {}

        normalized: Dict[str, Any] = {}
        for tag_id, value in exif.items():
            tag = TAGS.get(tag_id, tag_id)
            normalized[tag] = value

        # Pull raw gps dictionary to resolve string refs.
        gps_info = normalized.get("GPSInfo")
        if isinstance(gps_info, dict):
            return normalized, {k: gps_info[k] for k in gps_info}

        raw_bytes = image.info.get("exif")
        return normalized, {}

    @staticmethod
    def _decode_xmp(image: Image.Image) -> Dict[str, Any]:
        raw_xmp = image.info.get("xmp")
        if raw_xmp is None:
            return {}

        if isinstance(raw_xmp, bytes):
            xmp_bytes = raw_xmp
        else:
            xmp_bytes = str(raw_xmp).encode("utf-8", errors="ignore")

        uri_to_prefix: Dict[str, str] = {}
        try:
            for _, (prefix, uri) in ET.iterparse(BytesIO(xmp_bytes), events=("start-ns",)):
                if uri and uri not in uri_to_prefix:
                    uri_to_prefix[uri] = prefix or ""
        except ET.ParseError:
            return {}

        def _resolve_name(name: str) -> str:
            if not name.startswith("{") or "}" not in name:
                return name
            uri, local = name[1:].split("}", 1)
            prefix = uri_to_prefix.get(uri, "")
            return f"{prefix}:{local}" if prefix else local

        try:
            root = ET.fromstring(xmp_bytes)
        except ET.ParseError:
            return {}

        parsed: Dict[str, Any] = {}
        for elem in root.iter():
            tag_name = _resolve_name(elem.tag)
            if tag_name.endswith("Description"):
                for raw_key, value in elem.attrib.items():
                    resolved_key = _resolve_name(raw_key)
                    parsed[resolved_key] = value
                continue
            text = (elem.text or "").strip()
            if text and tag_name not in parsed:
                parsed[tag_name] = text
        return parsed

    def extract_from_path(self, path: Path) -> ParsedMetadata:
        image = Image.open(path)
        return self._extract(image, path.name)

    def extract_from_bytes(self, data: bytes) -> ParsedMetadata:
        image = Image.open(BytesIO(data))
        return self._extract(image, None)

    def _extract(self, image: Image.Image, filename: Optional[str]) -> ParsedMetadata:
        normalized, gps_info = self._decode_exif(image)
        xmp = self._decode_xmp(image)

        captured_at = None
        for key in ["DateTimeOriginal", "DateTime", "DateTimeDigitized"]:
            if key in normalized:
                captured_at = self._parse_time(normalized.get(key))
                if captured_at:
                    break
        if captured_at is None:
            captured_at = self._parse_iso_time(xmp.get("xmp:CreateDate")) or self._parse_iso_time(xmp.get("xmp:ModifyDate"))

        gps_values = self._parse_gps(gps_info)
        if gps_values["latitude"] is None:
            gps_values["latitude"] = self._parse_signed_float(xmp.get("drone-dji:GpsLatitude"))
        if gps_values["longitude"] is None:
            gps_values["longitude"] = self._parse_signed_float(xmp.get("drone-dji:GpsLongitude"))
        if gps_values["altitude_m"] is None:
            gps_values["altitude_m"] = self._parse_signed_float(
                xmp.get("drone-dji:AbsoluteAltitude") or xmp.get("drone-dji:RelativeAltitude")
            )

        parsed = ParsedMetadata(
            captured_at=captured_at,
            latitude=gps_values["latitude"],
            longitude=gps_values["longitude"],
            altitude_m=gps_values["altitude_m"],
            drone_model=normalized.get("Model") or xmp.get("drone-dji:ProductName"),
            camera_make=normalized.get("Make") or xmp.get("tiff:Make"),
            camera_model=normalized.get("Model") or xmp.get("tiff:Model"),
            focal_length_mm=self._to_float(normalized.get("FocalLength")),
            image_width=image.width,
            image_height=image.height,
            software=normalized.get("Software") or xmp.get("drone-dji:Version"),
            extra={
                "exif": normalized,
                "xmp": xmp,
                "source_file": filename,
            },
        )

        # Keep image object small and release resources.
        image.close()
        return parsed
