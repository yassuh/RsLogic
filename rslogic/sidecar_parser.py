"""Sidecar and image metadata parsing helpers."""

import json
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from pathlib import Path

from PIL import Image, ExifTags
from PIL.TiffImagePlugin import IFDRational
from typing import Any


def _sanitize_json_text(value: str) -> str:
    return value.replace("\x00", "")

def _xml_to_dict(node: ET.Element) -> dict:
    out: dict = dict(node.attrib)
    text = (node.text or "").strip()
    if text:
        out["_text"] = text
    children = list(node)
    if not children and out:
        return out
    if not children:
        return {"_text": text} if text else {}
    for child in node:
        key = child.tag.split("}", 1)[-1]
        value = _xml_to_dict(child)
        existing = out.get(key)
        if existing is None:
            out[key] = value
        elif isinstance(existing, list):
            existing.append(value)
        else:
            out[key] = [existing, value]
    return out


def _to_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, str):
            return _sanitize_json_text(value)
        return value
    if isinstance(value, IFDRational):
        return float(value)
    if isinstance(value, bytes):
        try:
            return _sanitize_json_text(value.decode("utf-8"))
        except UnicodeDecodeError:
            return value.hex()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _to_json_value(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_to_json_value(v) for v in value]
    if isinstance(value, list):
        return [_to_json_value(v) for v in value]
    if isinstance(value, set):
        return [_to_json_value(v) for v in sorted(value, key=repr)]
    if isinstance(value, dict):
        return {str(k): _to_json_value(v) for k, v in value.items()}
    if isinstance(value, complex):
        return {"real": _to_json_value(value.real), "imag": _to_json_value(value.imag)}
    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        den = getattr(value, "denominator", 1)
        if den:
            try:
                return float(getattr(value, "numerator")) / float(den)
            except Exception:
                return float(value)
        return 0.0
    return str(value)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, IFDRational):
        return float(value)
    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        den = getattr(value, "denominator", 1)
        if den == 0:
            return None
        try:
            return float(getattr(value, "numerator")) / float(den)
        except Exception:
            try:
                return float(value)
            except Exception:
                return None
    if isinstance(value, (tuple, list)) and len(value) == 3:
        degrees = _to_float(value[0])
        minutes = _to_float(value[1])
        seconds = _to_float(value[2])
        if degrees is None or minutes is None or seconds is None:
            return None
        return degrees + (minutes / 60.0) + (seconds / 3600.0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _find_any(mapping: Any, keys: tuple[str, ...]) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping[key]
        if isinstance(key, str) and key.isdigit():
            key_int = int(key)
            if key_int in mapping:
                return mapping[key_int]
        elif isinstance(key, int) and str(key) in mapping:
            return mapping[str(key)]
        elif isinstance(key, int) and key in mapping:
            return mapping[key]
    return None


def _extract_embedded_xmp(xmp_payload: bytes | str | None) -> dict[str, Any]:
    if not xmp_payload:
        return {}
    if isinstance(xmp_payload, bytes):
        text = xmp_payload.decode("utf-8", errors="ignore")
    else:
        text = xmp_payload

    stripped = (text or "").strip()
    if not stripped:
        return {}

    try:
        root = ET.fromstring(stripped)
    except ET.ParseError as exc:
        return {"raw": stripped, "xml_error": str(exc)}

    parsed = _xml_to_dict(root)
    attributes: dict[str, Any] = {}
    for desc in root.iter():
        if desc.tag.split("}", 1)[-1] == "Description" and desc.attrib:
            for key, value in desc.attrib.items():
                normalized = key.split("}", 1)[-1]
                attributes[normalized] = _to_json_value(value)
    result: dict[str, Any] = {"raw": stripped, "xml": parsed}
    if attributes:
        result["attributes"] = attributes
    return result


def extract_gps_from_exif(exif: dict[str, Any]) -> dict[str, float | None]:
    """Extract GPS coordinates and altitude from parsed EXIF data."""

    # Expecting either flattened GPS keys or PIL's GPSInfo dictionary.
    gps = exif.get("GPSInfo")
    if not isinstance(gps, dict):
        lat = _find_any(exif, ("GPSLatitude", "gpslatitude"))
        lat_ref = _find_any(exif, ("GPSLatitudeRef", "gpslatituderef"))
        lon = _find_any(exif, ("GPSLongitude", "gpslongitude"))
        lon_ref = _find_any(exif, ("GPSLongitudeRef", "gpslongituderef"))
        alt = _find_any(exif, ("GPSAltitude", "gpsaltitude"))
        alt_ref = _find_any(exif, ("GPSAltitudeRef", "gpsaltituderef"))
    else:
        lat = _find_any(gps, ("2", "GPSLatitude"))
        lat_ref = _find_any(gps, ("1", "GPSLatitudeRef"))
        lon = _find_any(gps, ("4", "GPSLongitude"))
        lon_ref = _find_any(gps, ("3", "GPSLongitudeRef"))
        alt = _find_any(gps, ("6", "GPSAltitude"))
        alt_ref = _find_any(gps, ("5", "GPSAltitudeRef"))

    latitude = _to_float(lat)
    longitude = _to_float(lon)
    if latitude is not None and isinstance(lat_ref, str) and lat_ref.upper().startswith("S"):
        latitude = -abs(latitude)
    if longitude is not None and isinstance(lon_ref, str) and lon_ref.upper().startswith("W"):
        longitude = -abs(longitude)

    altitude = _to_float(alt)
    if altitude is not None:
        try:
            ref_val = int(_to_float(alt_ref) or 0.0)
            if ref_val == 1:
                altitude = -abs(altitude)
        except Exception:
            pass

    if latitude is None or longitude is None or altitude is None:
        xmp_data = exif.get("xmp") or {}
        xmp_attrs = xmp_data.get("attributes", {}) if isinstance(xmp_data, dict) else {}
        if latitude is None:
            lat = _find_any(xmp_attrs, ("GpsLatitude", "latitude"))
            latitude = _to_float(lat)
            lat_ref = _find_any(xmp_attrs, ("GpsLatitudeRef", "LatitudeRef"))
            if latitude is not None and isinstance(lat_ref, str) and lat_ref.upper().startswith("S"):
                latitude = -abs(latitude)
        if longitude is None:
            lon = _find_any(xmp_attrs, ("GpsLongitude", "longitude"))
            longitude = _to_float(lon)
            lon_ref = _find_any(xmp_attrs, ("GpsLongitudeRef", "LongitudeRef"))
            if longitude is not None and isinstance(lon_ref, str) and lon_ref.upper().startswith("W"):
                longitude = -abs(longitude)
        if altitude is None:
            # DJI embeds AbsoluteAltitude and RelativeAltitude in XMP.
            altitude = (
                _to_float(_find_any(xmp_attrs, ("AbsoluteAltitude", "Altitude", "AltitudeAboveSeaLevel")))
                or _to_float(_find_any(xmp_attrs, ("RelativeAltitude",)))
            )

    return {
        "latitude": latitude,
        "longitude": longitude,
        "altitude_m": altitude,
    }


def parse_exif(path: Path) -> dict:
    with Image.open(path) as image:
        raw = image._getexif() or image.getexif()
        if not raw:
            return {}
        tags = {
            ExifTags.TAGS.get(tag_id, str(tag_id)): _to_json_value(value)
            for tag_id, value in raw.items()
        }

        exif_info = getattr(image, "info", {}) or {}
        xmp_payload = _extract_embedded_xmp(exif_info.get("xmp"))
        if xmp_payload:
            tags["xmp"] = xmp_payload

        return {"exif": tags}


def parse_sidecar(path: Path) -> dict:
    suffix = path.suffix.lower()
    if suffix in {".json", ".js"}:
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            return {"json": json.loads(fh.read() or "{}")}
    if suffix in {".xml", ".xmp"}:
        try:
            root = ET.parse(path).getroot()
            return {"xml": _xml_to_dict(root)}
        except ET.ParseError as exc:
            return {"xml_error": str(exc), "raw": path.read_text(encoding="utf-8", errors="ignore")}
    return {"text": path.read_text(encoding="utf-8", errors="ignore")}
