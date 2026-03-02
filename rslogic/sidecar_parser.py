"""Sidecar and image metadata parsing helpers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import json
import xml.etree.ElementTree as ET

from PIL import Image, ExifTags
from PIL.TiffImagePlugin import IFDRational
from typing import Any

def _xml_to_dict(node: ET.Element) -> dict:
    out: dict = dict(node.attrib)
    text = (node.text or "").strip()
    if text:
        out["_text"] = text
    children = [child for child in node if len(child)]
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
        return value
    if isinstance(value, IFDRational):
        return float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
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
        if key.isdigit() and int(key) in mapping:
            return mapping[int(key)]
    return None


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

    return {
        "latitude": latitude,
        "longitude": longitude,
        "altitude_m": altitude,
    }


def parse_exif(path: Path) -> dict:
    with Image.open(path) as image:
        raw = image.getexif()
        if not raw:
            return {}
        tags = {
            ExifTags.TAGS.get(tag_id, str(tag_id)): _to_json_value(value)
            for tag_id, value in raw.items()
        }
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
