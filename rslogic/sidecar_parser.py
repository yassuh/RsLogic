"""Sidecar and image metadata parsing helpers."""

from __future__ import annotations

from pathlib import Path
import json
import xml.etree.ElementTree as ET

from PIL import Image, ExifTags


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


def parse_exif(path: Path) -> dict:
    with Image.open(path) as image:
        raw = image.getexif()
        if not raw:
            return {}
        tags = {ExifTags.TAGS.get(tag_id, str(tag_id)): value for tag_id, value in raw.items()}
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
