from __future__ import annotations

import io
from datetime import datetime
from fractions import Fraction

from PIL import Image

from rslogic.metadata import DroneMetadataExtractor, PRIMARY_IMAGE_EXTENSIONS


def test_extractor_supports_core_parsers():
    assert PRIMARY_IMAGE_EXTENSIONS.issuperset({".jpg", ".jpeg", ".png", ".tif", ".tiff", ".heic"})


def test_to_float_and_parse_helpers():
    assert DroneMetadataExtractor._to_float(Fraction(1, 2)) == 0.5
    assert DroneMetadataExtractor._to_float((1, 4)) == 0.25
    assert DroneMetadataExtractor._to_float("n/a") is None
    assert DroneMetadataExtractor._parse_time("2026:02:27 21:00:00") == datetime(2026, 2, 27, 21, 0, 0)
    assert DroneMetadataExtractor._parse_time("bad-format") is None
    assert DroneMetadataExtractor._parse_iso_time("2026-02-27T21:00:00Z").year == 2026
    assert DroneMetadataExtractor._parse_iso_time("") is None
    assert DroneMetadataExtractor._parse_signed_float("+12.3") == 12.3
    assert DroneMetadataExtractor._parse_signed_float("-0.5") == -0.5
    assert DroneMetadataExtractor._parse_signed_float("   ") is None


def test_parse_gps_prefers_exif_and_falls_back_for_missing():
    gps = {
        1: "N",
        2: ((34, 1), (3, 1), (0, 1)),
        3: "E",
        4: ((118, 1), (0, 1), (0, 1)),
        6: (1, 1),
    }
    parsed = DroneMetadataExtractor._parse_gps(gps)
    assert parsed["latitude"] is not None
    assert round(parsed["latitude"], 6) == 34.05
    assert round(parsed["longitude"], 6) == 118.0
    assert parsed["altitude_m"] == 1.0


def test_extract_from_bytes_with_minimal_image():
    stream = io.BytesIO()
    image = Image.new("RGB", (8, 16), color="blue")
    image.save(stream, format="JPEG")
    stream.seek(0)

    parser = DroneMetadataExtractor()
    parsed = parser.extract_from_bytes(stream.getvalue())
    data = parsed.as_dict()

    assert parsed.image_width == 8
    assert parsed.image_height == 16
    assert parsed.captured_at is None
    assert data["extra"]["exif"] == {}
    assert data["extra"]["xmp"] == {}
    assert data["extra"]["source_file"] is None
