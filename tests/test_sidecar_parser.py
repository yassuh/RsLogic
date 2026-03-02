"""Tests for metadata parsing normalization."""

from __future__ import annotations

import json
from pathlib import Path

from PIL.TiffImagePlugin import IFDRational
from unittest.mock import patch

from rslogic.sidecar_parser import _to_json_value, extract_gps_from_exif, parse_exif
from rslogic.ingest import IngestService


class _FakeImage:
    def __enter__(self) -> "_FakeImage":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool | None:
        return None

    def getexif(self):
        return {
            9_999_991: IFDRational(3, 2),
            9_999_992: [IFDRational(1, 3), IFDRational(1, 6)],
            9_999_993: b"abc",
            9_999_994: {"nested": IFDRational(5, 10)},
        }


class _FakeImageForIngest:
    def __enter__(self) -> "_FakeImageForIngest":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool | None:
        return None

    def getexif(self):
        return {9_999_991: IFDRational(1, 2)}


class _FakeImageForIngestGps:
    def __enter__(self) -> "_FakeImageForIngestGps":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool | None:
        return None

    def getexif(self):
        return {
            34853: {
                1: "N",
                2: [IFDRational(10, 1), IFDRational(30, 1), IFDRational(0, 1)],
                3: "W",
                4: [IFDRational(20, 1), IFDRational(0, 1), IFDRational(0, 1)],
                5: 0,
                6: IFDRational(250, 1),
            }
        }


def test_extract_gps_from_exif_works() -> None:
    exif = {
        "GPSInfo": {
            "1": "N",
            "2": [IFDRational(10, 1), IFDRational(30, 1), IFDRational(0, 1)],
            "3": "E",
            "4": [IFDRational(20, 1), IFDRational(15, 1), IFDRational(0, 1)],
            "5": 0,
            "6": IFDRational(100, 1),
        }
    }
    assert extract_gps_from_exif(exif) == {
        "latitude": 10.5,
        "longitude": 20.25,
        "altitude_m": 100.0,
    }


def test_to_json_value_converts_ifdrational() -> None:
    assert _to_json_value(IFDRational(3, 2)) == 1.5


def _assert_payload_tags(payload: dict) -> None:
    exif_payload = payload["exif"]
    exif_values = list(exif_payload.values())
    assert any(v == 1.5 for v in exif_values)
    assert any(v == [1 / 3, 1 / 6] for v in exif_values)
    assert any(v == "abc" for v in exif_values)
    assert any(v == {"nested": 0.5} for v in exif_values)


def test_parse_exif_is_json_serializable(tmp_path: Path) -> None:
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"fake-jpeg")

    with patch("rslogic.sidecar_parser.Image.open", return_value=_FakeImage()):
        payload = parse_exif(image_path)
        json.dumps(payload)
    _assert_payload_tags(payload)


def test_ingest_parse_payload_uses_json_safe_exif(tmp_path: Path) -> None:
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"fake-jpeg")

    service = IngestService.__new__(IngestService)
    service.waiting = "drone-imagery-waiting"
    service.processed = "drone-imagery"

    sidecar_data = {"img/side.json": '{"ok": true}'}

    class _FakeS3:
        def download_file(self, bucket: str, key: str, filename: str) -> None:
            Path(filename).write_text(sidecar_data.get(key, ""), encoding="utf-8")

    service.s3 = _FakeS3()
    with patch("rslogic.sidecar_parser.Image.open", return_value=_FakeImageForIngest()):
        payload = service._parse_payload(image_path, ["img/side.json"], "img/image.jpg")

    json.dumps(payload)
    _assert_payload_tags(payload)
    assert payload["sidecars"][0]["parsed"]["json"] == {"ok": True}


def test_ingest_parse_payload_includes_geodata(tmp_path: Path) -> None:
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"fake-jpeg")

    service = IngestService.__new__(IngestService)
    service.waiting = "drone-imagery-waiting"
    service.processed = "drone-imagery"

    class _FakeS3:
        def download_file(self, bucket: str, key: str, filename: str) -> None:
            Path(filename).write_text("{}", encoding="utf-8")

    service.s3 = _FakeS3()
    with patch("rslogic.sidecar_parser.Image.open", return_value=_FakeImageForIngestGps()):
        payload = service._parse_payload(image_path, [], "img/image.jpg")

    assert payload["geodata"]["latitude"] == 10.5
    assert payload["geodata"]["longitude"] == -20.0
    assert payload["geodata"]["altitude_m"] == 250.0
