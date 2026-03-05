"""Tests for ingest progress callback wiring."""

from __future__ import annotations

from pathlib import Path
from types import MethodType, SimpleNamespace

from rslogic.ingest import IngestItem, IngestService
import rslogic.ingest as ingest_module


class _FakeS3:
    def __init__(self) -> None:
        self.downloads: list[tuple[str, str, str]] = []

    def download_file(self, bucket: str, source: str, target: str) -> None:
        self.downloads.append((bucket, source, target))
        Path(target).write_bytes(b"")


class _FakeDb:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.updates: list[tuple[str, dict[str, object]]] = []
        self.group_attached: list[tuple[str, str]] = []

    def get_or_create_group(self, name: str):
        return SimpleNamespace(id="g-1"), False

    def create_image_asset(self, **_: object) -> SimpleNamespace:
        image_id = f"img-{len(self.created) + 1}"
        self.created.append(image_id)
        return SimpleNamespace(id=image_id)

    def update_asset_state(self, image_id: str, updates: dict[str, object]) -> None:
        self.updates.append((image_id, updates))

    def attach_asset_to_group(self, group_id: str, image_id: str, role: str | None = None) -> None:
        self.group_attached.append((group_id, image_id))


def _fake_move_object(_s3: object, source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None:
    _fake_move_object.moves.append((source_bucket, source_key, target_bucket, target_key))


_fake_move_object.moves: list[tuple[str, str, str, str]] = []


def test_ingest_run_reports_progress(monkeypatch) -> None:
    s3 = _FakeS3()
    db = _FakeDb()

    service = IngestService.__new__(IngestService)
    service.s3 = s3
    service.waiting = "drone-imagery-waiting"
    service.processed = "drone-imagery"
    service.db = db
    service.max_workers = 2

    items = [
        IngestItem(
            image_key="img-one.jpg",
            sidecar_keys=["img-one.xmp"],
            filename="img-one.jpg",
            stem_key="img-one",
        ),
        IngestItem(
            image_key="img-two.jpg",
            sidecar_keys=[],
            filename="img-two.jpg",
            stem_key="img-two",
        ),
    ]

    service._pair_objects = lambda on_status=None: (  # type: ignore[method-assign]
        items,
        [],
        {"scanned_objects": 0, "images": 2, "sidecars": 1, "images_with_sidecars": 1, "unmatched_objects": 0},
    )
    service._parse_payload = MethodType(
        lambda _self, image_tmp, sidecar_keys, image_key: {
            "exif": {},
            "sidecars": [{"key": sidecar, "parsed": {}} for sidecar in sidecar_keys],
            "source": {
                "waiting_bucket": _self.waiting,
                "waiting_key": image_key,
                "s3_url": f"s3://{_self.waiting}/{image_key}",
            },
            "geodata": {},
        },
        service,
    )

    progress: list[tuple[int, int]] = []
    _fake_move_object.moves.clear()
    monkeypatch.setattr(ingest_module, "move_object", _fake_move_object)

    results = service.run(
        group_name="group-1",
        on_progress=lambda done, total: progress.append((done, total)),
    )

    assert results
    assert len(results) == 2
    assert progress and progress[-1] == (2, 2)
    assert ("drone-imagery-waiting", "img-one.jpg", "drone-imagery", "img-one.jpg") in _fake_move_object.moves
    assert ("drone-imagery-waiting", "img-two.jpg", "drone-imagery", "img-two.jpg") in _fake_move_object.moves
    assert ("drone-imagery-waiting", "img-one.xmp", "drone-imagery", "img-one.xmp") in _fake_move_object.moves
    assert db.created == ["img-1", "img-2"]
