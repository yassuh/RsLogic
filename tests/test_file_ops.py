from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from rslogic.client.file_ops import FileExecutor


@dataclass
class FakeAsset:
    id: str
    object_key: str
    filename: str
    bucket_name: str | None = None
    uri: str | None = None


class FakeDb:
    def __init__(self, assets_by_group: dict[str, list[FakeAsset]]) -> None:
        self.assets_by_group = assets_by_group

    def image_assets_for_group(self, group_id: str) -> list[FakeAsset]:
        return list(self.assets_by_group.get(group_id, []))


class FakeS3:
    def download_file(self, bucket: str, key: str, target: str) -> None:
        Path(target).write_text(f"{bucket}:{key}", encoding="utf-8")


def test_stage_group_uses_shared_staging_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rslogic.client.file_ops.make_client", lambda **_: FakeS3())
    db = FakeDb(
        {
            "group-a": [
                FakeAsset(id="asset-1", object_key="a/one.jpg", filename="one.jpg"),
            ]
        }
    )
    executor = FileExecutor(db=db, working_root=tmp_path)

    staging_dir = executor.stage_group("group-a", "job-1")

    assert staging_dir == tmp_path / "staging"
    assert (tmp_path / "staging" / "asset-1_one.jpg").is_file()


def test_write_manifest_uses_db_group_membership(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rslogic.client.file_ops.make_client", lambda **_: FakeS3())
    db = FakeDb(
        {
            "group-a": [
                FakeAsset(id="asset-1", object_key="a/one.jpg", filename="one.jpg"),
            ]
        }
    )
    executor = FileExecutor(db=db, working_root=tmp_path)
    staging_dir = executor.stage_group("group-a", "job-1")

    manifest_path = executor.write_manifest("job-1", staging_dir, "group-a")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["group_id"] == "group-a"
    assert payload["files"] == [str(tmp_path / "staging" / "asset-1_one.jpg")]


def test_copy_staging_to_session_replaces_target_directory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rslogic.client.file_ops.make_client", lambda **_: FakeS3())
    db = FakeDb(
        {
            "group-a": [
                FakeAsset(id="asset-1", object_key="selected.jpg", filename="selected.jpg"),
            ]
        }
    )
    executor = FileExecutor(db=db, working_root=tmp_path)
    staging_dir = executor.stage_group("group-a", "job-1")

    session_dir = tmp_path / "sessions" / "session-123" / "_data" / "Imagery"
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "stale.jpg").write_text("stale", encoding="utf-8")

    executor.copy_staging_to_session("job-1", staging_dir, session_dir, "group-a")

    assert not (session_dir / "stale.jpg").exists()
    assert (session_dir / "asset-1_selected.jpg").read_text(encoding="utf-8") == "drone-imagery:selected.jpg"


def test_copy_staging_to_session_copies_only_group_files(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rslogic.client.file_ops.make_client", lambda **_: FakeS3())
    db = FakeDb(
        {
            "group-a": [
                FakeAsset(id="asset-1", object_key="selected-a.jpg", filename="selected-a.jpg"),
                FakeAsset(id="asset-2", object_key="selected-b.jpg", filename="selected-b.jpg"),
            ],
            "group-b": [
                FakeAsset(id="asset-3", object_key="ignored.jpg", filename="ignored.jpg"),
            ],
        }
    )
    executor = FileExecutor(db=db, working_root=tmp_path)
    staging_dir = executor.stage_group("group-a", "job-1")
    executor.stage_group("group-b", "job-2")

    session_dir = tmp_path / "sessions" / "session-123" / "_data" / "Imagery"
    executor.copy_staging_to_session("job-1", staging_dir, session_dir, "group-a")

    assert (session_dir / "asset-1_selected-a.jpg").read_text(encoding="utf-8") == "drone-imagery:selected-a.jpg"
    assert (session_dir / "asset-2_selected-b.jpg").read_text(encoding="utf-8") == "drone-imagery:selected-b.jpg"
    assert not (session_dir / "asset-3_ignored.jpg").exists()


def test_copy_staging_to_session_requires_group_files_present(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rslogic.client.file_ops.make_client", lambda **_: FakeS3())
    db = FakeDb(
        {
            "group-a": [
                FakeAsset(id="asset-1", object_key="selected.jpg", filename="selected.jpg"),
            ]
        }
    )
    executor = FileExecutor(db=db, working_root=tmp_path)

    with pytest.raises(RuntimeError, match="staged file missing"):
        executor.copy_staging_to_session(
            "job-1",
            tmp_path / "staging",
            tmp_path / "sessions" / "session-123" / "_data" / "Imagery",
            "group-a",
        )
