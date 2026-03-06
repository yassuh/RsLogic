from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

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


def test_stage_group_uses_job_scoped_staging_dir(monkeypatch, tmp_path: Path) -> None:
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

    assert staging_dir == tmp_path / "staging" / ".jobs" / "job-1"
    assert (tmp_path / "staging" / "asset-1_one.jpg").is_file()
    assert (staging_dir / "stage-map.json").is_file()


def test_copy_staging_to_session_replaces_target_directory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rslogic.client.file_ops.make_client", lambda **_: FakeS3())
    executor = FileExecutor(db=FakeDb({}), working_root=tmp_path)
    staging_dir = tmp_path / "staging" / ".jobs" / "job-1"
    staging_dir.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "staging" / "selected.jpg"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("selected", encoding="utf-8")
    (staging_dir / "stage-map.json").write_text(
        json.dumps(
            {
                "group_id": "group-a",
                "job_id": "job-1",
                "files": [
                    {
                        "asset_id": "asset-1",
                        "image": {
                            "local_path": str(source),
                            "filename": "selected.jpg",
                            "bucket": "bucket",
                            "key": "selected.jpg",
                            "cached": True,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    session_dir = tmp_path / "sessions" / "session-123" / "_data" / "Imagery"
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "stale.jpg").write_text("stale", encoding="utf-8")

    executor.copy_staging_to_session("job-1", staging_dir, session_dir)

    assert not (session_dir / "stale.jpg").exists()
    assert (session_dir / "selected.jpg").read_text(encoding="utf-8") == "selected"
