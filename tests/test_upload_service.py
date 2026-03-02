"""Tests for upload keying strategy."""

from __future__ import annotations

from pathlib import Path

from rslogic.upload_service import FolderUploader, _hash_file


class _FakeS3:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str]] = []

    def upload_file(self, source: str, bucket: str, key: str) -> None:
        self.uploads.append((source, bucket, key))


def test_image_and_sidecar_share_hash_key_stem(tmp_path: Path) -> None:
    image_path = tmp_path / "image.jpg"
    sidecar_path = tmp_path / "image.xmp"
    image_path.write_bytes(b"image-bytes")
    sidecar_path.write_bytes(b"sidecar-bytes")

    uploader = FolderUploader.__new__(FolderUploader)
    uploader.bucket = "drone-imagery-waiting"
    uploader.max_workers = 1
    uploader.manifest_dir = tmp_path / "manifests"
    uploader.manifest_dir.mkdir()
    uploader.s3 = _FakeS3()

    image_hash = _hash_file(image_path)

    uploader.run(tmp_path)

    uploaded_keys = [item[2] for item in uploader.s3.uploads]
    assert f"{image_hash}.jpg" in uploaded_keys
    assert f"{image_hash}.xmp" in uploaded_keys
    assert len(uploaded_keys) == 2
    assert all("/" not in key for key in uploaded_keys)


def test_run_reports_upload_progress(tmp_path: Path) -> None:
    image_path = tmp_path / "image.jpg"
    sidecar_path = tmp_path / "image.xmp"
    image_path.write_bytes(b"image-bytes")
    sidecar_path.write_bytes(b"sidecar-bytes")

    uploader = FolderUploader.__new__(FolderUploader)
    uploader.bucket = "drone-imagery-waiting"
    uploader.max_workers = 1
    uploader.manifest_dir = tmp_path / "manifests"
    uploader.manifest_dir.mkdir()
    uploader.s3 = _FakeS3()

    progress: list[tuple[int, int]] = []

    uploader.run(
        tmp_path,
        on_progress=lambda done, total: progress.append((done, total)),
    )

    assert progress
    assert progress[-1] == (2, 2)
