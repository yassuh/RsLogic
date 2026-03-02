"""CLI wrapper for folder upload."""

from __future__ import annotations

import argparse
from pathlib import Path

from rslogic.upload_service import FolderUploader


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload images + sidecars to waiting bucket")
    parser.add_argument("folder", type=Path)
    parser.add_argument("--workers", type=int, default=None)
    args = parser.parse_args()
    uploader = FolderUploader(max_workers=args.workers)
    results = uploader.run(args.folder)
    print(f"Uploaded {len(results)} objects")


