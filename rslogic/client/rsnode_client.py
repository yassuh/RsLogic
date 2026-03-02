"""Client service entrypoint."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _ensure_repo_root_on_syspath() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)

    existing = os.environ.get("PYTHONPATH", "")
    if root not in existing.split(os.pathsep):
        os.environ["PYTHONPATH"] = f"{root}{os.pathsep}{existing}" if existing else root

    return repo_root


_REPO_ROOT = _ensure_repo_root_on_syspath()

from rslogic.client.runtime import main


if __name__ == "__main__":
    main()
