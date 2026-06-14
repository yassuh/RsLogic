"""Standalone entry point that bootstraps the package import path."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Sequence


_REPO_ROOT = Path(__file__).resolve().parent


def _ensure_import_path() -> Path:
    if not (_REPO_ROOT / "config.py").exists() or not (_REPO_ROOT / "pyproject.toml").exists():
        raise RuntimeError(f"Expected repo root at {_REPO_ROOT}, but config.py/pyproject.toml were not found")

    path = str(_REPO_ROOT)
    if path not in sys.path:
        sys.path.insert(0, path)
    py_path = os.environ.get("PYTHONPATH", "")
    if path not in py_path.split(os.pathsep):
        os.environ["PYTHONPATH"] = f"{path}{os.pathsep}{py_path}" if py_path else path

    return _REPO_ROOT


def main(argv: Sequence[str] | None = None) -> None:
    _ensure_import_path()
    control_tui = importlib.import_module("rslogic.client.control_tui")

    if argv is None:
        argv = tuple(sys.argv[1:])
    control_tui.main(list(argv))


if __name__ == "__main__":
    main()
