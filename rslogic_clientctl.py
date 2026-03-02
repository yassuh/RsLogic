"""Standalone entry point that bootstraps the package import path."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Sequence
from os import path as os_path


def _candidate_project_roots() -> list[Path]:
    roots = []
    env_root = os.getenv("RSLOGIC_ROOT")
    if env_root:
        roots.append(Path(env_root).resolve())
    roots.append(Path.cwd().resolve())
    roots.append(Path(__file__).resolve().parent)
    return roots


def _looks_like_repo_root(path: Path) -> bool:
    return (path / "rslogic").is_dir() and (path / "config.py").exists() and (path / "pyproject.toml").exists()


def _path_already_on_syspath(candidate: str) -> bool:
    wanted = os_path.normcase(os_path.abspath(candidate))
    return any(os_path.normcase(os_path.abspath(existing)) == wanted for existing in sys.path if existing)


def _ensure_import_path() -> Path:
    selected: Path | None = None
    for root in _candidate_project_roots():
        if _looks_like_repo_root(root):
            selected = root
            break
        for parent in root.parents:
            if _looks_like_repo_root(parent):
                selected = parent
                break
        if selected is not None:
            break

    if selected is None:
        raise RuntimeError(
            "Could not auto-detect rslogic repo root. Set RSLOGIC_ROOT to C:\\ProgramData\\RsLogic (or your repo path) before running."
        )

    path = str(selected)
    if not _path_already_on_syspath(path):
        sys.path.insert(0, path)
    py_path = os.environ.get("PYTHONPATH", "")
    if path not in py_path.split(os.pathsep):
        os.environ["PYTHONPATH"] = f"{path}{os.pathsep}{py_path}" if py_path else path
    return selected


def main(argv: Sequence[str] | None = None) -> None:
    repo_root = _ensure_import_path()
    try:
        from rslogic.client import control_tui
    except ModuleNotFoundError:
        control_tui = Path(__file__).resolve().parent / "rslogic" / "client" / "control_tui.py"
        if not control_tui.exists():
            control_tui = repo_root / "rslogic" / "client" / "control_tui.py"
        if not control_tui.exists():
            raise
        import runpy

        runpy.run_path(str(control_tui), run_name="__main__")
        return

    if argv is None:
        argv = tuple(sys.argv[1:])
    control_tui.main(list(argv))


if __name__ == "__main__":
    main()
