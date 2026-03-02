"""Standalone entry point that bootstraps the package import path."""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path
from typing import Sequence
from os import path as os_path


def _candidate_project_roots() -> list[Path]:
    roots: list[Path] = []
    env_root = os.getenv("RSLOGIC_ROOT")
    if env_root:
        roots.append(Path(env_root).resolve())
    roots.append(Path.cwd().resolve())
    roots.append(Path(__file__).resolve().parent)

    # Common layout in remote deployments nests the repo under an extra folder.
    # Include one level of nested candidate directories to avoid hardcoding a path.
    nested_candidates: list[Path] = []
    for base in list(roots):
        for child in [base / "RsLogic", base / "rslogic", base / "RsLogic".lower()]:
            if child.is_dir():
                nested_candidates.append(child)
    roots.extend(nested_candidates)

    # Include immediate child directories that look like a repo root.
    for base in list(roots):
        if not base.is_dir():
            continue
        try:
            for child in base.iterdir():
                if child.is_dir() and child.name.lower() in {"rslogic", "repo", "work"}:
                    nested_candidates.append(child)
        except Exception:
            continue
    roots.extend(nested_candidates)

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

    os.environ.setdefault("RSLOGIC_ROOT", path)
    return selected


def _run_control_tui_from_source(argv: Sequence[str] | None = None) -> None:
    repo_root = _ensure_import_path()
    control_tui = repo_root / "rslogic" / "client" / "control_tui.py"
    if not control_tui.exists():
        raise RuntimeError("could not locate rslogic/client/control_tui.py")

    sys.argv = [str(control_tui), *(argv or sys.argv[1:])]
    runpy.run_path(str(control_tui), run_name="__main__")


def main(argv: Sequence[str] | None = None) -> None:
    _ensure_import_path()
    try:
        from rslogic.client import control_tui
    except ModuleNotFoundError:
        _run_control_tui_from_source(argv)
        return
    except Exception:
        # If importing the packaged module fails for any environment-specific reason
        # (editable install path, packaging metadata, transient import errors), execute
        # from source as a resilient fallback.
        _run_control_tui_from_source(argv)
        return

    if argv is None:
        argv = tuple(sys.argv[1:])
    control_tui.main(list(argv))


if __name__ == "__main__":
    main()
