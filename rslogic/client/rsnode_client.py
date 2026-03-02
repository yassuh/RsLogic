"""Client service entrypoint."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _looks_like_repo_root(path: Path) -> bool:
    return (path / "rslogic" / "client" / "runtime.py").exists() and (path / "pyproject.toml").exists()


def _ensure_repo_root_on_syspath() -> Path:
    candidates: list[Path] = [Path(__file__).resolve()]
    env_root = os.getenv("RSLOGIC_ROOT")
    if env_root:
        candidates.append(Path(env_root))
    candidates.append(Path.cwd())
    if Path.cwd().parent != Path.cwd():
        candidates.append(Path.cwd().parent)

    seen: set[str] = set()
    for candidate in candidates:
        try:
            current = candidate.resolve()
        except Exception:
            current = candidate
        for path in (current, *current.parents):
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            if _looks_like_repo_root(path):
                root = str(path)
                if root not in sys.path:
                    sys.path.insert(0, root)
                existing = os.environ.get("PYTHONPATH", "")
                if root not in existing.split(os.pathsep):
                    os.environ["PYTHONPATH"] = f"{root}{os.pathsep}{existing}" if existing else root
                return path

            try:
                for child in path.iterdir():
                    if child.is_dir() and child.name.lower() in {"rslogic", "RsLogic".lower()} and _looks_like_repo_root(child):
                        child_root = str(child)
                        if child_root not in sys.path:
                            sys.path.insert(0, child_root)
                        existing = os.environ.get("PYTHONPATH", "")
                        if child_root not in existing.split(os.pathsep):
                            os.environ["PYTHONPATH"] = f"{child_root}{os.pathsep}{existing}" if existing else child_root
                        return child
            except Exception:
                pass

    # Fallback to expected layout for current script location.
    fallback = Path(__file__).resolve().parents[2]
    fallback_str = str(fallback)
    if fallback_str not in sys.path:
        sys.path.insert(0, fallback_str)
    existing = os.environ.get("PYTHONPATH", "")
    if fallback_str not in existing.split(os.pathsep):
        os.environ["PYTHONPATH"] = f"{fallback_str}{os.pathsep}{existing}" if existing else fallback_str
    return fallback


_REPO_ROOT = _ensure_repo_root_on_syspath()

from rslogic.client.runtime import main


if __name__ == "__main__":
    main()
