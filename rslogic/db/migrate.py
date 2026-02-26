"""Database migration wrapper for shared label-db Alembic sources."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

from config import load_config


def _load_db_env() -> dict[str, str]:
    cfg = load_config()
    env = os.environ.copy()
    # Alembic stores this into configparser, so `%` must be escaped.
    env["DATABASE_URL"] = cfg.label_db.database_url.replace("%", "%%")
    return env


def _run_alembic(*, root: Path, ini_file: Path, args: list[str]) -> int:
    try:
        completed = subprocess.run(
            ["alembic", "-c", str(ini_file), *args],
            cwd=str(root),
            env=_load_db_env(),
            check=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Alembic is not installed in this environment. Install it with `pip install alembic`."
        ) from exc
    return completed.returncode


def main(argv: list[str] | None = None) -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Run shared label-db Alembic migrations")
    parser.add_argument(
        "--root",
        default=cfg.label_db.migration_root,
        help="Path to label-db migrations root (defaults to config path)",
    )
    parser.add_argument(
        "--ini",
        default=cfg.label_db.alembic_ini,
        help="Path to alembic.ini in the migration root",
    )
    parser.add_argument(
        "alembic_args",
        nargs="*",
        default=["current"],
        help="Arguments forwarded directly to alembic command",
    )

    args = parser.parse_args(argv)

    root = Path(args.root)
    if not root.is_absolute():
        root = Path(__file__).resolve().parents[1] / root
    ini_file = Path(args.ini)
    if not ini_file.is_absolute():
        ini_file = root / ini_file

    if not root.exists():
        raise FileNotFoundError(f"Alembic root does not exist: {root}")
    if not ini_file.exists():
        raise FileNotFoundError(f"Alembic config not found: {ini_file}")

    if args.alembic_args == ["current"]:
        print(f"[rslogic] checking Alembic state for {root}")

    return_code = _run_alembic(root=root, ini_file=ini_file, args=args.alembic_args)
    raise SystemExit(return_code)


if __name__ == "__main__":
    main()
