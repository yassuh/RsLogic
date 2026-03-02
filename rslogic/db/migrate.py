"""Wrapper around alembic for label-db."""

from __future__ import annotations

import subprocess
import sys

from rslogic.config import CONFIG


def main() -> None:
    cfg = CONFIG.label_db.alembic_ini
    subprocess.run([sys.executable, "-m", "alembic", "-c", cfg, "upgrade", "head"], check=False)
