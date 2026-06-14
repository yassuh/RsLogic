"""Deprecated orchestrator TUI shim.

The operator UI now lives in the FastAPI web app served at `/ui`.
"""

from __future__ import annotations

from rslogic.tui.launcher import main as launch_web


def main() -> None:
    launch_web()


if __name__ == "__main__":
    main()
