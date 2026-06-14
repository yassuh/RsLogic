"""Bootstrap the operator web UI with the free-threaded GIL override enabled."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping, Sequence

PYTHON_GIL_ENV = "PYTHON_GIL"


def _supports_gil_override() -> bool:
    return hasattr(sys, "_is_gil_enabled")


def _should_reexec(env: Mapping[str, str] | None = None) -> bool:
    if not _supports_gil_override():
        return False
    current_env = os.environ if env is None else env
    return current_env.get(PYTHON_GIL_ENV) != "0"


def _reexec_argv(argv: Sequence[str] | None = None, executable: str | None = None) -> list[str]:
    program = sys.executable if executable is None else executable
    args = list(sys.argv[1:] if argv is None else argv)
    return [program, "-X", "gil=0", "-m", "rslogic.tui.launcher", *args]


def ensure_gil_disabled(
    *,
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    executable: str | None = None,
    execve: Callable[[str, list[str], dict[str, str]], None] | None = None,
) -> None:
    if not _should_reexec(env):
        return
    program = sys.executable if executable is None else executable
    next_env = dict(os.environ if env is None else env)
    next_env[PYTHON_GIL_ENV] = "0"
    runner = os.execve if execve is None else execve
    runner(program, _reexec_argv(argv, program), next_env)


def main() -> None:
    ensure_gil_disabled()
    from rslogic.api.server import main as run_server

    run_server(sys.argv[1:])


if __name__ == "__main__":
    main()
