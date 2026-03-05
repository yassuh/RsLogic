from __future__ import annotations

import sys

import pytest

import rslogic.api.server as server
from rslogic.tui import launcher


def test_reexec_argv_uses_launcher_module() -> None:
    argv = launcher._reexec_argv(["--demo"], "/tmp/python")
    assert argv == ["/tmp/python", "-X", "gil=0", "-m", "rslogic.tui.launcher", "--demo"]


def test_ensure_gil_disabled_reexecs_when_override_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.delenv("PYTHON_GIL", raising=False)

    def fake_execve(program: str, argv: list[str], env: dict[str, str]) -> None:
        captured["program"] = program
        captured["argv"] = argv
        captured["env"] = env
        raise SystemExit(0)

    with pytest.raises(SystemExit):
        launcher.ensure_gil_disabled(
            argv=["--demo"],
            env={},
            executable="/tmp/python",
            execve=fake_execve,
        )

    assert captured["program"] == "/tmp/python"
    assert captured["argv"] == ["/tmp/python", "-X", "gil=0", "-m", "rslogic.tui.launcher", "--demo"]
    assert captured["env"] == {"PYTHON_GIL": "0"}


def test_ensure_gil_disabled_skips_reexec_when_override_present() -> None:
    launcher.ensure_gil_disabled(
        argv=["--demo"],
        env={"PYTHON_GIL": "0"},
        executable="/tmp/python",
        execve=lambda *_args: (_ for _ in ()).throw(AssertionError("execve should not be called")),
    )


def test_main_launches_web_server(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(sys, "argv", ["rslogic-tui", "--port", "9001"])
    monkeypatch.setattr(launcher, "ensure_gil_disabled", lambda: captured.setdefault("gil_checked", True))
    monkeypatch.setattr(server, "main", lambda argv=None: captured.setdefault("server_argv", list(argv or [])))

    launcher.main()

    assert captured["gil_checked"] is True
    assert captured["server_argv"] == ["--port", "9001"]
