"""Textual client-control TUI for rslogic runtime process management."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
import venv as _stdlib_venv
import os.path

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Footer, Header, RichLog, Static

ROOT_DIR = Path(__file__).resolve().parents[2]
_VENV_BOOTSTRAP_ENV = "RSLOGIC_CLIENTCTL_BOOTSTRAPPED"

for _extra in (
    ROOT_DIR,
    ROOT_DIR / "rslogic" / "internal_tools" / "rstool-sdk" / "src",
):
    if str(_extra) not in sys.path:
        sys.path.insert(0, str(_extra))

from config import CONFIG


def _repo_root() -> Path:
    return ROOT_DIR


def _venv_python_path() -> Path:
    if os.name == "nt":
        return _repo_root() / ".venv" / "Scripts" / "python.exe"
    return _repo_root() / ".venv" / "bin" / "python"


def _with_project_pythonpath(env: dict[str, str] | None = None) -> dict[str, str]:
    base = dict(os.environ.copy() if env is None else env)
    extra_paths = [
        str(_repo_root()),
        str(_repo_root() / "rslogic" / "internal_tools" / "rstool-sdk" / "src"),
    ]
    existing = base.get("PYTHONPATH", "")
    prefix = [p for p in extra_paths if p and p not in existing]
    base["PYTHONPATH"] = os.pathsep.join([*prefix, existing]) if existing else os.pathsep.join(prefix)
    base["PYTHONUNBUFFERED"] = "1"
    return base


def _python_can_import(py: Path, *, require_textual: bool) -> bool:
    module_expr = "import rslogic, config"
    if require_textual:
        module_expr += "; import textual"
    command = [str(py), "-c", module_expr]
    try:
        result = subprocess.run(
            command,
            cwd=str(_repo_root()),
            env=_with_project_pythonpath(),
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _run_uv(cmd: list[str]) -> None:
    uv_binary = shutil.which("uv")
    if uv_binary is None:
        raise RuntimeError("uv is required for bootstrap but was not found on PATH")
    full = [uv_binary, *cmd]
    result = subprocess.run(full, cwd=str(_repo_root()), check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "uv command failed")


def _ensure_python_venv() -> None:
    venv_python = _venv_python_path()
    if venv_python.exists():
        return
    if not _repo_root().exists():
        raise RuntimeError("repo root not found; cannot bootstrap client venv")

    uv_binary = shutil.which("uv")
    if uv_binary is not None:
        _run_uv(["venv", "--python", "3.14t", str(_repo_root() / ".venv")])
        return

    _stdlib_venv.create(str(_repo_root() / ".venv"), with_pip=True, clear=False)


def _install_project(venv_python: Path) -> None:
    result = subprocess.run(
        [str(venv_python), "-m", "pip", "install", "-e", "."],
        cwd=str(_repo_root()),
        env=_with_project_pythonpath(),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode == 0:
        return

    uv_binary = shutil.which("uv")
    if uv_binary is not None:
        run = subprocess.run(
            [uv_binary, "pip", "install", "-e", "."],
            cwd=str(_repo_root()),
            capture_output=True,
            text=True,
            timeout=600,
        )
        if run.returncode == 0:
            return
    raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "failed to install rslogic in local venv")


def bootstrap_self(*, require_textual: bool = False) -> None:
    venv_python = _venv_python_path()
    if _python_can_import(venv_python, require_textual=require_textual):
        if os.environ.get(_VENV_BOOTSTRAP_ENV) == "1":
            return
        try:
            if os.path.samefile(sys.executable, str(venv_python)):
                return
        except Exception:
            pass
        if os.name == "nt":
            os.environ[_VENV_BOOTSTRAP_ENV] = "1"
            os.execv(str(venv_python), [str(venv_python), "-m", "rslogic.client.control_tui", *sys.argv[1:]])
        return

    _ensure_python_venv()
    _install_project(venv_python)

    if not _python_can_import(venv_python, require_textual=require_textual):
        # In some environments, editable metadata can momentarily report false negatives.
        # Keep behavior resilient and still continue by executing through the local venv.
        print("warn: bootstrap env import verification failed; continuing with best-effort launch.", file=sys.stderr)

    if os.environ.get(_VENV_BOOTSTRAP_ENV) == "1":
        return
    if os.name == "nt":
        os.environ[_VENV_BOOTSTRAP_ENV] = "1"
        os.execv(str(venv_python), [str(venv_python), "-m", "rslogic.client.control_tui", *sys.argv[1:]])


def _client_id() -> str:
    return os.getenv("RSLOGIC_CLIENT_ID", os.getenv("CLIENT_ID", "default-client"))


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _safe_json_loads(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


class _LogTailer:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._offset = 0

    def read(self, *, max_lines: int = 80) -> list[str]:
        if not self.path.exists():
            return []
        try:
            size = self.path.stat().st_size
            if self._offset > size:
                self._offset = 0
            with self.path.open("r", encoding="utf-8", errors="replace") as f:
                if self._offset:
                    f.seek(self._offset)
                text = f.read()
                self._offset = f.tell()
            if not text:
                return []
            lines = text.splitlines()
            return lines[-max_lines:]
        except Exception:
            return []


class ClientProcessManager:
    def __init__(self) -> None:
        self.root = _repo_root()
        self.logs_root = self.root / "logs" / "client"
        self.logs_root.mkdir(parents=True, exist_ok=True)
        self.log_stdout = self.logs_root / "rslogic-client-stdout.log"
        self.log_stderr = self.logs_root / "rslogic-client-stderr.log"
        self.pid_path = self.logs_root / "rslogic-client.pid"
        self.client_id = _client_id()
        self._proc: subprocess.Popen[bytes] | None = None
        self._stdout_handle = None
        self._stderr_handle = None
        self._stderr_tail = _LogTailer(self.log_stderr)
        self._stdout_tail = _LogTailer(self.log_stdout)

    @staticmethod
    def _is_windows_pid_alive(pid: int) -> bool:
        try:
            return os.kill(pid, 0) == 0
        except Exception:
            return False

    @property
    def _python_exec(self) -> str:
        if getattr(sys, "frozen", False):
            return shutil.which("python") or "python"
        if os.name == "nt":
            venv_exe = self.root / ".venv" / "Scripts" / "python.exe"
            if venv_exe.exists():
                return str(venv_exe)
            return self._fallback_python_exec()
        venv_exe = self.root / ".venv" / "bin" / "python"
        if venv_exe.exists():
            return str(venv_exe)
        return self._fallback_python_exec()

    def _fallback_python_exec(self) -> str:
        return sys.executable

    def _load_pid(self) -> int | None:
        if not self.pid_path.exists():
            return None
        try:
            raw = self.pid_path.read_text(encoding="utf-8").strip()
            if not raw:
                return None
            return int(raw)
        except Exception:
            return None

    def _write_pid(self, pid: int) -> None:
        self.pid_path.write_text(str(pid), encoding="utf-8")

    def _clear_pid(self) -> None:
        if self.pid_path.exists():
            self.pid_path.unlink(missing_ok=True)

    def _cleanup_dead_process(self) -> None:
        pid = self._load_pid()
        if pid is None:
            return
        if not self._is_windows_pid_alive(pid):
            self._clear_pid()

    def _current_client_process(self) -> tuple[bool, int | None]:
        self._cleanup_dead_process()
        pid = self._load_pid()
        if pid is None:
            return False, None
        if self._is_windows_pid_alive(pid):
            return True, pid
        self._clear_pid()
        return False, None

    def _build_child_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        if not env.get("RSLOGIC_CLIENT_ENV_FILE"):
            client_env = self.root / "client.env"
            if client_env.exists():
                env["RSLOGIC_CLIENT_ENV_FILE"] = str(client_env)

        existing_python_path = env.get("PYTHONPATH", "")
        sdk_path = self.root / "rslogic" / "internal_tools" / "rstool-sdk" / "src"
        additions = [str(self.root)]
        if sdk_path.exists():
            additions.append(str(sdk_path))
        for extra in additions:
            if extra and extra not in existing_python_path:
                existing_python_path = f"{extra}{os.pathsep}{existing_python_path}" if existing_python_path else extra
        env["PYTHONPATH"] = existing_python_path
        return env

    def _command_key(self) -> str:
        command_key = CONFIG.control.command_queue_key
        if "{client_id}" in command_key:
            return command_key.format(client_id=self.client_id)
        return f"{command_key}:{self.client_id}:jobs"

    def _command_queue_depth(self) -> int | None:
        import redis

        try:
            with redis.Redis.from_url(CONFIG.queue.redis_url, decode_responses=True) as rc:
                return int(rc.llen(self._command_key()))
        except Exception:
            return None

    def _heartbeat(self) -> dict[str, Any] | None:
        import redis

        try:
            with redis.Redis.from_url(CONFIG.queue.redis_url, decode_responses=True) as rc:
                raw = rc.get(f"rslogic:clients:{self.client_id}:heartbeat")
                return _safe_json_loads(raw)
        except Exception:
            return None

    def _heartbeat_age(self, heartbeat: dict[str, Any] | None) -> float | None:
        if not heartbeat:
            return None
        ts = heartbeat.get("ts")
        if not isinstance(ts, (float, int)):
            return None
        return round(time.time() - float(ts), 2)

    def _rsnode_pids(self) -> list[int]:
        executable = (CONFIG.rstools.executable_path or "").strip()
        candidates = {
            "rsnode.exe",
            "RSNode.exe",
            "realityscan.exe",
            "RealityScan.exe",
        }
        if executable:
            candidates.add(Path(executable).name)
        if not candidates:
            return []

        pids: list[int] = []
        try:
            for candidate in sorted(set(candidates)):
                if os.name == "nt":
                    cp = subprocess.run(
                        ["tasklist", "/FI", f"imagename eq {candidate}", "/NH", "/FO", "CSV"],
                        capture_output=True,
                        text=True,
                        timeout=2,
                    )
                    if cp.returncode != 0 or not cp.stdout:
                        continue
                    for line in cp.stdout.splitlines():
                        parts = [p.strip('"') for p in line.split(",")]
                        if len(parts) < 2:
                            continue
                        if parts[0].strip().lower() != candidate.lower():
                            continue
                        try:
                            pids.append(int(parts[1]))
                        except ValueError:
                            continue
                else:
                    cp = subprocess.run(
                        ["pgrep", "-f", candidate],
                        capture_output=True,
                        text=True,
                        timeout=2,
                    )
                    if cp.returncode != 0 or not cp.stdout:
                        continue
                    for raw_pid in cp.stdout.splitlines():
                        try:
                            pids.append(int(raw_pid))
                        except ValueError:
                            continue
        except Exception:
            return []

        return sorted(set(pids))

    def start(self) -> str:
        running, _ = self._current_client_process()
        if running:
            raise RuntimeError("client is already running")

        self.logs_root.mkdir(parents=True, exist_ok=True)
        self._stdout_handle = self.log_stdout.open("a", encoding="utf-8", buffering=1)
        self._stderr_handle = self.log_stderr.open("a", encoding="utf-8", buffering=1)

        cmd = [self._python_exec, "-m", "rslogic.client.rsnode_client"]
        creationflags = 0
        if os.name == "nt":
            creationflags = subprocess.CREATE_NO_WINDOW

        self._proc = subprocess.Popen(
            cmd,
            cwd=str(self.root),
            stdout=self._stdout_handle,
            stderr=self._stderr_handle,
            env=self._build_child_env(),
            creationflags=creationflags,
        )
        self._write_pid(self._proc.pid)
        return f"started (pid={self._proc.pid})"

    def stop(self) -> str:
        pid = self._load_pid()
        if pid is None:
            return "no client pid found"

        stopped = False
        try:
            os.kill(pid, getattr(signal, "SIGTERM", signal.SIGINT))
        except Exception:
            pass
        end = time.monotonic() + 8
        while time.monotonic() < end:
            if not self._is_windows_pid_alive(pid):
                stopped = True
                break
            time.sleep(0.25)

        if not stopped and self._is_windows_pid_alive(pid):
            try:
                os.kill(pid, getattr(signal, "SIGKILL", signal.SIGABRT))
                stopped = True
            except Exception:
                stopped = False

        self._clear_pid()
        if not stopped:
            raise RuntimeError("unable to stop client process")

        for node_pid in self._rsnode_pids():
            if node_pid == pid:
                continue
            try:
                os.kill(node_pid, getattr(signal, "SIGTERM", signal.SIGINT))
            except Exception:
                pass
        return "stopped"

    def restart(self) -> str:
        try:
            self.stop()
        except Exception:
            pass
        return self.start()

    def status(self) -> dict[str, Any]:
        running, pid = self._current_client_process()
        heartbeat = self._heartbeat()
        age = self._heartbeat_age(heartbeat)
        rsnode = self._rsnode_pids()
        return {
            "running": running,
            "client_pid": pid,
            "heartbeat": heartbeat,
            "heartbeat_age": age,
            "queued_commands": self._command_queue_depth(),
            "rsnode_running": len(rsnode) > 0,
            "rsnode_pids": rsnode,
            "client_id": self.client_id,
        }

    def read_log_lines(self, *, max_lines: int = 120) -> list[str]:
        lines = []
        for line in self._stdout_tail.read(max_lines=max_lines):
            lines.append(f"[green][stdout][/green] {line}")
        for line in self._stderr_tail.read(max_lines=max_lines):
            lines.append(f"[red][stderr][/red] {line}")
        return lines[-max_lines:]

    def shutdown(self) -> None:
        if self._stdout_handle is not None and not self._stdout_handle.closed:
            self._stdout_handle.close()
        if self._stderr_handle is not None and not self._stderr_handle.closed:
            self._stderr_handle.close()


class ClientControlTUI(App):
    """Small supervisory TUI for rslogic client + rsnode + heartbeat."""

    CSS = """
    Screen {
        layout: vertical;
    }
    #status_grid {
        height: 12;
    }
    .panel {
        border: round $primary;
        padding: 1;
    }
    #controls {
        height: 3;
        margin-top: 1;
    }
    #log {
        height: 1fr;
        border: round $primary;
        margin-top: 1;
    }
    """

    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh_now", "Refresh")]

    def __init__(self) -> None:
        super().__init__()
        self._manager = ClientProcessManager()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(f"RsLogic Client Control | client-id: {_client_id()}")
        yield Static(f"Root: {_repo_root()}", id="root_path")
        with Horizontal(id="status_grid"):
            with Vertical(classes="panel"):
                yield Static("Client process", id="client_status")
                yield Static("-", id="client_pid")
            with Vertical(classes="panel"):
                yield Static("RSNode process", id="rsnode_status")
                yield Static("-", id="rsnode_pids")
            with Vertical(classes="panel"):
                yield Static("Heartbeat", id="heartbeat_status")
                yield Static("-", id="heartbeat_age")
            with Vertical(classes="panel"):
                yield Static("Queue / client", id="queue_status")
                yield Static("-", id="queue_depth")
        with Horizontal(id="controls"):
            yield Button("Start", id="action_start", variant="primary")
            yield Button("Stop", id="action_stop", variant="error")
            yield Button("Restart", id="action_restart", variant="default")
            yield Button("Refresh", id="action_refresh", variant="default")
            yield Button("Clear logs", id="action_clear", variant="default")
            yield Button("Quit", id="action_quit", variant="warning")
        with Vertical(id="log"):
            yield Static("[b]Live logs[/b]")
            yield RichLog(id="event_log", max_lines=500, highlight=True, markup=True)
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh_all)
        self._refresh_all()

    def action_refresh_now(self) -> None:
        self._refresh_all()

    def action_quit(self) -> None:
        self._manager.shutdown()
        self.exit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "action_start":
                self._run_action("start", self._manager.start)
            case "action_stop":
                self._run_action("stop", self._manager.stop)
            case "action_restart":
                self._run_action("restart", self._manager.restart)
            case "action_refresh":
                self._refresh_all()
            case "action_clear":
                self.query_one("#event_log", expect_type=RichLog).clear()
            case "action_quit":
                self.action_quit()

    def _run_action(self, name: str, func: Any) -> None:
        def runner() -> None:
            try:
                result = func()
                self.call_from_thread(self._log, f"{_now()} [green]{name}[/] {result}")
            except Exception as exc:
                self.call_from_thread(self._log, f"{_now()} [red]{name} failed:[/] {type(exc).__name__}: {exc}")
            self.call_from_thread(self._refresh_all)

        # no blocking on main thread
        self._log(f"{_now()} {name}...")
        import threading

        threading.Thread(target=runner, daemon=True).start()

    def _refresh_all(self) -> None:
        status = self._manager.status()
        if status.get("running"):
            self.query_one("#client_status", expect_type=Static).update(f"[green]Client: running[/]")
            pid = status.get("client_pid")
            self.query_one("#client_pid", expect_type=Static).update(f"PID: {pid}")
        else:
            self.query_one("#client_status", expect_type=Static).update("[red]Client: stopped[/]")
            self.query_one("#client_pid", expect_type=Static).update("PID: -")

        rsnode_running = bool(status.get("rsnode_running"))
        rsnode_text = "[green]RSNode: running[/]" if rsnode_running else "[red]RSNode: not running[/]"
        self.query_one("#rsnode_status", expect_type=Static).update(rsnode_text)
        rsnode_pids = ", ".join(str(p) for p in (status.get("rsnode_pids") or []))
        self.query_one("#rsnode_pids", expect_type=Static).update(f"PIDs: {rsnode_pids or '-'}")

        heartbeat = status.get("heartbeat")
        if heartbeat:
            state = heartbeat.get("status", "n/a")
            self.query_one("#heartbeat_status", expect_type=Static).update(f"Heartbeat: [green]{state}[/]")
            age = status.get("heartbeat_age")
            age_text = f"{age}s ago" if isinstance(age, (float, int)) else "n/a"
            self.query_one("#heartbeat_age", expect_type=Static).update(f"Last seen: {age_text}")
        else:
            self.query_one("#heartbeat_status", expect_type=Static).update("[yellow]Heartbeat: no data[/]")
            self.query_one("#heartbeat_age", expect_type=Static).update("Last seen: n/a")

        qlen = status.get("queued_commands")
        self.query_one("#queue_status", expect_type=Static).update("[blue]Queue[/] for this client")
        self.query_one("#queue_depth", expect_type=Static).update(f"Commands pending: {qlen if qlen is not None else 'n/a'}")

        for line in self._manager.read_log_lines():
            self.query_one("#event_log", expect_type=RichLog).write(line)

    def _log(self, message: str) -> None:
        self.query_one("#event_log", expect_type=RichLog).write(message)


def run_command(mode: str) -> None:
    manager = ClientProcessManager()
    actions = {
        "status": lambda: manager.status(),
        "start": lambda: manager.start(),
        "stop": lambda: manager.stop(),
        "restart": lambda: manager.restart(),
    }
    if mode == "status":
        result = actions[mode]()
        print(json.dumps(result, indent=2))
        return
    if mode in actions:
        print(actions[mode]())
        return
    raise RuntimeError(f"unsupported mode: {mode}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="RsLogic client control launcher")
    parser.add_argument(
        "mode",
        nargs="?",
        default="tui",
        choices=("tui", "start", "stop", "restart", "status"),
        help="Run the control TUI or manage one-off client actions.",
    )
    args = parser.parse_args(argv)
    try:
        bootstrap_self(require_textual=(args.mode == "tui"))
    except Exception as exc:
        print(f"bootstrap failed: {exc}", file=sys.stderr)
        return
    if args.mode == "tui":
        app = ClientControlTUI()
        app.run()
        return
    run_command(args.mode)


if __name__ == "__main__":
    main()
