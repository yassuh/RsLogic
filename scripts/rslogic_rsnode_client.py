#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, Sequence, Tuple
from urllib.error import URLError
from urllib.parse import quote_plus
from urllib.request import urlopen
from urllib.parse import urlparse
import socket

import logging


def _safe_program_data_path() -> Path:
    if os.name != "nt":
        return Path.home() / "ProgramData"
    program_data = os.getenv("ProgramData")
    return Path(program_data) if program_data else Path(os.environ.get("SYSTEMDRIVE", "C:") + "\\ProgramData")


def _safe_local_app_data_path() -> Path:
    if os.name != "nt":
        return Path.home() / ".local" / "share"
    local = os.getenv("LOCALAPPDATA")
    return Path(local) if local else Path.home() / "AppData" / "Local"


DEFAULT_REPO_URL = "https://github.com/yassuh/RsLogic.git"
DEFAULT_REPO_BRANCH = "main"
DEFAULT_SERVER_HOST = "192.168.193.56"
DEFAULT_REPO_ROOT = _safe_program_data_path() / "RsLogic" / "RsLogic"
DEFAULT_VENV_PATH = DEFAULT_REPO_ROOT / ".venv"
DEFAULT_NODE_EXECUTABLE = Path(os.getenv("ProgramFiles", str(Path("C:/Program Files"))) ) / "Epic Games" / "RealityScan_2.1" / "RSNode.exe"
DEFAULT_NODE_DATA_ROOT = _safe_local_app_data_path() / "Epic Games" / "RealityScan" / "RSNodeData"
DEFAULT_LOG_PATH = _safe_program_data_path() / "RsLogic" / "rsnode-orchestrator.log"
REQUIRED_CLIENT_MODULES = (
    "dotenv",
    "sqlalchemy",
    "redis",
    "requests",
    "httpx",
    "boto3",
    "PIL",
    "textual",
    "typer",
    "uvicorn",
    "psycopg",
    "geoalchemy2",
    "alembic",
)


@dataclass
class RunConfig:
    repo_url: str
    repo_branch: str
    repo_root: Path
    python_executable: str
    venv_path: Path
    node_executable: Path
    node_data_root: str
    node_data_root_argument: str
    node_arguments: List[str]
    redis_url: str
    redis_host: str
    redis_port: int
    redis_db: str
    redis_password: str
    control_command_queue: str
    control_result_queue: str
    queue_key: str
    server_host: str
    sdk_base_url: str
    sdk_client_id: str
    sdk_app_token: str
    sdk_auth_token: str
    client_workers: int
    node_poll_seconds: int
    node_startup_timeout_seconds: int
    repo_update_interval_seconds: int
    loop_sleep_seconds: int
    client_restart_delay_seconds: int
    node_restart_delay_seconds: int
    node_health_url: str
    log_path: Path
    no_auto_update: bool
    no_pull: bool
    no_deps: bool
    dry_run: bool
    git_sync_strategy: str


@dataclass
class ManagedProcess:
    proc: subprocess.Popen
    stdout_handle: IO[bytes]
    stderr_handle: IO[bytes]


def _parse_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, 1)


class FileSingletonLock:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._handle: Optional[Any] = None
        self.acquired = False

    def __enter__(self) -> "FileSingletonLock":
        self.acquired = self._acquire()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.release()

    def _acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.lock_path, "a+b")

        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                return True
            except OSError:
                return False

        import fcntl  # type: ignore[import-not-found]

        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def release(self) -> None:
        if not self._handle:
            return
        try:
            if os.name == "nt":
                import msvcrt

                try:
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                import fcntl  # type: ignore[import-not-found]

                try:
                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            try:
                self._handle.close()
            finally:
                self._handle = None


def setup_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("rslogic.rsnode_client_orchestrator")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "[{asctime}.{msecs:03.0f}] [{levelname}] {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def run_command(
    command: Sequence[str],
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        list(command),
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(command)} (exit={proc.returncode})\n"
            f"stdout: {proc.stdout}\n"
            f"stderr: {proc.stderr}"
        )
    return proc


def git_head(repo_root: Path) -> str:
    proc = run_command(["git", "-C", str(repo_root), "rev-parse", "HEAD"], check=False)
    return proc.stdout.strip()


def is_valid_repo(repo_root: Path) -> bool:
    return (repo_root / ".git").exists() and (repo_root / "pyproject.toml").exists()


def backup_dir(path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(f"{path}.{timestamp}")
    if backup_path.exists():
        backup_path = Path(f"{path}.{timestamp}_{time.time_ns()}")
    shutil.move(str(path), str(backup_path))
    return backup_path


def ensure_repository(cfg: RunConfig, logger: logging.Logger) -> bool:
    if cfg.no_pull:
        if not cfg.repo_root.exists() or not is_valid_repo(cfg.repo_root):
            raise RuntimeError(f"Repository missing or invalid at {cfg.repo_root} and --no-pull was requested.")
        return False

    if not cfg.repo_root.exists():
        cfg.repo_root.mkdir(parents=True, exist_ok=True)

    if not is_valid_repo(cfg.repo_root):
        if any(cfg.repo_root.iterdir()):
            logger.warning("Invalid repository checkout at %s; backing up then recloning.", cfg.repo_root)
            backup_dir(cfg.repo_root)
            cfg.repo_root.mkdir(parents=True, exist_ok=True)

        logger.info("Cloning %s into %s", cfg.repo_url, cfg.repo_root)
        if not cfg.dry_run:
            run_command(["git", "clone", "--branch", cfg.repo_branch, cfg.repo_url, str(cfg.repo_root)])
        return True

    logger.info("Checking for updates from %s (branch=%s)", cfg.repo_url, cfg.repo_branch)
    if cfg.dry_run:
        return False

    run_command(["git", "-C", str(cfg.repo_root), "fetch", "origin", "--prune", "--quiet"])
    run_command(["git", "-C", str(cfg.repo_root), "checkout", cfg.repo_branch])

    remote_ref = f"origin/{cfg.repo_branch}"
    relation = run_command(
        ["git", "-C", str(cfg.repo_root), "rev-list", "--left-right", "--count", f"HEAD...{remote_ref}"],
        check=False,
        capture_output=True,
    )
    if relation.returncode != 0:
        raise RuntimeError(
            f"Unable to compare against remote ref {remote_ref}: {relation.stderr.strip() or relation.stdout.strip()}"
        )

    parts = (relation.stdout or "").strip().split()
    if len(parts) != 2:
        raise RuntimeError(f"Unexpected git relation output from {remote_ref}: {relation.stdout!r}")

    try:
        behind = int(parts[0])
        ahead = int(parts[1])
    except ValueError:
        raise RuntimeError(f"Invalid git relation values from {remote_ref}: {relation.stdout!r}")

    if behind == 0 and ahead == 0:
        return False

    if behind > 0 and ahead == 0:
        logger.info("Remote is ahead by %s commit(s). Fast-forwarding.", behind)
        run_command(["git", "-C", str(cfg.repo_root), "merge", "--ff-only", remote_ref])
        return True

    if behind == 0 and ahead > 0:
        logger.warning("Local branch is ahead by %s commit(s); leaving local commits in place.", ahead)
        return False

    if ahead > 0 and behind > 0:
        logger.warning(
            "Branch is diverged (behind=%s, ahead=%s). Strategy=%s",
            behind,
            ahead,
            cfg.git_sync_strategy,
        )
        if cfg.git_sync_strategy == "hard-reset":
            logger.warning("Applying hard reset to %s for divergence repair.", remote_ref)
            run_command(["git", "-C", str(cfg.repo_root), "reset", "--hard", remote_ref])
            return True

        if cfg.git_sync_strategy == "rebase":
            logger.warning("Rebasing onto %s for divergence repair.", remote_ref)
            run_command(["git", "-C", str(cfg.repo_root), "rebase", remote_ref])
            return True

        if cfg.git_sync_strategy == "ff-only":
            raise RuntimeError(
                "Repository diverged and --git-sync-strategy=ff-only is set. "
                "Re-run with --git-sync-strategy hard-reset (safe) or rebase."
            )

        raise RuntimeError(f"Unknown git sync strategy '{cfg.git_sync_strategy}'.")

    raise RuntimeError(f"Unsupported git relation state behind={behind} ahead={ahead}")


def venv_python(cfg: RunConfig) -> Path:
    return cfg.venv_path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def resolve_python(candidate: str, logger: logging.Logger) -> str:
    candidates = []
    if candidate:
        candidates.append(candidate)
    candidates.extend(["py -3", "python3", "python"])

    last_error = "No python executable was found."
    for raw in candidates:
        if not raw:
            continue

        raw_candidate = raw.strip()
        if not raw_candidate:
            continue

        path_candidate = Path(raw_candidate)
        if path_candidate.exists():
            probe = subprocess.run(
                [str(path_candidate), "-c", "import sys; print(sys.executable)"],
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0 and probe.stdout.strip():
                resolved_python = probe.stdout.strip()
                logger.info("Resolved Python executable: %s", resolved_python)
                return resolved_python
            last_error = probe.stderr.strip() or f"{raw_candidate} returned code {probe.returncode}"
            continue

        tokens = shlex.split(raw_candidate)
        if not tokens:
            continue

        first = tokens[0]
        resolved = shutil.which(first)
        if not resolved:
            last_error = f"{raw_candidate} not found in PATH"
            continue

        try:
            probe = subprocess.run(
                [resolved, *tokens[1:], "-c", "import sys; print(sys.executable)"],
                capture_output=True,
                text=True,
            )
        except Exception as exc:
            last_error = f"{raw}: {exc}"
            continue
        if probe.returncode == 0 and probe.stdout.strip():
            resolved_python = probe.stdout.strip()
            logger.info("Resolved Python executable: %s", resolved_python)
            return resolved_python
        last_error = probe.stderr.strip() or f"{raw} returned code {probe.returncode}"
    raise RuntimeError(f"Python executable could not be resolved. Last error: {last_error}")


def ensure_venv(cfg: RunConfig, logger: logging.Logger) -> None:
    python_in_venv = venv_python(cfg)
    if python_in_venv.exists():
        return

    logger.info("Creating python virtual environment at %s", cfg.venv_path)
    if cfg.dry_run:
        return
    cfg.venv_path.parent.mkdir(parents=True, exist_ok=True)
    run_command([cfg.python_executable, "-m", "venv", str(cfg.venv_path)])


def missing_runtime_modules(python_executable: Path) -> List[str]:
    if not python_executable.exists():
        return ["python_executable"]

    import textwrap

    check_script = textwrap.dedent(
        """
        import importlib.util
        import json

        required = %s
        missing = [name for name in required if importlib.util.find_spec(name) is None]
        print(json.dumps(missing))
        """ % json.dumps(REQUIRED_CLIENT_MODULES)
    )
    proc = subprocess.run(
        [str(python_executable), "-c", check_script],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        if stderr:
            return [f"probe_error:{stderr}"]
        stdout = (proc.stdout or "").strip()
        if stdout:
            return [f"probe_error:{stdout}"]
        return ["runtime_probe_failure"]

    raw_output = (proc.stdout or "").strip().splitlines()
    if not raw_output:
        return ["runtime_probe_failure"]

    try:
        payload = raw_output[-1].strip()
        missing = json.loads(payload)
    except Exception:
        return [f"probe_error:non_json_output:{raw_output[-1].strip()[:240]}"]

    if not isinstance(missing, list):
        return ["runtime_probe_failure"]

    return [str(item) for item in missing if str(item).strip()]


def needs_dependency_install(cfg: RunConfig, head: str, marker_path: Path) -> bool:
    if cfg.no_deps:
        return False
    if not marker_path.exists():
        return True
    if not venv_python(cfg).exists():
        return True
    if missing_runtime_modules(venv_python(cfg)):
        return True
    try:
        installed_head = marker_path.read_text(encoding="utf-8").strip()
        return installed_head != head
    except Exception:
        return True


def install_project_dependencies(cfg: RunConfig, logger: logging.Logger) -> None:
    python_executable = venv_python(cfg)
    if not python_executable.exists():
        raise RuntimeError(f"Python executable not found in virtual environment: {python_executable}")

    if cfg.dry_run:
        logger.info("DRY RUN: python -m pip install --upgrade pip")
        logger.info("DRY RUN: python -m pip install -e .")
        return

    logger.info("Installing RsLogic in editable mode using %s", python_executable)
    run_command(
        [str(python_executable), "-m", "pip", "install", "--disable-pip-version-check", "--upgrade", "pip"],
        cwd=cfg.repo_root,
        capture_output=False,
    )
    run_command(
        [str(python_executable), "-m", "pip", "install", "--disable-pip-version-check", "-e", "."],
        cwd=cfg.repo_root,
        capture_output=False,
    )


def write_install_marker(marker_path: Path, head: str) -> None:
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text((head or "").strip(), encoding="utf-8")


def build_redis_url(explicit_url: str, host: str, port: int, db: str, password: str) -> str:
    if explicit_url:
        return explicit_url.strip()
    clean_host = host.strip() or "localhost"
    if password:
        return f"redis://:{quote_plus(password)}@{clean_host}:{port}/{db}"
    return f"redis://{clean_host}:{port}/{db}"


def build_client_env(cfg: RunConfig, redis_url: str, python_in_venv: Path) -> Dict[str, str]:
    heartbeat_interval = _parse_positive_int(os.getenv("RSLOGIC_CLIENT_HEARTBEAT_INTERVAL_SECONDS"), default=5)
    heartbeat_ttl = _parse_positive_int(
        os.getenv("RSLOGIC_CLIENT_HEARTBEAT_TTL_SECONDS"),
        default=max(heartbeat_interval * 3, 15),
    )
    if heartbeat_ttl < heartbeat_interval + 1:
        heartbeat_ttl = heartbeat_interval + 1

    return {
        "RSLOGIC_APP_NAME": "RsLogic RSNode Worker",
        "RSLOGIC_DEFAULT_GROUP_NAME": "default-group",
        "RSLOGIC_QUEUE_BACKEND": "redis",
        "RSLOGIC_REDIS_URL": redis_url,
        "RSLOGIC_REDIS_QUEUE_KEY": cfg.queue_key,
        "RSLOGIC_CONTROL_COMMAND_QUEUE": cfg.control_command_queue,
        "RSLOGIC_CONTROL_RESULT_QUEUE": cfg.control_result_queue,
        "RSLOGIC_CONTROL_BLOCK_TIMEOUT_SECONDS": "2",
        "RSLOGIC_CONTROL_RESULT_TTL_SECONDS": "3600",
        "RSLOGIC_CONTROL_REQUEST_TIMEOUT_SECONDS": "7200",
        "RSLOGIC_WORKER_COUNT": str(cfg.client_workers),
        "RSLOGIC_CLIENT_HEARTBEAT_INTERVAL_SECONDS": str(heartbeat_interval),
        "RSLOGIC_CLIENT_HEARTBEAT_TTL_SECONDS": str(heartbeat_ttl),
        "RSLOGIC_RSTOOLS_MODE": "remote",
        "RSLOGIC_RSTOOLS_SDK_BASE_URL": cfg.sdk_base_url,
        "RSLOGIC_RSTOOLS_SDK_CLIENT_ID": cfg.sdk_client_id,
        "RSLOGIC_RSTOOLS_SDK_APP_TOKEN": cfg.sdk_app_token,
        "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN": cfg.sdk_auth_token,
        "RSLOGIC_LOG_LEVEL": "INFO",
        "RSLOGIC_LOG_FORMAT": "%(asctime)s %(levelname)s %(name)s: %(message)s",
        "RSLOGIC_CLIENT_RESTART_SECONDS": str(cfg.client_restart_delay_seconds),
        "RSLOGIC_CLIENT_PYTHON": str(python_in_venv),
        "RSLOGIC_RSNODE_EXECUTABLE": str(cfg.node_executable),
        "RSLOGIC_RSNODE_DATA_ROOT": cfg.node_data_root,
        "RSLOGIC_RSNODE_WATCHDOG_POLL_SECONDS": str(cfg.node_poll_seconds),
        "RSLOGIC_RSNODE_WATCHDOG_STARTUP_TIMEOUT_SECONDS": str(cfg.node_startup_timeout_seconds),
        "RSLOGIC_RSNODE_WATCHDOG_RESTART_COOLDOWN_SECONDS": str(cfg.node_restart_delay_seconds),
        "RSLOGIC_RSNODE_REPO_URL": cfg.repo_url,
        "RSLOGIC_RSNODE_REPO_BRANCH": cfg.repo_branch,
        "RSLOGIC_RSNODE_AUTO_UPDATE": str(not cfg.no_auto_update).lower(),
        "RSLOGIC_RSNODE_REPO_UPDATE_INTERVAL_SECONDS": str(cfg.repo_update_interval_seconds),
    }


def write_client_env_file(env_file: Path, values: Dict[str, str]) -> None:
    lines = [f"{key}={values[key]}" for key in sorted(values) if values[key] is not None]
    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def get_node_health(url: str, logger: logging.Logger) -> bool:
    if not url:
        return True
    try:
        with urlopen(url, timeout=3) as response:
            return 200 <= response.status < 300
    except (URLError, OSError) as exc:  # pragma: no branch - environment dependent
        logger.debug("Node health check failed: %s", exc)
        return False


def tail_lines(path: Path, line_count: int = 30) -> str:
    if not path.exists():
        return ""
    try:
        return "\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-line_count:])
    except Exception:
        return ""


def _log_file_position(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return path.stat().st_size
    except Exception:
        return 0


def _read_new_log_lines(path: Path, cursor: int) -> Tuple[List[str], int]:
    if not path.exists():
        return [], cursor
    try:
        current = path.stat().st_size
    except Exception:
        return [], cursor

    if current < cursor:
        cursor = 0

    if current == cursor:
        return [], cursor

    try:
        with path.open("rb") as handle:
            handle.seek(cursor)
            raw = handle.read(current - cursor)
        text = raw.decode("utf-8", errors="replace")
        return [line for line in text.splitlines() if line], current
    except Exception:
        return [], current


def _check_redis_connectivity(redis_url: str, logger: logging.Logger) -> bool:
    if not redis_url:
        logger.warning("Redis URL empty; skipping connectivity check.")
        return False

    parsed = urlparse(redis_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 6379

    try:
        with socket.create_connection((host, port), timeout=3) as sock:
            sock.settimeout(3.0)
            sock.sendall(b"*1\r\n$4\r\nPING\r\n")
            response = sock.recv(64)
            if not response:
                logger.error("Redis connectivity check failed: empty response for %s:%s", host, port)
                return False
            text_response = response.decode("utf-8", errors="replace").strip()
            if text_response.startswith("+PONG"):
                logger.info("Redis ping successful for %s", redis_url)
                return True
            logger.warning("Redis ping attempt returned unexpected response: %r", text_response)
            return False
    except Exception as exc:
        logger.error("Redis connectivity check failed for %s:%s: %s", host, port, exc)
        return False


def format_command(cmd: Sequence[str]) -> str:
    return " ".join(f'"{part}"' if " " in part else part for part in cmd)


def build_node_root_candidates(node_data_root_argument: str, node_data_root: str) -> List[str]:
    if not node_data_root:
        return [""]

    candidates: List[str] = []
    argument = node_data_root_argument.strip() if node_data_root_argument else ""
    if argument:
        candidates.append(argument)

    if argument != "-dataRoot":
        candidates.append("-dataRoot")
    if argument != "--dataRoot":
        candidates.append("--dataRoot")

    deduped: List[str] = []
    for item in candidates:
        if item not in deduped:
            deduped.append(item)
    deduped.append("")
    return deduped


def detect_rsapp_path(node_executable: Path, explicit_args: Sequence[str]) -> Optional[str]:
    arg_set = {arg.lower() for arg in explicit_args}
    if "-rsapp" in arg_set or "--rsapp" in arg_set:
        return None

    candidates: List[Path] = []
    node_dir = node_executable.parent if node_executable else None
    if node_dir:
        candidates.extend(
            [
                node_dir / "RealityScan.exe",
                node_dir / "RealityScan_2.1" / "RealityScan.exe",
                node_dir.parent / "RealityScan.exe",
            ]
        )

    for env_name in ("ProgramFiles", "PROGRAMFILES(X86)"):
        root = os.environ.get(env_name)
        if not root:
            continue
        root_path = Path(root) / "Epic Games"
        candidates.extend(
            [
                root_path / "RealityScan_2.1" / "RealityScan.exe",
                root_path / "RealityScan" / "RealityScan.exe",
                root_path / "RealityScan_2.1" / "RealityScan" / "RealityScan.exe",
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    return None


def start_process_with_logs(
    command: Sequence[str],
    cwd: Path,
    env: Dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    logger: logging.Logger,
) -> ManagedProcess:
    stdout_handle = open(stdout_path, "ab", buffering=0)
    stderr_handle = open(stderr_path, "ab", buffering=0)

    creation_flags = 0
    if os.name == "nt":
        creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    try:
        proc = subprocess.Popen(
            list(command),
            cwd=str(cwd),
            env=env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creation_flags,
        )
        return ManagedProcess(proc=proc, stdout_handle=stdout_handle, stderr_handle=stderr_handle)
    except Exception as exc:
        stdout_handle.close()
        stderr_handle.close()
        raise RuntimeError(f"Failed to launch process {format_command(command)}: {exc}")


def stop_process(managed: Optional[ManagedProcess], name: str, logger: logging.Logger) -> None:
    if managed is None:
        return
    proc = managed.proc
    if proc.poll() is not None:
        try:
            managed.stdout_handle.close()
            managed.stderr_handle.close()
        except Exception:
            pass
        return

    logger.info("Stopping %s (pid=%s)", name, proc.pid)
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass
    finally:
        try:
            managed.stdout_handle.close()
            managed.stderr_handle.close()
        except Exception:
            pass


def run_rsnode(cfg: RunConfig, logger: logging.Logger, log_dir: Path) -> Tuple[Optional[ManagedProcess], str]:
    candidates = build_node_root_candidates(cfg.node_data_root_argument, cfg.node_data_root)
    node_launch_dir = cfg.node_executable.parent if cfg.node_executable else cfg.repo_root
    node_base_args = list(cfg.node_arguments)
    if not any(arg.lower() in ("-console", "--console") for arg in node_base_args):
        node_base_args.append("-console")

    rs_app = detect_rsapp_path(cfg.node_executable, node_base_args)
    if rs_app:
        node_base_args.extend(["-rsapp", rs_app])

    last_error = "not-started"
    for attempt, root_arg in enumerate(candidates, start=1):
        root_arg = root_arg.strip()
        node_args: List[str] = []
        node_args.extend(node_base_args)
        if root_arg and cfg.node_data_root:
            node_args.extend([root_arg, cfg.node_data_root])
        command = [str(cfg.node_executable), *node_args]
        logger.info(
            "Starting RSNode (attempt=%s arg=%s): %s",
            attempt,
            root_arg or "none",
            format_command(command),
        )
        if cfg.dry_run:
            return None, "dry-run"

        managed: Optional[ManagedProcess] = None
        try:
            managed = start_process_with_logs(
                command=command,
                cwd=node_launch_dir,
                env=dict(os.environ),
                stdout_path=log_dir / "rsnode-stdout.log",
                stderr_path=log_dir / "rsnode-stderr.log",
                logger=logger,
            )
            time.sleep(1.0)
            if managed.proc.poll() is not None:
                err_tail = tail_lines(log_dir / "rsnode-stderr.log")
                exit_code = managed.proc.returncode
                msg = f"exit-code={exit_code}"
                logger.error("RSNode exited immediately (%s). stderr tail:\n%s", msg, err_tail or "<empty>")
                stop_process(managed, "RSNode.exe", logger)
                last_error = f"attempt={attempt}-{msg}"
                managed = None
                continue
            return managed, "running"
        except Exception as exc:
            if managed is not None:
                stop_process(managed, "RSNode.exe", logger)
            last_error = str(exc)
            logger.warning("Start attempt %s failed (arg='%s'): %s", attempt, root_arg or "none", exc)
    return None, last_error


def run_rslogic_client(cfg: RunConfig, env_values: Dict[str, str], logger: logging.Logger, log_dir: Path) -> Tuple[Optional[ManagedProcess], str]:
    python_executable = str(venv_python(cfg))
    command = [
        python_executable,
        "-m",
        "rslogic.client.rsnode_client",
        "run",
        "--workers",
        str(cfg.client_workers),
    ]
    logger.info("Starting rslogic-client: %s", format_command(command))
    if cfg.dry_run:
        return None, "dry-run"

    managed: Optional[ManagedProcess] = None
    env = dict(os.environ)
    env.update({k: str(v) for k, v in env_values.items()})

    try:
        managed = start_process_with_logs(
            command=command,
            cwd=cfg.repo_root,
            env=env,
            stdout_path=log_dir / "rslogic-client-stdout.log",
            stderr_path=log_dir / "rslogic-client-stderr.log",
            logger=logger,
        )
        time.sleep(1.0)
        if managed.proc.poll() is not None:
            err_tail = tail_lines(log_dir / "rslogic-client-stderr.log")
            exit_code = managed.proc.returncode
            if err_tail:
                logger.error("Client process exited immediately (exit-code=%s). stderr: %s", exit_code, err_tail)
            else:
                logger.error("Client process exited immediately (exit-code=%s)", exit_code)
            stop_process(managed, "rslogic-client", logger)
            return None, f"exit-code={exit_code}"
        return managed, "running"
    except Exception as exc:
        if managed is not None:
            stop_process(managed, "rslogic-client", logger)
        return None, str(exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RsLogic RSNode client orchestrator")
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL)
    parser.add_argument("--repo-branch", default=DEFAULT_REPO_BRANCH)
    parser.add_argument("--repo-root", default=str(DEFAULT_REPO_ROOT))
    parser.add_argument(
        "--git-sync-strategy",
        choices=["ff-only", "rebase", "hard-reset"],
        default="hard-reset",
        help="How to resolve branch divergence: ff-only, rebase, or hard-reset",
    )
    parser.add_argument("--python-executable", default="")
    parser.add_argument("--venv-path", default="")
    parser.add_argument("--node-executable", default=str(DEFAULT_NODE_EXECUTABLE))
    parser.add_argument("--node-data-root", default=str(DEFAULT_NODE_DATA_ROOT))
    parser.add_argument(
        "--node-data-root-argument",
        nargs="?",
        const="-dataRoot",
        default="-dataRoot",
    )
    parser.add_argument("--node-arguments", nargs="*", default=[])

    parser.add_argument("--redis-url", default="")
    parser.add_argument("--redis-host", default="")
    parser.add_argument("--redis-port", type=int, default=9002)
    parser.add_argument("--redis-db", default="0")
    parser.add_argument("--redis-password", default="")
    parser.add_argument("--control-command-queue", default="rslogic:control:commands")
    parser.add_argument("--control-result-queue", default="rslogic:control:results")
    parser.add_argument("--queue-key", default="rslogic:jobs:queue")

    parser.add_argument("--server-host", default=DEFAULT_SERVER_HOST)
    parser.add_argument("--sdk-base-url", default="")
    parser.add_argument("--sdk-client-id", default="")
    parser.add_argument("--sdk-app-token", default="")
    parser.add_argument("--sdk-auth-token", default="")

    parser.add_argument("--client-workers", type=int, default=1)
    parser.add_argument("--node-poll-seconds", type=int, default=10)
    parser.add_argument("--node-startup-timeout-seconds", type=int, default=60)
    parser.add_argument("--repo-update-interval-seconds", type=int, default=300)
    parser.add_argument("--loop-sleep-seconds", type=int, default=8)
    parser.add_argument("--client-restart-delay-seconds", type=int, default=8)
    parser.add_argument("--node-restart-delay-seconds", type=int, default=5)
    parser.add_argument("--node-health-url", default="")

    parser.add_argument("--log-path", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--no-auto-update", action="store_true")
    parser.add_argument("--no-pull", action="store_true")
    parser.add_argument("--no-deps", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def normalize_config(ns: argparse.Namespace) -> RunConfig:
    repo_root = Path(ns.repo_root).expanduser()
    if not repo_root.is_absolute():
        repo_root = (Path.cwd() / repo_root).resolve()

    venv_path = Path(ns.venv_path).expanduser() if ns.venv_path else repo_root / ".venv"
    if not venv_path.is_absolute():
        venv_path = (repo_root / venv_path).resolve()

    node_executable = Path(ns.node_executable).expanduser()
    if not node_executable.is_absolute():
        node_executable = (repo_root / node_executable).resolve()

    server_host = (ns.server_host or "").strip()
    redis_host = (ns.redis_host or "").strip()
    if not redis_host or redis_host.lower() == "localhost":
        redis_host = server_host or "localhost"

    sdk_base_url = (ns.sdk_base_url or "").strip()
    if not sdk_base_url and server_host and server_host.lower() != "localhost":
        sdk_base_url = f"http://{server_host}:8000"

    return RunConfig(
        repo_url=ns.repo_url,
        repo_branch=ns.repo_branch,
        repo_root=repo_root,
        python_executable=ns.python_executable,
        venv_path=venv_path,
        node_executable=node_executable,
        node_data_root=ns.node_data_root,
        node_data_root_argument=ns.node_data_root_argument,
        node_arguments=list(ns.node_arguments),
        redis_url=ns.redis_url,
        redis_host=redis_host,
        redis_port=int(ns.redis_port),
        redis_db=str(ns.redis_db),
        redis_password=ns.redis_password,
        control_command_queue=ns.control_command_queue,
        control_result_queue=ns.control_result_queue,
        queue_key=ns.queue_key,
        server_host=server_host,
        sdk_base_url=sdk_base_url,
        sdk_client_id=ns.sdk_client_id,
        sdk_app_token=ns.sdk_app_token,
        sdk_auth_token=ns.sdk_auth_token,
        client_workers=ns.client_workers,
        node_poll_seconds=ns.node_poll_seconds,
        node_startup_timeout_seconds=ns.node_startup_timeout_seconds,
        repo_update_interval_seconds=ns.repo_update_interval_seconds,
        loop_sleep_seconds=ns.loop_sleep_seconds,
        client_restart_delay_seconds=ns.client_restart_delay_seconds,
        node_restart_delay_seconds=ns.node_restart_delay_seconds,
        node_health_url=ns.node_health_url,
        log_path=Path(ns.log_path),
        no_auto_update=ns.no_auto_update,
        no_pull=ns.no_pull,
        no_deps=ns.no_deps,
        dry_run=ns.dry_run,
        git_sync_strategy=ns.git_sync_strategy,
    )


def wait_for_node_health(cfg: RunConfig, logger: logging.Logger, logger_interval_seconds: int = 2) -> bool:
    if not cfg.node_health_url:
        return True
    deadline = time.time() + cfg.node_startup_timeout_seconds
    while time.time() < deadline:
        if get_node_health(cfg.node_health_url, logger):
            return True
        time.sleep(logger_interval_seconds)
    return False


def stop_all(node_proc: Optional[ManagedProcess], client_proc: Optional[ManagedProcess], logger: logging.Logger) -> None:
    stop_process(node_proc, "RSNode.exe", logger)
    stop_process(client_proc, "rslogic-client", logger)

def main() -> int:
    args = parse_args()
    cfg = normalize_config(args)
    cfg.log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger(cfg.log_path)
    log_dir = cfg.log_path.parent
    client_stdout_log = log_dir / "rslogic-client-stdout.log"
    client_stderr_log = log_dir / "rslogic-client-stderr.log"

    loop_start = datetime.now()
    marker_path = cfg.venv_path / ".rslogic_install_head.txt"
    env_file = cfg.repo_root / ".env.rsnode-worker"

    should_stop = False
    node_proc: Optional[ManagedProcess] = None
    client_proc: Optional[ManagedProcess] = None
    node_stop_reason = "not-started"
    client_stop_reason = "not-started"
    client_bootstrap_state = {
        "redis": "unknown",
        "heartbeat": "unknown",
    }
    client_log_offsets = {
        str(client_stdout_log): _log_file_position(client_stdout_log),
        str(client_stderr_log): _log_file_position(client_stderr_log),
    }

    def _poll_client_bootstrap_state() -> None:
        for path_key, offset in list(client_log_offsets.items()):
            path = Path(path_key)
            new_lines, new_offset = _read_new_log_lines(path, offset)
            client_log_offsets[path_key] = new_offset
            for line in new_lines:
                if "RSNode client startup: redis ping successful" in line:
                    client_bootstrap_state["redis"] = "connected"
                if "RSNode presence heartbeat:" in line:
                    client_bootstrap_state["heartbeat"] = "enabled"

    def request_stop(_signum: Optional[int] = None, _frame: Any = None) -> None:
        nonlocal should_stop
        should_stop = True
        if _signum is not None:
            logger.info("Shutdown requested (signal=%s).", _signum)

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    lock_path = cfg.log_path.parent / "rslogic-rsnode-client.lock"
    if not env_file.parent.exists():
        env_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with FileSingletonLock(lock_path) as lock:
            if not lock.acquired:
                print("Another orchestrator instance is already running. Exiting this invocation.")
                return 0

            logger.info("RsLogic RSNode client orchestrator bootstrapping")
            logger.info("Repo path: %s", cfg.repo_root)
            logger.info("Repository HEAD before bootstrap/update: %s", git_head(cfg.repo_root))
            redis_connection = build_redis_url(cfg.redis_url, cfg.redis_host, cfg.redis_port, cfg.redis_db, cfg.redis_password)
            redis_preflight_ok = _check_redis_connectivity(redis_connection, logger)
            logger.info("Redis connectivity preflight: %s", "connected" if redis_preflight_ok else "disconnected")

            bootstrapped = ensure_repository(cfg, logger)
            if bootstrapped:
                logger.info("Repository bootstrap detected; fresh install complete.")
            if not is_valid_repo(cfg.repo_root):
                raise RuntimeError(f"Repository still invalid at {cfg.repo_root}")

            cfg.python_executable = resolve_python(cfg.python_executable, logger)
            ensure_venv(cfg, logger)
            python_in_venv = venv_python(cfg)
            if not python_in_venv.exists():
                raise RuntimeError(f"Python executable not found in virtual environment: {python_in_venv}")

            env_values = build_client_env(cfg, redis_connection, python_in_venv)
            write_client_env_file(env_file, env_values)
            logger.info(
                "Client heartbeat config: interval=%ss ttl=%ss",
                env_values["RSLOGIC_CLIENT_HEARTBEAT_INTERVAL_SECONDS"],
                env_values["RSLOGIC_CLIENT_HEARTBEAT_TTL_SECONDS"],
            )

            current_head = git_head(cfg.repo_root)
            if needs_dependency_install(cfg, current_head, marker_path):
                missing_modules = missing_runtime_modules(venv_python(cfg))
                if missing_modules:
                    logger.warning("Runtime module probe before install: missing %s", ", ".join(sorted(missing_modules)))
                logger.info("Installing dependencies for checkout %s", current_head)
                install_project_dependencies(cfg, logger)
                post_install_missing = missing_runtime_modules(venv_python(cfg))
                if post_install_missing:
                    raise RuntimeError(
                        f"Dependency install completed but still missing modules: {', '.join(sorted(post_install_missing))}"
                    )
                write_install_marker(marker_path, current_head)
            else:
                missing_modules = missing_runtime_modules(venv_python(cfg))
                if missing_modules:
                    logger.warning("Missing runtime modules despite dependency marker match: %s", ", ".join(sorted(missing_modules)))
                    if cfg.no_deps:
                        raise RuntimeError(
                            "Refusing to run with --no-deps because required runtime modules are missing: "
                            + ", ".join(sorted(missing_modules))
                        )
                logger.info(
                    "Skipping dependency install; environment already initialized for repository commit %s",
                    current_head,
                )

            last_update_check = time.time()
            next_status = time.time()
            logger.info("Startup complete. Entering watch loop.")

            while not should_stop:
                updated = False
                if (
                    not cfg.no_auto_update
                    and not cfg.no_pull
                    and cfg.repo_update_interval_seconds > 0
                    and time.time() - last_update_check >= cfg.repo_update_interval_seconds
                ):
                    last_update_check = time.time()
                    try:
                        updated = ensure_repository(cfg, logger)
                    except Exception as exc:
                        logger.warning("Update check failed: %s", exc)

                if updated:
                    current_head = git_head(cfg.repo_root)
                    logger.info("Repository changed. Refreshing dependencies and restarting managed services.")
                    if needs_dependency_install(cfg, current_head, marker_path):
                        missing_modules = missing_runtime_modules(venv_python(cfg))
                        if missing_modules:
                            logger.warning(
                                "Runtime module probe before refresh: missing %s",
                                ", ".join(sorted(missing_modules)),
                            )
                        install_project_dependencies(cfg, logger)
                        post_install_missing = missing_runtime_modules(venv_python(cfg))
                        if post_install_missing:
                            raise RuntimeError(
                                "Dependency refresh completed but still missing modules: "
                                f"{', '.join(sorted(post_install_missing))}"
                            )
                        write_install_marker(marker_path, current_head)
                    else:
                        logger.info(
                            "Dependency marker up to date for checkout %s. No install required.",
                            current_head,
                        )
                    stop_all(node_proc, client_proc, logger)
                    node_proc = None
                    client_proc = None
                    node_stop_reason = "restarting"
                    client_stop_reason = "restarting"
                    env_values = build_client_env(cfg, redis_connection, python_in_venv)
                    write_client_env_file(env_file, env_values)

                if node_proc is None or node_proc.proc.poll() is not None:
                    if node_proc is not None and node_proc.proc.poll() is not None:
                        node_stop_reason = f"exit-code={node_proc.proc.returncode}"
                    node_proc, node_stop_reason = run_rsnode(cfg, logger, log_dir)
                    if node_proc:
                        node_stop_reason = "running"
                        if not wait_for_node_health(cfg, logger):
                            logger.warning("RSNode failed health check after startup; restarting.")
                            node_stop_reason = "health-check-failed"
                            stop_process(node_proc, "RSNode.exe", logger)
                            node_proc = None
                            time.sleep(cfg.node_restart_delay_seconds)
                    else:
                        logger.warning("RSNode start failed. Reason: %s", node_stop_reason)
                        time.sleep(cfg.node_restart_delay_seconds)

                if client_proc is None or client_proc.proc.poll() is not None:
                    if client_proc is not None and client_proc.proc.poll() is not None:
                        client_stop_reason = f"exit-code={client_proc.proc.returncode}"
                    if node_proc and node_proc.proc.poll() is None:
                        client_log_offsets[str(client_stdout_log)] = _log_file_position(client_stdout_log)
                        client_log_offsets[str(client_stderr_log)] = _log_file_position(client_stderr_log)
                        client_bootstrap_state["redis"] = "unknown"
                        client_bootstrap_state["heartbeat"] = "unknown"
                        client_proc, client_stop_reason = run_rslogic_client(cfg, env_values, logger, log_dir)
                        _poll_client_bootstrap_state()
                        if not client_proc and "exit-code" in client_stop_reason:
                            logger.warning("rslogic-client failed: %s", client_stop_reason)
                            time.sleep(cfg.client_restart_delay_seconds)
                    else:
                        client_stop_reason = "waiting-for-node"

                if node_proc and node_proc.proc.poll() is None and cfg.node_health_url:
                    if not get_node_health(cfg.node_health_url, logger):
                        logger.warning("RSNode health check failed. Restarting RSNode and client.")
                        stop_all(node_proc, client_proc, logger)
                        node_proc = None
                        client_proc = None
                        node_stop_reason = "health-check-failed"
                        client_stop_reason = "health-check-failed"
                        time.sleep(cfg.node_restart_delay_seconds)

                if time.time() >= next_status:
                    if client_proc and client_proc.proc.poll() is None:
                        _poll_client_bootstrap_state()
                    current_head = git_head(cfg.repo_root)
                    node_up = str(node_proc.proc.pid) if node_proc and node_proc.proc.poll() is None else f"stopped/{node_stop_reason}"
                    client_up = (
                        str(client_proc.proc.pid)
                        if client_proc and client_proc.proc.poll() is None
                        else f"stopped/{client_stop_reason}"
                    )
                    health = "ok"
                    if cfg.node_health_url and not get_node_health(cfg.node_health_url, logger):
                        health = "degraded"
                    uptime = str(timedelta(seconds=max(0, int((datetime.now() - loop_start).total_seconds()))))
                    logger.info(
                        "STATUS node=%s client=%s autoUpdate=%s health=%s repo=%s uptime=%s clientRedis=%s clientHeartbeat=%s",
                        node_up,
                        client_up,
                        not cfg.no_auto_update,
                        health,
                        current_head,
                        uptime,
                        client_bootstrap_state["redis"],
                        client_bootstrap_state["heartbeat"],
                    )
                    next_status = time.time() + max(cfg.loop_sleep_seconds, 5)

                time.sleep(cfg.loop_sleep_seconds)

            logger.info("Shutdown requested. Stopping managed processes.")
            stop_all(node_proc, client_proc, logger)
            logger.info("Orchestrator stopped.")
            return 0

    except Exception as exc:
        logger.error("Fatal error: %s", exc)
        logger.debug("Exception details:\n%s", traceback.format_exc())
        try:
            stop_all(node_proc, client_proc, logger)
        finally:
            logger.error("Orchestrator stopped with error.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
