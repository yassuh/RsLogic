"""Ensure rsnode process is running."""

from __future__ import annotations

import logging
import os
import shlex
import shutil
import time
import threading
import subprocess
import sys
from pathlib import Path

_LOGGER = logging.getLogger("rslogic.client.process_guard")


class RsNodeProcess:
    def __init__(self, executable_path: str, executable_args: str | None = None) -> None:
        self.executable_path = executable_path
        self.executable_args = executable_args
        self._proc: subprocess.Popen[str] | None = None
        self._external_pid: int | None = None
        self._last_existing_check_ts: float | None = None
        self._existing_check_interval_s = 2.0
        self._lock = threading.Lock()

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            return os.kill(pid, 0) == 0
        except Exception:
            return False
    @staticmethod
    def _parse_csv_pid(raw: str) -> int | None:
        parts = [p.strip().strip('"') for p in raw.split(",")]
        if len(parts) < 2:
            return None
        try:
            return int(parts[1])
        except ValueError:
            return None

    @staticmethod
    def _is_exe_running_by_name_windows(name: str) -> int | None:
        try:
            cp = subprocess.run(
                ["tasklist", "/FI", f"imagename eq {name}", "/NH", "/FO", "CSV"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if cp.returncode != 0:
                return None
            for raw_line in (cp.stdout or "").splitlines():
                pid = RsNodeProcess._parse_csv_pid(raw_line)
                if pid is not None:
                    return pid
            return None
        except Exception:
            return None

    @staticmethod
    def _is_exe_running_by_name_unix(name: str) -> int | None:
        try:
            cp = subprocess.run(["pgrep", "-f", name], capture_output=True, text=True, timeout=2)
            if cp.returncode != 0:
                return None
            for raw in (cp.stdout or "").splitlines():
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    return int(raw)
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    def _find_existing_rsnode_pid(self) -> int | None:
        now = time.monotonic()
        if self._last_existing_check_ts is not None and (now - self._last_existing_check_ts) < self._existing_check_interval_s:
            if self._external_pid is None:
                return None
            if self._pid_alive(self._external_pid):
                return self._external_pid
            self._external_pid = None

        self._last_existing_check_ts = now
        if sys.platform.startswith("win"):
            pid = self._is_exe_running_by_name_windows("RSNode.exe")
        else:
            exe_name = Path(self.executable_path).name if self.executable_path else "RSNode"
            pid = self._is_exe_running_by_name_unix(exe_name)
        self._external_pid = pid
        return pid

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def ensure_running(self) -> None:
        with self._lock:
            if self.is_alive():
                return
            if self._find_existing_rsnode_pid() is not None:
                _LOGGER.debug("existing rsnode process detected; reusing external pid=%s", self._external_pid)
                return
            self.start()

    def start(self) -> None:
        if self.executable_path and not shutil.which(self.executable_path) and not os.path.exists(self.executable_path):
            raise FileNotFoundError(f"rsnode executable missing: {self.executable_path}")
        if self.is_alive():
            _LOGGER.debug("rsnode already running pid=%s", self._proc.pid if self._proc else "unknown")
            return
        existing_pid = self._find_existing_rsnode_pid()
        if existing_pid is not None:
            _LOGGER.info("existing rsnode process found pid=%s; not launching new process", existing_pid)
            return
        if not self.executable_path:
            _LOGGER.warning("rsnode executable path not configured; skipping start")
            return
        exe = self.executable_path
        if not os.path.isabs(exe):
            exe = shutil.which(exe) or exe
        cmd = [exe]
        if self.executable_args:
            cmd.extend(shlex.split(self.executable_args))
        _LOGGER.info("starting rsnode cmd=%s", cmd)
        if sys.platform.startswith("win"):
            CREATE_NO_WINDOW = 0x08000000
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                creationflags=CREATE_NO_WINDOW,
                cwd=None,
            )
        else:
            self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=None)
        if self._proc is not None:
            _LOGGER.info("rsnode started pid=%s", self._proc.pid)

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            _LOGGER.info("stopping rsnode pid=%s", self._proc.pid)
            self._proc.terminate()
            try:
                self._proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._proc.kill()
