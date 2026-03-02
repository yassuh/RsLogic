"""Ensure rsnode process is running."""

from __future__ import annotations

import os
import shlex
import shutil
import threading
import subprocess
import sys


class RsNodeProcess:
    def __init__(self, executable_path: str, executable_args: str | None = None) -> None:
        self.executable_path = executable_path
        self.executable_args = executable_args
        self._proc: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def ensure_running(self) -> None:
        with self._lock:
            if self.is_alive():
                return
            self.start()

    def start(self) -> None:
        if self.executable_path and not shutil.which(self.executable_path) and not os.path.exists(self.executable_path):
            raise FileNotFoundError(f"rsnode executable missing: {self.executable_path}")
        if self.is_alive():
            return
        if not self.executable_path:
            return
        exe = self.executable_path
        if not os.path.isabs(exe):
            exe = shutil.which(exe) or exe
        cmd = [exe]
        if self.executable_args:
            cmd.extend(shlex.split(self.executable_args))
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

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._proc.kill()
