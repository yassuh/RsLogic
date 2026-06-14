#!/usr/bin/env python3
"""Local RsLogic v2 smoke test.

Starts the management server and agent, approves enrollment, queues a no-input
job, and waits for worker events to mark the job completed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--container-runtime", default="docker")
    parser.add_argument("--realityscan-image", default="alpine:latest")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    target = repo / "target" / "debug"
    server_bin = target / binary_name("rslogic-server")
    agent_bin = target / binary_name("rslogic-agent")
    worker_bin = target / binary_name("rslogic-worker")

    if not args.no_build:
        run(["cargo", "build", "--workspace"], cwd=repo)

    for binary in (server_bin, agent_bin, worker_bin):
        if not binary.exists():
            raise RuntimeError(f"missing binary: {binary}")

    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    job_id = f"smoke-{int(time.time())}"
    temp = Path(tempfile.mkdtemp(prefix="rslogic-smoke-"))
    server_log = temp / "server.log"
    agent_log = temp / "agent.log"
    processes: list[subprocess.Popen] = []

    try:
        server = spawn(
            [str(server_bin), "--bind", f"127.0.0.1:{port}"],
            cwd=repo,
            log_path=server_log,
        )
        processes.append(server)
        wait_for_health(base_url, args.timeout_seconds)

        agent = spawn(
            [
                str(agent_bin),
                "--management-url",
                base_url,
                "--state-dir",
                str(temp / "agent-state"),
                "--worker-bin",
                str(worker_bin),
                "--worker-state-dir",
                str(temp / "worker-state"),
                "--container-runtime",
                args.container_runtime,
                "--heartbeat-seconds",
                "1",
            ],
            cwd=repo,
            log_path=agent_log,
        )
        processes.append(agent)

        client_id = approve_first_pending_client(base_url, args.timeout_seconds)
        queue_job(base_url, client_id, job_id, args.realityscan_image)
        wait_for_job_completed(base_url, job_id, args.timeout_seconds)

        print(json.dumps({"status": "ok", "client_id": client_id, "job_id": job_id}, indent=2))
        return 0
    except Exception as error:
        print(f"smoke failed: {error}", file=sys.stderr)
        dump_log("server", server_log)
        dump_log("agent", agent_log)
        return 1
    finally:
        for process in reversed(processes):
            terminate(process)
        shutil.rmtree(temp, ignore_errors=True)


def binary_name(name: str) -> str:
    return f"{name}.exe" if os.name == "nt" else name


def run(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def spawn(command: list[str], cwd: Path, log_path: Path) -> subprocess.Popen:
    log = log_path.open("wb")
    return subprocess.Popen(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)


def terminate(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_health(base_url: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = request_json("GET", f"{base_url}/healthz")
            if response.get("status") == "ok":
                return
        except Exception:
            time.sleep(0.25)
    raise TimeoutError("server health check timed out")


def approve_first_pending_client(base_url: str, timeout_seconds: int) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        records = request_json("GET", f"{base_url}/api/admin/client-enrollment/requests")
        for record in records:
            if record["status"] == "pending":
                approval = request_json(
                    "POST",
                    f"{base_url}/api/admin/client-enrollment/requests/{record['request_id']}/approve",
                )
                return str(approval["client_id"])
        time.sleep(0.5)
    raise TimeoutError("no pending enrollment request appeared")


def queue_job(base_url: str, client_id: str, job_id: str, image: str) -> None:
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    job = {
        "job_id": job_id,
        "job_name": "smoke no-op",
        "manifest": {"job_id": job_id, "expires_at": expires_at, "inputs": []},
        "output_targets": [],
        "realityscan_image": image,
    }
    request_json("POST", f"{base_url}/api/admin/clients/{client_id}/jobs", job)


def wait_for_job_completed(base_url: str, job_id: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_state = None
    while time.time() < deadline:
        jobs = request_json("GET", f"{base_url}/api/admin/jobs")
        for job in jobs:
            if job["job_id"] != job_id:
                continue
            last_state = job["state"]
            if last_state == "completed":
                return
            if last_state in {"failed", "cancelled"}:
                raise RuntimeError(f"job ended as {last_state}")
        time.sleep(0.5)
    raise TimeoutError(f"job did not complete; last state was {last_state}")


def request_json(method: str, url: str, body: object | None = None) -> object:
    data = None
    headers = {"accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["content-type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: {error.code} {detail}") from error
    return json.loads(raw.decode("utf-8"))


def dump_log(name: str, path: Path) -> None:
    if not path.exists():
        return
    print(f"\n--- {name} log ---", file=sys.stderr)
    print(path.read_text(errors="replace")[-8000:], file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
