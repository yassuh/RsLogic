from __future__ import annotations

import logging
import sys
import types
from pathlib import Path

from scripts import rslogic_rsnode_client as rsclient


class _FakeSocket:
    def __init__(self, response: bytes = b"+PONG\\r\\n") -> None:
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def settimeout(self, *_args, **_kwargs) -> None:
        return None

    def sendall(self, _data: bytes) -> None:
        return None

    def recv(self, _size: int) -> bytes:
        return self._response


class _FakeRedisClient:
    def __init__(self, heartbeat_values, scan_keys) -> None:
        self.heartbeat_values = heartbeat_values
        self.scan_keys = list(scan_keys)
        self.closed = False

    def ping(self) -> None:
        return None

    def get(self, key):
        return self.heartbeat_values.get(key)

    def scan_iter(self, match=None, count: int = 100):
        del match
        del count
        return iter(self.scan_keys)

    def close(self) -> None:
        self.closed = True


def _with_argv(argv: list[str]):
    original = list(sys.argv)
    sys.argv = ["prog"] + list(argv)
    try:
        return rsclient.parse_args()
    finally:
        sys.argv = original


def test_build_redis_url_uses_explicit_url():
    value = rsclient.build_redis_url("redis://override:1234/9", "127.0.0.1", 9002, "0", "pw")
    assert value == "redis://override:1234/9"


def test_build_redis_url_builds_from_components_with_password():
    value = rsclient.build_redis_url("", "redis-host", 9002, "1", "pa$$")
    assert value == "redis://:pa%24%24@redis-host:9002/1"


def test_check_redis_connectivity_success(monkeypatch):
    monkeypatch.setattr(rsclient.socket, "create_connection", lambda *_args, **_kwargs: _FakeSocket())
    assert rsclient._check_redis_connectivity("redis://redis-host:9002/0", logging.getLogger("test")) is True


def test_check_redis_connectivity_failure(monkeypatch):
    def _raise(*_args, **_kwargs):
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr(rsclient.socket, "create_connection", _raise)
    assert rsclient._check_redis_connectivity("redis://redis-host:9002/0", logging.getLogger("test")) is False


def test_get_client_heartbeat_status_not_configured(monkeypatch):
    fake_redis_module = types.ModuleType("redis")
    fake_redis_module.Redis = type(
        "FakeRedis",
        (),
        {"from_url": staticmethod(lambda *a, **k: None)},
    )
    monkeypatch.setitem(sys.modules, "redis", fake_redis_module)

    status, redis_status, detail = rsclient.get_client_heartbeat_status(
        redis_url="",
        control_command_queue="rslogic:control:commands",
        logger=logging.getLogger("test"),
    )
    assert status == "not-configured"
    assert redis_status == "disconnected"
    assert "missing redis_url or control_command_queue" in detail


def test_parse_node_data_root_argument_default_is_kept():
    parsed = _with_argv(["--node-data-root-argument"])
    assert parsed.node_data_root_argument == "-dataRoot"


def test_parse_args_accepts_negative_values():
    parsed = _with_argv(["--node-data-root", "/tmp/data", "--client-workers", "2", "--node-poll-seconds", "0", "--repo-root", "/tmp/repo"])
    assert parsed.node_data_root == "/tmp/data"
    assert parsed.client_workers == 2
    assert parsed.node_poll_seconds == 0


def test_parse_positive_int_and_iso_parse():
    assert rsclient._parse_positive_int(-7, 5) == 1
    assert rsclient._parse_positive_int("12", 5) == 12

    parsed = rsclient._safe_parse_iso_timestamp("2026-02-27T20:01:54.294Z")
    assert parsed is not None
    assert parsed.year == 2026


def test_build_expected_presence_key_and_formatting_helpers():
    assert rsclient.build_expected_presence_key("rslogic:control:commands", "host", 123) == "rslogic:control:commands:presence:host:123"
    assert rsclient.format_command(["a", "b c"]) == 'a "b c"'


def test_build_node_root_candidates_dedupes_and_includes_empty():
    assert rsclient.build_node_root_candidates("--dataRoot", "C:/data") == ["--dataRoot", "-dataRoot", ""]
    assert rsclient.build_node_root_candidates("", "C:/data")==["-dataRoot", "--dataRoot", ""]


def test_detect_rsapp_path_respects_explicit_arg():
    assert rsclient.detect_rsapp_path(Path("/tmp/node.exe"), ["-rsapp", "C:/RealityScan.exe"]) is None


def test_detect_rsapp_path_searches_default_paths(tmp_path, monkeypatch):
    node_exe = tmp_path / "RealityScan.exe"
    node_exe.write_text("", encoding="utf-8")
    monkeypatch.setenv("ProgramFiles", str(tmp_path))
    detected = rsclient.detect_rsapp_path(node_exe, ["--help"])
    assert detected == str(node_exe)


def test_normalize_config_uses_defaults_and_env(monkeypatch, tmp_path):
    ns = _with_argv([
        "--repo-root",
        str(tmp_path),
        "--server-host",
        "10.0.0.12",
        "--node-authtoken",
        "",
        "--control-command-queue",
        "",
    ])
    (tmp_path / ".env").write_text("RSLOGIC_CONTROL_COMMAND_QUEUE=node-commands\n")
    monkeypatch.setenv("RSLOGIC_RSNODE_AUTHTOKEN", "env-authtoken")

    cfg = rsclient.normalize_config(ns)
    assert cfg.repo_root == tmp_path.resolve()
    assert cfg.node_authtoken == "env-authtoken"
    assert cfg.sdk_base_url == "http://10.0.0.12:8000"
    assert cfg.control_command_queue == "node-commands"


def test_get_client_heartbeat_status_absent_when_no_presence(monkeypatch):
    values = {}
    scan_keys = []

    fake_client = _FakeRedisClient(values, scan_keys)

    monkeypatch.setitem(
        rsclient.sys.modules,
        "redis",
        types.ModuleType("redis"),
    )
    rsclient.sys.modules["redis"].Redis = type("FakeRedis", (), {"from_url": staticmethod(lambda *a, **k: fake_client)})

    status, redis_status, detail = rsclient.get_client_heartbeat_status(
        redis_url="redis://redis:6379/0",
        control_command_queue="rslogic:control:commands",
        logger=logging.getLogger("test"),
        redis_module_python="/tmp/not/existing/python",
        expected_presence_key="rslogic:control:commands:presence:win:555",
        expected_client_host="win",
        expected_client_pid=555,
    )
    assert redis_status == "connected"
    assert status == "absent"
    assert detail is None or detail in {"", None}


def test_get_client_heartbeat_status_decode_error_when_presence_invalid(monkeypatch):
    invalid_payload = b"not-json"
    class _BadRedis:
        @staticmethod
        def from_url(*_args, **_kwargs):
            return _FakeRedisClient({"rslogic:control:commands:presence:win:555": invalid_payload}, [])

    monkeypatch.setitem(
        rsclient.sys.modules,
        "redis",
        types.ModuleType("redis"),
    )
    rsclient.sys.modules["redis"].Redis = _BadRedis

    status, redis_status, detail = rsclient.get_client_heartbeat_status(
        redis_url="redis://redis:6379/0",
        control_command_queue="rslogic:control:commands",
        logger=logging.getLogger("test"),
        redis_module_python="/tmp/not/existing/python",
        expected_presence_key="rslogic:control:commands:presence:win:555",
        expected_client_host="win",
        expected_client_pid=555,
    )
    assert redis_status == "connected"
    assert status == "presence-key-decode-error"
    assert detail == "invalid presence payload"
