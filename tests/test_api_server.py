from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")

import rslogic.api.server as server


def test_api_server_main_invokes_uvicorn_with_config(monkeypatch):
    captured = {}

    class _FakeConfig:
        log = SimpleNamespace(level="INFO", format="%(message)s")

    monkeypatch.setattr(server, "load_config", lambda: _FakeConfig())

    def fake_run(app, host, port, reload, log_level, access_log):
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port
        captured["reload"] = reload
        captured["log_level"] = log_level
        captured["access_log"] = access_log

    monkeypatch.setattr(server.uvicorn, "run", fake_run)
    server.main(host="127.0.0.1", port=8111)

    assert captured["app"] == "rslogic.api.app:app"
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8111
    assert captured["reload"] is False
    assert captured["access_log"] is True
