from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from rslogic.jobs import runners
from tests.conftest import build_test_app_config


def test_sdk_runner_stage_only_skips_rstool_commands(monkeypatch):
    monkeypatch.setattr(runners, "RealityScanClient", object())

    runner = runners.RsToolsSdkRunner(
        base_url="http://127.0.0.1:8000",
        client_id="client-id",
        app_token="app-token",
        auth_token="auth-token",
    )

    observed: list[tuple[float, str, dict]] = []

    def progress(progress: float, message: str, details: dict | None) -> None:
        observed.append((progress, message, details or {}))

    result = runner.run(
        working_directory=Path("/tmp/stage-only"),
        image_keys=["a.jpg", "b.jpg"],
        filters={"stage_only": True, "sdk_imagery_folder": "Imagery"},
        progress_callback=progress,
        job_id="job-stage-only",
    )

    assert result["status"] == "staged"
    assert result["job_id"] == "job-stage-only"
    assert result["selected_images"] == 2
    assert result["filters"]["stage_only"] is True
    assert any(payload.get("stage") == "stage_only_complete" for _, __, payload in observed)


def test_sdk_runner_rejects_all_disabled_stages_without_stage_only(monkeypatch):
    monkeypatch.setattr(runners, "RealityScanClient", object())

    runner = runners.RsToolsSdkRunner(
        base_url="http://127.0.0.1:8000",
        client_id="client-id",
        app_token="app-token",
        auth_token="auth-token",
    )

    with pytest.raises(RuntimeError, match="At least one SDK processing stage"):
        runner.run(
            working_directory=Path("/tmp/no-stages"),
            image_keys=[],
            filters={
                "sdk_run_align": False,
                "sdk_run_normal_model": False,
                "sdk_run_ortho_projection": False,
                "sdk_imagery_folder": "Imagery",
            },
            job_id="job-no-stages",
        )


def test_sdk_helpers_coerce_types():
    assert runners.RsToolsSdkRunner._as_bool("yes", default=False) is True
    assert runners.RsToolsSdkRunner._as_bool("0", default=True) is False
    assert runners.RsToolsSdkRunner._as_bool(None, default=True) is True

    assert runners.RsToolsSdkRunner._as_int("7", default=5) == 7
    assert runners.RsToolsSdkRunner._as_int("-7", default=5) == 5
    assert runners.RsToolsSdkRunner._as_int("bad", default=5) == 5

    assert runners.RsToolsSdkRunner._as_float("0.5", default=1.0) == 0.5
    assert runners.RsToolsSdkRunner._as_float("bad", default=1.0) == 1.0

    assert runners.RsToolsSdkRunner._as_str("  value ", default="x") == "value"
    assert runners.RsToolsSdkRunner._as_str("   ", default="x") == "x"
    assert runners.RsToolsSdkRunner._as_str(None, default="x") == "x"
    assert runners.RsToolsSdkRunner._build_progress(-10.0) == 0.0
    assert runners.RsToolsSdkRunner._build_progress(200.0) == 100.0


def test_build_runner_from_config_defaults_to_stub():
    cfg = build_test_app_config()
    cfg = replace(cfg, rstools=replace(cfg.rstools, mode="stub", executable_path=None, sdk_base_url=None))
    runner = runners.build_runner_from_config(cfg.rstools)
    assert runner.__class__.__name__ == "StubRsToolsRunner"


def test_build_runner_from_config_subprocess_mode(monkeypatch):
    cfg = build_test_app_config()
    cfg = replace(cfg, rstools=replace(cfg.rstools, mode="cli", executable_path="/usr/bin/rstools"))
    runner = runners.build_runner_from_config(cfg.rstools)
    assert runner.__class__.__name__ == "SubprocessRsToolsRunner"


def test_build_runner_from_config_sdk_mode_requires_credentials():
    cfg = build_test_app_config()
    cfg = replace(cfg, rstools=replace(cfg.rstools, mode="sdk", sdk_base_url=None, sdk_client_id=None, sdk_app_token=None, sdk_auth_token=None))
    try:
        runners.build_runner_from_config(cfg.rstools)
    except RuntimeError as exc:
        assert "RealityScan SDK mode requires" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")


def test_build_runner_from_config_remote_mode_requires_runtime_config(monkeypatch):
    cfg = build_test_app_config()
    cfg = replace(
        cfg,
        rstools=replace(
            cfg.rstools,
            mode="remote",
            sdk_base_url="http://127.0.0.1:8000",
            sdk_client_id="client-id",
            sdk_app_token="app-token",
            sdk_auth_token="auth-token",
        ),
    )

    class _FakeBus:
        def __init__(self, *_, **__):
            pass

        def push(self, *_, **__):
            pass

        def pop(self, *_, **__):
            return None

        def close(self):
            pass

    monkeypatch.setattr(runners, "RedisCommandBus", _FakeBus)
    monkeypatch.setattr(runners, "load_config", lambda: cfg)
    runner = runners.build_runner_from_config(cfg.rstools)
    assert runner.__class__.__name__ == "RsToolsRemoteRunner"
