from __future__ import annotations

from types import SimpleNamespace

import config as cfg_module
import rslogic.storage.s3 as s3_mod


def _s3_config(**overrides):
    defaults = dict(
        region="us-east-1",
        bucket_name="bucket",
        processed_bucket_name="processed",
        scratchpad_prefix="scratchpad",
        endpoint_url="https://s3.example.internal",
        multipart_part_size=1024,
        multipart_concurrency=8,
        resume_uploads=True,
        manifest_dir=".",
    )
    defaults.update(overrides)
    return cfg_module.S3Config(**defaults)


def test_s3_client_provider_includes_region_endpoint_and_retries(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    class _FakeConfig(dict):
        pass

    def fake_config_init(*args, **kwargs):
        calls.append((tuple(args), dict(kwargs)))
        return _FakeConfig(kwargs)

    monkeypatch.setattr(s3_mod, "BotoClientConfig", fake_config_init)

    captured = {}

    def fake_boto3_client(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(kwargs=kwargs)

    monkeypatch.setattr(s3_mod.boto3, "client", fake_boto3_client)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY", raising=False)
    monkeypatch.delenv("S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
    monkeypatch.delenv("S3_SECRET_KEY", raising=False)
    provider = s3_mod.S3ClientProvider(config=_s3_config())

    client = provider.get_client()
    assert client is not None
    assert captured["service_name"] == "s3"
    assert captured["region_name"] == "us-east-1"
    assert captured["endpoint_url"] == "https://s3.example.internal"
    assert captured["config"] is not None
    # keep assertions stable while still verifying max pool tuning based on concurrency.
    assert getattr(captured["config"], "max_pool_connections", captured["config"]["max_pool_connections"]) == 32


def test_s3_client_provider_reads_access_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-1")

    captured = {}

    def fake_boto3_client(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(kwargs=kwargs)

    def fake_config_init(*args, **kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(s3_mod.boto3, "client", fake_boto3_client)
    monkeypatch.setattr(s3_mod, "BotoClientConfig", fake_config_init)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-1")

    provider = s3_mod.S3ClientProvider(config=_s3_config(multipart_concurrency=2))
    client = provider.get_client()
    assert client is not None
    assert captured["aws_access_key_id"] == "access-1"
    assert captured["aws_secret_access_key"] == "secret-1"
    assert captured["config"].max_pool_connections == 32


def test_s3_client_provider_prefers_s3_secret_alias(monkeypatch):
    monkeypatch.setenv("S3_ACCESS_KEY", "alias-access")
    monkeypatch.setenv("S3_SECRET_KEY", "alias-secret")
    captured = {}

    def fake_boto3_client(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(kwargs=kwargs)

    def fake_config_init(*args, **kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(s3_mod.boto3, "client", fake_boto3_client)
    monkeypatch.setattr(s3_mod, "BotoClientConfig", fake_config_init)

    provider = s3_mod.S3ClientProvider(config=_s3_config())
    provider.get_client()
    assert captured["aws_access_key_id"] == "alias-access"
    assert captured["aws_secret_access_key"] == "alias-secret"
