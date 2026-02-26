"""S3 client helpers."""

from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from typing import Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config as BotoClientConfig

from config import S3Config, load_config


@dataclass(frozen=True)
class S3ClientProvider:
    """Build reusable boto3 clients using project config by default."""

    config: Optional[S3Config] = None

    def _config(self) -> S3Config:
        return self.config or load_config().s3

    def get_client(self) -> BaseClient:
        cfg = self._config()
        aws_access_key_id = (
            getenv("AWS_ACCESS_KEY_ID")
            or getenv("AWS_ACCESS_KEY")
            or getenv("S3_ACCESS_KEY")
        )
        aws_secret_access_key = (
            getenv("AWS_SECRET_ACCESS_KEY")
            or getenv("AWS_SECRET_KEY")
            or getenv("AWS_SECRET_ACCESS_KEY_ID")
            or getenv("S3_SECRET_KEY")
        )
        client_kwargs = {
            "service_name": "s3",
            "region_name": cfg.region,
            "endpoint_url": cfg.endpoint_url,
        }
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
        client_kwargs["config"] = BotoClientConfig(
            max_pool_connections=max(cfg.multipart_concurrency * 4, 32),
            connect_timeout=10,
            read_timeout=120,
            retries={"max_attempts": 8, "mode": "adaptive"},
        )

        return boto3.client(
            **client_kwargs,
        )
