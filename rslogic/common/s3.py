"""S3 helper wrappers."""

from __future__ import annotations

import os
from typing import Any, Iterable

import boto3
from botocore.client import BaseClient


def make_client(endpoint_url: str | None = None, region_name: str | None = None, profile: str | None = None) -> BaseClient:
    session = boto3.session.Session(profile_name=profile)
    access_key = os.getenv("S3_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def s3_object_keys(s3_client: BaseClient, bucket: str, prefix: str | None = None) -> Iterable[dict[str, Any]]:
    token = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": 1000}
        if prefix:
            kwargs["Prefix"] = prefix
        if token:
            kwargs["ContinuationToken"] = token
        response = s3_client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            yield item
        if not response.get("IsTruncated"):
            return
        token = response.get("NextContinuationToken")


def copy_object(s3_client: BaseClient, source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None:
    s3_client.copy_object(
        Bucket=target_bucket,
        Key=target_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
        MetadataDirective="COPY",
    )


def move_object(s3_client: BaseClient, source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None:
    copy_object(s3_client, source_bucket, source_key, target_bucket, target_key)
    s3_client.delete_object(Bucket=source_bucket, Key=source_key)
