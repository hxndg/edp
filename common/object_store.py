"""MinIO（S3 兼容对象存储）帮助函数。

Bucket 内用前缀模拟 README 2.3 里的分区：raw/ bronze/ silver/ gold/
dataset/ artifact/ quarantine/ annotation_packages/。MVP 用单 bucket +
前缀，生产化时可以按前缀拆成独立 bucket，不影响上层调用方式。
"""
from __future__ import annotations

import functools

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from common.config import settings

PREFIX_RAW = "raw"
PREFIX_BRONZE = "bronze"
PREFIX_SILVER = "silver"
PREFIX_GOLD = "gold"
PREFIX_DATASET = "dataset"
PREFIX_ARTIFACT = "artifact"
PREFIX_QUARANTINE = "quarantine"
PREFIX_ANNOTATION_PKG = "annotation_packages"


@functools.lru_cache(maxsize=1)
def client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(bucket: str | None = None) -> None:
    bucket = bucket or settings.minio_bucket
    c = client()
    try:
        c.head_bucket(Bucket=bucket)
    except ClientError:
        c.create_bucket(Bucket=bucket)


def object_uri(key: str, bucket: str | None = None) -> str:
    bucket = bucket or settings.minio_bucket
    return f"s3://{bucket}/{key}"


def presigned_put_url(key: str, *, bucket: str | None = None, expires_in: int = 3600) -> str:
    bucket = bucket or settings.minio_bucket
    ensure_bucket(bucket)
    return client().generate_presigned_url(
        "put_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires_in
    )


def presigned_get_url(key: str, *, bucket: str | None = None, expires_in: int = 3600) -> str:
    bucket = bucket or settings.minio_bucket
    return client().generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires_in
    )


def put_bytes(key: str, data: bytes, *, bucket: str | None = None) -> str:
    bucket = bucket or settings.minio_bucket
    ensure_bucket(bucket)
    client().put_object(Bucket=bucket, Key=key, Body=data)
    return object_uri(key, bucket)


def put_file(key: str, local_path: str, *, bucket: str | None = None) -> str:
    bucket = bucket or settings.minio_bucket
    ensure_bucket(bucket)
    client().upload_file(local_path, bucket, key)
    return object_uri(key, bucket)


def get_bytes(key: str, *, bucket: str | None = None) -> bytes:
    bucket = bucket or settings.minio_bucket
    resp = client().get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def list_prefix(prefix: str, *, bucket: str | None = None) -> list[str]:
    bucket = bucket or settings.minio_bucket
    paginator = client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys
