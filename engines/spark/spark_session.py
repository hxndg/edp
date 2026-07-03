"""Spark local mode + Iceberg REST Catalog 的统一入口（README 2.4 / 4.2）。

所有 Spark 作业都必须通过 `build_spark_session()` 拿 session，不要各自拼 config——
Iceberg catalog、S3 endpoint 这些连接参数只在这里配一次。
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from common.config import settings

ICEBERG_VERSION = "1.6.1"
SPARK_ICEBERG_ARTIFACT = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
ICEBERG_AWS_BUNDLE = f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"

CATALOG = settings.iceberg_catalog_name


def build_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", f"{SPARK_ICEBERG_ARTIFACT},{ICEBERG_AWS_BUNDLE}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", settings.iceberg_rest_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", settings.iceberg_warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", settings.minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", settings.minio_root_user)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", settings.minio_root_password)
        # MinIO 不校验 region，但 AWS SDK v2 的 region provider chain 找不到值就直接
        # 抛异常（不会静默给默认值），必须显式填一个占位 region 才能过 SdkClientException。
        .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
    )
    return builder.getOrCreate()


def qualified(table_name: str) -> str:
    from common.iceberg import NAMESPACE

    return f"{CATALOG}.{NAMESPACE}.{table_name}"
