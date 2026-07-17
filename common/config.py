"""所有组件共用的配置读取入口。

只读环境变量（配合 .env），不做业务逻辑。任何组件（gateway / orchestration /
engines / tools）都应该 `from common.config import settings` 而不是自己读 os.environ，
避免同一个变量在不同地方有不同默认值。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

# 允许在仓库根目录放一份 .env，本地跑脚本时自动加载；容器里通常直接由
# docker-compose 的 environment 注入，load_dotenv 找不到文件时是no-op。
load_dotenv()


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Settings:
    # MinIO
    minio_endpoint: str = field(default_factory=lambda: _env("MINIO_ENDPOINT", "http://localhost:9000"))
    minio_root_user: str = field(default_factory=lambda: _env("MINIO_ROOT_USER", "minioadmin"))
    minio_root_password: str = field(default_factory=lambda: _env("MINIO_ROOT_PASSWORD", "minioadmin123"))
    minio_bucket: str = field(default_factory=lambda: _env("MINIO_BUCKET", "lake"))

    # Lance（样本本体，README 组件清单：本地/MinIO 后端二选一，MVP 默认本地磁盘）
    lance_root: str = field(default_factory=lambda: _env("LANCE_ROOT", "./data/lance"))

    # Postgres
    postgres_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    postgres_port: int = field(default_factory=lambda: int(_env("POSTGRES_PORT", "5432")))
    postgres_user: str = field(default_factory=lambda: _env("POSTGRES_USER", "edp"))
    postgres_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "edp123"))
    postgres_dagster_db: str = field(default_factory=lambda: _env("POSTGRES_DAGSTER_DB", "dagster"))
    postgres_platform_db: str = field(default_factory=lambda: _env("POSTGRES_PLATFORM_DB", "platform"))

    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9094"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "edp.events"))
    # ingest 触发专用 topic（区别于 kafka_topic 账本）：gateway 提交 manifest 后
    # 发一条 ingest.requested，Dagster 的 kafka sensor 消费它拉起 run
    kafka_ingest_topic: str = field(default_factory=lambda: _env("KAFKA_INGEST_TOPIC", "edp.ingest.requests"))
    # 通用任务触发 topic（README 3.7.4）：platform_job（training 等）走这条
    kafka_jobs_topic: str = field(default_factory=lambda: _env("KAFKA_JOBS_TOPIC", "edp.jobs.requests"))

    # Iceberg REST catalog
    iceberg_rest_uri: str = field(default_factory=lambda: _env("ICEBERG_REST_URI", "http://localhost:8181"))
    iceberg_warehouse: str = field(default_factory=lambda: _env("ICEBERG_WAREHOUSE", "s3://lake/warehouse"))
    iceberg_catalog_name: str = field(default_factory=lambda: _env("ICEBERG_CATALOG_NAME", "edp"))

    # OpenSearch（tag 检索投影，README 3.5；SoT 在 Iceberg，索引可整体重建）
    opensearch_url: str = field(default_factory=lambda: _env("OPENSEARCH_URL", "http://localhost:9200"))

    # MLflow（README 3.7：实验记录 + Model Registry 操作台，不是 SoT）
    mlflow_tracking_uri: str = field(default_factory=lambda: _env("MLFLOW_TRACKING_URI", "http://localhost:5000"))

    # Gateway / Dagster
    gateway_host: str = field(default_factory=lambda: _env("GATEWAY_HOST", "0.0.0.0"))
    gateway_port: int = field(default_factory=lambda: int(_env("GATEWAY_PORT", "8000")))
    dagster_host: str = field(default_factory=lambda: _env("DAGSTER_HOST", "localhost"))
    dagster_port: int = field(default_factory=lambda: int(_env("DAGSTER_PORT", "3000")))

    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    # Saga（docs/saga-consistency-guide.md）：RUNNING 状态多久没有心跳（advance）
    # 就允许被新的 run 接管 / 被 stuck sensor 重新入队
    saga_takeover_minutes: int = field(default_factory=lambda: int(_env("SAGA_TAKEOVER_MINUTES", "30")))
    # stuck 自动重试上限：超过后不再自动入队，转为 failed + alert 等人工介入
    saga_max_attempts: int = field(default_factory=lambda: int(_env("SAGA_MAX_ATTEMPTS", "3")))

    # pod fan-out（README 3.6.3）：run pod 给批内每个 upload 起 worker Job 用的镜像
    # 与命名空间。镜像默认复用 code location 注入的 DAGSTER_CURRENT_IMAGE——
    # "worker 跑的代码 == 编排看到的代码"，与 run pod 同源。
    edp_image: str = field(
        default_factory=lambda: _env("EDP_IMAGE", os.environ.get("DAGSTER_CURRENT_IMAGE", "edp:dev"))
    )
    k8s_namespace: str = field(default_factory=lambda: _env("K8S_NAMESPACE", "data"))

    @property
    def platform_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_platform_db}"
        )

    def s3_client_kwargs(self) -> dict:
        return dict(
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.minio_root_user,
            aws_secret_access_key=self.minio_root_password,
        )


settings = Settings()
