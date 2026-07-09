# EDP 统一镜像：Dagster webserver / daemon / code-location gRPC / run pod / gateway
# 全部共用这一个镜像（deploy/k8s 里通过不同的 command 区分角色），保证
# "编排看到的代码 == run pod 执行的代码"，不会出现镜像漂移。
FROM python:3.10-slim

# pyspark（freeze/export/compaction 作业）需要 JVM
ARG http_proxy
ARG https_proxy
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先只拷依赖清单再 sync，让依赖层可以被 docker 缓存复用
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev && rm -rf /root/.cache

COPY . .

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p /opt/dagster/dagster_home /data/lance
