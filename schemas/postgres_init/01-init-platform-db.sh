#!/usr/bin/env bash
# docker-entrypoint-initdb.d 钩子：postgres 容器首次启动、`dagster` 主库建好后，
# 额外创建 `platform` 库并灌入 platform 的 DDL（README 3.1.2）。
# Dagster 自身的元数据表由 dagster-postgres 在应用层自动建表，这里不需要手动建。
set -euo pipefail

: "${POSTGRES_PLATFORM_DB:=platform}"

echo "[init] creating database '${POSTGRES_PLATFORM_DB}' owned by ${POSTGRES_USER}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c "CREATE DATABASE ${POSTGRES_PLATFORM_DB} OWNER ${POSTGRES_USER};"

echo "[init] applying platform DDL"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_PLATFORM_DB" \
  -f /docker-entrypoint-initdb.d/postgres_platform.sql
