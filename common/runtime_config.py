"""运行时配置读取（README 3.6.2）。

微批大小、背压上限、保留天数这类"运维想随时调、又不想重启组件"的参数放
PG `runtime_config` 表里：sensor 每个 tick 重新读一遍，`UPDATE runtime_config
SET value = ...` 之后下一个 tick 即生效。

模块自带幂等 DDL + 默认值播种：老部署（postgres 卷
已初始化过、不会重跑 init 脚本）第一次 import 时也能拿到这张表。
"""
from __future__ import annotations

import threading

from common.db import execute, fetch_one

# 与 schemas/postgres_platform.sql 保持一致
_DDL = """
CREATE TABLE IF NOT EXISTS runtime_config (
    key                 TEXT PRIMARY KEY,
    value               TEXT NOT NULL,
    description         TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# 也是 getter 在数据库不可用/键缺失时由调用方传入的文档化默认值。
DEFAULTS: dict[str, str] = {
    "INGEST_BATCH_MAX": "200",
    "INGEST_MAX_INFLIGHT_BATCHES": "3",
    "RETENTION_DAYS": "30",
    # staging 交接区（run↔worker）残留文件的保留天数，retention job 按 mtime 清
    "STAGING_RETENTION_DAYS": "7",
    # 训练（README 3.7）：背压 / 质量门；worker 执行参数在不可变 Profile。
    "TRAIN_MAX_INFLIGHT": "2",
    "TRAIN_GATE_MIN_ACCURACY": "0.6",
}

_ddl_lock = threading.Lock()
_ddl_done = False


def _ensure_table() -> None:
    global _ddl_done
    if _ddl_done:
        return
    with _ddl_lock:
        if not _ddl_done:
            execute(_DDL)
            for key, value in DEFAULTS.items():
                execute(
                    "INSERT INTO runtime_config (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                    (key, value),
                )
            _ddl_done = True


def get_str(key: str, default: str) -> str:
    _ensure_table()
    row = fetch_one("SELECT value FROM runtime_config WHERE key = %s", (key,))
    return row["value"] if row else default


def get_int(key: str, default: int) -> int:
    raw = get_str(key, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def get_float(key: str, default: float) -> float:
    raw = get_str(key, str(default))
    try:
        return float(raw)
    except ValueError:
        return default
