"""运行时配置读取（README 3.6.2）。

微批大小、背压上限、保留天数这类"运维想随时调、又不想重启组件"的参数放
PG `runtime_config` 表里：sensor 每个 tick 重新读一遍，`UPDATE runtime_config
SET value = ...` 之后下一个 tick 即生效。

和 `common/saga.py` 一样，模块自带幂等 DDL + 默认值播种：老部署（postgres 卷
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

DEFAULTS: dict[str, str] = {
    "INGEST_BATCH_MAX": "200",
    "INGEST_MAX_INFLIGHT_BATCHES": "3",
    "RETENTION_DAYS": "30",
    # pod fan-out（README 3.6.3）：单个解析 worker 的硬超时 & 同时在跑的 worker 数上限
    "INGEST_WORKER_TIMEOUT_SECONDS": "600",
    "INGEST_WORKER_MAX_PARALLEL": "20",
    # worker 内存 limit 按 saga attempt 升档（docs/pod-fanout-guide.md 错误处理）：
    # 第 1 次用第 1 档，OOM 后自动重试时第 2 次用第 2 档……到顶仍 OOM 转人工
    "INGEST_WORKER_MEMORY_TIERS": "1Gi,2Gi,4Gi",
    # worker 流式解析的分块行数（bronze/silver 每攒这么多行 flush 一个 row group）
    "INGEST_WORKER_CHUNK_ROWS": "50000",
    # failed 会话按 error_code 自动重试前的退避分钟数（stuck sensor 第 3 类修复）
    "INGEST_RETRY_BACKOFF_MINUTES": "5",
    # staging 交接区（run↔worker）残留文件的保留天数，retention job 按 mtime 清
    "STAGING_RETENTION_DAYS": "7",
    # 训练（README 3.7）：背压 / worker 超时与内存档 / 重试退避 / 质量门
    "TRAIN_MAX_INFLIGHT": "2",
    "TRAIN_WORKER_TIMEOUT_SECONDS": "1800",
    "TRAIN_WORKER_MEMORY_TIERS": "1Gi,2Gi,4Gi",
    "TRAIN_RETRY_BACKOFF_MINUTES": "5",
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
