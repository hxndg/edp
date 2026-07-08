"""规则打标签（README 3.1.7 stage=`entity_tag`）：小批量、秒起秒停，DuckDB 执行。

`rules_default` / `rules_strict` 是策略注册表里登记的两个策略，函数签名统一：
`(samples: pandas.DataFrame) -> list[dict]`，返回值是待写入 `entity_tag` 的行。
这两个函数本身故意写得很朴素——MVP 要证明的是"换算法只改配置"这件事本身，
不是规则打标签这个业务逻辑有多复杂。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

TAG_KEY = "quality_band"


def _band(score: float, high: float, mid: float) -> str:
    if score >= high:
        return "high"
    if score >= mid:
        return "medium"
    return "low"


def rules_default(samples: pd.DataFrame) -> list[dict]:
    now = datetime.now(timezone.utc)
    return [
        {
            "target_type": "sample",
            "target_id": row.sample_id,
            "tag_key": TAG_KEY,
            "tag_value": _band(row.quality_score, high=0.8, mid=0.5),
            "source": "rule",
            "tagged_by": "entity_tag:default",
            "tagged_at": now,
            "robot_id": row.robot_id,
        }
        for row in samples.itertuples()
    ]


def rules_strict(samples: pd.DataFrame) -> list[dict]:
    """备用策略：阈值更严格，供某科研团队按自己的质量口径试验（README 3.1.7 示例）。"""
    now = datetime.now(timezone.utc)
    return [
        {
            "target_type": "sample",
            "target_id": row.sample_id,
            "tag_key": TAG_KEY,
            "tag_value": _band(row.quality_score, high=0.9, mid=0.7),
            "source": "rule",
            "tagged_by": "entity_tag:strict",
            "tagged_at": now,
            "robot_id": row.robot_id,
        }
        for row in samples.itertuples()
    ]


def run(*, run_id: str, strategy_id: str | None = None) -> dict:
    import pyarrow as pa

    from common.audit import make_batch_id
    from common.iceberg import with_audit_columns, upsert
    from common.saga import uncommitted_episode_ids
    from common.strategy_registry import run_strategy
    from engines.duckdb.duckdb_conn import iceberg_arrow, query
    from schemas.iceberg_tables import ENTITY_TAG

    samples_arrow = iceberg_arrow("sample")
    if samples_arrow.num_rows == 0:
        return {"num_tags": 0, "strategy_id": None}

    # 读侧过滤（docs/saga-consistency-guide.md）：ingest saga 还没走到终态 done 的
    # 批次，其 sample 行可能是半成品/失败残留/正在被 correct 重写，先隔离不消费。
    excluded = pa.table({"episode_id": pa.array(uncommitted_episode_ids(), type=pa.string())})
    samples_df = query(
        """
        SELECT sample_id, robot_id, quality_score FROM samples
        WHERE sample_id IS NOT NULL
          AND episode_id NOT IN (SELECT episode_id FROM excluded_episodes)
        """,
        samples=samples_arrow,
        excluded_episodes=excluded,
    )

    strategy, tag_rows = run_strategy("entity_tag", strategy_id, samples_df)
    if not tag_rows:
        return {"num_tags": 0, "strategy_id": strategy.strategy_id}

    batch_id = make_batch_id(robot_id="entity_tag", upload_id=run_id)
    tbl = pa.Table.from_pylist(tag_rows)
    tbl = with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri="asset:entity_tag")
    upsert(ENTITY_TAG, tbl, join_cols=["target_type", "target_id", "tag_key"])

    return {"num_tags": len(tag_rows), "strategy_id": strategy.strategy_id}
