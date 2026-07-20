"""分析类资产 `analytics_summary`（README 3.2.5）：DuckDB 直接对着
episode / sample / dataset 的 Arrow 快照跑聚合查询，验证"自助数据探索"
（不用工程团队写脚本，科研人员自己就能查）这条能力。

每个 metric 用确定性 `summary_id = f"{scope}:{metric_name}"` upsert，
这样这张表始终是"当前值"快照，而不是无限增长的日志表，配合 Dagster
Declarative Automation 在上游变化时自动刷新（README 4.4）。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from common.audit import make_batch_id
from common.iceberg import upsert, with_audit_columns
from common.ingest_state import uncommitted_episode_ids
from engines.duckdb.duckdb_conn import iceberg_arrow, query
from schemas.iceberg_tables import ANALYTICS_SUMMARY


def run(*, run_id: str) -> dict:
    episodes = iceberg_arrow("episode")
    samples = iceberg_arrow("sample")
    datasets = iceberg_arrow("dataset")

    metrics: list[dict] = []
    now = datetime.now(timezone.utc)

    # 读侧过滤（docs/saga-consistency-guide.md）：未到 done 终态的批次不计入指标，
    # 避免半成品数据把 total/avg 拉偏。
    excluded = pa.table({"episode_id": pa.array(uncommitted_episode_ids(), type=pa.string())})

    if episodes.num_rows:
        df = query(
            "SELECT COUNT(*) AS n FROM episodes WHERE episode_id NOT IN (SELECT episode_id FROM excluded_episodes)",
            episodes=episodes,
            excluded_episodes=excluded,
        )
        metrics.append(("episode", "total_episodes", float(df["n"][0])))

    if samples.num_rows:
        df = query(
            """
            SELECT COUNT(*) AS n, AVG(quality_score) AS avg_q FROM samples
            WHERE episode_id NOT IN (SELECT episode_id FROM excluded_episodes)
            """,
            samples=samples,
            excluded_episodes=excluded,
        )
        metrics.append(("sample", "total_samples", float(df["n"][0])))
        metrics.append(("sample", "avg_quality_score", float(df["avg_q"][0] or 0.0)))

    if datasets.num_rows:
        df = query("SELECT COUNT(*) AS n FROM datasets WHERE state = 'RELEASED'", datasets=datasets)
        metrics.append(("dataset", "released_datasets", float(df["n"][0])))

    if not metrics:
        return {"num_metrics": 0}

    batch_id = make_batch_id(robot_id="analytics", upload_id=run_id)
    rows = [
        {
            "summary_id": f"{scope}:{name}",
            "scope": scope,
            "metric_name": name,
            "metric_value": value,
            "computed_at": now,
        }
        for scope, name, value in metrics
    ]
    tbl = pa.Table.from_pylist(rows)
    tbl = with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri="asset:analytics_summary")
    upsert(ANALYTICS_SUMMARY, tbl, join_cols=["summary_id"])

    return {"num_metrics": len(rows), "metrics": {f"{s}:{n}": v for s, n, v in metrics}}
