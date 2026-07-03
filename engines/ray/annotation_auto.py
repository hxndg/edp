"""`annotation_auto` 分支的核心逻辑（README 3.2.2）：`pipeline_profile=auto_only`
时，预标结果不派人工，直接按置信度阈值决定"转正"还是"留 pending 待复核"。

`promote_default` 是策略注册表 `annotation_promote` stage 的默认策略——
阈值本身是行为性的，未来想换个更保守/更激进的阈值，只加一行配置。
"""
from __future__ import annotations

import pyarrow as pa

from common.audit import make_batch_id
from common.db import execute, to_json
from common.iceberg import in_filter, load_table, upsert, with_audit_columns
from schemas.iceberg_tables import ANNOTATION

CONFIDENCE_THRESHOLD = 0.7


def promote_default(sample_ids: list[str]) -> list[dict]:
    rows = load_table("annotation").scan(row_filter=in_filter("target_id", sample_ids)).to_arrow().to_pylist()
    prelabels = [r for r in rows if r["source"] == "auto" and r["anno_id"].endswith("-prelabel")]

    updated = []
    for r in prelabels:
        r = dict(r)
        r["review_status"] = "passed" if (r.get("confidence") or 0.0) >= CONFIDENCE_THRESHOLD else "pending"
        updated.append(r)
    return updated


def run(sample_ids: list[str], *, run_id: str, strategy_id: str | None = None) -> dict:
    from common.strategy_registry import run_strategy

    strategy, rows = run_strategy("annotation_promote", strategy_id, sample_ids)
    if not rows:
        return {"num_promoted": 0, "num_low_confidence": 0, "strategy_id": strategy.strategy_id}

    batch_id = make_batch_id(robot_id="annotation_auto", upload_id=run_id)
    tbl = pa.Table.from_pylist(rows)
    tbl = with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri="asset:annotation_auto")
    upsert(ANNOTATION, tbl, join_cols=["anno_id"])

    low_confidence = [r for r in rows if r["review_status"] == "pending"]
    for r in low_confidence:
        execute(
            "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
            (
                "warning",
                "annotation_auto",
                run_id,
                f"sample {r['target_id']} 预标置信度不足（auto_only 分支不会派人工），需要人工关注",
                to_json({"sample_id": r["target_id"], "confidence": r.get("confidence")}),
            ),
        )

    return {
        "num_promoted": len(rows) - len(low_confidence),
        "num_low_confidence": len(low_confidence),
        "strategy_id": strategy.strategy_id,
    }
