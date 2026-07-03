"""自动质检（mock，README 2.4 / 3.2.2）：对已经"转正"的标注跑一次质检，
产出 `qc_result`。`qc_default` 是策略注册表 `qc` stage 的默认策略。
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

import pyarrow as pa
import ray

from common.audit import make_batch_id
from common.iceberg import in_filter, load_table, with_audit_columns
from engines.ray.ray_utils import ensure_ray


@ray.remote
def _qc_one(target_id: str, confidence: float | None) -> dict:
    rng = random.Random(hash(target_id) & 0xFFFFFFFF)
    base = confidence if confidence is not None else 0.8
    score = min(1.0, max(0.0, base + rng.uniform(-0.1, 0.1)))
    verdict = "pass" if score >= 0.6 else ("need_review" if score >= 0.4 else "fail")
    return {"target_id": target_id, "score": round(score, 4), "verdict": verdict}


def qc_default(target_ids: list[str], *, run_id: str) -> list[dict]:
    if not target_ids:
        return []
    ensure_ray()
    annos = load_table("annotation").scan(row_filter=in_filter("target_id", target_ids)).to_arrow().to_pylist()
    confidence_by_target = {a["target_id"]: a.get("confidence") for a in annos}

    futures = [_qc_one.remote(tid, confidence_by_target.get(tid)) for tid in target_ids]
    results = ray.get(futures)

    now = datetime.now(timezone.utc)
    rows = [
        {
            "qc_id": f"{r['target_id']}-qc",
            "target_id": r["target_id"],
            "check_type": "annotation",
            "verdict": r["verdict"],
            "score": r["score"],
            "checked_by": "auto",
        }
        for r in results
    ]
    return rows


def run(target_ids: list[str], *, run_id: str, strategy_id: str | None = None) -> dict:
    from common.strategy_registry import run_strategy
    from schemas.iceberg_tables import QC_RESULT

    strategy, rows = run_strategy("qc", strategy_id, target_ids, run_id=run_id)
    if rows:
        batch_id = make_batch_id(robot_id="qc", upload_id=run_id)
        tbl = pa.Table.from_pylist(rows)
        tbl = with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri="asset:qc_result")
        from common.iceberg import upsert

        upsert(QC_RESULT, tbl, join_cols=["qc_id"])
    return {"num_qc": len(rows), "strategy_id": strategy.strategy_id}
