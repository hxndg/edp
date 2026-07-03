"""自动预标（mock，README 2.4 / 3.2.2）：对新样本跑一个"假模型"，
占位真实 VLM/检测模型的接口位置。用 `@ray.remote` 表达"这是一个
GPU/模型形态的任务"这个接口契约，函数体本身只用规则 + 随机数。

写出来的是 `annotation` 表里 `source='auto'` 的草稿行（`review_status='pending'`），
下游 `annotation_auto` / `annotation_dispatch` 两个条件物化分支再决定
"直接转正" 还是 "派给人工"。
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

import pyarrow as pa
import ray

from common.audit import make_batch_id
from common.iceberg import in_filter, load_table, upsert, with_audit_columns
from engines.ray.ray_utils import ensure_ray
from schemas.iceberg_tables import ANNOTATION

CANNED_CAPTIONS = [
    "机器人沿直线路径移动，动作平稳",
    "检测到轻微振动，可能经过不平整地面",
    "采集片段包含一次转向动作",
    "静止片段，传感器读数平稳",
    "疑似碰撞或急停，建议人工复核",
]


@ray.remote
def _prelabel_one(sample_id: str, quality_score: float) -> dict:
    rng = random.Random(hash(sample_id) & 0xFFFFFFFF)
    confidence = min(1.0, max(0.0, quality_score + rng.uniform(-0.15, 0.15)))
    caption = rng.choice(CANNED_CAPTIONS)
    return {"sample_id": sample_id, "caption": caption, "confidence": round(confidence, 4)}


def run(sample_ids: list[str], *, run_id: str) -> dict:
    if not sample_ids:
        return {"num_prelabeled": 0}

    ensure_ray()
    samples = load_table("sample").scan(row_filter=in_filter("sample_id", sample_ids)).to_arrow().to_pylist()
    quality_by_id = {s["sample_id"]: s["quality_score"] or 0.0 for s in samples}

    futures = [_prelabel_one.remote(sid, quality_by_id.get(sid, 0.0)) for sid in sample_ids]
    results = ray.get(futures)

    now = datetime.now(timezone.utc)
    batch_id = make_batch_id(robot_id="prelabel", upload_id=run_id)
    rows = [
        {
            "anno_id": f"{r['sample_id']}-prelabel",
            "target_type": "sample",
            "target_id": r["sample_id"],
            "type": "lang",
            "value_or_uri": r["caption"],
            "source": "auto",
            "anno_version": "prelabel-v1",
            "review_status": "pending",
            "confidence": r["confidence"],
        }
        for r in results
    ]
    tbl = pa.Table.from_pylist(rows)
    tbl = with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri="asset:prelabel_annotation")
    upsert(ANNOTATION, tbl, join_cols=["anno_id"])

    return {"num_prelabeled": len(rows), "sample_ids": sample_ids}
