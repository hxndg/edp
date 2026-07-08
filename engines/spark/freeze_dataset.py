"""`freeze_dataset` 的核心逻辑（README 3.2.3）：按过滤条件 + 质量阈值挑样本，
生成不可变、带版本和 hash 的 Dataset。冻结前的质量门（非空/质量分/标注状态）
是硬性前置条件——不过就直接抛异常，不写 `dataset`/`dataset_sample`，
配合 `orchestration/checks.py` 里同名的 Asset Check 在 UI 上给出同样的结论。
"""
from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone

import pyarrow as pa
from pyspark.sql import functions as F

from common.audit import make_batch_id
from common.iceberg import with_audit_columns
from engines.spark.spark_session import build_spark_session, qualified
from schemas.iceberg_tables import DATASET, DATASET_SAMPLE


class FreezeGateError(Exception):
    """冻结前置质量门没过，携带具体指标供上层写进 alerts / asset check。"""

    def __init__(self, message: str, stats: dict):
        super().__init__(message)
        self.stats = stats


def compute_candidates(spark, filter_expr: dict, quality_threshold: float):
    samples = spark.table(qualified("sample")).filter(F.col("quality_score") >= F.lit(quality_threshold))

    for tag_key, tag_value in (filter_expr or {}).items():
        matching = (
            spark.table(qualified("entity_tag"))
            .filter((F.col("tag_key") == tag_key) & (F.col("tag_value") == str(tag_value)))
            .select(F.col("target_id").alias("sample_id"))
            .distinct()
        )
        samples = samples.join(matching, "sample_id", "left_semi")

    passed_annotations = (
        spark.table(qualified("annotation"))
        .filter((F.col("target_type") == "sample") & (F.col("review_status") == "passed"))
        .select(F.col("target_id").alias("sample_id"))
        .distinct()
    )
    passed_qc = (
        spark.table(qualified("qc_result"))
        .filter(F.col("verdict") == "pass")
        .select(F.col("target_id").alias("sample_id"))
        .distinct()
    )

    gated = samples.join(passed_annotations, "sample_id", "left_semi").join(passed_qc, "sample_id", "left_semi")
    return gated


def _quality_gate(candidates_pdf, quality_threshold: float) -> dict:
    n = len(candidates_pdf)
    mean_quality = float(candidates_pdf["quality_score"].mean()) if n else 0.0
    stats = {"num_samples": n, "mean_quality_score": mean_quality, "quality_threshold": quality_threshold}
    if n == 0:
        raise FreezeGateError("候选样本为空，Dataset freeze 被挡住", stats)
    if mean_quality < quality_threshold:
        raise FreezeGateError(
            f"候选样本平均质量分 {mean_quality:.3f} 低于阈值 {quality_threshold}", stats
        )
    return stats


def _assign_split(sample_ids: list[str], split: dict) -> dict[str, str]:
    split = split or {"train": 0.8, "val": 0.1, "test": 0.1}
    names, weights = zip(*split.items())
    rng = random.Random(42)  # 固定种子：同一批 sample_ids 重跑得到同样的 split，符合幂等原则
    return {sid: rng.choices(names, weights=weights)[0] for sid in sorted(sample_ids)}


def run(
    *,
    request_id: str,
    dataset_name: str,
    filter_expr: dict,
    quality_threshold: float,
    split: dict,
    run_id: str,
) -> dict:
    spark = build_spark_session("freeze_dataset")
    candidates = compute_candidates(spark, filter_expr, quality_threshold)
    candidates_pdf = candidates.select("sample_id", "quality_score").toPandas()

    stats = _quality_gate(candidates_pdf, quality_threshold)

    sample_ids = candidates_pdf["sample_id"].tolist()
    dataset_version = f"v{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    manifest_hash = hashlib.sha256(",".join(sorted(sample_ids)).encode()).hexdigest()[:16]
    splits = _assign_split(sample_ids, split)

    batch_id = make_batch_id(robot_id="freeze", upload_id=request_id)
    dataset_row = pa.Table.from_pylist(
        [
            {
                "dataset_name": dataset_name,
                "dataset_version": dataset_version,
                "manifest_hash": manifest_hash,
                "filter_expr_json": _to_json(filter_expr),
                "code_ver": "mvp-1",
                "state": "RELEASED",
            }
        ]
    )
    dataset_row = with_audit_columns(dataset_row, batch_id=batch_id, run_id=run_id, source_uri=f"dataset_request:{request_id}")

    dataset_sample_rows = pa.Table.from_pylist(
        [
            {"dataset_name": dataset_name, "dataset_version": dataset_version, "sample_id": sid, "split": splits[sid]}
            for sid in sample_ids
        ]
    )
    dataset_sample_rows = with_audit_columns(
        dataset_sample_rows, batch_id=batch_id, run_id=run_id, source_uri=f"dataset_request:{request_id}"
    )

    from common.iceberg import append

    # 写入顺序有讲究（docs/saga-consistency-guide.md）：先写明细 dataset_sample，
    # 最后写"头行" dataset（state=RELEASED 相当于本次冻结的 COMMIT 标记）。
    # 读者按"先查到 dataset 头行才去读明细"的协议消费，中途崩溃只会留下
    # 没有头行的孤儿明细（无害，重跑会生成新 version），不会出现
    # "头行已 RELEASED、明细缺失"的脏数据。
    append(DATASET_SAMPLE, dataset_sample_rows)
    append(DATASET, dataset_row)

    return {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "num_samples": len(sample_ids),
        **stats,
    }


def _to_json(value: dict) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, default=str)
