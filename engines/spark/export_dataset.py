"""`export_dataset` 的核心逻辑（README 3.2.3）：把冻结的 Dataset 清单物化成
可被 dataloader 消费的 shard（MVP 只做一种格式：JSONL，一行一个 sample）。

`export_default` 是策略注册表 `export` stage 的默认策略（README 3.1.2.2），
入口签名统一为 `(dataset_name, dataset_version) -> dict`，方便未来新增
`tfrecord`/`webdataset` 等格式时只加一行配置，不改 Dagster 资产图。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pyarrow as pa
from pyspark.sql import functions as F

from common import object_store
from common.audit import make_batch_id
from common.iceberg import append, with_audit_columns
from engines.spark.spark_session import build_spark_session, qualified
from schemas.iceberg_tables import DATASET_EXPORT

SHARD_SIZE = 500


def export_default(dataset_name: str, dataset_version: str, *, run_id: str) -> dict:
    spark = build_spark_session("export_dataset")
    ds_samples = spark.table(qualified("dataset_sample")).filter(
        (F.col("dataset_name") == dataset_name) & (F.col("dataset_version") == dataset_version)
    )
    samples = spark.table(qualified("sample")).select("sample_id", "episode_id", "lance_uri", "quality_score")
    joined = ds_samples.join(samples, "sample_id").select(
        "sample_id", "episode_id", "lance_uri", "quality_score", "split"
    )
    rows = [r.asDict() for r in joined.collect()]
    if not rows:
        raise ValueError(f"dataset {dataset_name}/{dataset_version} 没有任何样本，无法导出")

    shards: list[list[dict]] = [rows[i : i + SHARD_SIZE] for i in range(0, len(rows), SHARD_SIZE)]
    shard_uris = []
    for i, shard_rows in enumerate(shards):
        body = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in shard_rows).encode("utf-8")
        key = f"{object_store.PREFIX_DATASET}/{dataset_name}/{dataset_version}/shard-{i:05d}.jsonl"
        uri = object_store.put_bytes(key, body)
        shard_uris.append(uri)

    overall_hash = hashlib.sha256(",".join(sorted(r["sample_id"] for r in rows)).encode()).hexdigest()[:16]
    batch_id = make_batch_id(robot_id="export", upload_id=dataset_version)

    export_rows = pa.Table.from_pylist(
        [
            {
                "dataset_version": dataset_version,
                "format": "jsonl",
                "shard_uri": uri,
                "num_shards": len(shard_uris),
                "hash": overall_hash,
            }
            for uri in shard_uris
        ]
    )
    export_rows = with_audit_columns(export_rows, batch_id=batch_id, run_id=run_id, source_uri=f"dataset:{dataset_name}/{dataset_version}")
    append(DATASET_EXPORT, export_rows)

    return {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "num_samples": len(rows),
        "num_shards": len(shard_uris),
        "shard_uris": shard_uris,
        "hash": overall_hash,
    }


def run(dataset_name: str, dataset_version: str, *, run_id: str, strategy_id: str | None = None) -> tuple[str, dict]:
    """走策略注册表解析 export stage（README 4.3），返回 (实际用的 strategy_id, 结果)。"""
    from common.strategy_registry import run_strategy

    strategy, result = run_strategy("export", strategy_id, dataset_name, dataset_version, run_id=run_id)
    return strategy.strategy_id, result
