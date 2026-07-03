"""训练/评测消费（mock，README 3.2.4）：不跑真训练，读导出包、跑几秒、
产出 `model_artifact`，验证消费侧接口契约。
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pyarrow as pa
import ray

from common import object_store
from common.audit import make_batch_id
from common.iceberg import append, in_filter, load_table, with_audit_columns
from engines.ray.ray_utils import ensure_ray
from engines.spark.ingest_common import new_id
from schemas.iceberg_tables import MODEL_ARTIFACT, TRAIN_RUN


@ray.remote
def _mock_train(dataset_version: str, num_samples: int, params: dict) -> dict:
    time.sleep(2)  # 模拟训练耗时，验证的是接口契约不是真实训练效果
    loss = round(max(0.05, 1.0 / (1 + num_samples)) + params.get("noise", 0.0), 4)
    return {"final_loss": loss, "steps": params.get("steps", 100), "num_samples": num_samples}


def run(dataset_version: str, params: dict, *, run_id: str) -> dict:
    ensure_ray()
    exports = load_table("dataset_export").scan(row_filter=in_filter("dataset_version", [dataset_version])).to_arrow().to_pylist()
    if not exports:
        raise ValueError(f"dataset_version '{dataset_version}' 还没有导出，先跑 export_dataset")

    total_samples = 0
    for exp in exports:
        bucket, key = exp["shard_uri"][len("s3://") :].split("/", 1)
        body = object_store.get_bytes(key, bucket=bucket)
        total_samples += sum(1 for line in body.decode().splitlines() if line.strip())

    metrics = ray.get(_mock_train.remote(dataset_version, total_samples, params))

    now = datetime.now(timezone.utc)
    train_run_id = new_id("train")
    model_id = new_id("model")
    artifact_uri = object_store.put_bytes(
        f"{object_store.PREFIX_ARTIFACT}/{train_run_id}/model.json",
        json.dumps({"model_id": model_id, "metrics": metrics}, ensure_ascii=False).encode(),
    )

    batch_id = make_batch_id(robot_id="mock_train", upload_id=train_run_id)
    train_row = pa.Table.from_pylist(
        [
            {
                "run_id": train_run_id,
                "dataset_version": dataset_version,
                "code_ver": "mvp-1",
                "params_json": json.dumps(params, ensure_ascii=False),
                "metrics_json": json.dumps(metrics, ensure_ascii=False),
                "state": "SUCCEEDED",
            }
        ]
    )
    train_row = with_audit_columns(train_row, batch_id=batch_id, run_id=run_id, source_uri=f"dataset_export:{dataset_version}")
    append(TRAIN_RUN, train_row)

    artifact_row = pa.Table.from_pylist(
        [
            {
                "model_id": model_id,
                "run_id": train_run_id,
                "dataset_version": dataset_version,
                "format": "mock-json",
                "artifact_uri": artifact_uri,
            }
        ]
    )
    artifact_row = with_audit_columns(artifact_row, batch_id=batch_id, run_id=run_id, source_uri=f"train_run:{train_run_id}")
    append(MODEL_ARTIFACT, artifact_row)

    return {"train_run_id": train_run_id, "model_id": model_id, "metrics": metrics}
