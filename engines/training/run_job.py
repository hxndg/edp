"""`model_training` asset 的 run 侧逻辑（README 3.7.2/3.7.3）：控制面 + 单写者。

与 ingest 的 run_batch 同构，只是 fan-out 数为 1（一次训练一个 worker pod）：

    run pod                                     训练 worker pod
    ────────────────────────────────────        ───────────────────────────
    Saga claim（scope=training，CAS 互斥）
    解析 dataset_sample 清单 → samples.parquet
    写 input.json 到 staging  ──────────────▶   读清单，逐样本 Lance 抽特征
    提交 Argo Workflow（轮询时刷 saga 心跳）       SGD 逐 epoch 训练
                                                ├ MLflow 打点（best-effort）
                                                ├ 权重直传 MinIO models/
    收 manifest.json（指标/版本/error_code）◀──    └ manifest + eval.parquet 收尾
    归档四表（每表一次 commit，job_id 幂等键）
    saga succeed → platform_job done

失败语义（common/errors.py）：
- worker 业务失败（数据集为空等）：manifest 带码 → saga fail + platform_job failed；
- pod 级失败（OOM/超时/丢失）：无清单 → PodOutcome.classify() 推断码；
- run 侧失败（归档 commit / PG 挂）：classify_exception(where="run") 定码后上抛。
重试由 watchdog 按码决定（common/jobs.py），OOM 重试时 worker 内存自动升档。

**以 PG 状态为起点**：status=done 的 job 直接跳过，UI Re-execute 天然安全。
归档以 job_id 为幂等键（upsert / replace_where），重跑不产生重复档案。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pyarrow as pa

from common.audit import make_batch_id
from common.config import settings
from common.db import execute, fetch_one, to_json
from common.errors import ErrorCode, classify_exception, format_error
from common.iceberg import in_filter, load_table, replace_where, upsert, with_audit_columns
from common.runtime_config import get_int, get_str
from common.saga import Saga, SagaConflictError
from common.argo_workflows import WorkerSpec, launch_wave
from engines.worker import staging
from schemas.iceberg_tables import (
    DATASET_SAMPLE,
    ML_EVAL_SAMPLE,
    ML_METRIC_EPOCH,
    ML_MODEL_VERSION,
    ML_TRAINING_RUN,
    SAMPLE,
)

logger = logging.getLogger(__name__)

SCOPE = "training"


class TrainingFailed(Exception):
    """业务级训练失败：saga/platform_job 已按码落终态，携带码上抛让 run 显式失败。"""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(format_error(code, message))
        self.code = code


def run_training(job_id: str, op_context) -> dict:
    run_id = op_context.run_id
    row = fetch_one(
        "SELECT * FROM platform_job WHERE job_id = %s AND job_type = 'training' AND status <> 'done'",
        (job_id,),
    )
    if row is None:
        logger.info("job %s 不存在/已 done，跳过（UI Re-execute 的幂等路径）", job_id)
        return {"status": "skipped", "job_id": job_id}

    saga = Saga(SCOPE, job_id, run_id)
    try:
        saga.claim()
    except SagaConflictError as e:
        logger.warning("%s", e)
        return {"status": "skipped", "job_id": job_id, "reason": "saga 被另一个活跃 run 持有"}

    execute(
        "UPDATE platform_job SET status = 'running', updated_at = now() WHERE job_id = %s",
        (job_id,),
    )
    try:
        result = _execute(row, saga, run_id, op_context)
    except TrainingFailed:
        raise  # 终态已由 _fail 落好，不要重复分类
    except Exception as e:  # noqa: BLE001 - run 侧异常（典型：归档 commit / PG 故障）
        code = classify_exception(e, where="run")
        _fail(saga, job_id, run_id, code, f"{type(e).__name__}: {e}")
        raise

    return result


def _execute(row: dict, saga: Saga, run_id: str, op_context) -> dict:
    job_id = row["job_id"]
    payload = row["payload"]

    # ---- PREPARE：解析冻结清单，物化 worker 输入（worker 不碰 Iceberg catalog）----
    saga.advance("PREPARE")
    samples = _resolve_samples(payload["dataset_version"])
    if not samples:
        _fail(saga, job_id, run_id, ErrorCode.DATA_EMPTY,
              f"dataset_version {payload['dataset_version']} 不存在或没有样本")
        raise TrainingFailed(ErrorCode.DATA_EMPTY, f"dataset_version {payload['dataset_version']} 无样本")

    prefix = staging.prefix(run_id, job_id)
    samples_key = f"{prefix}/samples.parquet"
    staging.write_parquet(samples_key, samples)
    staging.write_json(
        f"{prefix}/{staging.INPUT_JSON}",
        {
            "job_id": job_id,
            "run_id": run_id,
            "model_name": payload["model_name"],
            "dataset_name": payload.get("dataset_name"),
            "dataset_version": payload["dataset_version"],
            "params": payload.get("params") or {},
            "seed": payload["seed"],
            "samples_key": samples_key,
        },
    )

    # ---- TRAIN：提交 Argo Workflow 拉起训练 worker（fan-out=1），等待并收清单 ----
    saga.advance("TRAIN")
    timeout = get_int("TRAIN_WORKER_TIMEOUT_SECONDS", 1800)
    tiers = [t.strip() for t in get_str("TRAIN_WORKER_MEMORY_TIERS", "1Gi,2Gi,4Gi").split(",") if t.strip()]
    attempt = saga.attempt or 1
    spec = WorkerSpec(
        upload_id=job_id,
        staging_prefix=prefix,
        memory_limit=tiers[min(attempt, len(tiers)) - 1],
        command=[
            "python", "-m", "engines.worker.train_mock",
            "--job-id", job_id,
            "--run-id", run_id,
            "--staging-prefix", prefix,
        ],
    )
    outcomes = launch_wave(
        op_context,
        [spec],
        run_id=run_id,
        timeout_seconds=timeout,
        # 心跳防 watchdog 误判 owner 已死；fencing 在下一次 advance 边界统一检查
        heartbeat=lambda: saga.advance("TRAIN"),
    )

    manifest = staging.try_read_json(f"{prefix}/{staging.MANIFEST_JSON}")
    if manifest is None:
        code, detail = outcomes[job_id].classify()
        _fail(saga, job_id, run_id, code, detail)
        raise TrainingFailed(code, detail)
    if manifest.get("status") != "ok":
        code = ErrorCode(manifest.get("error_code") or ErrorCode.INTERNAL.value)
        detail = manifest.get("error", "worker 报告未知错误")
        _fail(saga, job_id, run_id, code, detail)
        raise TrainingFailed(code, detail)

    # ---- ARCHIVE：归档四表（单写者，job_id 幂等键，每表一次 commit）----
    saga.advance("ARCHIVE")
    _archive(job_id, payload, manifest, run_id)

    # ---- 终态 ----
    saga.succeed()
    result_summary = {
        "model_name": manifest["model_name"],
        "model_version": job_id,
        "mlflow_run_id": manifest.get("mlflow_run_id"),
        "mlflow_version": manifest.get("mlflow_version"),
        "artifact_uri": manifest["artifact_uri"],
        "metrics": manifest["metrics"],
        "dataset_version": manifest["dataset_version"],
    }
    execute(
        "UPDATE platform_job SET status = 'done', result = %s, updated_at = now() WHERE job_id = %s",
        (to_json(result_summary), job_id),
    )
    return {"status": "done", "job_id": job_id, **result_summary}


def _resolve_samples(dataset_version: str) -> list[dict]:
    """冻结清单（dataset_sample）join 样本索引（sample）→ worker 的输入行。"""
    ds = (
        load_table(DATASET_SAMPLE)
        .scan(row_filter=in_filter("dataset_version", [dataset_version]))
        .to_arrow()
        .to_pylist()
    )
    if not ds:
        return []
    splits = {r["sample_id"]: r.get("split") for r in ds}
    sample_rows = (
        load_table(SAMPLE)
        .scan(
            row_filter=in_filter("sample_id", list(splits)),
            selected_fields=("sample_id", "lance_uri", "quality_score"),
        )
        .to_arrow()
        .to_pylist()
    )
    return [
        {
            "sample_id": r["sample_id"],
            "lance_uri": r["lance_uri"],
            "quality_score": r["quality_score"],
            "split": splits.get(r["sample_id"]) or "train",
        }
        for r in sample_rows
    ]


def _archive(job_id: str, payload: dict, manifest: dict, run_id: str) -> None:
    batch_id = make_batch_id(robot_id="training", upload_id=job_id)
    source = f"platform_job:{job_id}"
    now = datetime.now(timezone.utc)

    def _stamp(table: pa.Table) -> pa.Table:
        return with_audit_columns(table, batch_id=batch_id, run_id=run_id, source_uri=source)

    # 1. 训练档案（upsert by job_id：重跑覆盖同一行）
    upsert(
        ML_TRAINING_RUN,
        _stamp(pa.Table.from_pylist([{
            "job_id": job_id,
            "model_name": manifest["model_name"],
            "dataset_name": manifest.get("dataset_name"),
            "dataset_version": manifest["dataset_version"],
            "mlflow_run_id": manifest.get("mlflow_run_id"),
            "mlflow_experiment": manifest.get("mlflow_experiment"),
            "image": settings.edp_image,
            "params_json": json.dumps(manifest.get("params") or {}, ensure_ascii=False),
            "seed": int(manifest["seed"]),
            "metrics_json": json.dumps(manifest["metrics"], ensure_ascii=False),
            "artifact_uri": manifest["artifact_uri"],
            "artifact_sha256": manifest["artifact_sha256"],
            "num_train": int(manifest["metrics"].get("num_train") or 0),
            "num_val": int(manifest["metrics"].get("num_val") or 0),
            "status": "SUCCEEDED",
            "trained_at": now,
        }])),
        join_cols=["job_id"],
    )

    # 2. 逐 epoch 曲线（replace_where by job_id：重跑整段重写，无重复）
    epoch_rows = [
        {"job_id": job_id, "mlflow_run_id": manifest.get("mlflow_run_id"), **m}
        for m in manifest.get("epoch_metrics") or []
    ]
    replace_where(
        ML_METRIC_EPOCH,
        in_filter("job_id", [job_id]),
        _stamp(pa.Table.from_pylist(epoch_rows)) if epoch_rows else None,
    )

    # 3. 逐样本评测明细（worker 已带全列，这里只盖审计章）
    eval_key = manifest.get("eval_key")
    eval_table = staging.read_parquet(eval_key) if eval_key else None
    replace_where(
        ML_EVAL_SAMPLE,
        in_filter("job_id", [job_id]),
        _stamp(eval_table) if eval_table is not None and eval_table.num_rows else None,
    )

    # 4. 模型版本登记（version = job_id，不可变；mlflow_version 仅是操作台侧的键）
    upsert(
        ML_MODEL_VERSION,
        _stamp(pa.Table.from_pylist([{
            "model_name": manifest["model_name"],
            "version": job_id,
            "mlflow_version": manifest.get("mlflow_version"),
            "job_id": job_id,
            "dataset_version": manifest["dataset_version"],
            "artifact_uri": manifest["artifact_uri"],
            "artifact_sha256": manifest["artifact_sha256"],
            "registered_at": now,
        }])),
        join_cols=["model_name", "version"],
    )


def _fail(saga: Saga, job_id: str, run_id: str, code: ErrorCode, message: str) -> None:
    error = format_error(code, message)
    if saga.fail(error, error_code=code.value):
        execute(
            "UPDATE platform_job SET status = 'failed', updated_at = now() WHERE job_id = %s AND status = 'running'",
            (job_id,),
        )
        execute(
            "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
            ("error", SCOPE, run_id, f"training job {job_id} 失败：{error}",
             to_json({"job_id": job_id, "error_code": code.value, "error": message})),
        )
