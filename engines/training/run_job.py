"""`model_training` asset 的 run 侧逻辑（README 3.7.2/3.7.3）：控制面 + 单写者。

与 ingest 的 run_batch 同构，只是 fan-out 数为 1（一次训练一个 worker pod）：

    run pod                                     训练 worker pod
    ────────────────────────────────────        ───────────────────────────
    execution_claim（scope=training，CAS 互斥）
    解析 dataset_sample 清单 → samples.parquet
    写 input.json 到 staging  ──────────────▶   读清单，逐样本 Lance 抽特征
    提交 Argo Workflow（轮询时续 claim）            SGD 逐 epoch 训练
                                                ├ MLflow 打点（best-effort）
                                                ├ 权重直传 MinIO models/
    收 manifest.json（指标/版本/error_code）◀──    └ manifest + eval.parquet 收尾
    归档四表（每表一次 commit，job_id 幂等键）
    最终事务 → platform_job done + release claim

失败语义（common/errors.py）：
- worker 业务失败（数据集为空等）：manifest 带码 → platform_job failed；
- pod 级失败（OOM/超时/丢失）：无清单 → PodOutcome.classify() 推断码；
- run 侧失败（归档 commit / PG 挂）：classify_exception(where="run") 定码后上抛。
Argo 管 task retry、OOM 内存升档和 phase/exit/log；Dagster只写最终业务摘要。

**以 PG 状态为起点**：status=done 的 job 直接跳过，UI Re-execute 天然安全。
归档以 job_id 为幂等键（upsert / replace_where），重跑不产生重复档案。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pyarrow as pa

from common.audit import make_batch_id
from common.db import fetch_one, to_json, transaction
from common.errors import ErrorCode, classify_exception, format_error
from common.execution_claim import ClaimBatch
from common.iceberg import in_filter, load_table, replace_where, upsert, with_audit_columns
from common.processing_registry import ProcessingDefinition, resolve_processing_type
from common.argo_workflows import PodOutcome, WorkerSpec, launch_wave
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
    """携带 worker/Argo 终局事实，由 run 外壳一次性写业务终态。"""

    def __init__(self, code: ErrorCode, message: str, outcome: PodOutcome | None = None):
        super().__init__(format_error(code, message))
        self.code = code
        self.message = message
        self.outcome = outcome


def run_training(job_id: str, op_context) -> dict:
    run_id = op_context.run_id
    row = fetch_one(
        "SELECT * FROM platform_job WHERE job_id = %s AND job_type = 'training' AND status <> 'done'",
        (job_id,),
    )
    if row is None:
        logger.info("job %s 不存在/已 done，跳过（UI Re-execute 的幂等路径）", job_id)
        return {"status": "skipped", "job_id": job_id}

    processing_type = (row["payload"] or {}).get("processing_type", "training_mock")
    definition = resolve_processing_type(processing_type, expected_kind="training")
    claim = ClaimBatch(SCOPE, [job_id], run_id)
    with transaction() as conn:
        claimed = claim.acquire_many(conn=conn)
        if not claimed:
            return {"status": "skipped", "job_id": job_id, "reason": "被另一个活跃 run 持有"}
        conn.execute(
            """
            UPDATE platform_job SET status = 'running', last_dagster_run_id = %s,
                last_execution_profile_id = %s,
                last_error_code = NULL, last_error = NULL, updated_at = now()
            WHERE job_id = %s
            """,
            (run_id, definition.profile.profile_id, job_id),
        )
    try:
        result = _execute(row, claim, run_id, definition)
    except TrainingFailed as e:
        _finalize_training(claim, job_id, run_id, error=(e.code, e.message), outcome=e.outcome)
        raise
    except Exception as e:  # noqa: BLE001 - run 侧异常（典型：归档 commit / PG 故障）
        code = classify_exception(e, where="run")
        _finalize_training(claim, job_id, run_id, error=(code, f"{type(e).__name__}: {e}"))
        raise

    _finalize_training(claim, job_id, run_id, result=result)
    return result


def _execute(
    row: dict, claim: ClaimBatch, run_id: str, definition: ProcessingDefinition
) -> dict:
    job_id = row["job_id"]
    payload = row["payload"]

    # ---- PREPARE：解析冻结清单，物化 worker 输入（worker 不碰 Iceberg catalog）----
    samples = _resolve_samples(payload["dataset_version"])
    if not samples:
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
    profile = definition.profile
    spec = WorkerSpec(
        upload_id=job_id,
        staging_prefix=prefix,
        memory_tiers=list(profile.memory_tiers),
        command=[
            "python", "-m", definition.worker_module,
            "--job-id", job_id,
            "--run-id", run_id,
            "--staging-prefix", prefix,
        ],
    )
    outcomes = launch_wave(
        [spec],
        run_id=run_id,
        timeout_seconds=profile.timeout_seconds,
        heartbeat=lambda: claim.heartbeat_many([job_id]),
        parallelism=profile.parallelism,
        workflow_template_name=profile.workflow_template_name,
        image_ref=profile.image_ref,
        processing_type=definition.processing_type,
        execution_profile_id=profile.profile_id,
    )

    manifest = staging.try_read_json(f"{prefix}/{staging.MANIFEST_JSON}")
    outcome = outcomes.get(job_id)
    if manifest is None:
        code, detail = (outcome.classify() if outcome else (ErrorCode.WORKER_LOST, "无 Argo 观测"))
        raise TrainingFailed(code, detail, outcome)
    if manifest.get("status") != "ok":
        code = ErrorCode(manifest.get("error_code") or ErrorCode.INTERNAL.value)
        detail = manifest.get("error", "worker 报告未知错误")
        raise TrainingFailed(code, detail, outcome)

    # ---- ARCHIVE：归档四表（单写者，job_id 幂等键，每表一次 commit）----
    if job_id not in claim.heartbeat_many([job_id]):
        raise RuntimeError(f"training job {job_id} 已被其他 run 接管")
    _archive(job_id, payload, manifest, run_id, image_ref=profile.image_ref)

    result_summary = {
        "model_name": manifest["model_name"],
        "model_version": job_id,
        "mlflow_run_id": manifest.get("mlflow_run_id"),
        "mlflow_version": manifest.get("mlflow_version"),
        "artifact_uri": manifest["artifact_uri"],
        "metrics": manifest["metrics"],
        "dataset_version": manifest["dataset_version"],
    }
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


def _archive(
    job_id: str, payload: dict, manifest: dict, run_id: str, *, image_ref: str
) -> None:
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
            "image": image_ref,
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


def _finalize_training(
    claim: ClaimBatch,
    job_id: str,
    run_id: str,
    *,
    result: dict | None = None,
    error: tuple[ErrorCode, str] | None = None,
    outcome: PodOutcome | None = None,
) -> bool:
    """仅当前 claim owner 可写最终业务态；写入与 release 在同一事务。"""
    with transaction() as conn:
        owned = conn.execute(
            """
            SELECT 1 FROM execution_claim
            WHERE scope = %s AND business_id = %s AND run_id = %s
            FOR UPDATE
            """,
            (claim.scope, job_id, run_id),
        ).fetchone()
        if owned is None:
            return False
        if error is None:
            conn.execute(
                """
                UPDATE platform_job
                SET status = 'done', result = %s, last_error_code = NULL,
                    last_error = NULL, updated_at = now()
                WHERE job_id = %s AND last_dagster_run_id = %s
                """,
                (to_json({k: v for k, v in (result or {}).items() if k not in ("status", "job_id")}), job_id, run_id),
            )
        else:
            code, message = error
            context = {"job_id": job_id, "error_code": code.value, "error": message}
            if outcome is not None:
                context["argo"] = outcome.to_dict()
            conn.execute(
                """
                UPDATE platform_job
                SET status = 'failed', last_error_code = %s, last_error = %s, updated_at = now()
                WHERE job_id = %s AND last_dagster_run_id = %s
                """,
                (code.value, message[:2000], job_id, run_id),
            )
            conn.execute(
                """
                INSERT INTO alerts (severity, source, run_id, message, context)
                VALUES ('error', %s, %s, %s, %s)
                """,
                (SCOPE, run_id, f"training job {job_id} 失败：{format_error(code, message)}", to_json(context)),
            )
        claim.release_many([job_id], conn=conn)
    return True
