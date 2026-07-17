"""FastAPI 上传网关（README 2.4 / 3.2.1 / 3.2.2 / 4.1）。

职责边界很窄，刻意保持"薄"：
  - 创建/查询 upload_session，签发 MinIO 预签名 URL，接收 manifest —— 只写 Postgres + Kafka，
    不碰 Iceberg，不做任何清洗/分支判断。
  - 建数据集请求 / 标注完成 webhook —— 转发一次 Dagster job launch 调用。
真正"这批数据该走哪条路"的判断，全部下沉到 Dagster 的 sensor/asset 里。
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException

from common import object_store
from common.config import settings
from common.dagster_client import launch_job
from common.db import execute, fetch_all, fetch_one, to_json
from common.kafka_ledger import emit, emit_ingest_request
from gateway.models import (
    AnnotationCompleteWebhook,
    CreateSessionRequest,
    CreateSessionResponse,
    DatasetRequestIn,
    DatasetRequestOut,
    ManifestSubmitRequest,
    PresignRequest,
    PresignResponse,
    PromoteRequestIn,
    SessionStatusResponse,
    TagSearchRequest,
    TagSearchResponse,
    TrainJobOut,
    TrainRequestIn,
)

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

app = FastAPI(title="EDP Gateway", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# 上传会话（README 3.2.1）
# ---------------------------------------------------------------------------

@app.post("/sessions", response_model=CreateSessionResponse)
def create_session(req: CreateSessionRequest) -> CreateSessionResponse:
    upload_id = f"upload-{uuid.uuid4().hex[:10]}"
    execute(
        """
        INSERT INTO upload_session (upload_id, robot_id, task_id, operator, manifest_op, pipeline_profile, status)
        VALUES (%(upload_id)s, %(robot_id)s, %(task_id)s, %(operator)s, %(manifest_op)s, %(pipeline_profile)s, 'created')
        """,
        {
            "upload_id": upload_id,
            "robot_id": req.robot_id,
            "task_id": req.task_id,
            "operator": req.operator,
            "manifest_op": req.manifest_op,
            "pipeline_profile": req.pipeline_profile,
        },
    )
    emit("upload.created", key=upload_id, payload=req.model_dump())
    return CreateSessionResponse(
        upload_id=upload_id, status="created", manifest_op=req.manifest_op, pipeline_profile=req.pipeline_profile
    )


def _get_session_or_404(upload_id: str) -> dict:
    row = fetch_one("SELECT * FROM upload_session WHERE upload_id = %s", (upload_id,))
    if row is None:
        raise HTTPException(status_code=404, detail=f"upload session '{upload_id}' not found")
    return row


@app.get("/sessions/{upload_id}", response_model=SessionStatusResponse)
def get_session(upload_id: str) -> SessionStatusResponse:
    row = _get_session_or_404(upload_id)
    return SessionStatusResponse(**row)


@app.post("/sessions/{upload_id}/presign", response_model=PresignResponse)
def presign_upload(upload_id: str, req: PresignRequest) -> PresignResponse:
    session = _get_session_or_404(upload_id)
    key = f"{object_store.PREFIX_RAW}/{session['robot_id']}/{upload_id}/{req.file_name}"
    url = object_store.presigned_put_url(key)
    if session["status"] == "created":
        execute("UPDATE upload_session SET status = 'uploading', updated_at = now() WHERE upload_id = %s", (upload_id,))
    return PresignResponse(upload_url=url, file_uri=object_store.object_uri(key))


@app.post("/sessions/{upload_id}/manifest")
def submit_manifest(upload_id: str, req: ManifestSubmitRequest) -> dict:
    session = _get_session_or_404(upload_id)
    if not req.files:
        raise HTTPException(status_code=400, detail="manifest.files 不能为空")
    if session["manifest_op"] == "correct" and not req.episode_id:
        raise HTTPException(status_code=400, detail="manifest_op=correct 时必须指定 episode_id")

    manifest_payload = {
        "files": [f.model_dump() for f in req.files],
        "episode_id": req.episode_id,
        "affected_start_ts": req.affected_start_ts,
        "affected_end_ts": req.affected_end_ts,
        "manifest_op": session["manifest_op"],
    }
    execute(
        """
        UPDATE upload_session
        SET status = 'ready', manifest = %(manifest)s, manifest_uri = %(manifest_uri)s, updated_at = now()
        WHERE upload_id = %(upload_id)s
        """,
        {
            "manifest": to_json(manifest_payload),
            "manifest_uri": object_store.object_uri(
                f"{object_store.PREFIX_RAW}/{session['robot_id']}/{upload_id}/manifest.json"
            ),
            "upload_id": upload_id,
        },
    )
    object_store.put_bytes(
        f"{object_store.PREFIX_RAW}/{session['robot_id']}/{upload_id}/manifest.json",
        to_json(manifest_payload).encode("utf-8"),
    )
    emit("manifest.submitted", key=upload_id, payload=manifest_payload)
    # 触发事件：ingest_kafka_sensor 消费这条消息拉起对应 job。发失败也没关系——
    # session 已是 ready，T+1 兜底 schedule 轮询 PG 会补触发；重复发也没关系——
    # sensor 端会校验 status=ready + run_key 去重 + saga claim 三层兜底。
    emit_ingest_request(upload_id, session["manifest_op"])
    return {"upload_id": upload_id, "status": "ready", "manifest_op": session["manifest_op"]}


def _manual_retry_or_http(kind, business_id: str) -> dict:
    """common/jobs.py::manual_retry 的 HTTP 翻译层：404 / 409 语义统一。"""
    from common.jobs import RetryNotAllowed, manual_retry

    try:
        return manual_retry(kind, business_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"'{business_id}' not found") from None
    except RetryNotAllowed as e:
        raise HTTPException(status_code=409, detail=str(e)) from None


@app.post("/sessions/{upload_id}/retry")
def retry_session(upload_id: str) -> dict:
    """人工重试入口（docs/pod-fanout-guide.md 错误处理 / README 3.7.4 协议的一部分）：
    把 failed 的会话重置回 ready 并补发触发事件。适合"数据/环境已修好，重跑这
    一个 upload"的场景——和 Dagster UI 的 Re-execute（整批以 PG 状态为起点重放，
    done 的廉价跳过）互补：这里是单 upload 粒度、不需要找到原来的 run。

    只允许 failed → ready（common/jobs.py::manual_retry，与训练 retry 共用实现）。
    重置 updated_at → 新 run_key；引擎侧 saga claim 的 attempt 会继续累加，
    OOM 类失败重试时 worker 内存自动升档（INGEST_WORKER_MEMORY_TIERS）。
    """
    from common.jobs import UPLOAD_KIND

    session = _get_session_or_404(upload_id)
    result = _manual_retry_or_http(UPLOAD_KIND, upload_id)
    emit("upload.retry_requested", key=upload_id, payload={"upload_id": upload_id, "source": "gateway"})
    return {
        "upload_id": upload_id,
        "status": result["status"],
        "manifest_op": session["manifest_op"],
        "previous_attempt": result["previous_attempt"],
        "previous_error_code": result["previous_error_code"],
        "previous_error": result["previous_error"],
    }


# ---------------------------------------------------------------------------
# 建数据集请求（README 4.1：API 触发）
# ---------------------------------------------------------------------------

@app.post("/dataset-requests", response_model=DatasetRequestOut)
def create_dataset_request(req: DatasetRequestIn) -> DatasetRequestOut:
    request_id = f"dsreq-{uuid.uuid4().hex[:10]}"
    execute(
        """
        INSERT INTO dataset_request (request_id, requested_by, dataset_name, filter_expr, quality_threshold, split, status)
        VALUES (%(request_id)s, %(requested_by)s, %(dataset_name)s, %(filter_expr)s, %(quality_threshold)s, %(split)s, 'pending')
        """,
        {
            "request_id": request_id,
            "requested_by": req.requested_by,
            "dataset_name": req.dataset_name,
            "filter_expr": to_json(req.filter_expr),
            "quality_threshold": req.quality_threshold,
            "split": to_json(req.split),
        },
    )
    run_id = launch_job(
        "freeze_dataset_job",
        run_config={
            "ops": {
                "dataset": {
                    "config": {
                        "request_id": request_id,
                        "dataset_name": req.dataset_name,
                        "filter_expr": req.filter_expr,
                        "quality_threshold": req.quality_threshold,
                        "split": req.split,
                        # 手工挑样本（README 3.7.2）：非空时冻结引擎跳过条件圈选
                        "sample_ids": req.sample_ids,
                    }
                }
            }
        },
        tags={"request_id": request_id},
    )
    execute(
        "UPDATE dataset_request SET status = 'building', dagster_run_id = %s, updated_at = now() WHERE request_id = %s",
        (run_id, request_id),
    )
    emit("dataset_request.created", key=request_id, payload=req.model_dump())
    return DatasetRequestOut(request_id=request_id, status="building", dagster_run_id=run_id)


# ---------------------------------------------------------------------------
# tag 检索（README 3.5：查 OpenSearch 投影，不碰 Iceberg/PG）
# ---------------------------------------------------------------------------

@app.post("/search/tags", response_model=TagSearchResponse)
def search_tags(req: TagSearchRequest) -> TagSearchResponse:
    from common.search import search_by_tags

    try:
        result = search_by_tags(req.tags, target_type=req.target_type, size=req.size)
    except Exception as exc:  # 投影不可用不应该被误读成"没有数据"
        raise HTTPException(status_code=503, detail=f"OpenSearch 查询失败：{exc}") from exc
    return TagSearchResponse(**result)


# ---------------------------------------------------------------------------
# 模型训练与管理（README 3.7）：状态点查走 PG platform_job，深度分析走 Iceberg
# ---------------------------------------------------------------------------

@app.post("/train", response_model=TrainJobOut)
def create_training_job(req: TrainRequestIn) -> TrainJobOut:
    """发起训练：platform_job 落 ready + 发 Kafka 触发（README 3.7.2）。
    网关保持薄：不校验 dataset_version 是否存在（那要碰 Iceberg）——不存在
    由训练 run 以 DATA_EMPTY 终态报出，状态可查、不自动重试。"""
    import random

    payload = {
        "model_name": req.model_name,
        "dataset_name": req.dataset_name,
        "dataset_version": req.dataset_version,
        "params": req.params,
        # seed 此刻固定进 payload：dataset_version + image + params + seed 是复现配方
        "seed": req.seed if req.seed is not None else random.randint(0, 2**31 - 1),
    }
    from common.jobs import create_job

    job_id = create_job("training", payload, requested_by=req.requested_by)
    emit("training.requested", key=job_id, payload={"job_id": job_id, **payload})
    return TrainJobOut(job_id=job_id, status="ready", payload=payload)


def _get_training_job_or_404(job_id: str) -> dict:
    row = fetch_one(
        "SELECT * FROM platform_job WHERE job_id = %s AND job_type = 'training'", (job_id,)
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"training job '{job_id}' not found")
    return row


@app.get("/training-jobs/{job_id}", response_model=TrainJobOut)
def get_training_job(job_id: str) -> TrainJobOut:
    row = _get_training_job_or_404(job_id)
    saga = fetch_one(
        "SELECT error_code, error FROM saga_log WHERE scope = 'training' AND business_id = %s", (job_id,)
    )
    return TrainJobOut(
        job_id=job_id,
        status=row["status"],
        payload=row["payload"],
        result=row["result"],
        error_code=saga["error_code"] if saga else None,
        error=saga["error"] if saga else None,
    )


@app.post("/training-jobs/{job_id}/retry")
def retry_training_job(job_id: str) -> dict:
    """训练的人工重试：与 /sessions/{id}/retry 同一份协议实现（README 3.7.4）。"""
    from common.jobs import TRAINING_KIND

    _get_training_job_or_404(job_id)
    result = _manual_retry_or_http(TRAINING_KIND, job_id)
    emit("training.retry_requested", key=job_id, payload={"job_id": job_id, "source": "gateway"})
    return {"job_id": job_id, **{k: v for k, v in result.items() if k != "id"}}


@app.get("/models/{model_name}/versions")
def list_model_versions(model_name: str) -> dict:
    """模型版本快查（点查走 PG 的 result 摘要；权威档案与血缘在 Iceberg
    ml_model_version / ml_training_run，深度分析用 DuckDB 查那边）。"""
    rows = fetch_all(
        """
        SELECT job_id, result, updated_at FROM platform_job
        WHERE job_type = 'training' AND status = 'done' AND result ->> 'model_name' = %s
        ORDER BY updated_at DESC
        """,
        (model_name,),
    )
    return {
        "model_name": model_name,
        "versions": [
            {
                "version": r["job_id"],
                "mlflow_version": r["result"].get("mlflow_version"),
                "dataset_version": r["result"].get("dataset_version"),
                "metrics": r["result"].get("metrics"),
                "artifact_uri": r["result"].get("artifact_uri"),
                "trained_at": r["updated_at"].isoformat(),
            }
            for r in rows
        ],
    }


@app.post("/models/{model_name}/versions/{version}/promote")
def promote_model(model_name: str, version: str, req: PromoteRequestIn) -> dict:
    """版本流转（README 3.7.2）：MLflow Registry 设 alias（操作台指针）+
    向 Iceberg `ml_promotion_event` 追加一条审计事件。账本是追加式的：两写
    之间崩了顶多"alias 已改、账本没记"，重放本 API 幂等补齐。

    promote 是一次性同步操作，不需要状态机（README 3.7.4 的判据）。
    """
    row = fetch_one(
        "SELECT result FROM platform_job WHERE job_type = 'training' AND status = 'done' AND job_id = %s",
        (version,),
    )
    if row is None or row["result"].get("model_name") != model_name:
        raise HTTPException(status_code=404, detail=f"model '{model_name}' version '{version}' 不存在或未训练成功")

    # MLflow alias：只有拿到过 Registry 版本号才有指针可设（训练时 MLflow 不可用则跳过）
    mlflow_version = row["result"].get("mlflow_version")
    mlflow_status = "skipped(训练时未登记 MLflow 版本)"
    if mlflow_version:
        try:
            from mlflow import MlflowClient

            MlflowClient(tracking_uri=settings.mlflow_tracking_uri).set_registered_model_alias(
                model_name, req.to_stage, mlflow_version
            )
            mlflow_status = "ok"
        except Exception as exc:  # noqa: BLE001 - 操作台不可达 → 明确失败，操作员稍后重放
            raise HTTPException(status_code=503, detail=f"MLflow 设置 alias 失败：{exc}") from exc

    # Iceberg 审计账本（追加式，事件为主体，重放可得任意时刻的指针）
    import uuid as _uuid
    from datetime import datetime as _dt, timezone as _tz

    import pyarrow as pa

    from common.audit import make_batch_id
    from common.iceberg import append, with_audit_columns
    from schemas.iceberg_tables import ML_PROMOTION_EVENT

    event_id = f"promo-{_uuid.uuid4().hex[:10]}"
    event = {
        "event_id": event_id,
        "model_name": model_name,
        "version": version,
        "from_stage": req.from_stage or "none",
        "to_stage": req.to_stage,
        "actor": req.actor,
        "reason": req.reason,
        "occurred_at": _dt.now(_tz.utc),
    }
    append(
        ML_PROMOTION_EVENT,
        with_audit_columns(
            pa.Table.from_pylist([event]),
            batch_id=make_batch_id(robot_id="promote", upload_id=version),
            run_id="gateway",
            source_uri=f"platform_job:{version}",
        ),
    )
    emit("model.promoted", key=f"{model_name}:{version}", payload={**event, "occurred_at": event["occurred_at"].isoformat()})
    return {"event_id": event_id, "model_name": model_name, "version": version, "to_stage": req.to_stage, "mlflow": mlflow_status}


# ---------------------------------------------------------------------------
# 标注完成 webhook（README 3.2.2 / 4.1：唤醒 job-B）
# ---------------------------------------------------------------------------

@app.post("/webhooks/annotation-complete")
def annotation_complete(req: AnnotationCompleteWebhook) -> dict:
    batch = fetch_one("SELECT * FROM annotation_batch WHERE batch_id = %s", (req.batch_id,))
    if batch is None:
        raise HTTPException(status_code=404, detail=f"annotation batch '{req.batch_id}' not found")

    execute(
        """
        UPDATE annotation_batch
        SET status = 'RETURNED', package_uri = %(package_result_uri)s, updated_at = now()
        WHERE batch_id = %(batch_id)s
        """,
        {"package_result_uri": req.package_result_uri, "batch_id": req.batch_id},
    )
    run_id = launch_job(
        "annotation_collect_job",
        run_config={"ops": {"annotation_collect": {"config": {"batch_id": req.batch_id}}}},
        tags={"batch_id": req.batch_id},
    )
    execute(
        "UPDATE annotation_batch SET collect_run_id = %s, updated_at = now() WHERE batch_id = %s",
        (run_id, req.batch_id),
    )
    emit("annotation.completed", key=req.batch_id, payload=req.model_dump())
    return {"batch_id": req.batch_id, "status": "RETURNED", "dagster_run_id": run_id}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("gateway.main:app", host=settings.gateway_host, port=settings.gateway_port, reload=False)
