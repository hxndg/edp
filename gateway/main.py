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
from common.db import execute, fetch_one, to_json
from common.kafka_ledger import emit
from gateway.models import (
    AnnotationCompleteWebhook,
    CreateSessionRequest,
    CreateSessionResponse,
    DatasetRequestIn,
    DatasetRequestOut,
    ManifestSubmitRequest,
    PresignRequest,
    PresignResponse,
    SessionStatusResponse,
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
    # 只记账，不触发——真正拉起 ingest_append/ingest_correct 的是 Dagster 自己的 sensor（README 2.2 原则 5）
    emit("manifest.submitted", key=upload_id, payload=manifest_payload)
    return {"upload_id": upload_id, "status": "ready", "manifest_op": session["manifest_op"]}


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
