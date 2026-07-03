"""触发层（README 2.3 / 4.1）：定时、近实时事件感知、Webhook 全部收敛进 Dagster
自己的机制，Kafka 不参与触发（README 2.2 原则 5）。
"""
from __future__ import annotations

from dagster import DefaultSensorStatus, RunRequest, SensorResult, sensor

from common.db import fetch_all
from orchestration.jobs import annotation_collect_job, ingest_append_job, ingest_correct_job
from orchestration.partitions import upload_sessions_partitions_def


def _pending_upload_ids(manifest_op: str) -> list[str]:
    rows = fetch_all(
        "SELECT upload_id FROM upload_session WHERE status = 'ready' AND manifest_op = %s ORDER BY created_at",
        (manifest_op,),
    )
    return [r["upload_id"] for r in rows]


def _build_ingest_sensor_result(context, manifest_op: str) -> SensorResult:
    upload_ids = _pending_upload_ids(manifest_op)
    if not upload_ids:
        return SensorResult(run_requests=[])

    existing_partitions = set(context.instance.get_dynamic_partitions(upload_sessions_partitions_def.name))
    new_partitions = [u for u in upload_ids if u not in existing_partitions]

    run_requests = [RunRequest(run_key=f"{manifest_op}-{uid}", partition_key=uid) for uid in upload_ids]
    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=(
            [upload_sessions_partitions_def.build_add_request(new_partitions)] if new_partitions else []
        ),
    )


@sensor(job=ingest_append_job, minimum_interval_seconds=15, default_status=DefaultSensorStatus.RUNNING)
def ingest_append_sensor(context):
    """轮询 manifest_op=append 且 status=ready 的 upload_session（README 3.2.1 SENSOR_A）。"""
    return _build_ingest_sensor_result(context, "append")


@sensor(job=ingest_correct_job, minimum_interval_seconds=15, default_status=DefaultSensorStatus.RUNNING)
def ingest_correct_sensor(context):
    """轮询 manifest_op=correct 且 status=ready 的 upload_session（README 3.2.1 SENSOR_C）。"""
    return _build_ingest_sensor_result(context, "correct")


@sensor(job=annotation_collect_job, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def annotation_collect_sensor(context):
    """兜底：CLI 已经把结果包传到 MinIO、webhook 却因为网络问题没打成功时，
    靠轮询 `annotation_batch.status = RETURNED` 兜底唤醒 job-B（README 3.2.2）。
    """
    rows = fetch_all("SELECT batch_id FROM annotation_batch WHERE status = 'RETURNED'")
    run_requests = [RunRequest(run_key=f"collect-{r['batch_id']}", partition_key=r["batch_id"]) for r in rows]
    return run_requests
