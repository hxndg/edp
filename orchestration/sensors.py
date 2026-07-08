"""触发层（README 2.3 / 4.1）：定时、近实时事件感知、Webhook 全部收敛进 Dagster
自己的机制，Kafka 不参与触发（README 2.2 原则 5）。
"""
from __future__ import annotations

from dagster import DefaultSensorStatus, RunRequest, SensorResult, SkipReason, sensor

from common.config import settings
from common.db import execute, fetch_all, to_json
from orchestration.jobs import annotation_collect_job, ingest_append_job, ingest_correct_job
from orchestration.partitions import upload_sessions_partitions_def


def _pending_upload_rows(manifest_op: str) -> list[dict]:
    return fetch_all(
        "SELECT upload_id, updated_at FROM upload_session WHERE status = 'ready' AND manifest_op = %s ORDER BY created_at",
        (manifest_op,),
    )


def _run_key(manifest_op: str, row: dict) -> str:
    """run_key = op + upload_id + updated_at 时间戳。

    带上 updated_at 的意义（docs/saga-consistency-guide.md）：Dagster 对同一个
    run_key 只会创建一次 run，如果上一个 run 崩溃了、状态被 stuck sensor 重置回
    ready（updated_at 随之刷新），新的 run_key 才能触发新 run——纯 `op-upload_id`
    的旧写法会因为 run_key 已消费而永远无法重试。同一行没被动过时 updated_at
    不变，sensor 每 15s 轮询产生的 run_key 相同，去重语义不受影响。
    """
    return f"{manifest_op}-{row['upload_id']}-{int(row['updated_at'].timestamp())}"


def _build_ingest_sensor_result(context, manifest_op: str) -> SensorResult:
    rows = _pending_upload_rows(manifest_op)
    if not rows:
        return SensorResult(run_requests=[])

    upload_ids = [r["upload_id"] for r in rows]
    existing_partitions = set(context.instance.get_dynamic_partitions(upload_sessions_partitions_def.name))
    new_partitions = [u for u in upload_ids if u not in existing_partitions]

    run_requests = [
        RunRequest(run_key=_run_key(manifest_op, row), partition_key=row["upload_id"]) for row in rows
    ]
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


@sensor(
    job=ingest_append_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Saga 卡死看护：心跳超时的 ingesting 会话重新入队（有次数上限），超限转 failed + alert",
)
def ingest_stuck_sensor(context):
    """处理"run 挂了、状态悬在 ingesting"的场景（docs/saga-consistency-guide.md）。

    本 sensor 不直接发 RunRequest，只做两类状态修复，修复后由普通 ingest sensor
    按新 run_key 重新拉起 run；即使这里的判断和一个"其实还活着"的旧 run 撞车，
    新 run 的 saga.claim() CAS 也只允许一个写者，不会双写。

    1. status=ingesting 且 saga 心跳超时：owner 大概率已死。
       attempt < 上限 → 重置回 ready（updated_at 刷新 → 新 run_key → 自动重试）；
       attempt 达上限 → saga/session 都落 failed 终态 + alert，等人工介入。
    2. status=ready 放了很久没被拉起：典型原因是上一个 run 在 claim 之前就崩了，
       run_key 已被消费。刷新 updated_at 生成新 run_key，让普通 sensor 重新触发。
    """
    stale = fetch_all(
        """
        SELECT us.upload_id, us.manifest_op, sl.scope, sl.attempt, sl.run_id
        FROM upload_session us
        JOIN saga_log sl
          ON sl.business_id = us.upload_id AND sl.scope = 'ingest_' || us.manifest_op
        WHERE us.status = 'ingesting' AND sl.status = 'RUNNING'
          AND sl.updated_at < now() - make_interval(mins => %s)
        """,
        (settings.saga_takeover_minutes,),
    )
    requeued, exhausted = 0, 0
    for row in stale:
        if row["attempt"] < settings.saga_max_attempts:
            execute(
                "UPDATE upload_session SET status = 'ready', updated_at = now() WHERE upload_id = %s AND status = 'ingesting'",
                (row["upload_id"],),
            )
            requeued += 1
        else:
            # 达到重试上限：CAS 收尾（条件重查心跳，避免误杀刚被新 run 接管的 saga）
            execute(
                """
                UPDATE saga_log SET status = 'FAILED', error = 'stuck: 心跳超时且重试次数耗尽', updated_at = now()
                WHERE scope = %s AND business_id = %s AND status = 'RUNNING'
                  AND updated_at < now() - make_interval(mins => %s)
                """,
                (row["scope"], row["upload_id"], settings.saga_takeover_minutes),
            )
            execute(
                "UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = %s AND status = 'ingesting'",
                (row["upload_id"],),
            )
            execute(
                "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
                (
                    "error",
                    "ingest_stuck_sensor",
                    row["run_id"],
                    f"upload {row['upload_id']} 卡死且重试 {row['attempt']} 次仍失败，已转 failed，需人工介入",
                    to_json({"upload_id": row["upload_id"], "scope": row["scope"], "attempt": row["attempt"]}),
                ),
            )
            exhausted += 1

    execute(
        "UPDATE upload_session SET updated_at = now() WHERE status = 'ready' AND updated_at < now() - make_interval(mins => %s)",
        (settings.saga_takeover_minutes,),
    )

    if requeued or exhausted:
        context.log.warning("stuck sessions: requeued=%s, exhausted=%s", requeued, exhausted)
        return SkipReason(f"修复了 {requeued} 个重新入队、{exhausted} 个转 failed 的卡死会话")
    return SkipReason("没有卡死的 ingest 会话")


@sensor(job=annotation_collect_job, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def annotation_collect_sensor(context):
    """兜底：CLI 已经把结果包传到 MinIO、webhook 却因为网络问题没打成功时，
    靠轮询 `annotation_batch.status = RETURNED` 兜底唤醒 job-B（README 3.2.2）。
    """
    rows = fetch_all("SELECT batch_id FROM annotation_batch WHERE status = 'RETURNED'")
    run_requests = [RunRequest(run_key=f"collect-{r['batch_id']}", partition_key=r["batch_id"]) for r in rows]
    return run_requests
