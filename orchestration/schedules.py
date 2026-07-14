"""定时触发（README 4.1 / 3.6）：T+1 兜底 + 定期 compaction + 30 天保留清理。"""
from __future__ import annotations

from dagster import DefaultScheduleStatus, RunRequest, ScheduleEvaluationContext, schedule

from common.runtime_config import get_int
from orchestration.compaction import compaction_job
from orchestration.jobs import ingest_append_job, ingest_correct_job
from orchestration.retention import retention_job
from orchestration.sensors import _batch_run_request, _pending_upload_rows


def _fallback_run_requests(context: ScheduleEvaluationContext, manifest_op: str):
    """兜底触发同样按微批合并（README 3.6.2）：ready 的会话切成最多
    INGEST_BATCH_MAX 大小的批，每批一个 RunRequest。

    与 kafka sensor 的互斥：正常情况下 sensor 已经把会话拉进 ingesting/done，
    这里查 status=ready 自然查不到；就算撞上（比如 sensor 刚发出 RunRequest、
    引擎还没来得及置 ingesting），引擎侧 SagaBatch.claim_many() 的逐 upload
    CAS 也保证同一个 upload 只有一个写者（docs/saga-consistency-guide.md）。
    """
    rows = _pending_upload_rows(manifest_op)
    batch_max = get_int("INGEST_BATCH_MAX", 200)
    for i in range(0, len(rows), batch_max):
        yield _batch_run_request(manifest_op, rows[i : i + batch_max], trigger="fallback_schedule")


@schedule(
    job=ingest_append_job,
    cron_schedule="0 9 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="README 3.2.1 CRON：T+1 兜底，防止 sensor 守护进程故障导致 append 请求卡住（微批合并）",
)
def ingest_append_fallback_schedule(context: ScheduleEvaluationContext):
    yield from _fallback_run_requests(context, "append")


@schedule(
    job=ingest_correct_job,
    cron_schedule="15 9 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="README 3.2.1 CRON：T+1 兜底，防止 sensor 守护进程故障导致 correct 请求卡住（微批合并）",
)
def ingest_correct_fallback_schedule(context: ScheduleEvaluationContext):
    yield from _fallback_run_requests(context, "correct")


@schedule(
    job=compaction_job,
    cron_schedule="30 2 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="README 4.6：每天凌晨对索引表做一次 compaction，维持分区裁剪长期有效",
)
def compaction_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(
    job=retention_job,
    cron_schedule="0 4 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="README 3.6.4：每天凌晨清理 30 天前的 Dagster run 记录与 PG 终态行（事实在 Iceberg + Kafka）",
)
def retention_schedule(context: ScheduleEvaluationContext):
    return RunRequest()
