"""定时触发（README 4.1）：T+1 兜底 + 定期 compaction。"""
from __future__ import annotations

from dagster import DefaultScheduleStatus, RunRequest, ScheduleEvaluationContext, schedule

from orchestration.compaction import compaction_job
from orchestration.jobs import ingest_append_job, ingest_correct_job
from orchestration.sensors import _pending_upload_ids


def _fallback_run_requests(context: ScheduleEvaluationContext, manifest_op: str):
    for upload_id in _pending_upload_ids(manifest_op):
        # run_key 跟 sensor 用同一个命名规则：即使 sensor 已经先一步触发过，
        # Dagster 也会因为 run_key 重复而跳过，天然幂等，不会重复处理。
        yield RunRequest(run_key=f"{manifest_op}-{upload_id}", partition_key=upload_id)


@schedule(
    job=ingest_append_job,
    cron_schedule="0 9 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="README 3.2.1 CRON：T+1 兜底，防止 sensor 守护进程故障导致 append 请求卡住",
)
def ingest_append_fallback_schedule(context: ScheduleEvaluationContext):
    yield from _fallback_run_requests(context, "append")


@schedule(
    job=ingest_correct_job,
    cron_schedule="15 9 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="README 3.2.1 CRON：T+1 兜底，防止 sensor 守护进程故障导致 correct 请求卡住",
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
