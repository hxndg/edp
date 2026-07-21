"""业务执行对账：核对 PostgreSQL claim、Dagster run 与 Argo Workflow。"""
from __future__ import annotations

from dagster import job, op

from common.jobs import JOB_KINDS, reconciliation_pass


@op
def reconcile_execution_state(context) -> dict[str, dict[str, int]]:
    summaries: dict[str, dict[str, int]] = {}
    for kind in JOB_KINDS:
        summaries[kind.name] = reconciliation_pass(kind, context.instance, context.log)
    context.log.info("execution reconciliation 完成：%s", summaries)
    return summaries


@job(description="核对 stale execution claim；只落 failed + alert，不自动创建 run 或投 Kafka")
def reconciliation_job():
    reconcile_execution_state()
