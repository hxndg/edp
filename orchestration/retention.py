"""数据保留（README 3.6.4）：run 记录与 PG 终态行只留 RETENTION_DAYS（默认 30 天）。

前提是 4.7 的 SoT 域划分：事实永远在 Iceberg（数据 + 审计列血缘）+ Kafka
（事件账本），Dagster 元数据和 PG platform 库都是瞬态投影，删了不丢事实——
血缘反查走 Iceberg 的 `_run_id`/`_batch_id` 审计列，不依赖 Dagster run 记录。

清理对象：
- Dagster run + event log（`dagster` 库）：删终态（成功/失败/取消）且创建时间
  超过保留期的 run，`instance.delete_run` 会连带删掉 event log；
  sensor/schedule tick 历史由 dagster.yaml 的 retention 配置负责，不在这里。
- PG platform 库：已释放的孤儿 claim、annotation_batch(DONE)、业务终态行。
- MinIO staging/ 前缀（README 3.6.3 pod fan-out 的 run↔worker 交接区）：run
  结束后有意不删（保留现场方便排查 worker 问题），staging 文件不被 Iceberg
  快照引用、对读者不存在，这里按 mtime 超过 STAGING_RETENTION_DAYS 统一清。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dagster import DagsterRunStatus, RunsFilter, job, op

from common import object_store
from common.db import fetch_all
from common.execution_claim import ensure_schema as ensure_claim_schema
from common.runtime_config import get_int
from engines.worker.staging import STAGING_ROOT

_TERMINAL_STATUSES = [
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
]

_DELETE_PAGE_SIZE = 200


@op
def purge_expired_records(context) -> dict:
    days = get_int("RETENTION_DAYS", 30)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    context.log.info("保留期 %s 天，清理 %s 之前的记录", days, cutoff.isoformat())

    # ---- Dagster run + event log：分页删，避免一次拉全量 ----
    deleted_runs = 0
    instance = context.instance
    while True:
        records = instance.get_run_records(
            filters=RunsFilter(statuses=_TERMINAL_STATUSES, created_before=cutoff),
            limit=_DELETE_PAGE_SIZE,
        )
        if not records:
            break
        for rec in records:
            instance.delete_run(rec.dagster_run.run_id)
            deleted_runs += 1
        if len(records) < _DELETE_PAGE_SIZE:
            break

    # ---- 已无对应 running 业务行的孤儿租约 ----
    ensure_claim_schema()
    deleted_claims = len(
        fetch_all(
            """
            DELETE FROM execution_claim c
            WHERE NOT EXISTS (
                SELECT 1 FROM upload_session us
                WHERE c.scope IN ('ingest_append', 'ingest_correct')
                  AND us.upload_id = c.business_id AND us.status = 'ingesting'
            )
              AND NOT EXISTS (
                SELECT 1 FROM platform_job pj
                WHERE c.scope = 'training'
                  AND pj.job_id = c.business_id AND pj.status = 'running'
            )
            RETURNING business_id
            """
        )
    )
    deleted_batches = len(
        fetch_all(
            "DELETE FROM annotation_batch WHERE status = 'DONE' AND updated_at < %s RETURNING batch_id",
            (cutoff,),
        )
    )
    # 通用状态机的终态任务行（training 等，README 3.7.4）：档案在 Iceberg ml_* 表
    from common.jobs import _ensure_table as _ensure_platform_job

    _ensure_platform_job()  # 老部署（不重跑 init 脚本）第一次清理前先幂等建表
    deleted_jobs = len(
        fetch_all(
            "DELETE FROM platform_job WHERE status IN ('done', 'failed') AND updated_at < %s RETURNING job_id",
            (cutoff,),
        )
    )
    # 还被 annotation_batch 引用的 session（LABELING 中的批次）不删，等批次 DONE 过期后一起走
    deleted_sessions = len(
        fetch_all(
            """
            DELETE FROM upload_session us
            WHERE us.status IN ('done', 'failed') AND us.updated_at < %s
              AND NOT EXISTS (SELECT 1 FROM annotation_batch ab WHERE ab.upload_id = us.upload_id)
            RETURNING upload_id
            """,
            (cutoff,),
        )
    )

    # ---- MinIO staging 前缀（worker 交接区）：按对象 mtime 清 ----
    staging_days = get_int("STAGING_RETENTION_DAYS", 7)
    staging_cutoff = datetime.now(timezone.utc) - timedelta(days=staging_days)
    stale_keys = [
        key
        for key, mtime in object_store.list_prefix_with_mtime(f"{STAGING_ROOT}/")
        if mtime < staging_cutoff
    ]
    deleted_staging = object_store.delete_keys(stale_keys) if stale_keys else 0

    summary = {
        "retention_days": days,
        "deleted_dagster_runs": deleted_runs,
        "deleted_execution_claims": deleted_claims,
        "deleted_annotation_batches": deleted_batches,
        "deleted_upload_sessions": deleted_sessions,
        "deleted_platform_jobs": deleted_jobs,
        "deleted_staging_objects": deleted_staging,
    }
    context.log.info("retention 清理完成：%s", summary)
    return summary


@job(description="README 3.6.4：清理超过保留期的 Dagster run 记录与 PG 终态行（事实在 Iceberg + Kafka，删了不丢）")
def retention_job():
    purge_expired_records()
