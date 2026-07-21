"""通用业务状态、人工重试与跨 Dagster/Argo 执行对账。"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Callable

from common.config import settings
from common.db import execute, fetch_all, fetch_one, to_json, transaction
from common.execution_claim import ensure_schema as ensure_claim_schema
from common.processing_registry import ensure_schema as ensure_processing_schema

# 与 schemas/postgres_platform.sql 保持一致；模块自带幂等 DDL 让老部署
# （postgres 卷已初始化、不重跑 init 脚本）第一次 import 时也能拿到这张表。
_DDL = """
CREATE TABLE IF NOT EXISTS platform_job (
    job_id              TEXT PRIMARY KEY,
    job_type            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ready'
                        CHECK (status IN ('ready', 'running', 'done', 'failed')),
    payload             JSONB NOT NULL DEFAULT '{}',
    result              JSONB NOT NULL DEFAULT '{}',
    requested_by        TEXT,
    last_dagster_run_id TEXT,
    last_error_code     TEXT,
    last_error          TEXT,
    execution_attempt_count INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_platform_job_type_status ON platform_job (job_type, status);
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_dagster_run_id TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_error_code TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS execution_attempt_count INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_platform_job_last_run
    ON platform_job (last_dagster_run_id) WHERE last_dagster_run_id IS NOT NULL
"""

_ddl_lock = threading.Lock()
_ddl_done = False


def _ensure_table() -> None:
    global _ddl_done
    if _ddl_done:
        return
    with _ddl_lock:
        if not _ddl_done:
            execute(_DDL)
            ensure_claim_schema()
            ensure_processing_schema()
            _ddl_done = True


@dataclass(frozen=True)
class JobKind:
    """一类异步任务在通用状态机协议下的绑定描述（README 3.7.4）。

    table/id_col/状态名是**受信任的内部常量**（拼进 SQL 的只有这些字段，
    业务值全部走参数绑定）。scope_sql 是能在状态表行上下文里求值的 SQL
    表达式（如 `'ingest_' || manifest_op`），reconciliation 用它 join execution_claim。
    """

    name: str                              # 注册键，也用于日志/alert source
    table: str                             # 状态表名
    id_col: str                            # 业务主键列
    ready: str                             # 协议四状态在这张表里的实际取值
    running: str
    done: str
    failed: str
    scope_sql: str                         # execution_claim.scope 的 SQL 表达式
    emit_request: Callable[[dict], None]   # 补发触发消息（参数：状态表整行）
    type_filter_sql: str = "TRUE"          # platform_job 需要 job_type = '...'


def _upload_emit(row: dict) -> None:
    from common.kafka_ledger import emit_ingest_request

    emit_ingest_request(row["upload_id"], row["manifest_op"], row["processing_type"])


def _training_emit(row: dict) -> None:
    from common.kafka_ledger import emit_job_request

    emit_job_request(row["job_id"], "training")


UPLOAD_KIND = JobKind(
    name="upload",
    table="upload_session",
    id_col="upload_id",
    ready="ready",
    running="ingesting",  # 历史状态名，保留（改列值要迁移存量数据，不值得）
    done="done",
    failed="failed",
    scope_sql="'ingest_' || manifest_op",
    emit_request=_upload_emit,
)

TRAINING_KIND = JobKind(
    name="training",
    table="platform_job",
    id_col="job_id",
    ready="ready",
    running="running",
    done="done",
    failed="failed",
    scope_sql="'training'",
    emit_request=_training_emit,
    type_filter_sql="job_type = 'training'",
)

JOB_KINDS: list[JobKind] = [UPLOAD_KIND, TRAINING_KIND]


# ---------------------------------------------------------------------------
# platform_job 的协议原语（upload 的对应操作在 gateway/引擎里已有，走各自 SQL）
# ---------------------------------------------------------------------------

def create_job(job_type: str, payload: dict, requested_by: str | None = None) -> str:
    """落一条 ready 的任务并发触发消息，返回 job_id。"""
    _ensure_table()
    job_id = f"{job_type[:5]}-{uuid.uuid4().hex[:10]}"
    execute(
        "INSERT INTO platform_job (job_id, job_type, status, payload, requested_by) VALUES (%s, %s, 'ready', %s, %s)",
        (job_id, job_type, to_json(payload), requested_by),
    )
    kind = next((k for k in JOB_KINDS if k.name == job_type), None)
    if kind is not None:
        kind.emit_request({"job_id": job_id, "job_type": job_type})
    return job_id


def get_job(job_id: str) -> dict | None:
    _ensure_table()
    return fetch_one("SELECT * FROM platform_job WHERE job_id = %s", (job_id,))


class RetryNotAllowed(Exception):
    """任务不处于 failed，人工重试被拒绝（调用方翻译成 HTTP 409）。"""


def manual_retry(kind: JobKind, business_id: str) -> dict:
    """仅 failed → ready + Kafka；上次错误与最后 run 直接来自业务表。"""
    _ensure_table()
    row = fetch_one(
        f"SELECT * FROM {kind.table} WHERE {kind.id_col} = %s AND {kind.type_filter_sql}",
        (business_id,),
    )
    if row is None:
        raise KeyError(business_id)
    if row["status"] != kind.failed:
        raise RetryNotAllowed(f"只有 {kind.failed} 的任务可以重试，当前状态是 '{row['status']}'")
    updated = fetch_one(
        f"""
        UPDATE {kind.table} SET status = %s, updated_at = now()
        WHERE {kind.id_col} = %s AND status = %s
        RETURNING {kind.id_col}
        """,
        (kind.ready, business_id, kind.failed),
    )
    if updated is None:
        raise RetryNotAllowed("任务状态已变化，请刷新后重试")
    kind.emit_request(row)
    return {
        "id": business_id,
        "status": kind.ready,
        "previous_run_id": row.get("last_dagster_run_id"),
        "previous_error_code": row.get("last_error_code"),
        "previous_error": row.get("last_error"),
        "execution_attempt_count": row.get("execution_attempt_count", 0),
    }


# ---------------------------------------------------------------------------
# reconciliation：心跳过期只是候选条件；必须再确认 Dagster run 与 Argo
# Workflow 都不活跃，才把业务态收敛为 failed。此处绝不自动投 Kafka。
# ---------------------------------------------------------------------------

def reconciliation_pass(kind: JobKind, instance, log, *, workflow_observer=None) -> dict[str, int]:
    """核对 stale claim 的两层执行事实；确认执行消失后转 failed 并告警。"""
    if workflow_observer is None:
        from common.argo_workflows import workflow_phases_for_run

        workflow_observer = workflow_phases_for_run
    _ensure_table()
    ensure_claim_schema()
    takeover = settings.claim_takeover_minutes

    stale = fetch_all(
        f"""
        SELECT t.*, c.scope AS claim_scope, c.run_id AS claim_run_id
        FROM {kind.table} t
        JOIN execution_claim c ON c.business_id = t.{kind.id_col} AND c.scope = {kind.scope_sql}
        WHERE {kind.type_filter_sql} AND t.status = %s
          AND c.heartbeat_at < now() - make_interval(mins => %s)
        """,
        (kind.running, takeover),
    )
    counts = {"candidates": len(stale), "active": 0, "failed": 0, "observation_errors": 0}
    for row in stale:
        run_id = row["claim_run_id"]
        try:
            dagster_run = instance.get_run_by_id(run_id)
            dagster_status = (
                getattr(getattr(dagster_run, "status", None), "value", None)
                if dagster_run is not None
                else None
            )
            workflows = workflow_observer(run_id)
        except Exception as exc:  # noqa: BLE001 - 看不清执行事实时宁可保留 claim
            counts["observation_errors"] += 1
            log.exception(
                "reconciliation 无法观测 %s %s（run=%s），本轮不改状态：%s",
                kind.name,
                row[kind.id_col],
                run_id,
                exc,
            )
            continue

        dagster_active = dagster_status in {"STARTING", "STARTED", "CANCELING"}
        argo_active = any(phase not in {"Succeeded", "Failed", "Error"} for phase in workflows.values())
        if dagster_active or argo_active:
            counts["active"] += 1
            log.info(
                "reconciliation 保留活跃任务 %s %s：Dagster=%s, Argo=%s",
                kind.name,
                row[kind.id_col],
                dagster_status,
                workflows,
            )
            continue

        with transaction() as conn:
            deleted = conn.execute(
                """
                DELETE FROM execution_claim
                WHERE scope = %s AND business_id = %s AND run_id = %s
                  AND heartbeat_at < now() - make_interval(mins => %s)
                RETURNING business_id
                """,
                (row["claim_scope"], row[kind.id_col], row["claim_run_id"], takeover),
            ).fetchone()
            if deleted is None:
                continue
            message = (
                f"claim 心跳超时且执行已消失：Dagster={dagster_status or 'missing'}, "
                f"Argo={workflows or 'missing'}；系统未自动重试，请检查后人工 retry"
            )
            updated = conn.execute(
                f"""
                UPDATE {kind.table}
                SET status = %s, last_error_code = 'EXECUTION_LOST',
                    last_error = %s, updated_at = now()
                WHERE {kind.id_col} = %s AND status = %s AND last_dagster_run_id = %s
                RETURNING {kind.id_col}
                """,
                (kind.failed, message, row[kind.id_col], kind.running, run_id),
            ).fetchone()
            if updated is None:
                continue
            conn.execute(
                """
                INSERT INTO alerts (severity, source, run_id, message, context)
                VALUES ('error', %s, %s, %s, %s)
                """,
                (
                    f"{kind.name}_reconciliation",
                    run_id,
                    f"{kind.name} {row[kind.id_col]} 执行已消失",
                    to_json({
                        "id": row[kind.id_col],
                        "scope": row["claim_scope"],
                        "dagster_status": dagster_status,
                        "argo_workflows": workflows,
                        "execution_attempt_count": row.get("execution_attempt_count", 0),
                    }),
                ),
            )
            counts["failed"] += 1
            log.warning(
                "reconciliation 确认执行消失：%s %s → failed（attempt=%s）",
                kind.name,
                row[kind.id_col],
                row.get("execution_attempt_count", 0),
            )

    return counts
