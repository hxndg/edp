"""PostgreSQL 薄执行租约。

这里只回答「当前哪个 Dagster run 有权处理这个业务 id」。Argo 保存单次
Workflow 的 task phase/exit/retry/log；业务终态写回 upload_session/platform_job。
"""
from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import dataclass

from common.config import settings
from common.db import execute, fetch_all, transaction

_DDL = """
CREATE TABLE IF NOT EXISTS execution_claim (
    scope           TEXT NOT NULL,
    business_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    heartbeat_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, business_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_claim_heartbeat ON execution_claim (heartbeat_at);
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_dagster_run_id TEXT;
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_error_code TEXT;
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_error TEXT;
CREATE INDEX IF NOT EXISTS idx_upload_session_last_run
    ON upload_session (last_dagster_run_id) WHERE last_dagster_run_id IS NOT NULL;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_dagster_run_id TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_error_code TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_error TEXT;
CREATE INDEX IF NOT EXISTS idx_platform_job_last_run
    ON platform_job (last_dagster_run_id) WHERE last_dagster_run_id IS NOT NULL;
"""

_ddl_lock = threading.Lock()
_ddl_done = False


def ensure_schema() -> None:
    global _ddl_done
    if _ddl_done:
        return
    with _ddl_lock:
        if not _ddl_done:
            execute(_DDL)
            _ddl_done = True


def _acquire(conn, scope: str, business_ids: list[str], run_id: str) -> list[str]:
    if not business_ids:
        return []
    rows = conn.execute(
        """
        INSERT INTO execution_claim (scope, business_id, run_id)
        SELECT %(scope)s, unnest(%(ids)s::text[]), %(run_id)s
        ON CONFLICT (scope, business_id) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            heartbeat_at = now()
        WHERE execution_claim.run_id = EXCLUDED.run_id
           OR execution_claim.heartbeat_at < now() - make_interval(mins => %(takeover)s)
        RETURNING business_id
        """,
        {
            "scope": scope,
            "ids": business_ids,
            "run_id": run_id,
            "takeover": settings.claim_takeover_minutes,
        },
    ).fetchall()
    return [row["business_id"] for row in rows]


def acquire_many(
    scope: str, business_ids: Iterable[str], run_id: str, *, conn=None
) -> list[str]:
    """批量 CAS claim；传 conn 可与业务状态切换放进同一事务。"""
    ensure_schema()
    ids = list(dict.fromkeys(business_ids))
    if conn is not None:
        return _acquire(conn, scope, ids, run_id)
    with transaction() as tx:
        return _acquire(tx, scope, ids, run_id)


def heartbeat_many(scope: str, business_ids: Iterable[str], run_id: str) -> list[str]:
    """续租并返回仍归当前 run 的 id；返回集同时是 fencing 检查结果。"""
    ensure_schema()
    ids = list(dict.fromkeys(business_ids))
    if not ids:
        return []
    rows = fetch_all(
        """
        UPDATE execution_claim SET heartbeat_at = now()
        WHERE scope = %(scope)s AND business_id = ANY(%(ids)s) AND run_id = %(run_id)s
        RETURNING business_id
        """,
        {"scope": scope, "ids": ids, "run_id": run_id},
    )
    return [row["business_id"] for row in rows]


def check_many(scope: str, business_ids: Iterable[str], run_id: str) -> list[str]:
    ensure_schema()
    ids = list(dict.fromkeys(business_ids))
    if not ids:
        return []
    rows = fetch_all(
        """
        SELECT business_id FROM execution_claim
        WHERE scope = %(scope)s AND business_id = ANY(%(ids)s) AND run_id = %(run_id)s
        """,
        {"scope": scope, "ids": ids, "run_id": run_id},
    )
    return [row["business_id"] for row in rows]


def release_many(
    scope: str, business_ids: Iterable[str], run_id: str, *, conn=None
) -> list[str]:
    """只释放当前 run 自己的租约；传 conn 可与最终业务状态原子提交。"""
    ensure_schema()
    ids = list(dict.fromkeys(business_ids))
    if not ids:
        return []
    sql = """
        DELETE FROM execution_claim
        WHERE scope = %(scope)s AND business_id = ANY(%(ids)s) AND run_id = %(run_id)s
        RETURNING business_id
    """
    params = {"scope": scope, "ids": ids, "run_id": run_id}
    if conn is not None:
        rows = conn.execute(sql, params).fetchall()
    else:
        rows = fetch_all(sql, params)
    return [row["business_id"] for row in rows]


@dataclass
class ClaimBatch:
    scope: str
    business_ids: list[str]
    run_id: str

    def acquire_many(self, *, conn=None) -> list[str]:
        return acquire_many(self.scope, self.business_ids, self.run_id, conn=conn)

    def heartbeat_many(self, business_ids: Iterable[str]) -> list[str]:
        return heartbeat_many(self.scope, business_ids, self.run_id)

    def check_many(self, business_ids: Iterable[str]) -> list[str]:
        return check_many(self.scope, business_ids, self.run_id)

    def release_many(self, business_ids: Iterable[str], *, conn=None) -> list[str]:
        return release_many(self.scope, business_ids, self.run_id, conn=conn)
