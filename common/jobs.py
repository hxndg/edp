"""通用异步任务状态机（README 3.1.2.1 / 3.7.4）。

`upload_session` 验证过的状态机语义（ready → running → done/failed + saga 互斥
+ 心跳看护 + 按 error_code 重试）是所有异步任务共同需要的**协议**，不该每来
一类新任务就克隆一张表 + 一个 stuck sensor。这里把协议抽成一份实现，把
"每类任务不一样的部分"收进 `JobKind` 注册项：

- **状态存哪**：upload 绑定既有的 `upload_session`（表结构一字不动，连
  `ingesting` 这个历史状态名都保留）；training 及未来类型统一落 `platform_job`
  （job_type 区分，payload/result JSONB 装类型专属字段，协议层不解释）。
- **saga scope 怎么拼**：upload 是 `'ingest_' || manifest_op`（append/correct
  两个 scope），training 是常量 `'training'`。
- **触发消息发哪**：upload 发 `edp.ingest.requests`，training 发
  `edp.jobs.requests`；watchdog 修复状态后补发的就是这条。
- **退避读哪个配置键**：INGEST_RETRY_BACKOFF_MINUTES / TRAIN_RETRY_BACKOFF_MINUTES。

新增一类任务 = 注册一条 `JobKind` + 写它的 run/worker 引擎；看护
（`watchdog_pass`）与人工重试（`manual_retry`）自动覆盖，不新增表、不新增 sensor。

判断"要不要用这套协议"：任务是否异步、是否可能失败重试、是否要防并发双写。
满足就注册；一次性同步操作（如模型 promote）不需要状态机，直接做。
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Callable

from common.config import settings
from common.db import execute, fetch_all, fetch_one, to_json
from common.errors import Retry, retry_policy
from common.runtime_config import get_int

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
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_platform_job_type_status ON platform_job (job_type, status)
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
            _ddl_done = True


@dataclass(frozen=True)
class JobKind:
    """一类异步任务在通用状态机协议下的绑定描述（README 3.7.4）。

    table/id_col/状态名是**受信任的内部常量**（拼进 SQL 的只有这些字段，
    业务值全部走参数绑定）。scope_sql 是能在状态表行上下文里求值的 SQL
    表达式（如 `'ingest_' || manifest_op`），watchdog 用它 join saga_log。
    """

    name: str                              # 注册键，也用于日志/alert source
    table: str                             # 状态表名
    id_col: str                            # 业务主键列
    ready: str                             # 协议四状态在这张表里的实际取值
    running: str
    done: str
    failed: str
    scope_sql: str                         # saga_log.scope 的 SQL 表达式
    backoff_key: str                       # failed 自动重试的退避配置键
    emit_request: Callable[[dict], None]   # 补发触发消息（参数：状态表整行）
    type_filter_sql: str = "TRUE"          # platform_job 需要 job_type = '...'


def _upload_emit(row: dict) -> None:
    from common.kafka_ledger import emit_ingest_request

    emit_ingest_request(row["upload_id"], row["manifest_op"])


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
    backoff_key="INGEST_RETRY_BACKOFF_MINUTES",
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
    backoff_key="TRAIN_RETRY_BACKOFF_MINUTES",
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
    """人工重试（协议的一部分，README 3.7.4）：仅 failed → ready + 补发触发消息。

    done 没什么可重试的；ready/running 说明系统正在处理，人工插手只会制造并发。
    重置 updated_at → 新 run_key；引擎侧 saga claim 的 attempt 继续累加，OOM 类
    失败重试时 worker 内存自动升档。返回上一次失败的 saga 信息供响应体展示。
    """
    _ensure_table()
    row = fetch_one(
        f"SELECT *, {kind.scope_sql} AS saga_scope FROM {kind.table} "
        f"WHERE {kind.id_col} = %s AND {kind.type_filter_sql}",
        (business_id,),
    )
    if row is None:
        raise KeyError(business_id)
    if row["status"] != kind.failed:
        raise RetryNotAllowed(f"只有 {kind.failed} 的任务可以重试，当前状态是 '{row['status']}'")
    last_error = fetch_one(
        "SELECT error_code, error, attempt FROM saga_log WHERE business_id = %s AND scope = %s",
        (business_id, row["saga_scope"]),
    )
    execute(
        f"UPDATE {kind.table} SET status = %s, updated_at = now() WHERE {kind.id_col} = %s AND status = %s",
        (kind.ready, business_id, kind.failed),
    )
    kind.emit_request(row)
    return {
        "id": business_id,
        "status": kind.ready,
        "previous_attempt": last_error["attempt"] if last_error else None,
        "previous_error_code": last_error["error_code"] if last_error else None,
        "previous_error": last_error["error"] if last_error else None,
    }


# ---------------------------------------------------------------------------
# watchdog：原 ingest_stuck_sensor 的三类修复，对 JobKind 泛化成一份实现
# ---------------------------------------------------------------------------

def watchdog_pass(kind: JobKind, log) -> dict[str, int]:
    """对一类任务跑一轮看护，返回 {"requeued": n, "exhausted": n, "retried": n, "dangling": n}。

    修复动作只是"重置状态 + 补发 Kafka 触发消息"，真正的互斥由引擎侧
    saga claim 的 CAS 保证——即使这里和一个"其实还活着"的旧 run 撞车，
    新 run 也抢不到锁，不会双写。补发的消息丢了也没关系（upload 有 T+1
    兜底 schedule；training 下一轮 watchdog 的 dangling 修复会再补）。

    1. running 且 saga 心跳超时：owner 大概率已死。attempt < 上限 → 重置回
       ready（updated_at 刷新 → 新 run_key）+ 补发；达上限 → saga/状态表都落
       failed 终态（STUCK_EXHAUSTED）+ alert，等人工。
    2. ready 悬置太久：触发消息丢了或上一个 run 在 claim 前就崩了。刷新
       updated_at 生成新 run_key 并补发。
    3. failed 且按码可自动重试（common/errors.py RETRY_POLICY）：退避到期后
       重置回 ready。NOT_RETRYABLE（数据问题）不碰——等人工修数据后走
       manual_retry 或重传。
    """
    _ensure_table()
    takeover = settings.saga_takeover_minutes

    # ---- 1. running 卡死（saga 心跳超时）----
    stale = fetch_all(
        f"""
        SELECT t.*, sl.scope AS saga_scope, sl.attempt, sl.run_id AS saga_run_id
        FROM {kind.table} t
        JOIN saga_log sl ON sl.business_id = t.{kind.id_col} AND sl.scope = {kind.scope_sql}
        WHERE {kind.type_filter_sql} AND t.status = %s AND sl.status = 'RUNNING'
          AND sl.updated_at < now() - make_interval(mins => %s)
        """,
        (kind.running, takeover),
    )
    requeued, exhausted = 0, 0
    for row in stale:
        if row["attempt"] < settings.saga_max_attempts:
            execute(
                f"UPDATE {kind.table} SET status = %s, updated_at = now() WHERE {kind.id_col} = %s AND status = %s",
                (kind.ready, row[kind.id_col], kind.running),
            )
            kind.emit_request(row)
            requeued += 1
        else:
            # 达到重试上限：CAS 收尾（条件重查心跳，避免误杀刚被新 run 接管的 saga）
            execute(
                """
                UPDATE saga_log SET status = 'FAILED', error_code = 'STUCK_EXHAUSTED',
                       error = 'stuck: 心跳超时且重试次数耗尽', updated_at = now()
                WHERE scope = %s AND business_id = %s AND status = 'RUNNING'
                  AND updated_at < now() - make_interval(mins => %s)
                """,
                (row["saga_scope"], row[kind.id_col], takeover),
            )
            execute(
                f"UPDATE {kind.table} SET status = %s, updated_at = now() WHERE {kind.id_col} = %s AND status = %s",
                (kind.failed, row[kind.id_col], kind.running),
            )
            execute(
                "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
                (
                    "error",
                    f"{kind.name}_watchdog",
                    row["saga_run_id"],
                    f"{kind.name} {row[kind.id_col]} 卡死且重试 {row['attempt']} 次仍失败，已转 failed，需人工介入",
                    to_json({"id": row[kind.id_col], "scope": row["saga_scope"], "attempt": row["attempt"]}),
                ),
            )
            exhausted += 1

    # ---- 2. ready 悬置太久：刷新 updated_at（→ 新 run_key）并补发触发消息 ----
    dangling = fetch_all(
        f"""
        UPDATE {kind.table} SET updated_at = now()
        WHERE {kind.type_filter_sql} AND status = %s AND updated_at < now() - make_interval(mins => %s)
        RETURNING *
        """,
        (kind.ready, takeover),
    )
    for row in dangling:
        kind.emit_request(row)

    # ---- 3. failed 且按码可自动重试：退避到期后重置回 ready ----
    backoff = get_int(kind.backoff_key, 5)
    failed_rows = fetch_all(
        f"""
        SELECT t.*, sl.error_code, sl.attempt
        FROM {kind.table} t
        JOIN saga_log sl ON sl.business_id = t.{kind.id_col} AND sl.scope = {kind.scope_sql}
        WHERE {kind.type_filter_sql} AND t.status = %s AND sl.status = 'FAILED'
          AND sl.attempt < %s
          AND sl.updated_at < now() - make_interval(mins => %s)
        """,
        (kind.failed, settings.saga_max_attempts, backoff),
    )
    retried = 0
    for row in failed_rows:
        if retry_policy(row["error_code"]) == Retry.NOT_RETRYABLE:
            continue
        execute(
            f"UPDATE {kind.table} SET status = %s, updated_at = now() WHERE {kind.id_col} = %s AND status = %s",
            (kind.ready, row[kind.id_col], kind.failed),
        )
        kind.emit_request(row)
        log.info(
            "自动重试 %s %s（error_code=%s, 下一次是第 %s 次尝试）",
            kind.name, row[kind.id_col], row["error_code"], row["attempt"] + 1,
        )
        retried += 1

    return {"requeued": requeued, "exhausted": exhausted, "retried": retried, "dangling": len(dangling)}
