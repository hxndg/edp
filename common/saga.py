"""Saga 执行外壳：给"一次业务操作 = 多次 Iceberg commit"的引擎流程补事务语义。

背景（docs/saga-consistency-guide.md 有完整讨论）：
- Iceberg 单表 commit 是原子的，但 ingest 一次要写多张表（raw_file/episode/
  episode_file/bronze/silver/sample/gold），中途崩溃会留下"写了一半"的状态；
- 触发侧有 sensor + 定时兜底 + stuck 重试等多条路径，同一个 upload_id 可能被
  并发拉起两个 run，必须保证同一时刻只有一个 run 在写。

这里的实现是"前向恢复型 Saga"：不做补偿回滚（所有写入本身幂等可重写），
只负责三件事：
1. **互斥抢占（claim）**：Postgres 上对 (scope, business_id) 做 CAS——只有
   "没人在跑 / 上一次已终结 / 上一个 owner 心跳超时"三种情况能抢到；抢不到
   直接抛 SagaConflictError，本 run 立即放弃，绝不双写。
2. **步骤日志 + 心跳（advance）**：每完成一个阶段就推进 step 并刷新
   updated_at；updated_at 同时是心跳，stuck sensor 靠它判断 owner 是否已死。
   advance 时带 run_id 做 fencing：如果发现自己已经被接管（另一个 run 抢走了
   saga），当场抛 SagaOwnershipLostError 自杀，把世界留给新 owner。
3. **显式终态（succeed / fail）**：成功/失败都落一条明确的记录，下游按终态
   过滤，"ingesting 悬空"不再是不可判定状态。
"""
from __future__ import annotations

import threading

from common.config import settings
from common.db import execute, fetch_all, fetch_one

# 与 schemas/postgres_platform.sql 保持一致；这里再执行一遍是为了让"postgres 卷
# 已经初始化过、不会重跑 init 脚本"的老部署也能拿到这张表（CREATE IF NOT EXISTS 幂等）。
_DDL = """
CREATE TABLE IF NOT EXISTS saga_log (
    scope               TEXT NOT NULL,
    business_id         TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    step                TEXT NOT NULL DEFAULT 'CLAIM',
    attempt             INT  NOT NULL DEFAULT 1,
    error               TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, business_id)
)
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


class SagaConflictError(RuntimeError):
    """另一个 run 正持有这个 saga（RUNNING 且心跳未超时），本 run 应立即放弃。"""


class SagaOwnershipLostError(RuntimeError):
    """本 run 曾持有 saga，但已被新 owner 接管（fencing 失败），应立即中止。"""


class Saga:
    """一个 (scope, business_id) 上的 Saga 句柄。用法：

        saga = Saga("ingest_append", upload_id, run_id)
        saga.claim()                  # 抢不到抛 SagaConflictError
        try:
            saga.advance("INDEX")     # 每个阶段推进一步（兼作心跳 + fencing 检查）
            ...
            saga.advance("SAMPLES")
        except Exception as e:
            saga.fail(str(e))         # 落 FAILED 终态（若已被接管则静默放弃）
            raise
        saga.succeed()                # 落 SUCCEEDED 终态
    """

    def __init__(self, scope: str, business_id: str, run_id: str):
        self.scope = scope
        self.business_id = business_id
        self.run_id = run_id
        self.attempt: int | None = None
        _ensure_table()

    def claim(self) -> int:
        """CAS 抢占。三种情况能抢到：从未有人跑过 / 上一次已终结（重跑、重试）/
        上一个 owner 的心跳超过 SAGA_TAKEOVER_MINUTES（视为已死，接管）。
        RUNNING 且心跳新鲜 → 抛 SagaConflictError（典型场景：sensor 与定时兜底
        同时触发、或 stuck 重试与"其实还活着"的旧 run 撞车）。返回 attempt 序号。
        """
        row = fetch_one(
            """
            INSERT INTO saga_log (scope, business_id, run_id, status, step, attempt)
            VALUES (%(scope)s, %(bid)s, %(rid)s, 'RUNNING', 'CLAIM', 1)
            ON CONFLICT (scope, business_id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                status = 'RUNNING',
                step = 'CLAIM',
                attempt = saga_log.attempt + 1,
                error = NULL,
                started_at = now(),
                updated_at = now()
            WHERE saga_log.status <> 'RUNNING'
               OR saga_log.updated_at < now() - make_interval(mins => %(takeover)s)
            RETURNING attempt
            """,
            {
                "scope": self.scope,
                "bid": self.business_id,
                "rid": self.run_id,
                "takeover": settings.saga_takeover_minutes,
            },
        )
        if row is None:
            raise SagaConflictError(
                f"saga ({self.scope}, {self.business_id}) 正被另一个活跃 run 持有，"
                f"本 run {self.run_id} 放弃执行（这是并发触发下的预期行为，不是故障）"
            )
        self.attempt = row["attempt"]
        return self.attempt

    def advance(self, step: str) -> None:
        """推进步骤 + 刷新心跳。WHERE 带 run_id：一旦被新 owner 接管，本 run 的
        advance 影响 0 行 → 抛 SagaOwnershipLostError，让旧 run（zombie）尽早自杀。
        """
        row = fetch_one(
            """
            UPDATE saga_log SET step = %(step)s, updated_at = now()
            WHERE scope = %(scope)s AND business_id = %(bid)s
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING step
            """,
            {"step": step, "scope": self.scope, "bid": self.business_id, "rid": self.run_id},
        )
        if row is None:
            raise SagaOwnershipLostError(
                f"saga ({self.scope}, {self.business_id}) 已被其他 run 接管，"
                f"本 run {self.run_id} 在推进到 {step} 前中止"
            )

    def succeed(self) -> None:
        row = fetch_one(
            """
            UPDATE saga_log SET status = 'SUCCEEDED', step = 'COMMIT', updated_at = now()
            WHERE scope = %(scope)s AND business_id = %(bid)s
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING step
            """,
            {"scope": self.scope, "bid": self.business_id, "rid": self.run_id},
        )
        if row is None:
            raise SagaOwnershipLostError(
                f"saga ({self.scope}, {self.business_id}) 在最终提交前被接管，"
                f"本 run {self.run_id} 的结果以新 owner 为准"
            )

    def fail(self, error: str) -> bool:
        """落 FAILED 终态。返回 True 表示本 run 仍是 owner、终态写入成功；
        返回 False 表示已被接管（新 owner 正在重跑），本 run 不应再改任何状态。
        """
        row = fetch_one(
            """
            UPDATE saga_log SET status = 'FAILED', error = %(err)s, updated_at = now()
            WHERE scope = %(scope)s AND business_id = %(bid)s
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING step
            """,
            {"err": error[:2000], "scope": self.scope, "bid": self.business_id, "rid": self.run_id},
        )
        return row is not None


class SagaBatch:
    """同一 scope 下一批 business_id 的批量 Saga 句柄（README 3.6.3）。

    **锁粒度仍然是单个 business_id（upload_id）**——批次不是稳定的业务身份
    （重试时批次重组），互斥必须锁在稳定身份上。批量只体现在执行形态：
    每个阶段用一条 SQL 对整批刷 step/心跳，语义与单条版 `Saga` 完全一致。

    用法（engines/spark/ingest_append.py::run_batch）：

        batch = SagaBatch("ingest_append", upload_ids, run_id)
        claimed = batch.claim_many()          # 抢到的才处理，没抢到的跳过
        alive = batch.advance_many("INDEX", claimed)   # 返回仍归本 run 的子集
        batch.fail_one(uid, "...")            # 逐条失败隔离，不拖垮同批
        batch.succeed_many(alive)
    """

    def __init__(self, scope: str, business_ids: list[str], run_id: str):
        self.scope = scope
        self.business_ids = list(business_ids)
        self.run_id = run_id
        self.attempts: dict[str, int] = {}
        _ensure_table()

    def claim_many(self) -> list[str]:
        """批量 CAS 抢占，一条 SQL。可抢占条件与 `Saga.claim` 相同（从未跑过 /
        已终结 / 心跳超时），返回**抢到的子集**；没抢到的说明另一个 run 正持有
        （并发批次撞车是预期行为），调用方直接跳过它们，不算错误。
        """
        if not self.business_ids:
            return []
        rows = fetch_all(
            """
            INSERT INTO saga_log (scope, business_id, run_id, status, step, attempt)
            SELECT %(scope)s, unnest(%(bids)s::text[]), %(rid)s, 'RUNNING', 'CLAIM', 1
            ON CONFLICT (scope, business_id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                status = 'RUNNING',
                step = 'CLAIM',
                attempt = saga_log.attempt + 1,
                error = NULL,
                started_at = now(),
                updated_at = now()
            WHERE saga_log.status <> 'RUNNING'
               OR saga_log.updated_at < now() - make_interval(mins => %(takeover)s)
            RETURNING business_id, attempt
            """,
            {
                "scope": self.scope,
                "bids": self.business_ids,
                "rid": self.run_id,
                "takeover": settings.saga_takeover_minutes,
            },
        )
        self.attempts = {r["business_id"]: r["attempt"] for r in rows}
        return [r["business_id"] for r in rows]

    def advance_many(self, step: str, business_ids: list[str]) -> list[str]:
        """批量推进步骤 + 刷新心跳，一条 SQL。WHERE 带 run_id 做 fencing：
        返回**仍归本 run 所有**的子集；没返回的已被新 owner 接管，调用方应把
        它们从后续所有写入中剔除（数据世界留给新 owner）。
        """
        if not business_ids:
            return []
        rows = fetch_all(
            """
            UPDATE saga_log SET step = %(step)s, updated_at = now()
            WHERE scope = %(scope)s AND business_id = ANY(%(bids)s)
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING business_id
            """,
            {"step": step, "scope": self.scope, "bids": list(business_ids), "rid": self.run_id},
        )
        return [r["business_id"] for r in rows]

    def succeed_many(self, business_ids: list[str]) -> list[str]:
        """批量落 SUCCEEDED 终态，返回实际写入终态的子集（被接管的除外）。"""
        if not business_ids:
            return []
        rows = fetch_all(
            """
            UPDATE saga_log SET status = 'SUCCEEDED', step = 'COMMIT', updated_at = now()
            WHERE scope = %(scope)s AND business_id = ANY(%(bids)s)
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING business_id
            """,
            {"scope": self.scope, "bids": list(business_ids), "rid": self.run_id},
        )
        return [r["business_id"] for r in rows]

    def fail_one(self, business_id: str, error: str) -> bool:
        """单条落 FAILED 终态（逐条失败隔离：某个 upload 的毒数据不拖垮同批）。
        返回 False 表示已被接管，本 run 不应再改它的任何状态。
        """
        row = fetch_one(
            """
            UPDATE saga_log SET status = 'FAILED', error = %(err)s, updated_at = now()
            WHERE scope = %(scope)s AND business_id = %(bid)s
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING step
            """,
            {"err": error[:2000], "scope": self.scope, "bid": business_id, "rid": self.run_id},
        )
        return row is not None

    def fail_many(self, business_ids: list[str], error: str) -> list[str]:
        """批量落 FAILED（整批级异常，如 Iceberg commit 失败时兜底收尾）。"""
        if not business_ids:
            return []
        rows = fetch_all(
            """
            UPDATE saga_log SET status = 'FAILED', error = %(err)s, updated_at = now()
            WHERE scope = %(scope)s AND business_id = ANY(%(bids)s)
              AND run_id = %(rid)s AND status = 'RUNNING'
            RETURNING business_id
            """,
            {"err": error[:2000], "scope": self.scope, "bids": list(business_ids), "rid": self.run_id},
        )
        return [r["business_id"] for r in rows]


def uncommitted_episode_ids() -> list[str]:
    """下游读侧过滤用：返回"业务上尚未 COMMIT"的 episode_id 列表。

    - append 类上传：episode_id 确定为 `ep-{upload_id}`，只要 session 还没到
      done，这个 episode 的数据都可能是半成品（或失败残留），下游不应消费；
    - correct 类上传：目标 episode 在 manifest 里声明；只有进入 ingesting /
      failed 之后旧数据才可能被动过（ready 之前引擎还没碰过表，旧数据仍然完整
      可用，不用隔离）。
    """
    _ensure_table()
    rows = fetch_all(
        """
        SELECT 'ep-' || upload_id AS episode_id
        FROM upload_session
        WHERE manifest_op = 'append' AND status <> 'done'
        UNION
        SELECT manifest ->> 'episode_id' AS episode_id
        FROM upload_session
        WHERE manifest_op = 'correct'
          AND status IN ('ingesting', 'failed')
          AND manifest ? 'episode_id'
        """
    )
    return [r["episode_id"] for r in rows if r["episode_id"]]
