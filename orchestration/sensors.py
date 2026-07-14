"""触发层（README 2.3 / 4.1 / 3.6，云上形态见 docs/saga-consistency-guide.md 讨论）。

ingest 的实时触发消费 Kafka（方案 A）：gateway 提交 manifest 后向
`edp.ingest.requests` 发一条 ingest.requested，`ingest_kafka_sensor` 消费它。

微批 + 背压（README 3.6.2）：sensor 每个 tick 先做在跑批次背压检查（达上限
本轮不消费，消息留在 Kafka 排队），然后拉最多 INGEST_BATCH_MAX 条消息、按
manifest_op 分成至多两组（append/correct 是两个 job、两套引擎、两个 saga
scope），**每组 = 一个批次 = 一个 RunRequest = 一个 run pod**，`run_config`
携带该组的 upload_ids 列表。run 数量由 tick 间隔与批次上限决定，与上传量解耦。

可靠性模型（至少一次触发 + 三层去重/互斥）：
- offset 存在 Dagster sensor cursor 里，和 RunRequest 的提交一起持久化，
  不用 Kafka 自身的 group commit——避免"offset 提交了、run 没发出去"的缝隙；
- 消息重复/重放：sensor 先查 PG，只有 status=ready 才进批（廉价跳过）；
  再有 run_key（批内容摘要）去重；最终引擎侧 SagaBatch.claim_many() 的
  逐 upload CAS 保证同一 upload 只有一个写者；
- Kafka 消息丢了/发失败：T+1 兜底 schedule 仍然轮询 PG（见 schedules.py），
  ready 的会话最迟第二天早上被补触发（同样按微批合并）。
"""
from __future__ import annotations

import hashlib
import json

from dagster import DagsterRunStatus, DefaultSensorStatus, RunRequest, RunsFilter, SensorResult, SkipReason, sensor

from common.config import settings
from common.db import execute, fetch_all, to_json
from common.runtime_config import get_int
from orchestration.jobs import annotation_collect_job, ingest_append_job, ingest_correct_job

INGEST_JOB_NAMES = ("ingest_append_job", "ingest_correct_job")

# "在跑"= 已提交但还没到终态：排队中 + 启动中 + 执行中
_INFLIGHT_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]


def _pending_upload_rows(manifest_op: str) -> list[dict]:
    return fetch_all(
        "SELECT upload_id, updated_at FROM upload_session WHERE status = 'ready' AND manifest_op = %s ORDER BY created_at",
        (manifest_op,),
    )


def _batch_run_key(manifest_op: str, rows: list[dict]) -> str:
    """run_key = op + 批内容摘要（upload_id + updated_at 的有序哈希）。

    带上 updated_at 的意义（docs/saga-consistency-guide.md）：上一个 run 崩溃后
    stuck sensor 把会话重置回 ready 时会刷新 updated_at，重组出的批才有新
    run_key，能触发新 run。批次成员不同 → run_key 不同，所以跨触发路径
    （kafka sensor / T+1 兜底）的 run_key 去重只挡"完全相同的批"；真正的
    互斥兜底在引擎侧 SagaBatch.claim_many() 的逐 upload CAS。
    """
    digest = hashlib.sha256(
        "|".join(sorted(f"{r['upload_id']}:{int(r['updated_at'].timestamp())}" for r in rows)).encode()
    ).hexdigest()
    return f"{manifest_op}-batch-{digest[:16]}"


def _batch_run_request(manifest_op: str, rows: list[dict], trigger: str) -> RunRequest:
    """一个批次 → 一个 RunRequest（README 3.6.2）：upload_ids 走 run_config，
    批大小/触发路径打进 tags（UI 可搜）。"""
    upload_ids = [r["upload_id"] for r in rows]
    job_name = "ingest_append_job" if manifest_op == "append" else "ingest_correct_job"
    return RunRequest(
        job_name=job_name,
        run_key=_batch_run_key(manifest_op, rows),
        run_config={
            "ops": {
                "ingest_multi_asset": {
                    "config": {"upload_ids": upload_ids, "manifest_op": manifest_op}
                }
            },
            # run pod 内步骤串行执行：ingest 链路本身接近线性（entity_tag 与
            # prelabel 是仅有的并行位），串行几乎不损吞吐，却把 Spark/Ray/DuckDB
            # 同时起在一个 pod 里的峰值内存压力砍半——批的并行度由"多个批次
            # 多个 pod"承担（3.6.2 背压控制），不靠 pod 内多进程。
            "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
        },
        tags={"trigger": trigger, "manifest_op": manifest_op, "batch_size": str(len(upload_ids))},
    )


def _inflight_ingest_batches(instance) -> int:
    return sum(
        instance.get_runs_count(RunsFilter(job_name=name, statuses=_INFLIGHT_STATUSES))
        for name in INGEST_JOB_NAMES
    )


def _consume_ingest_requests(cursor: dict[str, int], max_messages: int) -> tuple[list[dict], dict[str, int]]:
    """从 cursor 记录的 offset 开始消费最多 max_messages 条 ingest.requested 消息。

    返回 (消息列表, 新 cursor)。cursor 形如 {"0": 42} —— partition 号 → 下一条
    要读的 offset。不用 Kafka consumer group：offset 的"提交"就是 Dagster 持久化
    sensor cursor 这个动作本身，与 RunRequest 提交原子。
    """
    from kafka import KafkaConsumer, TopicPartition

    topic = settings.kafka_ingest_topic
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=3000,
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return [], cursor  # topic 还没被创建（第一条消息发出时自动创建）

        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(tps)
        for tp in tps:
            if str(tp.partition) in cursor:
                consumer.seek(tp, cursor[str(tp.partition)])
            else:
                consumer.seek_to_beginning(tp)  # 首次启动从头读：老消息会被 status 校验廉价跳过

        messages: list[dict] = []
        new_cursor = dict(cursor)
        batches = consumer.poll(timeout_ms=2000, max_records=max_messages)
        for tp, records in batches.items():
            for rec in records:
                messages.append(rec.value)
                new_cursor[str(tp.partition)] = rec.offset + 1
        return messages, new_cursor
    finally:
        consumer.close()


@sensor(
    jobs=[ingest_append_job, ingest_correct_job],
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="消费 Kafka edp.ingest.requests，微批合并 + 在跑批次背压，按 manifest_op 分批拉起 ingest job（README 3.6.2）",
)
def ingest_kafka_sensor(context):
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}

    # 1. 运行时配置（PG runtime_config，UPDATE 后下个 tick 生效）
    batch_max = get_int("INGEST_BATCH_MAX", 200)
    max_inflight = get_int("INGEST_MAX_INFLIGHT_BATCHES", 3)

    # 2. 源头背压：在跑批次达上限 → 本轮不消费，offset 不动，积压留在 Kafka
    #    （最擅长积压的地方），不产生 run 记录、不写 PG。
    inflight = _inflight_ingest_batches(context.instance)
    if inflight >= max_inflight:
        return SkipReason(f"背压：{inflight} 个 ingest 批次在跑（上限 {max_inflight}），本轮不消费 Kafka")

    # 3. 拉最多 batch_max 条消息
    try:
        messages, new_cursor = _consume_ingest_requests(cursor, batch_max)
    except Exception as e:  # noqa: BLE001 - Kafka 不可达时跳过本轮，下一轮重试；兜底 schedule 仍在
        return SkipReason(f"kafka 不可达，跳过本轮：{type(e).__name__}: {e}")

    if not messages:
        return SensorResult(run_requests=[], cursor=json.dumps(new_cursor))

    # 4. 校验 status=ready（PG 是状态真相，done/failed/ingesting 的重放消息廉价
    #    跳过），一次批量查询；按 manifest_op 分组
    requested_ids: list[str] = []
    seen: set[str] = set()
    for msg in messages:
        upload_id = msg.get("payload", {}).get("upload_id")
        if upload_id and upload_id not in seen:
            seen.add(upload_id)
            requested_ids.append(upload_id)

    ready_rows = fetch_all(
        "SELECT upload_id, manifest_op, updated_at FROM upload_session WHERE upload_id = ANY(%s) AND status = 'ready'",
        (requested_ids,),
    ) if requested_ids else []
    skipped = len(requested_ids) - len(ready_rows)
    if skipped:
        context.log.info("跳过 %s 条非 ready 的重放/过期消息", skipped)

    groups: dict[str, list[dict]] = {}
    for row in ready_rows:
        groups.setdefault(row["manifest_op"], []).append(row)

    # 5. 每组（至多两组：append/correct）= 一个批次 = 一个 RunRequest = 一个 run pod
    run_requests = [_batch_run_request(op, rows, trigger="kafka") for op, rows in groups.items()]
    if run_requests:
        context.log.info(
            "微批触发：%s",
            ", ".join(f"{rr.tags['manifest_op']}×{rr.tags['batch_size']}" for rr in run_requests),
        )
    return SensorResult(run_requests=run_requests, cursor=json.dumps(new_cursor))


@sensor(
    job=ingest_append_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Saga 卡死看护：心跳超时的 ingesting 会话重新入队（有次数上限），超限转 failed + alert",
)
def ingest_stuck_sensor(context):
    """处理"run 挂了、状态悬在 ingesting"的场景（docs/saga-consistency-guide.md）。

    本 sensor 不直接发 RunRequest，只做两类状态修复，修复后向 Kafka 补发一条
    ingest.requested（由 ingest_kafka_sensor 按新 run_key 重新拉起）；即使这里的
    判断和一个"其实还活着"的旧 run 撞车，新 run 的 saga.claim() CAS 也只允许
    一个写者，不会双写。补发的 Kafka 消息丢了也没关系，T+1 兜底 schedule 轮询
    PG 会补上。

    1. status=ingesting 且 saga 心跳超时：owner 大概率已死。
       attempt < 上限 → 重置回 ready（updated_at 刷新 → 新 run_key）+ 补发触发事件；
       attempt 达上限 → saga/session 都落 failed 终态 + alert，等人工介入。
    2. status=ready 放了很久没被拉起：典型原因是触发消息丢了，或上一个 run 在
       claim 之前就崩了、run_key 已被消费。刷新 updated_at 生成新 run_key 并补发
       触发事件。
    """
    from common.kafka_ledger import emit_ingest_request
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
            emit_ingest_request(row["upload_id"], row["manifest_op"])
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

    # ready 悬置太久：刷新 updated_at（→ 新 run_key）并补发触发事件
    dangling = fetch_all(
        """
        UPDATE upload_session SET updated_at = now()
        WHERE status = 'ready' AND updated_at < now() - make_interval(mins => %s)
        RETURNING upload_id, manifest_op
        """,
        (settings.saga_takeover_minutes,),
    )
    for row in dangling:
        emit_ingest_request(row["upload_id"], row["manifest_op"])

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
    return [
        RunRequest(
            run_key=f"collect-{r['batch_id']}",
            run_config={"ops": {"annotation_collect": {"config": {"batch_id": r["batch_id"]}}}},
            tags={"batch_id": r["batch_id"]},
        )
        for r in rows
    ]
