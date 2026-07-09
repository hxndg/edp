"""触发层（README 2.3 / 4.1，云上形态见 docs/saga-consistency-guide.md 讨论）。

本版把 ingest 的实时触发从"轮询 Postgres"换成"消费 Kafka"（方案 A）：
gateway 提交 manifest 后向 `edp.ingest.requests` 发一条 ingest.requested，
`ingest_kafka_sensor` 消费它、按 manifest_op 路由到 append/correct 两个 job。

可靠性模型（至少一次触发 + 三层去重/互斥）：
- offset 存在 Dagster sensor cursor 里，和 RunRequest 的提交一起持久化，
  不用 Kafka 自身的 group commit——避免"offset 提交了、run 没发出去"的缝隙；
- 消息重复/重放：sensor 先查 PG，只有 status=ready 才发 RunRequest（廉价跳过）；
  再有 run_key 去重；最终引擎侧 saga.claim() CAS 保证同一 upload 只有一个写者；
- Kafka 消息丢了/发失败：T+1 兜底 schedule 仍然轮询 PG（见 schedules.py），
  ready 的会话最迟第二天早上被补触发。
"""
from __future__ import annotations

import json

from dagster import DefaultSensorStatus, RunRequest, SensorResult, SkipReason, sensor

from common.config import settings
from common.db import execute, fetch_all, fetch_one, to_json
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
    的旧写法会因为 run_key 已消费而永远无法重试。kafka sensor 与 T+1 兜底
    schedule 共用这个规则，两条触发路径互相去重。
    """
    return f"{manifest_op}-{row['upload_id']}-{int(row['updated_at'].timestamp())}"


def _consume_ingest_requests(cursor: dict[str, int]) -> tuple[list[dict], dict[str, int]]:
    """从 cursor 记录的 offset 开始消费一批 ingest.requested 消息。

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
        batches = consumer.poll(timeout_ms=2000, max_records=200)
        for tp, records in batches.items():
            for rec in records:
                messages.append(rec.value)
                new_cursor[str(tp.partition)] = rec.offset + 1
        return messages, new_cursor
    finally:
        consumer.close()


@sensor(
    jobs=[ingest_append_job, ingest_correct_job],
    minimum_interval_seconds=5,
    default_status=DefaultSensorStatus.RUNNING,
    description="消费 Kafka edp.ingest.requests，按 manifest_op 路由拉起 ingest_append/ingest_correct（方案 A）",
)
def ingest_kafka_sensor(context):
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}
    try:
        messages, new_cursor = _consume_ingest_requests(cursor)
    except Exception as e:  # noqa: BLE001 - Kafka 不可达时跳过本轮，下一轮重试；兜底 schedule 仍在
        return SkipReason(f"kafka 不可达，跳过本轮：{type(e).__name__}: {e}")

    if not messages:
        return SensorResult(run_requests=[], cursor=json.dumps(new_cursor))

    run_requests: list[RunRequest] = []
    new_partition_keys: list[str] = []
    existing_partitions = set(context.instance.get_dynamic_partitions(upload_sessions_partitions_def.name))
    seen: set[str] = set()

    for msg in messages:
        payload = msg.get("payload", {})
        upload_id = payload.get("upload_id")
        if not upload_id or upload_id in seen:
            continue
        seen.add(upload_id)

        # PG 是状态真相：只有 ready 才值得起 run。done/failed/ingesting 的重放消息
        # 在这里被廉价跳过，不产生垃圾 run。
        row = fetch_one(
            "SELECT upload_id, manifest_op, updated_at FROM upload_session WHERE upload_id = %s AND status = 'ready'",
            (upload_id,),
        )
        if row is None:
            context.log.info("skip ingest request for %s: session 不存在或非 ready", upload_id)
            continue

        manifest_op = row["manifest_op"]
        job_name = "ingest_append_job" if manifest_op == "append" else "ingest_correct_job"
        if upload_id not in existing_partitions:
            new_partition_keys.append(upload_id)
        run_requests.append(
            RunRequest(
                job_name=job_name,
                run_key=_run_key(manifest_op, row),
                partition_key=upload_id,
                tags={"upload_id": upload_id, "trigger": "kafka"},
            )
        )

    return SensorResult(
        run_requests=run_requests,
        cursor=json.dumps(new_cursor),
        dynamic_partitions_requests=(
            [upload_sessions_partitions_def.build_add_request(new_partition_keys)] if new_partition_keys else []
        ),
    )


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
    run_requests = [RunRequest(run_key=f"collect-{r['batch_id']}", partition_key=r["batch_id"]) for r in rows]
    return run_requests
