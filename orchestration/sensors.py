"""触发层（README 2.3 / 4.1 / 3.6，操作态分层见 README 3.1.2.3）。

ingest 的实时触发消费 Kafka：gateway 提交 manifest 后向 `edp.ingest.requests`
发一条 ingest.requested，`ingest_kafka_sensor` 消费它。

微批 + 背压（README 3.6.2）：sensor 每个 tick 先做在跑批次背压检查（达上限
本轮不消费，消息留在 Kafka 排队），然后拉最多 INGEST_BATCH_MAX 条消息、按
消息里的 manifest_op 分成至多两组，**每组 = 一个批次 = 一个 RunRequest**。

可靠性模型（至少一次触发 + 引擎侧解读 SoT）：
- offset 存在 Dagster sensor cursor 里，和 RunRequest 的提交一起持久化；
- **Sensor 不查 PG SoT**：Kafka 只当叫醒铃；重放/过期消息照常组批发 run，
  由引擎 `status <> 'done'` + `claim_many` CAS 决定真做或跳过（3.1.2.3）；
- run_key 含本批消费位点盐，避免 stuck 再投后被 Dagster 当成同一 run 去重掉；
- Kafka 消息丢了：T+1 兜底 schedule 仍轮询 PG ready；
- 卡死：platform_stuck_sensor → ready + 再投 Kafka；
- failed 重试：gateway manual_retry → ready + Kafka。
"""
from __future__ import annotations

import hashlib
import json

from dagster import DagsterRunStatus, DefaultSensorStatus, RunRequest, RunsFilter, SensorResult, SkipReason, sensor

from common.config import settings
from common.db import fetch_all
from common.runtime_config import get_int
from orchestration.jobs import annotation_collect_job, ingest_append_job, ingest_correct_job, model_training_job

INGEST_JOB_NAMES = ("ingest_append_job", "ingest_correct_job")

# "在跑"= 已提交但还没到终态：排队中 + 启动中 + 执行中
_INFLIGHT_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]


def _pending_upload_rows(manifest_op: str) -> list[dict]:
    """T+1 兜底 schedule 用：仍以 PG ready 为待办扫描（与 kafka sensor 职责不同）。"""
    return fetch_all(
        """
        SELECT upload_id, processing_type, updated_at
        FROM upload_session
        WHERE status = 'ready' AND manifest_op = %s
        ORDER BY created_at
        """,
        (manifest_op,),
    )


def _batch_run_key(manifest_op: str, processing_type: str, rows: list[dict]) -> str:
    """run_key = op + processing_type + 批内容摘要。

    仅 T+1 schedule 路径使用（仍读 PG）；updated_at 让业务状态变化后可产生
    新 run_key。
    """
    digest = hashlib.sha256(
        "|".join(sorted(f"{r['upload_id']}:{int(r['updated_at'].timestamp())}" for r in rows)).encode()
    ).hexdigest()
    return f"{manifest_op}-{processing_type}-batch-{digest[:16]}"


def _batch_run_request(
    manifest_op: str, processing_type: str, rows: list[dict], trigger: str
) -> RunRequest:
    """同一 op + processing_type 的批次 → 一个独立 RunRequest。"""
    upload_ids = [r["upload_id"] for r in rows]
    job_name = "ingest_append_job" if manifest_op == "append" else "ingest_correct_job"
    return RunRequest(
        job_name=job_name,
        run_key=_batch_run_key(manifest_op, processing_type, rows),
        run_config={
            "ops": {
                "ingest_multi_asset": {
                    "config": {
                        "upload_ids": upload_ids,
                        "manifest_op": manifest_op,
                        "processing_type": processing_type,
                    }
                }
            },
            "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
        },
        tags={
            "trigger": trigger,
            "manifest_op": manifest_op,
            "processing_type": processing_type,
            "batch_size": str(len(upload_ids)),
        },
    )


def _kafka_ingest_run_request(
    manifest_op: str, processing_type: str, upload_ids: list[str], cursor_salt: str
) -> RunRequest:
    """Kafka 路径：不读 PG；run_key = 批成员 + 消费位点盐（再投递 → 新位点 → 新 run）。"""
    job_name = "ingest_append_job" if manifest_op == "append" else "ingest_correct_job"
    digest = hashlib.sha256(
        ("|".join(sorted(upload_ids)) + "|" + cursor_salt).encode()
    ).hexdigest()
    return RunRequest(
        job_name=job_name,
        run_key=f"{manifest_op}-{processing_type}-batch-{digest[:16]}",
        run_config={
            "ops": {
                "ingest_multi_asset": {
                    "config": {
                        "upload_ids": upload_ids,
                        "manifest_op": manifest_op,
                        "processing_type": processing_type,
                    }
                }
            },
            "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
        },
        tags={
            "trigger": "kafka",
            "manifest_op": manifest_op,
            "processing_type": processing_type,
            "batch_size": str(len(upload_ids)),
        },
    )


def _inflight_ingest_batches(instance) -> int:
    return sum(
        instance.get_runs_count(RunsFilter(job_name=name, statuses=_INFLIGHT_STATUSES))
        for name in INGEST_JOB_NAMES
    )


def _consume_topic(topic: str, cursor: dict[str, int], max_messages: int) -> tuple[list[dict], dict[str, int]]:
    """从 cursor 记录的 offset 开始消费某个触发 topic 最多 max_messages 条消息。

    ingest 与 training 两个 kafka sensor 共用（各自的 cursor 独立存储）。
    返回 (消息列表, 新 cursor)。cursor 形如 {"0": 42} —— partition 号 → 下一条
    要读的 offset。不用 Kafka consumer group：offset 的"提交"就是 Dagster 持久化
    sensor cursor 这个动作本身，与 RunRequest 提交原子。
    """
    from kafka import KafkaConsumer, TopicPartition

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
        beginnings = consumer.beginning_offsets(tps)
        ends = consumer.end_offsets(tps)
        for tp in tps:
            stored = cursor.get(str(tp.partition))
            if stored is None:
                consumer.seek_to_beginning(tp)  # 首次启动从头读；重放由引擎 claim / status 过滤
            elif stored < beginnings[tp] or stored > ends[tp]:
                # cursor 越界：retention 清掉了旧段（stored < lo），或 broker 无持久卷
                # 重启/topic 重建导致日志缩水（stored > hi）。回到最早可读处。
                consumer.seek(tp, beginnings[tp])
            else:
                consumer.seek(tp, stored)

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
    description="消费 Kafka edp.ingest.requests，微批合并 + 背压；不查 PG，引擎解读 SoT（README 3.1.2.3 / 3.6.2）",
)
def ingest_kafka_sensor(context):
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}

    batch_max = get_int("INGEST_BATCH_MAX", 200)
    max_inflight = get_int("INGEST_MAX_INFLIGHT_BATCHES", 3)

    inflight = _inflight_ingest_batches(context.instance)
    if inflight >= max_inflight:
        return SkipReason(f"背压：{inflight} 个 ingest 批次在跑（上限 {max_inflight}），本轮不消费 Kafka")

    try:
        messages, new_cursor = _consume_topic(settings.kafka_ingest_topic, cursor, batch_max)
    except Exception as e:  # noqa: BLE001 - Kafka 不可达时跳过本轮，下一轮重试；兜底 schedule 仍在
        return SkipReason(f"kafka 不可达，跳过本轮：{type(e).__name__}: {e}")

    if not messages:
        return SensorResult(run_requests=[], cursor=json.dumps(new_cursor))

    # 只信消息载荷组批；是否该跑由引擎读 PG + claim 决定（不在此查 SoT）
    groups: dict[tuple[str, str], list[str]] = {}
    seen: set[str] = set()
    for msg in messages:
        payload = msg.get("payload") or {}
        upload_id = payload.get("upload_id")
        manifest_op = payload.get("manifest_op")
        processing_type = payload.get("processing_type")
        if (
            not upload_id
            or manifest_op not in ("append", "correct")
            or not processing_type
            or upload_id in seen
        ):
            continue
        seen.add(upload_id)
        groups.setdefault((manifest_op, processing_type), []).append(upload_id)

    cursor_salt = json.dumps(new_cursor, sort_keys=True)
    run_requests = [
        _kafka_ingest_run_request(op, processing_type, ids, cursor_salt)
        for (op, processing_type), ids in groups.items()
        if ids
    ]
    if run_requests:
        context.log.info(
            "微批触发：%s",
            ", ".join(
                f"{rr.tags['manifest_op']}/{rr.tags['processing_type']}×{rr.tags['batch_size']}"
                for rr in run_requests
            ),
        )
    return SensorResult(run_requests=run_requests, cursor=json.dumps(new_cursor))


@sensor(
    job=model_training_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="消费 Kafka edp.jobs.requests，背压后按消息触发；不查 PG，引擎解读 SoT（README 3.7.2）",
)
def training_kafka_sensor(context):
    """训练触发：一个 job = 一个 RunRequest。Sensor 不查 platform_job.status；
    引擎入口 `status <> 'done'` + execution_claim 负责跳过/互斥。
    """
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}
    max_inflight = get_int("TRAIN_MAX_INFLIGHT", 2)

    inflight = context.instance.get_runs_count(
        RunsFilter(job_name="model_training_job", statuses=_INFLIGHT_STATUSES)
    )
    if inflight >= max_inflight:
        return SkipReason(f"背压：{inflight} 个训练 run 在跑（上限 {max_inflight}），本轮不消费 Kafka")

    budget = max(0, max_inflight - inflight)
    try:
        # 只消费本轮能提交的数量，避免 cursor 前移后把未提交 job 丢掉。
        messages, new_cursor = _consume_topic(settings.kafka_jobs_topic, cursor, budget)
    except Exception as e:  # noqa: BLE001 - Kafka 不可达时跳过本轮
        return SkipReason(f"kafka 不可达，跳过本轮：{type(e).__name__}: {e}")

    if not messages:
        return SensorResult(run_requests=[], cursor=json.dumps(new_cursor))

    requested: list[str] = []
    seen: set[str] = set()
    for msg in messages:
        payload = msg.get("payload") or {}
        job_id = payload.get("job_id")
        if payload.get("job_type") == "training" and job_id and job_id not in seen:
            seen.add(job_id)
            requested.append(job_id)

    cursor_salt = hashlib.sha256(json.dumps(new_cursor, sort_keys=True).encode()).hexdigest()[:8]
    run_requests = [
        RunRequest(
            run_key=f"training-{job_id}-{cursor_salt}",
            run_config={"ops": {"model_training": {"config": {"job_id": job_id}}}},
            tags={"trigger": "kafka", "job_id": job_id},
        )
        for job_id in requested[:budget]
    ]
    if run_requests:
        context.log.info("训练触发：%s", ", ".join(rr.tags["job_id"] for rr in run_requests))
    return SensorResult(run_requests=run_requests, cursor=json.dumps(new_cursor))


@sensor(
    job=ingest_append_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="卡死看护：running + claim 心跳超时 → failed，等待外部重新投递",
)
def platform_stuck_sensor(context):
    """只收敛长时间卡住的执行；不自动重试。"""
    from common.jobs import JOB_KINDS, watchdog_pass

    summaries = []
    for kind in JOB_KINDS:
        counts = watchdog_pass(kind, context.log)
        if any(counts.values()):
            context.log.warning("%s 卡死看护：%s", kind.name, counts)
            summaries.append(f"{kind.name}: 转 failed {counts['failed']}")
    return SkipReason("；".join(summaries) if summaries else "没有卡住的任务")


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
