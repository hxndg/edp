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
                consumer.seek_to_beginning(tp)  # 首次启动从头读：老消息会被 status 校验廉价跳过
            elif stored < beginnings[tp] or stored > ends[tp]:
                # cursor 越界：retention 清掉了旧段（stored < lo），或 broker 无持久卷
                # 重启/topic 重建导致日志缩水（stored > hi）。后者若不校准，seek 会指向
                # 不存在的 offset，poll 永远拉不到消息——新消息的 offset 也小于 cursor，
                # 触发链路整体失明。回到最早可读处，重放消息由 status=ready 校验廉价跳过。
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
        messages, new_cursor = _consume_topic(settings.kafka_ingest_topic, cursor, batch_max)
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
    job=model_training_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="消费 Kafka edp.jobs.requests，按 TRAIN_MAX_INFLIGHT 背压，一个训练 job 一个 run（README 3.7.2）",
)
def training_kafka_sensor(context):
    """训练触发（README 3.7.2）。与 ingest sensor 的差别只在批策略：训练任务
    少而重，不做微批合并——一个 job = 一个 RunRequest = 一个 run pod（内部再
    fan-out 一个训练 worker）。可靠性模型相同：status=ready 校验 + run_key
    （job_id + updated_at）去重 + 引擎侧 saga claim CAS 三层兜底；消息丢失由
    watchdog 的 ready 悬置修复补发。
    """
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}
    max_inflight = get_int("TRAIN_MAX_INFLIGHT", 2)

    inflight = context.instance.get_runs_count(
        RunsFilter(job_name="model_training_job", statuses=_INFLIGHT_STATUSES)
    )
    if inflight >= max_inflight:
        return SkipReason(f"背压：{inflight} 个训练 run 在跑（上限 {max_inflight}），本轮不消费 Kafka")

    try:
        messages, new_cursor = _consume_topic(settings.kafka_jobs_topic, cursor, 50)
    except Exception as e:  # noqa: BLE001 - Kafka 不可达时跳过本轮；watchdog 兜底仍在
        return SkipReason(f"kafka 不可达，跳过本轮：{type(e).__name__}: {e}")

    if not messages:
        return SensorResult(run_requests=[], cursor=json.dumps(new_cursor))

    requested: list[str] = []
    seen: set[str] = set()
    for msg in messages:
        payload = msg.get("payload", {})
        job_id = payload.get("job_id")
        if payload.get("job_type") == "training" and job_id and job_id not in seen:
            seen.add(job_id)
            requested.append(job_id)

    ready_rows = fetch_all(
        "SELECT job_id, updated_at FROM platform_job WHERE job_id = ANY(%s) AND job_type = 'training' AND status = 'ready'",
        (requested,),
    ) if requested else []

    # 留出背压余量：本轮最多补到 max_inflight 个在跑；超出的消息 offset 已前移，
    # 但 job 仍是 ready——watchdog 的 ready 悬置修复会重新补发触发消息
    budget = max(0, max_inflight - inflight)
    run_requests = [
        RunRequest(
            run_key=f"training-{r['job_id']}-{int(r['updated_at'].timestamp())}",
            run_config={"ops": {"model_training": {"config": {"job_id": r["job_id"]}}}},
            tags={"trigger": "kafka", "job_id": r["job_id"]},
        )
        for r in ready_rows[:budget]
    ]
    if run_requests:
        context.log.info("训练触发：%s", ", ".join(rr.tags["job_id"] for rr in run_requests))
    return SensorResult(run_requests=run_requests, cursor=json.dumps(new_cursor))


@sensor(
    job=ingest_append_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="通用状态机看护（README 3.7.4）：对每个注册的 JobKind 做卡死重入队 / ready 悬置补发 / failed 按码自动重试",
)
def platform_stuck_sensor(context):
    """处理"run 挂了/失败了，状态需要修复"的场景（docs/saga-consistency-guide.md、
    docs/pod-fanout-guide.md 错误处理）。原 ingest_stuck_sensor 的泛化版：
    三类修复逻辑只有 `common/jobs.py::watchdog_pass` 一份实现，对每个注册的
    任务类型（upload / training）各跑一遍。

    本 sensor 不直接发 RunRequest，只做状态修复 + 补发 Kafka 触发消息（由对应
    的 kafka sensor 按新 run_key 重新拉起）；即使这里的判断和一个"其实还活着"
    的旧 run 撞车，新 run 的 saga claim CAS 也只允许一个写者，不会双写。

    1. running 且 saga 心跳超时：attempt < 上限 → 重置回 ready + 补发；
       达上限 → 终态 failed（error_code=STUCK_EXHAUSTED）+ alert 等人工。
    2. ready 悬置太久（触发消息丢了）：刷新 updated_at（→ 新 run_key）+ 补发。
    3. failed 且按 error_code 可自动重试：退避到期后重置回 ready；
       NEEDS_ANALYSIS（OOM/超时）重试时 saga attempt 递增 → worker 内存升档；
       NOT_RETRYABLE（数据问题）不碰，等人工走 gateway retry API。
    """
    from common.jobs import JOB_KINDS, watchdog_pass

    summaries = []
    for kind in JOB_KINDS:
        counts = watchdog_pass(kind, context.log)
        if any(counts.values()):
            context.log.warning("%s 看护：%s", kind.name, counts)
            summaries.append(
                f"{kind.name}: 重入队 {counts['requeued']}、转 failed {counts['exhausted']}、"
                f"按码重试 {counts['retried']}、悬置补发 {counts['dangling']}"
            )
    return SkipReason("；".join(summaries) if summaries else "没有需要修复的任务")


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
