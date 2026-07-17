"""业务事件账本（README 2.2 原则 5 / 4.7）：Kafka 只用来记录"发生过什么"，
永远不是触发源。触发只走 Dagster 自己的 schedule/sensor/API/webhook。

生产者是进程内单例、懒连接，避免网关每个请求都新建连接。事件写失败只打日志，
不阻塞主流程——账本丢一条事件不影响 Iceberg 的数据事实，可从 Iceberg 重放补齐。
"""
from __future__ import annotations

import functools
import json
import logging
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

from common.config import settings

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=50,
    )


def emit(event_type: str, key: str, payload: dict[str, Any], *, topic: str | None = None) -> None:
    """写一条事实事件到账本。event_type 例如 upload.created / manifest.submitted。"""
    record = {
        "event_type": event_type,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    try:
        _producer().send(topic or settings.kafka_topic, key=key, value=record)
        _producer().flush(timeout=5)
    except Exception:  # noqa: BLE001 - 账本是派生态，写失败不应该打断主流程
        logger.exception("kafka ledger emit failed: event_type=%s key=%s", event_type, key)


def emit_ingest_request(upload_id: str, manifest_op: str) -> None:
    """发一条 ingest 触发事件到专用 topic，由 `ingest_kafka_sensor` 消费拉起 run。

    与账本 emit 一样"尽力而为"：发失败不打断主流程——upload_session 已经是
    ready 状态，T+1 兜底 schedule（轮询 PG）会补触发，不会丢。
    """
    emit(
        "ingest.requested",
        key=upload_id,
        payload={"upload_id": upload_id, "manifest_op": manifest_op},
        topic=settings.kafka_ingest_topic,
    )


def emit_job_request(job_id: str, job_type: str) -> None:
    """发一条通用任务触发事件（README 3.7.4），由对应类型的 kafka sensor 消费。

    同样尽力而为：platform_job 已是 ready，消息丢了由 watchdog 的
    "ready 悬置修复"补发。
    """
    emit(
        "job.requested",
        key=job_id,
        payload={"job_id": job_id, "job_type": job_type},
        topic=settings.kafka_jobs_topic,
    )
