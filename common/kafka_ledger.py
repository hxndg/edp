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


def emit(event_type: str, key: str, payload: dict[str, Any]) -> None:
    """写一条事实事件到账本。event_type 例如 upload.created / manifest.submitted。"""
    record = {
        "event_type": event_type,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    try:
        _producer().send(settings.kafka_topic, key=key, value=record)
        _producer().flush(timeout=5)
    except Exception:  # noqa: BLE001 - 账本是派生态，写失败不应该打断主流程
        logger.exception("kafka ledger emit failed: event_type=%s key=%s", event_type, key)
