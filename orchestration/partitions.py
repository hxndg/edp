"""共享分区定义。

`upload_sessions` 是一个动态分区集合，分区键就是 `upload_id`——每来一个新的
上传会话，`ingest_append_sensor`/`ingest_correct_sensor` 就往这个分区集合里
加一个新分区键，再对该分区发起一次 run。这同时满足了两件事：
  1. README 4.4 的"Partition + Backfill"——科研人员可以在 UI 上按 upload_id
     选范围重跑；
  2. 入湖 -> 预标 -> 标注路由 -> 收活 这条链路上的每个 asset 都能通过
     `context.partition_key` 天然拿到"这次是哪个 upload/batch"，不需要额外
     的 config 传参。
"""
from __future__ import annotations

from dagster import DynamicPartitionsDefinition

upload_sessions_partitions_def = DynamicPartitionsDefinition(name="upload_sessions")
