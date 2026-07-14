"""`entity_tag_index` 资产的核心逻辑（README 3.5）：把 Iceberg `entity_tag`（SoT）
聚合成 OpenSearch 里"一个对象一个文档"的检索投影。

同步策略是**全量重建式**：每次运行扫整张 entity_tag，按 (target_type, target_id)
聚合出文档，bulk 覆盖写入（_id 确定性）。不做增量 diff 的原因：
  - tag 表是"薄表"（行数 = 对象数 × 平均标签数），全量聚合秒级完成；
  - 幂等且自愈——投影丢失、落后、脏了，重物化本资产即可收敛，
    与"Postgres/投影皆派生态，可从 Iceberg 重建"的架构原则（README 4.7）一致。
被覆盖对象之外的旧文档不会残留：doc _id 确定性，同一对象永远命中同一文档；
对象级删除（Iceberg 里整个 target 的 tag 全没了）MVP 不处理，靠索引重建兜底。
"""
from __future__ import annotations

from datetime import datetime, timezone


def run(*, run_id: str) -> dict:
    from common.search import bulk_upsert, ensure_tag_index
    from engines.duckdb.duckdb_conn import iceberg_arrow

    ensure_tag_index()

    rows = iceberg_arrow("entity_tag").to_pylist()
    if not rows:
        return {"num_docs": 0, "num_tags": 0}

    # (target_type, target_id) -> 文档；同一 key 后写的行覆盖先写的（Iceberg 里
    # upsert 语义本来就保证一个 (type, id, key) 只有一行，这里只是防御性处理）
    docs: dict[tuple[str, str], dict] = {}
    indexed_at = datetime.now(timezone.utc).isoformat()
    for r in rows:
        key = (r["target_type"], r["target_id"])
        doc = docs.setdefault(
            key,
            {
                "target_type": r["target_type"],
                "target_id": r["target_id"],
                "robot_id": r.get("robot_id"),
                "tags": {},
                "tag_sources": {},
                "indexed_at": indexed_at,
            },
        )
        doc["tags"][r["tag_key"]] = r["tag_value"]
        doc["tag_sources"][r["tag_key"]] = r.get("source")

    for doc in docs.values():
        doc["num_tags"] = len(doc["tags"])

    num_docs = bulk_upsert(list(docs.values()))
    return {"num_docs": num_docs, "num_tags": len(rows)}
