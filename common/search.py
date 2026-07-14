"""OpenSearch 检索投影的客户端（README 3.5）。

定位：OpenSearch 只是"查询加速器"。tag 的 SoT 永远是 Iceberg `entity_tag`，
这里的索引是**可整体重建的派生投影**——丢了/落后了，重物化 `entity_tag_index`
资产即可收敛，因此单节点、无副本、不做持久化保证都是可接受的。

实现取舍：直接用 requests 调 REST API，不引入 opensearch-py——MVP 用到的
只有 ensure index / bulk 覆盖写 / bool 查询三个调用，SDK 反而是负担。
"""
from __future__ import annotations

import json
from typing import Any

import requests

from common.config import settings

# 一个对象一个文档：_id = "{target_type}:{target_id}"，天然幂等覆盖
TAG_INDEX = "edp-entity-tag"

_TAG_INDEX_BODY = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "target_type": {"type": "keyword"},
            "target_id": {"type": "keyword"},
            "robot_id": {"type": "keyword"},
            # flat_object：任意 tag_key 都不会撑爆 mapping（key 多变场景的标准解法），
            # 代价是值一律按字符串精确匹配，数值范围查询不归它管（那类条件走湖上查询）
            "tags": {"type": "flat_object"},
            "tag_sources": {"type": "flat_object"},
            "num_tags": {"type": "integer"},
            "indexed_at": {"type": "date"},
        }
    },
}


def ensure_tag_index() -> None:
    """建索引（已存在则跳过），mapping 版本变更时删除索引重建即可（投影可重建）。"""
    resp = requests.head(f"{settings.opensearch_url}/{TAG_INDEX}", timeout=10)
    if resp.status_code == 404:
        requests.put(f"{settings.opensearch_url}/{TAG_INDEX}", json=_TAG_INDEX_BODY, timeout=30).raise_for_status()


def bulk_upsert(docs: list[dict[str, Any]]) -> int:
    """按确定性 _id 覆盖写入，重复执行结果一致。返回写入文档数。"""
    if not docs:
        return 0
    lines: list[str] = []
    for doc in docs:
        doc_id = f"{doc['target_type']}:{doc['target_id']}"
        lines.append(json.dumps({"index": {"_index": TAG_INDEX, "_id": doc_id}}))
        lines.append(json.dumps(doc, ensure_ascii=False, default=str))
    resp = requests.post(
        f"{settings.opensearch_url}/_bulk",
        data=("\n".join(lines) + "\n").encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        params={"refresh": "true"},  # 同步刷新：物化结束即可查，MVP 量级代价可忽略
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        failed = [item for item in body["items"] if item["index"].get("error")]
        raise RuntimeError(f"OpenSearch bulk 部分失败 {len(failed)} 条，第一条：{failed[0]['index']['error']}")
    return len(docs)


def search_by_tags(
    tags: dict[str, str],
    *,
    target_type: str | None = None,
    size: int = 50,
) -> dict[str, Any]:
    """按 tag 的 key=value 组合精确检索，返回 {total, hits:[{target_type, target_id, robot_id, tags}]}。"""
    filters: list[dict] = [{"term": {f"tags.{k}": v}} for k, v in tags.items()]
    if target_type:
        filters.append({"term": {"target_type": target_type}})
    query = {"bool": {"filter": filters}} if filters else {"match_all": {}}
    resp = requests.post(
        f"{settings.opensearch_url}/{TAG_INDEX}/_search",
        json={"query": query, "size": size},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    return {
        "total": body["hits"]["total"]["value"],
        "hits": [h["_source"] for h in body["hits"]["hits"]],
    }
