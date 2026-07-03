"""标注任务包的读写约定（README 3.2.2 / 4.9 backlog）。

这里定义的 JSON 结构就是"标注 CLI ↔ Dagster"之间的接口边界——生产化时把 CLI
换成 Label Studio/CVAT，只要新工具按同样的结构读任务包、写结果包，
`annotation_dispatch`/`annotation_collect` 两个 asset 完全不用改。
"""
from __future__ import annotations

import json

from common import object_store
from common.iceberg import in_filter, load_table


def package_key(batch_id: str) -> str:
    return f"{object_store.PREFIX_ANNOTATION_PKG}/{batch_id}/task.json"


def result_key(batch_id: str) -> str:
    return f"{object_store.PREFIX_ANNOTATION_PKG}/{batch_id}/result.json"


def build_package(batch_id: str, sample_ids: list[str]) -> dict:
    prelabels = load_table("annotation").scan(row_filter=in_filter("target_id", sample_ids)).to_arrow().to_pylist()
    prelabel_by_sample = {r["target_id"]: r for r in prelabels if r["source"] == "auto"}
    samples = load_table("sample").scan(row_filter=in_filter("sample_id", sample_ids)).to_arrow().to_pylist()

    items = []
    for s in samples:
        pre = prelabel_by_sample.get(s["sample_id"], {})
        items.append(
            {
                "sample_id": s["sample_id"],
                "episode_id": s["episode_id"],
                "lance_uri": s["lance_uri"],
                "quality_score": s["quality_score"],
                "prelabel_caption": pre.get("value_or_uri"),
                "prelabel_confidence": pre.get("confidence"),
            }
        )
    return {"batch_id": batch_id, "samples": items}


def upload_package(batch_id: str, sample_ids: list[str]) -> str:
    package = build_package(batch_id, sample_ids)
    key = package_key(batch_id)
    return object_store.put_bytes(key, json.dumps(package, ensure_ascii=False, default=str, indent=2).encode())


def load_package(batch_id: str) -> dict:
    return json.loads(object_store.get_bytes(package_key(batch_id)))


def upload_result(batch_id: str, reviewer: str, results: list[dict]) -> str:
    payload = {"batch_id": batch_id, "reviewer": reviewer, "results": results}
    key = result_key(batch_id)
    return object_store.put_bytes(key, json.dumps(payload, ensure_ascii=False, default=str, indent=2).encode())


def load_result(batch_id: str) -> dict:
    return json.loads(object_store.get_bytes(result_key(batch_id)))
