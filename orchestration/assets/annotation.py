"""预标 + 标注 + 质检（README 3.2.2 / 3.6）——MVP 里编排最容易出问题的一段。

几条原则同时落地：
  1. `annotation_dispatch` 结束后这个 run 就完事了，Dagster 进程层面完全空闲，
     不会为了等人工标注挂着一个 run；`annotation_collect` 是被 webhook（或兜底
     sensor）唤醒的另一次独立 run，通过 run_config 里的 batch_id 找到自己的
     标注批次（batch_id == upload_id，1 upload 对应 1 episode 对应 1 标注批次）。
  2. `annotation_auto` / `annotation_dispatch` 是同一个 `@multi_asset` 里两个
     `is_required=False` 的 out。微批下一个 run 内可能同时有 auto_only 和
     human_required 两类 upload——两个 out 都可能被 yield（各覆盖对应子集），
     只有一类时另一个 out 在 UI 上自动显示为 skipped（条件物化）。
  3. `qc_result` 故意不通过函数参数接收 auto/collect 的返回值（那样会在另一条
     分支没跑这次 run 时报"找不到上游输出"）。它只声明 `deps`（纯血缘边），
     真正处理哪些数据靠自己去 Iceberg 查"哪些 annotation 还没对应的 qc_result"
     ——这也更符合 README 4.7 的硬规则："不用 Dagster 的记录回答数据存不存在，
     只查 Iceberg"。
"""

from dagster import AssetExecutionContext, AssetIn, AssetKey, AssetOut, Config, MetadataValue, Output, asset, multi_asset

from common.db import execute, fetch_all


@asset(
    group_name="annotation",
    ins={"sample": AssetIn(key=AssetKey("sample"))},
    description="对新样本跑一个假模型，产出预标注（README 2.4：Ray mock）",
)
def prelabel_annotation(context: AssetExecutionContext, sample: list[dict]) -> Output[list[dict]]:
    """上游 sample 的值是 per-upload 分组结构 [{upload_id, episode_id, sample_ids}, ...]；
    预标本身不关心分组（扁平跑一遍），但把分组结构原样传给下游 router 用。
    """
    from engines.ray.prelabel import run as prelabel_run

    all_sample_ids = [sid for entry in sample for sid in entry["sample_ids"]]
    result = prelabel_run(all_sample_ids, run_id=context.run_id)
    return Output(
        value=sample,
        metadata={"num_prelabeled": result.get("num_prelabeled", 0), "num_uploads": len(sample)},
    )


@multi_asset(
    name="annotation_router",
    group_name="annotation",
    ins={"prelabel_annotation": AssetIn(key=AssetKey("prelabel_annotation"))},
    outs={
        "annotation_auto": AssetOut(is_required=False, description="pipeline_profile=auto_only 子集：预标直接转正"),
        "annotation_dispatch": AssetOut(is_required=False, description="pipeline_profile=human_required 子集：派活"),
    },
)
def annotation_router(context: AssetExecutionContext, prelabel_annotation: list[dict]):
    """按每个 upload 的 pipeline_profile 路由（README 3.2.2）。微批下一个批次里
    两类 profile 可能混在一起：auto 子集合并成一次转正；human 子集每个 upload
    仍是独立的标注批次（batch_id = upload_id），因为人工标注的下发/收活天然
    以 upload 为单位（不同 upload 完工时间不同）。
    """
    upload_ids = [entry["upload_id"] for entry in prelabel_annotation]
    profile_rows = fetch_all(
        "SELECT upload_id, pipeline_profile FROM upload_session WHERE upload_id = ANY(%s)",
        (upload_ids,),
    )
    profile_by_upload = {r["upload_id"]: r["pipeline_profile"] for r in profile_rows}

    auto_sample_ids: list[str] = []
    human_entries: list[dict] = []
    for entry in prelabel_annotation:
        if profile_by_upload.get(entry["upload_id"], "auto_only") == "auto_only":
            auto_sample_ids.extend(entry["sample_ids"])
        else:
            human_entries.append(entry)

    if auto_sample_ids:
        from engines.ray.annotation_auto import run as promote_run

        result = promote_run(auto_sample_ids, run_id=context.run_id)
        yield Output(
            value=auto_sample_ids,
            output_name="annotation_auto",
            metadata={**result, "num_auto_uploads": len(prelabel_annotation) - len(human_entries)},
        )

    if human_entries:
        from common.annotation_batches import upload_package

        dispatched = []
        for entry in human_entries:
            batch_id = entry["upload_id"]  # 1 upload = 1 episode = 1 标注批次
            package_uri = upload_package(batch_id, entry["sample_ids"])
            execute(
                """
                INSERT INTO annotation_batch (batch_id, upload_id, sample_ids, package_uri, status)
                VALUES (%(batch_id)s, %(upload_id)s, %(sample_ids)s, %(package_uri)s, 'LABELING')
                ON CONFLICT (batch_id) DO UPDATE SET sample_ids = EXCLUDED.sample_ids, package_uri = EXCLUDED.package_uri, status = 'LABELING', updated_at = now()
                """,
                {
                    "batch_id": batch_id,
                    "upload_id": entry["upload_id"],
                    "sample_ids": _to_json(entry["sample_ids"]),
                    "package_uri": package_uri,
                },
            )
            dispatched.append({"batch_id": batch_id, "package_uri": package_uri, "num_samples": len(entry["sample_ids"])})
        yield Output(
            value=dispatched,
            output_name="annotation_dispatch",
            metadata={
                "num_batches": len(dispatched),
                "batches": MetadataValue.json(dispatched),
            },
        )


class CollectConfig(Config):
    """webhook（gateway）或兜底 sensor 在 run_config 里指明收哪个标注批次。"""

    batch_id: str


@asset(
    group_name="annotation",
    deps=[AssetKey("annotation_dispatch")],
    description="收活：标注 CLI 提交结果后由 webhook（或兜底 sensor）唤醒",
)
def annotation_collect(context: AssetExecutionContext, config: CollectConfig) -> Output[dict]:
    from common.annotation_batches import load_result

    batch_id = config.batch_id
    result_payload = load_result(batch_id)
    num_written = _write_human_annotations(batch_id, result_payload, run_id=context.run_id)
    execute("UPDATE annotation_batch SET status = 'DONE', updated_at = now() WHERE batch_id = %s", (batch_id,))
    return Output(
        value={"batch_id": batch_id, "num_written": num_written},
        metadata={"batch_id": batch_id, "num_written": num_written},
    )


@asset(
    group_name="annotation",
    deps=[AssetKey("annotation_auto"), AssetKey("annotation_collect")],
    description="自动数据质检（README 3.4：滑动窗口频率 + 位姿连续性），增量找已转正 annotation 对应的 sample",
)
def qc_result(context: AssetExecutionContext) -> Output[dict]:
    from engines.ray.qc import run as qc_run

    target_ids = _find_pending_qc_targets()
    result = qc_run(target_ids, run_id=context.run_id)
    return Output(value=result, metadata=result)


def _find_pending_qc_targets() -> list[str]:
    from common.iceberg import load_table
    from pyiceberg.expressions import EqualTo

    annos = load_table("annotation").scan(row_filter=EqualTo("review_status", "passed")).to_arrow().to_pylist()
    passed_targets = {a["target_id"] for a in annos}
    try:
        qcs = load_table("qc_result").scan().to_arrow().to_pylist()
        already_checked = {q["target_id"] for q in qcs}
    except Exception:  # noqa: BLE001 - 表可能还没有任何行
        already_checked = set()
    return sorted(passed_targets - already_checked)


def _write_human_annotations(batch_id: str, result_payload: dict, *, run_id: str) -> int:
    import pyarrow as pa

    from common.audit import make_batch_id
    from common.iceberg import upsert, with_audit_columns
    from schemas.iceberg_tables import ANNOTATION

    rows = [
        {
            "anno_id": f"{r['sample_id']}-human",
            "target_type": "sample",
            "target_id": r["sample_id"],
            "type": "lang",
            "value_or_uri": r.get("caption"),
            "source": "human",
            "anno_version": "cli-v1",
            "review_status": r.get("review_status", "passed"),
            "confidence": None,
        }
        for r in result_payload["results"]
    ]
    if not rows:
        return 0
    tbl = pa.Table.from_pylist(rows)
    tbl = with_audit_columns(
        tbl,
        batch_id=make_batch_id(robot_id="annotation_collect", upload_id=batch_id),
        run_id=run_id,
        source_uri=f"annotation_batch:{batch_id}",
    )
    upsert(ANNOTATION, tbl, join_cols=["anno_id"])
    return len(rows)


def _to_json(value) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, default=str)
