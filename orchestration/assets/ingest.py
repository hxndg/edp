"""上传入湖（README 3.2.1）。

`raw_file`/`episode`/`sample` 是同一个 `@multi_asset`：一次 Spark 作业原子地
把三张表一起写掉，在 Dagster 图上仍然是三个独立可点击、可查血缘的节点。

`manifest_op=append` 和 `manifest_op=correct` 的区分**不是**这个函数体里的
if/else 分支——README 2.2 原则 9 明确说这属于"结构性分支"，必须在图上可见。
这里的做法是：两个不同的 sensor（见 `orchestration/sensors.py`）各自拉起
`ingest_append_job` / `ingest_correct_job` 两个不同名字的 job，运行历史上
一眼就能分辨是哪一条链路；函数体内部只是根据 `upload_session.manifest_op`
（触发前就已经确定、不是本函数决定）选用对应的执行引擎模块，本质上等价于
"两个不同的 job 各自调用两个不同的函数"，只是共享同一组 asset 身份以对齐
Iceberg 里同一批表。
"""

from dagster import AssetExecutionContext, Output, multi_asset, AssetOut

from common.db import fetch_one
from orchestration.partitions import upload_sessions_partitions_def


@multi_asset(
    name="ingest_multi_asset",
    partitions_def=upload_sessions_partitions_def,
    group_name="ingest",
    outs={
        "raw_file": AssetOut(description="原始文件登记（README 3.1.2）"),
        "episode": AssetOut(description="一次连续采集的语义单元（README 3.1.2）"),
        "sample": AssetOut(description="切出的训练/评测样本，本体在 Lance（README 3.1.2）"),
    },
)
def ingest_multi_asset(context: AssetExecutionContext):
    upload_id = context.partition_key
    run_id = context.run_id
    session = fetch_one("SELECT manifest_op FROM upload_session WHERE upload_id = %s", (upload_id,))
    if session is None:
        raise ValueError(f"upload_session '{upload_id}' 不存在（sensor 传了一个野分区键？）")

    if session["manifest_op"] == "append":
        from engines.spark.ingest_append import run as ingest_run

        result = ingest_run(upload_id, run_id)
    else:
        from engines.spark.ingest_correct import run as ingest_run

        result = ingest_run(upload_id, run_id)

    episode_id = result["episode_id"]
    sample_ids = _list_new_sample_ids(episode_id)

    yield Output(
        value={"upload_id": upload_id, **_safe(result, ["num_files", "quarantined_files"])},
        output_name="raw_file",
        metadata={"manifest_op": session["manifest_op"], **_safe(result, ["num_files", "quarantined_files"])},
    )
    yield Output(
        value=episode_id,
        output_name="episode",
        metadata={"episode_id": episode_id, "manifest_op": session["manifest_op"]},
    )
    yield Output(
        value=sample_ids,
        output_name="sample",
        metadata={"num_samples": len(sample_ids), "silver_clean_strategy_id": result.get("silver_clean_strategy_id")},
    )


def _list_new_sample_ids(episode_id: str) -> list[str]:
    from common.iceberg import load_table
    from pyiceberg.expressions import EqualTo

    rows = load_table("sample").scan(row_filter=EqualTo("episode_id", episode_id)).to_arrow().to_pylist()
    return [r["sample_id"] for r in rows]


def _safe(d: dict, keys: list[str]) -> dict:
    return {k: d[k] for k in keys if k in d}
