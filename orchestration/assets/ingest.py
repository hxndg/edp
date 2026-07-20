"""上传入湖（README 3.2.1 / 3.6）。

`raw_file`/`episode`/`sample` 是同一个 `@multi_asset`：一次批量入湖作业原子地
把三张表一起写掉，在 Dagster 图上仍然是三个独立可点击、可查血缘的节点。

微批形态（README 3.6.2）："这次处理哪些 upload"不再走动态分区，而是由
sensor/schedule 在 RunRequest 的 `run_config` 里传 `upload_ids` 列表——
run 的数量由批次窗口决定，与上传量解耦，Dagster 的分区集合/事件日志不再
随 upload 数线性增长。

`manifest_op=append` 和 `manifest_op=correct` 的区分**不是**这个函数体里的
if/else 分支——README 2.2 原则 9 明确说这属于"结构性分支"，必须在图上可见。
两个不同的 sensor 触发路径各自拉起 `ingest_append_job` / `ingest_correct_job`
两个不同名字的 job（一个批次只含同一种 op，见 3.6.2），函数体内部只是根据
config 里的 manifest_op 选用对应的执行引擎模块。
"""

from dagster import AssetExecutionContext, AssetOut, Config, MetadataValue, Output, multi_asset


class IngestBatchConfig(Config):
    """一个微批：同一 manifest_op + processing_type，因而对应一个执行 Profile。"""

    upload_ids: list[str]
    manifest_op: str  # append | correct
    processing_type: str


@multi_asset(
    name="ingest_multi_asset",
    group_name="ingest",
    outs={
        "raw_file": AssetOut(description="原始文件登记（README 3.1.1.2）"),
        "episode": AssetOut(description="一次连续采集的语义单元（README 3.1.1.2）"),
        "sample": AssetOut(description="切出的训练/评测样本，本体在 Lance（README 3.1.1.2）"),
    },
)
def ingest_multi_asset(context: AssetExecutionContext, config: IngestBatchConfig):
    if config.manifest_op == "append":
        from engines.spark.ingest_append import run_batch
    else:
        from engines.spark.ingest_correct import run_batch

    # context 传给引擎：run_id 从它取（worker 由 Argo Workflow 监管，
    # 日志归档在 s3://lake/argo/，见 common/argo_workflows.py）
    result = run_batch(config.upload_ids, config.processing_type, context)
    per_upload = result["per_upload"]

    batch_meta = {
        "manifest_op": config.manifest_op,
        "processing_type": config.processing_type,
        "execution_profile_id": result["execution_profile_id"],
        "num_requested": result["num_requested"],
        "num_claimed": result["num_claimed"],
        "num_succeeded": result["num_succeeded"],
        "num_failed": result["num_failed"],
        "silver_clean_strategy_id": result["silver_clean_strategy_id"],
    }
    if result["failures"]:
        batch_meta["failures"] = MetadataValue.json(result["failures"])
    if result["skipped_uploads"]:
        batch_meta["skipped_uploads"] = MetadataValue.json(result["skipped_uploads"])

    yield Output(
        value={"upload_ids": [p["upload_id"] for p in per_upload]},
        output_name="raw_file",
        metadata={
            **batch_meta,
            "num_files": sum(p["num_files"] for p in per_upload),
            "quarantined_files": result["quarantined_files"],
        },
    )
    yield Output(
        value=[p["episode_id"] for p in per_upload],
        output_name="episode",
        metadata={**batch_meta, "episode_ids": MetadataValue.json([p["episode_id"] for p in per_upload])},
    )
    # sample 输出保留"哪个 upload 产出哪些 sample"的分组结构：下游
    # annotation_router 需要按 upload 的 pipeline_profile 逐个路由。
    yield Output(
        value=per_upload,
        output_name="sample",
        metadata={**batch_meta, "num_samples": result["num_samples"]},
    )
