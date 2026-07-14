"""Job 定义（README 2.2 原则 9 的落地）。

`ingest_append_job` / `ingest_correct_job` 选的是**同一组** asset（raw_file/
episode/sample/prelabel_annotation/annotation_auto/annotation_dispatch/
qc_result/entity_tag），区别只在于"谁触发了它"（两个不同的 sensor，见
`sensors.py`）——运行历史列表里 job 名字不同，一眼能分辨这次是新增采集还是
数据修正，这正是 3.2.1 节说的"最彻底的一种分支形式"。

`annotation_collect_job` 单独存在，因为它是被 webhook/兜底 sensor 在
**几天后**唤醒的一次独立 run，不应该跟入湖 job 绑在一起。
"""
from __future__ import annotations

from dagster import define_asset_job

_INGEST_AND_ANNOTATE_SELECTION = [
    "raw_file",
    "episode",
    "sample",
    "prelabel_annotation",
    "annotation_auto",
    "annotation_dispatch",
    "qc_result",
    "entity_tag",
    "entity_tag_index",
]

ingest_append_job = define_asset_job(
    name="ingest_append_job",
    selection=_INGEST_AND_ANNOTATE_SELECTION,
    description="README 3.2.1 / 3.6：manifest_op=append 的微批入湖 + 预标 + 路由链路（run_config 传 upload_ids）",
)

ingest_correct_job = define_asset_job(
    name="ingest_correct_job",
    selection=_INGEST_AND_ANNOTATE_SELECTION,
    description="README 3.2.1 / 3.6：manifest_op=correct 的微批范围限定 backfill + 重新标注链路",
)

annotation_collect_job = define_asset_job(
    name="annotation_collect_job",
    selection=["annotation_collect", "qc_result"],
    description="README 3.2.2：标注 CLI 提交结果后，收活 + 质检（run_config 传 batch_id）",
)

freeze_dataset_job = define_asset_job(
    name="freeze_dataset_job",
    selection=["dataset", "dataset_export"],
    description="README 3.2.3：建数据集请求触发的冻结 + 导出",
)

mock_train_job = define_asset_job(
    name="mock_train_job",
    selection=["train_run", "model_artifact"],
    description="README 3.2.4：科研人员直接调 API 触发的 mock 训练",
)

analytics_job = define_asset_job(
    name="analytics_job",
    selection=["analytics_summary"],
    description="手动/兜底刷新汇总指标（正常情况下由 Declarative Automation 自动触发）",
)
