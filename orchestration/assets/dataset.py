"""冻结 Dataset + 导出（README 3.2.3）。

`dataset` 由科研人员通过 Launchpad 填参数或网关的建数据集 API 触发
（README 4.1），配置走 Dagster 的类型化 Config Schema，不是裸 YAML。
冻结前的质量门是硬性前置条件（见 `engines/spark/freeze_dataset.py` 里的
`FreezeGateError`），过不了直接抛异常，这里额外注册一个同名 Asset Check
（`orchestration/checks.py`）用于在 UI 上给出同样的结论。
"""

from dagster import AssetExecutionContext, AssetIn, Config, Output, asset

from engines.spark.freeze_dataset import FreezeGateError


class FreezeDatasetConfig(Config):
    request_id: str = "manual"
    dataset_name: str
    filter_expr: dict = {}
    quality_threshold: float = 0.0
    split: dict = {}


@asset(
    group_name="dataset",
    deps=["sample", "annotation_auto", "annotation_collect", "qc_result", "entity_tag"],
    name="dataset",
)
def dataset_asset(context: AssetExecutionContext, config: FreezeDatasetConfig) -> Output[str]:
    from common.db import execute
    from engines.spark.freeze_dataset import run as freeze_run

    try:
        result = freeze_run(
            request_id=config.request_id,
            dataset_name=config.dataset_name,
            filter_expr=config.filter_expr,
            quality_threshold=config.quality_threshold,
            split=config.split,
            run_id=context.run_id,
        )
    except FreezeGateError as e:
        execute(
            "UPDATE dataset_request SET status = 'failed', updated_at = now() WHERE request_id = %s",
            (config.request_id,),
        )
        context.log.error(f"freeze gate failed: {e} stats={e.stats}")
        raise

    execute(
        "UPDATE dataset_request SET status = 'released', dataset_version = %s, updated_at = now() WHERE request_id = %s",
        (result["dataset_version"], config.request_id),
    )
    return Output(value=result["dataset_version"], metadata=result)


class ExportDatasetConfig(Config):
    strategy_id: str | None = None


@asset(
    group_name="dataset",
    ins={"dataset": AssetIn()},
    name="dataset_export",
    description="把 Dataset 清单物化成训练格式 shard（README 2.4 / 3.1.7：⚙ 策略注册表 stage=export）",
)
def dataset_export_asset(context: AssetExecutionContext, config: ExportDatasetConfig, dataset: str) -> Output[dict]:
    from engines.spark.export_dataset import run as export_run

    dataset_name = _dataset_name_of(dataset)
    strategy_id, result = export_run(dataset_name, dataset, run_id=context.run_id, strategy_id=config.strategy_id)
    return Output(value=result, metadata={"strategy_id": strategy_id, **_safe(result)})


def _dataset_name_of(dataset_version: str) -> str:
    from common.iceberg import in_filter, load_table

    rows = load_table("dataset").scan(row_filter=in_filter("dataset_version", [dataset_version])).to_arrow().to_pylist()
    if not rows:
        raise ValueError(f"dataset_version '{dataset_version}' 在 dataset 表里找不到对应的 dataset_name")
    return rows[0]["dataset_name"]


def _safe(d: dict) -> dict:
    return {k: v for k, v in d.items() if k != "shard_uris"}
