"""训练/评测消费（mock，README 3.2.4）：科研人员直接调 Dagster API/Launchpad
触发，不经过网关——网关只转发"建数据集"和"标注完成"这两类事件（README 2.3）。
"""

from dagster import AssetExecutionContext, AssetKey, AssetOut, Config, Output, multi_asset


class MockTrainConfig(Config):
    dataset_version: str
    params: dict = {}


@multi_asset(
    name="mock_train",
    group_name="training",
    deps=[AssetKey("dataset_export")],
    outs={
        "train_run": AssetOut(description="mock 训练任务记录"),
        "model_artifact": AssetOut(description="产物元数据"),
    },
    description="mock 训练：读导出包、跑几秒、验证消费侧接口契约（README 2.4：Ray mock）",
)
def mock_train_assets(context: AssetExecutionContext, config: MockTrainConfig):
    from engines.ray.mock_train import run as train_run

    result = train_run(config.dataset_version, config.params, run_id=context.run_id)
    yield Output(value=result["train_run_id"], output_name="train_run", metadata=result["metrics"])
    yield Output(value=result["model_id"], output_name="model_artifact", metadata={"model_id": result["model_id"]})
