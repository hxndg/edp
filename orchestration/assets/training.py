"""模型训练 asset（README 3.2.4 / 3.7）：模型也是 asset，dep 指向 `dataset`——
血缘图上"数据 → 模型"的那条边。触发走 gateway `POST /train` → platform_job →
`training_kafka_sensor`（orchestration/sensors.py），一个 job 一个 run。
"""

from dagster import AssetExecutionContext, Config, MetadataValue, Output, asset


class TrainingJobConfig(Config):
    """sensor 从 platform_job 取到的任务号；训练输入（模型名/数据集/超参/seed）
    都在 platform_job.payload 里——run_config 只传引用，保证 UI Re-execute 时
    以 PG 状态为起点，不携带可能过期的参数副本。"""

    job_id: str


@asset(
    name="model_training",
    group_name="training",
    deps=["dataset"],
    description="训练 worker pod（Argo Workflow）+ MLflow 打点 + Iceberg 四表归档（README 3.7）",
)
def model_training(context: AssetExecutionContext, config: TrainingJobConfig) -> Output[dict]:
    from engines.training.run_job import run_training

    result = run_training(config.job_id, context)
    metadata = {
        "job_id": config.job_id,
        "status": result["status"],
    }
    if result["status"] == "done":
        metadata.update(
            {
                "model_name": result["model_name"],
                "model_version": result["model_version"],
                "dataset_version": result["dataset_version"],
                "mlflow_run_id": result.get("mlflow_run_id") or "(mlflow 不可用)",
                "artifact_uri": result["artifact_uri"],
                "metrics": MetadataValue.json(result["metrics"]),
                "val_accuracy": float(result["metrics"].get("val_accuracy") or 0.0),
            }
        )
    return Output(value=result, metadata=metadata)
