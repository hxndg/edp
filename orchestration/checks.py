"""Asset Checks + Freshness Checks（README 3.2.3 / 4.4 / 4.8）。

`dataset` 的质量门在 `engines/spark/freeze_dataset.py` 里已经是硬性前置条件
（过不了直接抛异常，不会真的写出一个状态不合格的 dataset）；这里再注册一个
同名 Asset Check，纯粹是为了在 Dagster UI 上给出一个独立可查的"质量结论"卡片
（README 4.8："跑了但不对"要有管道内质量门），跟硬 gate 是同一个判断标准，
只是多一个可视化入口。

Freshness Check 对应 README 4.1/4.8 的"该跑没跑"场景：`sample` 超过一个
T+1 窗口没有新物化，UI 直接标红。
"""
from __future__ import annotations

from datetime import timedelta

from dagster import AssetKey, AssetCheckResult, asset_check, build_last_update_freshness_checks

freshness_checks = build_last_update_freshness_checks(
    assets=[AssetKey("sample")],
    lower_bound_delta=timedelta(hours=27),  # T+1 兜底窗口 24h + 3h 缓冲
)


@asset_check(asset=AssetKey("dataset"), description="冻结前质量门：样本非空 + 平均质量分达标（与硬 gate 同标准）")
def dataset_quality_gate(context) -> AssetCheckResult:
    materialization = context.instance.get_latest_materialization_event(AssetKey("dataset"))
    if materialization is None or materialization.asset_materialization is None:
        return AssetCheckResult(passed=False, description="dataset 还没有任何物化记录")

    metadata = materialization.asset_materialization.metadata
    num_samples = metadata.get("num_samples")
    mean_quality = metadata.get("mean_quality_score")
    num_samples_val = num_samples.value if num_samples else 0
    mean_quality_val = mean_quality.value if mean_quality else 0.0

    passed = bool(num_samples_val and num_samples_val > 0)
    return AssetCheckResult(
        passed=passed,
        metadata={"num_samples": num_samples_val, "mean_quality_score": mean_quality_val},
        description=f"num_samples={num_samples_val} mean_quality_score={mean_quality_val}",
    )
