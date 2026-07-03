"""自助数据探索（README 3.2.5 / 4.4）：DuckDB 直接对 episode/sample/dataset
的 Arrow 快照跑聚合查询。挂 `AutoMaterializePolicy.eager()`，上游一变就自动
刷新，不用为这种"总览指标"手写 cron（Declarative Automation，README 4.4）。
"""

from dagster import AssetExecutionContext, AssetKey, AutoMaterializePolicy, Output, asset


@asset(
    group_name="analytics",
    deps=[AssetKey("episode"), AssetKey("sample"), AssetKey("dataset")],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="episode/sample/dataset 的汇总指标，供科研人员自助查看（README 3.2.5）",
)
def analytics_summary(context: AssetExecutionContext) -> Output[dict]:
    from engines.duckdb.analytics_summary import run as analytics_run

    result = analytics_run(run_id=context.run_id)
    return Output(value=result, metadata=result)
