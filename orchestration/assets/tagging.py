"""Tag 自由组织（README 3.1.4 / 3.2.5）：DuckDB 规则打标签，行为性替换的
典型例子——具体规则由策略注册表 `entity_tag` stage 解析（README 4.3）。
"""

from dagster import AssetExecutionContext, AssetKey, Output, asset


@asset(
    group_name="tagging",
    deps=[AssetKey("sample")],
    description="规则打标签（README 2.4：DuckDB，⚙ 策略注册表 stage=entity_tag）",
)
def entity_tag(context: AssetExecutionContext) -> Output[dict]:
    from engines.duckdb.entity_tag import run as entity_tag_run

    result = entity_tag_run(run_id=context.run_id)
    return Output(value=result, metadata=result)
