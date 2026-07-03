"""策略注册表（README 3.1.7 / 4.3）：行为性替换的解析入口。

结构性分支（要不要人工、新增还是修正）在 Dagster 资产图里用独立 asset/sensor
表达，见 `orchestration/`；行为性替换（同一步骤换个算法）统一走这里——
asset 只知道 `stage`，运行时按 `(stage, strategy_id)` 查 `pipeline_step_config`
解析出具体函数，调用方把解析到的 `strategy_id` 写进物化 metadata。
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Callable

from common.db import fetch_all, fetch_one


@dataclass(frozen=True)
class Strategy:
    stage: str
    strategy_id: str
    entrypoint: str
    owner: str
    is_default: bool
    description: str | None = None

    def load(self) -> Callable[..., Any]:
        module_name, func_name = self.entrypoint.split(":")
        module = importlib.import_module(module_name)
        return getattr(module, func_name)


def get_default_strategy_id(stage: str) -> str:
    row = fetch_one(
        "SELECT strategy_id FROM pipeline_step_config WHERE stage = %s AND is_default = TRUE",
        (stage,),
    )
    if row is None:
        raise LookupError(f"pipeline_step_config 里 stage='{stage}' 没有登记默认策略")
    return row["strategy_id"]


def resolve(stage: str, strategy_id: str | None = None) -> Strategy:
    """解析 (stage, strategy_id) -> Strategy。strategy_id 为空时取该 stage 的默认策略。"""
    sid = strategy_id or get_default_strategy_id(stage)
    row = fetch_one(
        "SELECT * FROM pipeline_step_config WHERE stage = %s AND strategy_id = %s",
        (stage, sid),
    )
    if row is None:
        raise LookupError(f"pipeline_step_config 里找不到 stage='{stage}' strategy_id='{sid}'")
    return Strategy(
        stage=row["stage"],
        strategy_id=row["strategy_id"],
        entrypoint=row["entrypoint"],
        owner=row["owner"],
        is_default=row["is_default"],
        description=row.get("description"),
    )


def list_strategies(stage: str | None = None) -> list[Strategy]:
    if stage:
        rows = fetch_all("SELECT * FROM pipeline_step_config WHERE stage = %s ORDER BY strategy_id", (stage,))
    else:
        rows = fetch_all("SELECT * FROM pipeline_step_config ORDER BY stage, strategy_id")
    return [
        Strategy(
            stage=r["stage"],
            strategy_id=r["strategy_id"],
            entrypoint=r["entrypoint"],
            owner=r["owner"],
            is_default=r["is_default"],
            description=r.get("description"),
        )
        for r in rows
    ]


def run_strategy(stage: str, strategy_id: str | None, *args: Any, **kwargs: Any) -> tuple[Strategy, Any]:
    """解析并直接执行策略，返回 (解析到的 Strategy, 函数返回值)。

    调用方通常这样用：
        strategy, result = run_strategy("entity_tag", upstream_strategy_id, df)
        context.add_output_metadata({"strategy_id": strategy.strategy_id})
    """
    strategy = resolve(stage, strategy_id)
    func = strategy.load()
    return strategy, func(*args, **kwargs)
