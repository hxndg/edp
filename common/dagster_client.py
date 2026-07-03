"""网关 -> Dagster 的 API/Webhook 触发通道（README 4.1）。

网关自己不做任何编排判断，只把"建数据集请求""标注完成回调"这两类事件
转成一次 Dagster job 的 launch 调用；真正的路由/校验逻辑都在 Dagster 资产/job 里。
"""
from __future__ import annotations

import functools
import logging

from dagster_graphql import DagsterGraphQLClient

from common.config import settings

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def client() -> DagsterGraphQLClient:
    return DagsterGraphQLClient(settings.dagster_host, port_number=settings.dagster_port)


def launch_job(job_name: str, run_config: dict | None = None, tags: dict | None = None) -> str | None:
    """提交一次 job 运行，返回 run_id；Dagster 不可达时记日志并返回 None，
    不阻塞网关主流程——兜底 sensor 会在下一个轮询周期发现同样的状态并补上。
    """
    try:
        run_id = client().submit_job_execution(job_name, run_config=run_config or {}, tags=tags or {})
        logger.info("launched dagster job=%s run_id=%s", job_name, run_id)
        return run_id
    except Exception:  # noqa: BLE001
        logger.exception("failed to launch dagster job=%s (sensor 兜底会补上)", job_name)
        return None
