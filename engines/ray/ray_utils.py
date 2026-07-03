"""Ray local mode 的公共入口（README 2.4：GPU/ML 执行占位）。

真实生产环境这里会换成连接 K8s + Volcano 的远程 Ray 集群（README 4.9 backlog），
MVP 用本地单机 Ray 集群验证"任务只声明要什么资源，路由规则决定派给谁"这个
解耦点——调用方不需要关心 Ray 是本地起的还是远程集群。
"""
from __future__ import annotations

import ray


def ensure_ray() -> None:
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, include_dashboard=False, num_cpus=2, log_to_driver=False)
