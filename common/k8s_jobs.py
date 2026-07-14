"""K8s worker Job 的创建/等待封装（README 3.6.3 pod fan-out）。

run pod（单写者，负责 Iceberg commit）用这里的函数把批内每个 upload 的
解析/清洗/切片外包给一个独立的 worker pod：

- worker 是普通 K8s Job（`backoffLimit=0`）：业务级失败由 worker 把错误写进
  staging 清单（pod 正常退出），pod 级失败（OOMKill/驱逐/镜像拉不下来）表现
  为"没有清单"，两种都由 run pod 按 upload 粒度 `fail_one` 收口——重试语义
  归 Saga/stuck sensor 管，不用 K8s 的 backoff 重试（那会绕过 saga 计数）。
- `activeDeadlineSeconds` 是 worker 的硬超时（runtime_config
  `INGEST_WORKER_TIMEOUT_SECONDS`）：卡死的 worker 会被 K8s 杀掉并把 Job 置
  Failed，run pod 不会无限等。
- `ttlSecondsAfterFinished=3600`：完成的 Job/Pod 保留 1 小时供查日志，之后
  K8s 自动清理，不留垃圾。
"""
from __future__ import annotations

import logging
import time
from typing import Callable

from common.config import settings

logger = logging.getLogger(__name__)

_WORKER_LABEL = "edp-ingest-worker"


def _batch_api():
    from kubernetes import client, config

    try:
        config.load_incluster_config()
    except Exception:  # noqa: BLE001 - 本地调试时退回 kubeconfig
        config.load_kube_config()
    return client.BatchV1Api()


def launch_parse_worker(
    *,
    name: str,
    upload_id: str,
    run_id: str,
    staging_prefix: str,
    timeout_seconds: int,
) -> str:
    """创建一个解析 worker Job，立即返回（不等待）。已存在同名 Job 视为复用。"""
    from kubernetes import client
    from kubernetes.client.rest import ApiException

    command = [
        "python", "-m", "engines.worker.ingest_parse",
        "--upload-id", upload_id,
        "--run-id", run_id,
        "--staging-prefix", staging_prefix,
    ]
    container = client.V1Container(
        name="worker",
        image=settings.edp_image,
        image_pull_policy="IfNotPresent",
        command=command,
        env_from=[client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name="edp-env"))],
        # worker 只做单个 upload 的纯 Python 解析，资源画像小而稳定——这正是
        # fan-out 的意义：计算 pod 有自己的 requests/limits，不和 run pod 抢。
        resources=client.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"memory": "1Gi"},
        ),
        volume_mounts=[client.V1VolumeMount(name="lance", mount_path="/data/lance")],
    )
    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        containers=[container],
        volumes=[
            client.V1Volume(
                name="lance",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="edp-lance"),
            )
        ],
    )
    job = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=name,
            labels={"app": _WORKER_LABEL, "upload-id": upload_id[:63], "dagster-run-id": run_id[:63]},
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            active_deadline_seconds=timeout_seconds,
            ttl_seconds_after_finished=3600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": _WORKER_LABEL}),
                spec=pod_spec,
            ),
        ),
    )
    try:
        _batch_api().create_namespaced_job(namespace=settings.k8s_namespace, body=job)
        logger.info("launched worker job %s for upload %s", name, upload_id)
    except ApiException as e:
        if e.status == 409:  # 同名 Job 已存在（上一轮循环创建过）：复用
            logger.warning("worker job %s already exists, reusing", name)
        else:
            raise
    return name


def wait_for_jobs(
    names: list[str],
    *,
    timeout_seconds: int,
    poll_seconds: float = 5.0,
    on_tick: Callable[[], None] | None = None,
) -> dict[str, str]:
    """等一组 Job 到终态，返回 {job_name: 'succeeded' | 'failed' | 'timeout'}。

    on_tick 在每轮轮询时回调（run pod 用它刷 saga 心跳，防止等待期间被
    stuck sensor 误判为 owner 已死）。timeout 兜底 activeDeadlineSeconds
    失效的情况（比如 pod 一直 Pending 调度不上去）。
    """
    api = _batch_api()
    deadline = time.monotonic() + timeout_seconds
    results: dict[str, str] = {}
    pending = set(names)
    while pending:
        for name in list(pending):
            try:
                status = api.read_namespaced_job_status(name=name, namespace=settings.k8s_namespace).status
            except Exception:  # noqa: BLE001 - 瞬时 API 抖动，下一轮重试
                logger.exception("read job status failed: %s", name)
                continue
            if (status.succeeded or 0) >= 1:
                results[name] = "succeeded"
                pending.discard(name)
            elif (status.failed or 0) >= 1:
                results[name] = "failed"
                pending.discard(name)
        if not pending:
            break
        if time.monotonic() > deadline:
            for name in pending:
                results[name] = "timeout"
            logger.warning("worker jobs timed out: %s", sorted(pending))
            break
        if on_tick is not None:
            on_tick()
        time.sleep(poll_seconds)
    return results
