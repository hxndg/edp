"""解析 worker pod 的拉起/监视封装（README 3.6.3，基于 Dagster Pipes）。

run pod（单写者）用 `launch_wave` 把一波 upload 各自外包给一个 worker pod：

- **PipesK8sClient**：起 pod、等终态、把 worker 的 stdout 日志实时流回本 run 的
  compute log（Dagster UI 直接看），worker 的 pipes 消息（完成小结/错误上报）
  也从日志流里解析回来。pod 由 client 在结束后自动删除——现场不靠 pod 留存，
  靠三样东西：流回的日志、staging 里的 manifest.json、saga_log 的 error_code。
- **并发**：client.run 是同步等单个 pod 的，一波内用线程池并发（每 upload 一个
  线程）；`common/db.py` 每次调用新建连接，线程安全。
- **监视线程**：等待期间每 poll_seconds 干两件事——回调 heartbeat（刷 saga
  心跳，防止被 stuck sensor 误判 owner 已死）；按 label 抓取 pod 终态快照
  （containerStatuses.terminated.reason / pod reason）。**这是 OOM/超时分类的
  唯一来源**：被 OOMKill/超时杀掉的进程不可能自报状态码（Pipes 通道一起死），
  只能由 run 侧从 pod 终态推断（common/errors.py 的来源 2）。
- **超时兜底**：pod 级 activeDeadlineSeconds 是主超时（kubelet 杀 pod）；
  future.result 的 grace 超时兜底"pod 一直 Pending 调度不上、deadline 不生效"
  的情况，超时后主动删 pod 促使等待线程退出。

重试语义不在这层：pod 不重启（restart_policy=Never）、不重建，失败分类后交给
saga（fail_one 带码）+ 重试层（orchestration/sensors.py 按码决定自动重试）。
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field

from common.config import settings
from common.errors import ErrorCode

logger = logging.getLogger(__name__)

WORKER_LABEL = "edp-ingest-worker"


@dataclass
class WorkerSpec:
    upload_id: str
    staging_prefix: str
    memory_limit: str  # 按 saga attempt 升档（INGEST_WORKER_MEMORY_TIERS）


@dataclass
class PodOutcome:
    """一个 worker pod 的最终观测结果（pod 层，不含 manifest 内容）。"""

    upload_id: str
    exception: str | None = None          # client.run 抛出的异常（含 worker 非零退出）
    pod_phase: str | None = None          # 最后一次快照的 pod.status.phase
    pod_reason: str | None = None         # pod.status.reason（DeadlineExceeded 等）
    container_reason: str | None = None   # terminated.reason（OOMKilled 等）
    exit_code: int | None = None
    pod_names: list[str] = field(default_factory=list)

    def classify(self) -> tuple[ErrorCode, str]:
        """无清单时的 pod 级失败分类（common/errors.py 来源 2）。"""
        if self.container_reason == "OOMKilled" or self.exit_code == 137:
            return ErrorCode.WORKER_OOM, f"worker 容器被 OOMKill（exit={self.exit_code}）"
        if self.pod_reason == "DeadlineExceeded" or self.container_reason == "DeadlineExceeded":
            return ErrorCode.WORKER_TIMEOUT, "worker 超过 activeDeadlineSeconds 被杀"
        detail = self.exception or f"phase={self.pod_phase} reason={self.pod_reason}"
        return ErrorCode.WORKER_LOST, f"worker 无清单且原因不明：{detail}"


class _WaveMonitor(threading.Thread):
    """一波 worker 的后台监视线程：刷 saga 心跳 + 抓 pod 终态快照。"""

    def __init__(self, run_id: str, outcomes: dict[str, PodOutcome], heartbeat, poll_seconds: float = 5.0):
        super().__init__(daemon=True, name="worker-wave-monitor")
        self.run_id = run_id
        self.outcomes = outcomes
        self.heartbeat = heartbeat
        self.poll_seconds = poll_seconds
        # 注意不能叫 _stop：会遮蔽 threading.Thread 内部的 _stop() 方法
        #（join() 在线程结束时调用它），导致 TypeError: 'Event' object is not callable
        self._halt = threading.Event()

    def run(self) -> None:
        while not self._halt.wait(self.poll_seconds):
            try:
                self.heartbeat()
            except Exception:  # noqa: BLE001 - 心跳失败不致命，下一轮再试
                logger.exception("saga heartbeat failed")
            try:
                self._snapshot_pods()
            except Exception:  # noqa: BLE001 - API 抖动，下一轮再试
                logger.exception("pod snapshot failed")

    def stop(self) -> None:
        self._halt.set()
        self.join(timeout=self.poll_seconds * 2)

    def _snapshot_pods(self) -> None:
        api = _core_api()
        pods = api.list_namespaced_pod(
            namespace=settings.k8s_namespace,
            label_selector=f"app={WORKER_LABEL},dagster-run-id={self.run_id[:63]}",
        ).items
        for pod in pods:
            upload_id = (pod.metadata.labels or {}).get("upload-id")
            outcome = self.outcomes.get(upload_id)
            if outcome is None:
                continue
            if pod.metadata.name not in outcome.pod_names:
                outcome.pod_names.append(pod.metadata.name)
            outcome.pod_phase = pod.status.phase
            outcome.pod_reason = pod.status.reason
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.terminated:
                    outcome.container_reason = cs.state.terminated.reason
                    outcome.exit_code = cs.state.terminated.exit_code


def _core_api():
    import kubernetes

    try:
        kubernetes.config.load_incluster_config()
    except Exception:  # noqa: BLE001 - 本地调试退回 kubeconfig
        kubernetes.config.load_kube_config()
    return kubernetes.client.CoreV1Api()


def _pipes_client():
    from dagster_k8s import PipesK8sClient

    return PipesK8sClient(poll_interval=3.0)


def _run_one(client, op_context, spec: WorkerSpec, run_id: str, timeout_seconds: int, outcome: PodOutcome) -> None:
    """在线程池里跑一个 worker pod。所有异常收进 outcome——一个 upload 的失败
    只影响它自己（后续由 manifest/pod 终态分类），绝不打断同波其他线程。"""
    try:
        client.run(
            context=op_context,
            image=settings.edp_image,
            command=[
                "python", "-m", "engines.worker.ingest_parse",
                "--upload-id", spec.upload_id,
                "--run-id", run_id,
                "--staging-prefix", spec.staging_prefix,
            ],
            namespace=settings.k8s_namespace,
            base_pod_meta={
                "labels": {
                    "app": WORKER_LABEL,
                    "upload-id": spec.upload_id[:63],
                    "dagster-run-id": run_id[:63],
                }
            },
            base_pod_spec={
                "active_deadline_seconds": timeout_seconds,
                "containers": [
                    {
                        "name": "worker",
                        "image_pull_policy": "IfNotPresent",
                        "env_from": [{"config_map_ref": {"name": "edp-env"}}],
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "256Mi"},
                            "limits": {"memory": spec.memory_limit},
                        },
                        "volume_mounts": [{"name": "lance", "mount_path": "/data/lance"}],
                    }
                ],
                "volumes": [{"name": "lance", "persistent_volume_claim": {"claim_name": "edp-lance"}}],
            },
        )
    except Exception as e:  # noqa: BLE001
        outcome.exception = f"{type(e).__name__}: {e}"
        logger.warning("worker pod for upload %s failed: %s", spec.upload_id, outcome.exception)


def launch_wave(
    op_context,
    specs: list[WorkerSpec],
    *,
    run_id: str,
    timeout_seconds: int,
    heartbeat,
) -> dict[str, PodOutcome]:
    """并发跑一波 worker pod，阻塞到全部结束，返回 {upload_id: PodOutcome}。

    结果的真相判定不在这里：调用方拿 outcome 与 staging 的 manifest.json 组合——
    有清单看清单（worker 自报），无清单用 outcome.classify()（pod 终态推断）。
    """
    outcomes = {s.upload_id: PodOutcome(upload_id=s.upload_id) for s in specs}
    monitor = _WaveMonitor(run_id, outcomes, heartbeat)
    monitor.start()
    client = _pipes_client()
    grace = timeout_seconds + 120  # 兜底 Pending 调度不上：deadline 从 pod 启动才计时
    try:
        with ThreadPoolExecutor(max_workers=len(specs), thread_name_prefix="worker-pod") as pool:
            futures = {
                s.upload_id: pool.submit(_run_one, client, op_context, s, run_id, timeout_seconds, outcomes[s.upload_id])
                for s in specs
            }
            for upload_id, future in futures.items():
                try:
                    future.result(timeout=grace)
                except FutureTimeout:
                    outcomes[upload_id].exception = f"等待超过 {grace}s（pod 可能一直未调度）"
                    _delete_pods(outcomes[upload_id].pod_names)  # 促使等待线程收到 pod 消失而退出
                    future.result()  # pod 删除后 client.run 会很快返回/抛错
    finally:
        monitor.stop()
    return outcomes


def _delete_pods(pod_names: list[str]) -> None:
    api = _core_api()
    for name in pod_names:
        try:
            api.delete_namespaced_pod(name=name, namespace=settings.k8s_namespace)
        except Exception:  # noqa: BLE001 - 已经不存在等
            logger.exception("delete pod %s failed", name)
