"""worker pod 的拉起/监视——Argo Workflows 形态（README 3.6.3，替代 worker_pods.py）。

run pod 仍是控制面 + 单写者，但 pod 级监督全部外包给 Argo：

- **一个批次 = 一个 Workflow CR**：批内每个 upload/job 一个 DAG task（一个 pod），
  `spec.parallelism` 限并发（替代原来的线程池分波）；pod 超时用模板级
  activeDeadlineSeconds（kubelet 杀）；workflow 级 deadline 兜底整批卡死。
- **提交幂等**：workflow 名字由 run_id + 批内容摘要确定性生成，重复提交撞
  409 AlreadyExists 视为"已在跑"，直接跟踪等待。
- **等待 = 单线程轮询** workflow status，每个 tick 顺手回调 heartbeat 刷 saga
  心跳——原 _WaveMonitor 监视线程、grace 超时、主动删 pod 全部不再需要。
- **结果判定不变**：调用方仍以 staging 的 manifest.json 为真相（worker 自报
  error_code）；无清单时才用 Argo 节点终态推断（OOMKilled → WORKER_OOM 等），
  数据源从自己抓 pod 快照换成 Argo 记好的 node status。
- **日志**：controller 配了 archiveLogs → 每个 pod 的 stdout 归档进
  s3://lake/argo/{workflow}/{pod}，Argo UI 逐节点可查，pod 用后即删不丢现场。

重试语义整体不变（简单优先，Argo 侧 retryStrategy 暂不启用）：业务失败 worker
exit 0 + error manifest；pod 级失败节点 Failed；两者都由 saga fail 落码后交
watchdog 按码决定是否重回队列（common/jobs.py），OOM 重试时内存按 attempt 升档。
"""
from __future__ import annotations

import hashlib
import logging
import shlex
import time
from dataclasses import dataclass, field

from common.config import settings
from common.errors import ErrorCode

logger = logging.getLogger(__name__)

WORKER_LABEL = "edp-worker"
GROUP, VERSION, PLURAL = "argoproj.io", "v1alpha1", "workflows"
POLL_SECONDS = 5.0
_TERMINAL_PHASES = {"Succeeded", "Failed", "Error"}


@dataclass
class WorkerSpec:
    upload_id: str          # 业务 id（upload_id 或训练 job_id），也是 outcome 的键
    staging_prefix: str
    memory_limit: str       # 按 saga attempt 升档（*_WORKER_MEMORY_TIERS）
    # 自定义入口（训练 worker 用）；None = 默认 ingest 解析入口
    command: list[str] | None = None


@dataclass
class PodOutcome:
    """一个 worker 节点的最终观测结果（Argo node 层，不含 manifest 内容）。"""

    upload_id: str
    phase: str | None = None       # Argo node phase：Succeeded / Failed / Error / None(未观测到)
    message: str | None = None     # Argo node message（OOMKilled / deadline 等原因都在这里）
    exit_code: int | None = None   # node.outputs.exitCode（有则填）
    workflow: str | None = None
    pod_names: list[str] = field(default_factory=list)

    def classify(self) -> tuple[ErrorCode, str]:
        """无清单时的 pod 级失败分类（common/errors.py 来源 2）。"""
        msg = self.message or ""
        if "OOMKilled" in msg or self.exit_code == 137:
            return ErrorCode.WORKER_OOM, f"worker 容器被 OOMKill（node message: {msg}）"
        if "deadline" in msg.lower():
            return ErrorCode.WORKER_TIMEOUT, "worker 超过 activeDeadlineSeconds 被杀"
        detail = msg or f"phase={self.phase}（节点未观测到，pod 可能没调度上）"
        return ErrorCode.WORKER_LOST, f"worker 无清单且原因不明：{detail}"


def _custom_api():
    import kubernetes

    try:
        kubernetes.config.load_incluster_config()
    except Exception:  # noqa: BLE001 - 本地调试退回 kubeconfig
        kubernetes.config.load_kube_config()
    return kubernetes.client.CustomObjectsApi()


def _workflow_name(run_id: str, specs: list[WorkerSpec]) -> str:
    digest = hashlib.sha1(",".join(sorted(s.upload_id for s in specs)).encode()).hexdigest()[:6]
    return f"edp-{run_id[:8]}-{digest}".lower()


def _build_workflow(name: str, specs: list[WorkerSpec], run_id: str, timeout_seconds: int, parallelism: int) -> dict:
    """批 → Workflow CR。每个 spec 一个 task，全部引用同一个 worker 模板，
    命令行整体作为参数传入（shell 字符串，id 类参数无空格，shlex 安全）。"""
    tasks, args_list = [], []
    for i, s in enumerate(specs):
        command = s.command or [
            "python", "-m", "engines.worker.ingest_parse",
            "--upload-id", s.upload_id,
            "--run-id", run_id,
            "--staging-prefix", s.staging_prefix,
        ]
        tasks.append({
            "name": f"w-{i}",
            "template": "worker",
            "arguments": {"parameters": [
                {"name": "script", "value": shlex.join(command)},
                {"name": "memory", "value": s.memory_limit},
                {"name": "biz-id", "value": s.upload_id[:63]},
            ]},
        })
        args_list.append(s.upload_id)

    waves = -(-len(specs) // max(parallelism, 1))
    return {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Workflow",
        "metadata": {
            "name": name,
            "namespace": settings.k8s_namespace,
            "labels": {"app": WORKER_LABEL, "dagster-run-id": run_id[:63]},
        },
        "spec": {
            "entrypoint": "main",
            "serviceAccountName": "dagster",
            "parallelism": parallelism,
            # 兜底整批卡死（含 Pending 调度不上）：pod 级 deadline × 波数 + 余量
            "activeDeadlineSeconds": timeout_seconds * waves + 120,
            "volumes": [{"name": "lance", "persistentVolumeClaim": {"claimName": "edp-lance"}}],
            "templates": [
                {"name": "main", "dag": {"tasks": tasks}},
                {
                    "name": "worker",
                    "inputs": {"parameters": [{"name": "script"}, {"name": "memory"}, {"name": "biz-id"}]},
                    "activeDeadlineSeconds": timeout_seconds,
                    "metadata": {"labels": {"app": WORKER_LABEL, "biz-id": "{{inputs.parameters.biz-id}}"}},
                    # 动态资源走 podSpecPatch（模板字段里的 resources 不做参数替换）
                    "podSpecPatch": '{"containers":[{"name":"main","resources":'
                                    '{"requests":{"cpu":"100m","memory":"256Mi"},'
                                    '"limits":{"memory":"{{inputs.parameters.memory}}"}}}]}',
                    "container": {
                        "image": settings.edp_image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["bash", "-c"],
                        "args": ["{{inputs.parameters.script}}"],
                        "envFrom": [{"configMapRef": {"name": "edp-env"}}],
                        "volumeMounts": [{"name": "lance", "mountPath": "/data/lance"}],
                    },
                },
            ],
        },
    }


def _submit(api, workflow: dict) -> None:
    from kubernetes.client.rest import ApiException

    try:
        api.create_namespaced_custom_object(GROUP, VERSION, settings.k8s_namespace, PLURAL, workflow)
    except ApiException as e:
        if e.status == 409:  # 已存在（同 run 重试/并发触发撞车）：幂等，直接跟踪
            logger.info("workflow %s 已存在，直接跟踪", workflow["metadata"]["name"])
        else:
            raise


def _wait(api, name: str, deadline_seconds: int, heartbeat) -> dict:
    """轮询到 workflow 终态；每个 tick 刷一次 saga 心跳。返回最后一次的对象。"""
    deadline = time.monotonic() + deadline_seconds
    wf: dict = {}
    while time.monotonic() < deadline:
        try:
            heartbeat()
        except Exception:  # noqa: BLE001 - 心跳失败不致命，下一轮再试
            logger.exception("saga heartbeat failed")
        try:
            wf = api.get_namespaced_custom_object(GROUP, VERSION, settings.k8s_namespace, PLURAL, name)
            if (wf.get("status") or {}).get("phase") in _TERMINAL_PHASES:
                return wf
        except Exception:  # noqa: BLE001 - API 抖动，下一轮再试
            logger.exception("poll workflow %s failed", name)
        time.sleep(POLL_SECONDS)
    logger.warning("workflow %s 等待超时（%ss），按当前观测收尾", name, deadline_seconds)
    return wf


def _collect(wf: dict, specs: list[WorkerSpec], name: str) -> dict[str, PodOutcome]:
    """从 workflow.status.nodes 里按 task 名对出每个 spec 的节点终态。"""
    outcomes = {s.upload_id: PodOutcome(upload_id=s.upload_id, workflow=name) for s in specs}
    by_task = {f"w-{i}": s.upload_id for i, s in enumerate(specs)}
    for node in ((wf.get("status") or {}).get("nodes") or {}).values():
        uid = by_task.get(node.get("displayName"))
        if uid is None or node.get("type") != "Pod":
            continue
        o = outcomes[uid]
        o.phase = node.get("phase")
        o.message = node.get("message")
        o.pod_names.append(node.get("id", ""))
        exit_code = ((node.get("outputs") or {}).get("exitCode"))
        if exit_code is not None:
            try:
                o.exit_code = int(exit_code)
            except ValueError:
                pass
    return outcomes


def launch_wave(
    op_context,  # 兼容旧签名；Argo 形态不需要 Dagster 上下文
    specs: list[WorkerSpec],
    *,
    run_id: str,
    timeout_seconds: int,
    heartbeat,
    parallelism: int = 20,
) -> dict[str, PodOutcome]:
    """提交一个批次 Workflow 并阻塞到终态，返回 {upload_id: PodOutcome}。

    结果的真相判定不在这里：调用方拿 outcome 与 staging 的 manifest.json 组合——
    有清单看清单（worker 自报），无清单用 outcome.classify()（节点终态推断）。
    """
    if not specs:
        return {}
    api = _custom_api()
    name = _workflow_name(run_id, specs)
    _submit(api, _build_workflow(name, specs, run_id, timeout_seconds, parallelism))
    waves = -(-len(specs) // max(parallelism, 1))
    wf = _wait(api, name, timeout_seconds * waves + 180, heartbeat)
    phase = (wf.get("status") or {}).get("phase")
    logger.info("workflow %s 终态：%s（Argo UI 可按此名检索节点日志）", name, phase)
    return _collect(wf, specs, name)
