"""Argo Workflow 提交与最终观测。

Argo 管 task 并发、重试、phase/exit/message/log；Dagster 阻塞到 Workflow 终态，
随后一次性收割产物并写 Iceberg/PG。worker 模板在 `31-argo-worker-template.yaml`。
"""
from __future__ import annotations

import hashlib
import json
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
    memory_tiers: list[str]
    # 自定义入口（训练 worker 用）；None = 默认 ingest 解析入口
    command: list[str] | None = None


@dataclass
class PodOutcome:
    """一个 worker 节点的最终观测结果（Argo node 层，不含 manifest 内容）。"""

    upload_id: str
    phase: str | None = None       # Argo node phase：Succeeded / Failed / Error / None
    message: str | None = None     # OOMKilled / deadline 等
    exit_code: int | None = None
    workflow: str | None = None
    retry_count: int = 0
    pod_names: list[str] = field(default_factory=list)
    log_uris: list[str] = field(default_factory=list)  # MinIO 归档路径，pod GC 后仍可查

    def classify(self) -> tuple[ErrorCode, str]:
        """无清单时的 pod 级失败分类（common/errors.py 来源 2）。"""
        msg = self.message or ""
        if "OOMKilled" in msg or self.exit_code == 137:
            return ErrorCode.WORKER_OOM, f"worker 容器被 OOMKill（node message: {msg}）"
        if "deadline" in msg.lower():
            return ErrorCode.WORKER_TIMEOUT, "worker 超过 activeDeadlineSeconds 被杀"
        detail = msg or f"phase={self.phase}（节点未观测到，pod 可能没调度上）"
        return ErrorCode.WORKER_LOST, f"worker 无清单且原因不明：{detail}"

    def to_dict(self) -> dict:
        """供 run 日志 / alerts 汇总（phase、exit、归档地址）。"""
        return {
            "upload_id": self.upload_id,
            "phase": self.phase,
            "exit_code": self.exit_code,
            "message": self.message,
            "workflow": self.workflow,
            "retry_count": self.retry_count,
            "pod_names": list(self.pod_names),
            "log_uris": list(self.log_uris),
        }


def _log_uri(workflow: str, pod_name: str) -> str:
    """与 deploy/k8s/30-argo.yaml artifactRepository.keyFormat 对齐。"""
    if not workflow or not pod_name:
        return ""
    return f"s3://{settings.minio_bucket}/argo/{workflow}/{pod_name}"


def _custom_api():
    import kubernetes

    try:
        kubernetes.config.load_incluster_config()
    except Exception:  # noqa: BLE001 - 本地调试退回 kubeconfig
        kubernetes.config.load_kube_config()
    return kubernetes.client.CustomObjectsApi()


def workflow_phases_for_run(run_id: str) -> dict[str, str | None]:
    """查询某个 Dagster run 仍可见的 Argo Workflow 及 phase，供 reconciliation 使用。"""
    api = _custom_api()
    result = api.list_namespaced_custom_object(
        GROUP,
        VERSION,
        settings.k8s_namespace,
        PLURAL,
        label_selector=f"dagster-run-id={run_id[:63]}",
    )
    return {
        item.get("metadata", {}).get("name", "<unknown>"): (item.get("status") or {}).get("phase")
        for item in result.get("items", [])
    }


def _workflow_name(run_id: str, specs: list[WorkerSpec]) -> str:
    digest = hashlib.sha1(",".join(sorted(s.upload_id for s in specs)).encode()).hexdigest()[:6]
    return f"edp-{run_id[:8]}-{digest}".lower()


def _build_workflow(
    name: str,
    specs: list[WorkerSpec],
    run_id: str,
    timeout_seconds: int,
    parallelism: int,
    *,
    workflow_template_name: str,
    image_ref: str,
    processing_type: str,
    execution_profile_id: str,
) -> dict:
    """批 → 引用稳定 WorkflowTemplate 的轻量 Workflow CR。"""
    items = []
    for s in specs:
        command = s.command or [
            "python", "-m", "engines.worker.ingest_parse",
            "--upload-id", s.upload_id,
            "--run-id", run_id,
            "--staging-prefix", s.staging_prefix,
        ]
        tiers = (s.memory_tiers or ["1Gi"])[:3]
        tiers.extend([tiers[-1]] * (3 - len(tiers)))
        script = (
            f"python -m engines.worker.exit_policy --clear --staging-prefix "
            f"{shlex.quote(s.staging_prefix)}; {shlex.join(command)}; rc=$?; "
            f"if [ $rc -ne 0 ]; then exit $rc; fi; "
            f"python -m engines.worker.exit_policy --staging-prefix {shlex.quote(s.staging_prefix)}"
        )
        items.append({
            "biz_id": s.upload_id,
            "script": script,
            "memory_0": tiers[0],
            "memory_1": tiers[1],
            "memory_2": tiers[2],
        })

    waves = -(-len(specs) // max(parallelism, 1))
    return {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Workflow",
        "metadata": {
            "name": name,
            "namespace": settings.k8s_namespace,
            "labels": {"app": WORKER_LABEL, "dagster-run-id": run_id[:63]},
            "annotations": {
                "edp/processing-type": processing_type,
                "edp/execution-profile-id": execution_profile_id,
                "edp/image-ref": image_ref,
            },
        },
        "spec": {
            "workflowTemplateRef": {"name": workflow_template_name},
            "serviceAccountName": "dagster",
            "parallelism": parallelism,
            "activeDeadlineSeconds": timeout_seconds * waves + 120,
            "arguments": {"parameters": [
                {"name": "items", "value": json.dumps(items, ensure_ascii=False)},
                {"name": "image", "value": image_ref},
                {"name": "timeout-seconds", "value": str(timeout_seconds)},
            ]},
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
    """轮询到 Workflow 终态；每个 tick 续执行租约。"""
    deadline = time.monotonic() + deadline_seconds
    wf: dict = {}
    while time.monotonic() < deadline:
        try:
            heartbeat()
        except Exception:  # noqa: BLE001 - 心跳失败不致命，下一轮再试
            logger.exception("execution claim heartbeat failed")
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
    """按 Pod 输入里的完整 biz-id 聚合，并选择每个 task 最后结束的一次重试。"""
    outcomes = {s.upload_id: PodOutcome(upload_id=s.upload_id, workflow=name) for s in specs}
    candidates: dict[str, list[dict]] = {s.upload_id: [] for s in specs}
    for node in ((wf.get("status") or {}).get("nodes") or {}).values():
        if node.get("type") != "Pod":
            continue
        params = {
            p.get("name"): p.get("value")
            for p in ((node.get("inputs") or {}).get("parameters") or [])
        }
        uid = params.get("biz-id")
        if uid not in outcomes:
            continue
        candidates[uid].append(node)

    for uid, nodes in candidates.items():
        if not nodes:
            continue
        nodes.sort(key=lambda n: (n.get("finishedAt") or "", n.get("startedAt") or ""))
        o = outcomes[uid]
        o.retry_count = max(0, len(nodes) - 1)
        for node in nodes:
            pod_id = node.get("id") or ""
            if pod_id:
                o.pod_names.append(pod_id)
                uri = _log_uri(name, pod_id)
                if uri:
                    o.log_uris.append(uri)
        node = nodes[-1]
        o.phase = node.get("phase")
        o.message = node.get("message")
        exit_code = ((node.get("outputs") or {}).get("exitCode"))
        if exit_code is not None:
            try:
                o.exit_code = int(exit_code)
            except ValueError:
                pass
    return outcomes


def launch_wave(
    specs: list[WorkerSpec],
    *,
    run_id: str,
    timeout_seconds: int,
    heartbeat,
    parallelism: int = 20,
    workflow_template_name: str,
    image_ref: str,
    processing_type: str,
    execution_profile_id: str,
) -> dict[str, PodOutcome]:
    """提交一个批次 Workflow 并阻塞到终态，返回 {upload_id: PodOutcome}。

    调用方：有清单看清单；无清单用 outcome.classify()，并把 outcome.to_dict()
    写进 run 日志/alert（phase / exit_code / log_uris）。
    """
    if not specs:
        return {}
    api = _custom_api()
    name = _workflow_name(run_id, specs)
    _submit(
        api,
        _build_workflow(
            name,
            specs,
            run_id,
            timeout_seconds,
            parallelism,
            workflow_template_name=workflow_template_name,
            image_ref=image_ref,
            processing_type=processing_type,
            execution_profile_id=execution_profile_id,
        ),
    )
    waves = -(-len(specs) // max(parallelism, 1))
    wf = _wait(api, name, timeout_seconds * waves + 180, heartbeat)
    phase = (wf.get("status") or {}).get("phase")
    logger.info(
        "workflow %s 终态：%s（%s tasks, parallelism=%s）；日志前缀 s3://%s/argo/%s/",
        name, phase, len(specs), parallelism, settings.minio_bucket, name,
    )
    outcomes = _collect(wf, specs, name)
    for o in outcomes.values():
        if o.phase not in (None, "Succeeded"):
            logger.warning("worker %s argo 观测：%s", o.upload_id, o.to_dict())
    return outcomes
