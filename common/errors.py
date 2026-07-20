"""状态码体系（docs/pod-fanout-guide.md 第四节）：worker/run 失败的统一分类。

状态码与传输解耦——码本身有三个来源，各自的产生方式不同：

1. **worker 自报**（业务失败，进程活着能说清楚）：worker 把 code + message
   写进 staging 的 manifest.json（契约真相）。
2. **run 侧从 pod 终态推断**（pod 级失败，进程死了不可能自报）：OOMKill 是
   内核直接杀进程、超时是 kubelet 杀 pod；run pod 事后从 Argo 节点终态
   （message 里的 OOMKilled / deadline）分类，见 common/argo_workflows.py。
3. **run 侧自身错误**（commit 冲突、PG 挂了）：不经过 worker。

每个码带**可重试性**标记，worker 退出策略把它翻译给 Argo：
- RETRYABLE：瞬时环境问题，由 Argo task retry；
- NOT_RETRYABLE：数据自身的问题，重试一万次结果一样——直接终态 + 隔离 +
  alert，只有人工修数据后显式 reset 才会再跑；
- NEEDS_ANALYSIS：OOM/超时这类"可能是资源配置问题也可能是数据问题"，
  Argo retry 时按当前不可变执行 Profile 的 memory_tiers 升档。
"""
from __future__ import annotations

from enum import Enum


class Retry(str, Enum):
    RETRYABLE = "retryable"
    NOT_RETRYABLE = "not_retryable"
    NEEDS_ANALYSIS = "needs_analysis"


class ErrorCode(str, Enum):
    # ---- worker 自报（业务失败，写进 manifest.json）----
    DATA_PARSE_ERROR = "DATA_PARSE_ERROR"      # MCAP 解析失败 / 消息格式非法
    DATA_EMPTY = "DATA_EMPTY"                  # 文件里没有目标 topic 的消息 / 全部文件被隔离
    INPUT_MISSING = "INPUT_MISSING"            # input.json 或原始文件读不到（NoSuchKey）
    STORAGE_IO_ERROR = "STORAGE_IO_ERROR"      # 对象存储限流/超时（SlowDown 等瞬时故障）

    # ---- run 侧从 pod 终态推断（worker 死了，自报不了）----
    WORKER_OOM = "WORKER_OOM"                  # 容器 OOMKilled
    WORKER_TIMEOUT = "WORKER_TIMEOUT"          # activeDeadlineSeconds 到期被杀 / 等待总超时
    WORKER_LOST = "WORKER_LOST"                # 无清单且原因不明（驱逐/镜像拉不下/未调度）

    # ---- run 侧自身 ----
    COMMIT_CONFLICT = "COMMIT_CONFLICT"        # Iceberg commit 冲突/失败（整批级）
    PG_ERROR = "PG_ERROR"                      # platform 库不可达（整批级）
    STUCK_EXHAUSTED = "STUCK_EXHAUSTED"        # 心跳超时且自动重试次数耗尽（stuck sensor 落的）
    INTERNAL = "INTERNAL"                      # 兜底：未分类异常


RETRY_POLICY: dict[ErrorCode, Retry] = {
    ErrorCode.DATA_PARSE_ERROR: Retry.NOT_RETRYABLE,
    ErrorCode.DATA_EMPTY: Retry.NOT_RETRYABLE,
    ErrorCode.INPUT_MISSING: Retry.RETRYABLE,
    ErrorCode.STORAGE_IO_ERROR: Retry.RETRYABLE,
    ErrorCode.WORKER_OOM: Retry.NEEDS_ANALYSIS,
    ErrorCode.WORKER_TIMEOUT: Retry.NEEDS_ANALYSIS,
    ErrorCode.WORKER_LOST: Retry.RETRYABLE,
    ErrorCode.COMMIT_CONFLICT: Retry.RETRYABLE,
    ErrorCode.PG_ERROR: Retry.RETRYABLE,
    ErrorCode.STUCK_EXHAUSTED: Retry.NOT_RETRYABLE,
    ErrorCode.INTERNAL: Retry.RETRYABLE,
}


def retry_policy(code: str | None) -> Retry:
    """按码查重试策略。历史行没有码（升级前落的）按 RETRYABLE 处理——
    宁可多试一次幂等重写，也不把可救的数据判死。"""
    try:
        return RETRY_POLICY[ErrorCode(code)] if code else Retry.RETRYABLE
    except ValueError:
        return Retry.RETRYABLE


class WorkerError(Exception):
    """worker 内部用：带码的业务异常。worker 顶层把它翻译成 manifest 的
    error_code/error 字段；不带码的裸异常由 classify_exception 兜底归类。"""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code


def classify_exception(exc: BaseException, *, where: str = "worker") -> ErrorCode:
    """把一个异常归类成状态码（worker 顶层与 run 侧公用的兜底分类器）。

    只认"确定性强"的特征：boto 的错误码、psycopg 的异常基类；其余落 INTERNAL
    （策略是 RETRYABLE，重试一次不亏——幂等重写保证结果正确）。

    where 区分调用位置：ValueError/TypeError 这类裸异常在 worker 里大概率是
    数据的问题（→ DATA_PARSE_ERROR，不可重试）；在 run 侧则大概率是代码 bug
    （→ INTERNAL，可重试）——把好数据错判成"毒数据"比多重试一次代价高得多。
    """
    if isinstance(exc, WorkerError):
        return exc.code

    # botocore.exceptions.ClientError：看服务端错误码
    error_code = getattr(exc, "response", None)
    if isinstance(error_code, dict):
        s3_code = error_code.get("Error", {}).get("Code", "")
        if s3_code in ("NoSuchKey", "404", "NotFound"):
            return ErrorCode.INPUT_MISSING
        if s3_code:  # SlowDown / SlowDownRead / RequestTimeout / 503 ...
            return ErrorCode.STORAGE_IO_ERROR

    mod = type(exc).__module__ or ""
    if mod.startswith("botocore") or mod.startswith("boto3"):
        return ErrorCode.STORAGE_IO_ERROR
    if mod.startswith("psycopg"):
        return ErrorCode.PG_ERROR
    if mod.startswith("pyiceberg"):
        return ErrorCode.COMMIT_CONFLICT
    if where == "worker" and isinstance(exc, (ValueError, KeyError, TypeError)):
        return ErrorCode.DATA_PARSE_ERROR
    return ErrorCode.INTERNAL


def format_error(code: ErrorCode | str, message: str) -> str:
    """业务表错误摘要 / alerts 的统一格式：`[CODE] message`。"""
    code_str = code.value if isinstance(code, ErrorCode) else str(code)
    return f"[{code_str}] {message}"
