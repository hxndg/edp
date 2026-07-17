"""`ingest_append` job 的 run 侧逻辑（README 3.2.1 / 3.6.3）：新增采集，只新建/追加。

pod fan-out 形态（README 3.6.3，讲解见 docs/pod-fanout-guide.md）：run pod 是
**控制面 + 单写者**，重活外包：

    run pod                                worker pods（每 upload 一个）
    ────────────────────────────────       ─────────────────────────────
    claim_many（saga 互斥）
    写 input.json 到 staging  ──────────▶  下载 MCAP → 流式解析/清洗/切片
    提交 Argo Workflow（并发/超时归它管）      → 写 Lance → 厚表分块写 staging parquet
    收 manifest.json（缺失→查节点终态定码）◀─  薄表行 + error_code 内联在 manifest 里
    合并全批行，每表每批一次 Iceberg commit
    succeed_many + session done

失败语义（common/errors.py）：
- worker 业务失败：manifest 里带 error_code（worker 自报）→ fail_one 逐条隔离；
- pod 级失败（OOM/超时/丢失）：无清单 → 按 pod 终态推断码 → fail_one；
- 整批级（commit 冲突/PG 挂）：classify_exception 定码 → fail_many 收尾后上抛。
重试由触发层按码决定（orchestration/sensors.py），OOM 重试时 worker 内存自动升档。

**以 PG 状态为起点**：本函数只处理 status ≠ done 的 session。这让 Dagster UI 的
Re-execute（run_config 里的 upload_ids 原样重放）天然安全——已成功的 upload
被廉价跳过，只重跑失败/悬置的那部分。
"""
from __future__ import annotations

import logging

import pyarrow as pa

from common.db import execute, fetch_all, to_json
from common.errors import ErrorCode, classify_exception, format_error
from common.iceberg import in_filter, replace_where_chunked, upsert
from common.runtime_config import get_int, get_str
from common.saga import SagaBatch
from common.strategy_registry import resolve
from common.argo_workflows import WorkerSpec, launch_wave
from engines.worker import staging
from schemas.iceberg_tables import (
    BRONZE_IMU,
    EPISODE,
    EPISODE_FILE,
    GOLD_SAMPLE_INDEX,
    RAW_FILE,
    SAMPLE,
    SILVER_IMU,
)

logger = logging.getLogger(__name__)

SCOPE = "ingest_append"
THIN_TABLE_KEYS = {
    RAW_FILE: ["file_uri"],
    EPISODE: ["episode_id"],
    EPISODE_FILE: ["episode_id", "file_uri"],
    SAMPLE: ["sample_id"],
    GOLD_SAMPLE_INDEX: ["sample_id"],
}


def run_batch(upload_ids: list[str], op_context) -> dict:
    """批量 Saga 外壳（README 3.6.3）。op_context 是 Dagster 的执行上下文：
    run_id 从它取（worker 日志由 Argo 归档到 s3://lake/argo/）。"""
    run_id = op_context.run_id
    sessions = {
        row["upload_id"]: row
        for row in fetch_all(
            "SELECT * FROM upload_session WHERE upload_id = ANY(%s) AND manifest_op = 'append' AND status <> 'done'",
            (list(upload_ids),),
        )
    }
    not_pending = [uid for uid in upload_ids if uid not in sessions]
    if not_pending:
        logger.info("跳过不存在/非 append/已 done 的 upload：%s", not_pending)

    batch = SagaBatch(SCOPE, list(sessions), run_id)
    claimed = batch.claim_many()
    skipped = [uid for uid in sessions if uid not in claimed]
    if claimed:
        execute(
            "UPDATE upload_session SET status = 'ingesting', updated_at = now() WHERE upload_id = ANY(%s)",
            (claimed,),
        )

    try:
        result = _execute_batch(sessions, claimed, run_id, batch, op_context)
    except Exception as e:  # noqa: BLE001 - 整批级异常（典型：Iceberg commit / PG 故障）
        code = classify_exception(e, where="run")
        failed = batch.fail_many(claimed, format_error(code, f"{type(e).__name__}: {e}"), error_code=code.value)
        if failed:
            execute(
                "UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = ANY(%s) AND status = 'ingesting'",
                (failed,),
            )
        raise

    result["num_requested"] = len(upload_ids)
    result["skipped_uploads"] = skipped + not_pending
    return result


def _execute_batch(sessions: dict[str, dict], claimed: list[str], run_id: str, batch: SagaBatch, op_context) -> dict:
    strategy = resolve("silver_clean", None)

    # ---- PARSE：fan-out 到 worker pod（每 upload 一个），失败逐条隔离 ----
    alive = batch.advance_many("PARSE", claimed)
    manifests, failures = _fan_out_parse(sessions, alive, run_id, batch, strategy.entrypoint, op_context)

    def _advance(step: str) -> list[dict]:
        ids = batch.advance_many(step, list(manifests))
        return [manifests[uid] for uid in ids]

    # ---- INDEX：薄表（manifest 内联行）合并，每表一次 upsert commit ----
    ms = _advance("INDEX")
    for table in (RAW_FILE, EPISODE, EPISODE_FILE):
        _upsert_thin(table, ms)

    # ---- BRONZE / SILVER：厚表从 staging 逐 row group 收回，事务内分块追加，
    # 每表仍是一次 commit（内存与批大小解耦，见 replace_where_chunked）----
    ms = _advance("BRONZE")
    _replace_thick(BRONZE_IMU, ms)
    ms = _advance("SILVER")
    _replace_thick(SILVER_IMU, ms)

    # ---- SAMPLES：sample / gold_sample_index 薄表 upsert ----
    ms = _advance("SAMPLES")
    _upsert_thin(SAMPLE, ms)
    _upsert_thin(GOLD_SAMPLE_INDEX, ms)

    # ---- 终态 ----
    succeeded = batch.succeed_many([m["upload_id"] for m in ms])
    if succeeded:
        execute(
            "UPDATE upload_session SET status = 'done', updated_at = now() WHERE upload_id = ANY(%s)",
            (succeeded,),
        )

    per_upload = [
        {
            "upload_id": m["upload_id"],
            "episode_id": m["episode_id"],
            "sample_ids": m["sample_ids"],
            "num_files": m["num_files"],
            "quarantined_files": len(m["quarantined_files"]),
        }
        for uid, m in manifests.items()
        if uid in succeeded
    ]
    return {
        "status": "done",
        "num_claimed": len(claimed),
        "num_succeeded": len(succeeded),
        "num_failed": len(failures),
        "failures": failures,
        "per_upload": per_upload,
        "num_samples": sum(len(p["sample_ids"]) for p in per_upload),
        "quarantined_files": sum(p["quarantined_files"] for p in per_upload),
        "silver_clean_strategy_id": strategy.strategy_id,
    }


def _memory_tier(attempt: int, tiers: list[str]) -> str:
    """attempt 1 用第 1 档，OOM 后重试逐档升，到顶封顶。"""
    return tiers[min(max(attempt, 1), len(tiers)) - 1]


def _fan_out_parse(
    sessions: dict[str, dict],
    upload_ids: list[str],
    run_id: str,
    batch: SagaBatch,
    clean_entrypoint: str,
    op_context,
    mode: str = "append",
    extra_input: dict[str, dict] | None = None,
) -> tuple[dict[str, dict], dict[str, str]]:
    """给每个 upload 起一个解析 worker（分波，受 INGEST_WORKER_MAX_PARALLEL 限制），
    等待并收清单。返回 (成功的 {upload_id: manifest}, 失败的 {upload_id: "[CODE] msg"})。
    ingest_correct 复用本函数（mode="correct"，extra_input 注入 episode 锚点）。
    """
    timeout = get_int("INGEST_WORKER_TIMEOUT_SECONDS", 600)
    max_parallel = max(1, get_int("INGEST_WORKER_MAX_PARALLEL", 20))
    chunk_rows = get_int("INGEST_WORKER_CHUNK_ROWS", 50000)
    tiers = [t.strip() for t in get_str("INGEST_WORKER_MEMORY_TIERS", "1Gi,2Gi,4Gi").split(",") if t.strip()]

    manifests: dict[str, dict] = {}
    failures: dict[str, str] = {}

    for i in range(0, len(upload_ids), max_parallel):
        wave = upload_ids[i : i + max_parallel]
        specs: list[WorkerSpec] = []
        for uid in wave:
            prefix = staging.prefix(run_id, uid)
            payload = {
                "mode": mode,
                "upload_id": uid,
                "run_id": run_id,
                "session": {
                    k: sessions[uid][k] for k in ("upload_id", "robot_id", "task_id", "operator", "manifest")
                },
                "clean_entrypoint": clean_entrypoint,
                "chunk_rows": chunk_rows,
                **({"episode": extra_input[uid]} if extra_input else {}),
            }
            staging.write_json(f"{prefix}/{staging.INPUT_JSON}", payload)
            specs.append(
                WorkerSpec(
                    upload_id=uid,
                    staging_prefix=prefix,
                    memory_limit=_memory_tier(batch.attempts.get(uid, 1), tiers),
                )
            )

        outcomes = launch_wave(
            op_context,
            specs,
            run_id=run_id,
            timeout_seconds=timeout,
            # 心跳（防 stuck sensor 误判）：只刷本波，返回值不用——fencing 在
            # 每个写入阶段的 advance_many 边界统一执行
            heartbeat=lambda ids=list(wave): batch.advance_many("PARSE", ids),
        )

        # 真相判定：有清单看清单（worker 自报的 error_code），无清单查 pod 终态
        for uid in wave:
            m = staging.try_read_json(f"{staging.prefix(run_id, uid)}/{staging.MANIFEST_JSON}")
            if m is None:
                code, detail = outcomes[uid].classify()
                _fail_upload(batch, uid, code, detail, run_id, failures)
            elif m.get("status") != "ok":
                code = ErrorCode(m.get("error_code") or ErrorCode.INTERNAL.value)
                _fail_upload(batch, uid, code, m.get("error", "worker 报告未知错误"), run_id, failures)
            else:
                manifests[uid] = m
                for file_uri in m.get("quarantined_files", []):
                    execute(
                        "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
                        ("error", batch.scope, run_id, f"quarantined file {file_uri}", to_json({"upload_id": uid, "file_uri": file_uri})),
                    )
    return manifests, failures


def _fail_upload(
    batch: SagaBatch, upload_id: str, code: ErrorCode, message: str, run_id: str, failures: dict[str, str]
) -> None:
    error = format_error(code, message)
    failures[upload_id] = error
    if batch.fail_one(upload_id, error, error_code=code.value):
        execute(
            "UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = %s AND status = 'ingesting'",
            (upload_id,),
        )
        execute(
            "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
            (
                "error",
                batch.scope,
                run_id,
                f"upload {upload_id} 解析失败已隔离（同批其他不受影响）：{error}",
                to_json({"upload_id": upload_id, "error_code": code.value, "error": message}),
            ),
        )


def _upsert_thin(table: str, manifests: list[dict]) -> None:
    rows: list[dict] = []
    for m in manifests:
        rows.extend(m.get("thin_rows", {}).get(table, []))
    if rows:
        upsert(table, pa.Table.from_pylist(rows), join_cols=THIN_TABLE_KEYS[table])


def _replace_thick(table: str, manifests: list[dict]) -> None:
    """本批 episode 旧行删除 + 各 worker 的 staging parquet 逐 row group 追加，
    同一事务单 commit；任何时刻内存里只有一个 row group（≈ chunk_rows 行）。"""
    if not manifests:
        return

    def _batches():
        for m in manifests:
            ref = m.get("thick_files", {}).get(table)
            if ref:
                yield from staging.iter_parquet_batches(ref["key"])

    replace_where_chunked(table, in_filter("episode_id", [m["episode_id"] for m in manifests]), _batches())
