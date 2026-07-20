"""`ingest_append` job 的 run 侧逻辑（README 3.2.1 / 3.6.3）：新增采集，只新建/追加。

pod fan-out 形态（README 3.6.3，讲解见 docs/pod-fanout-guide.md）：run pod 是
**控制面 + 单写者**，重活外包：

    run pod                                worker pods（每 upload 一个）
    ────────────────────────────────       ─────────────────────────────
    execution_claim（PG CAS 互斥）
    写 input.json 到 staging  ──────────▶  下载 MCAP → 流式解析/清洗/切片
    提交 Argo Workflow（并发/超时归它管）      → 写 Lance → 厚表分块写 staging parquet
    收 manifest.json（缺失→查节点终态定码）◀─  薄表行 + error_code 内联在 manifest 里
    合并全批行，每表每批一次 Iceberg commit
    最终事务：session done/failed + release claim

Argo 管 task 重试、OOM 内存升档和节点终态；Dagster 等整批 Workflow 结束，
提交成功产物到 Iceberg，最后一次性写 PG 业务终态。

**以 PG 状态为起点**：本函数只处理 status ≠ done 的 session。这让 Dagster UI 的
Re-execute（run_config 里的 upload_ids 原样重放）天然安全——已成功的 upload
被廉价跳过，只重跑失败/悬置的那部分。
"""
from __future__ import annotations

import logging

import pyarrow as pa

from common.db import fetch_all, to_json, transaction
from common.errors import ErrorCode, classify_exception, format_error
from common.execution_claim import ClaimBatch
from common.iceberg import in_filter, replace_where_chunked, upsert
from common.processing_registry import ProcessingDefinition, resolve_processing_type
from common.runtime_config import get_int
from common.strategy_registry import resolve
from common.argo_workflows import PodOutcome, WorkerSpec, launch_wave
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
Failure = tuple[ErrorCode, str, PodOutcome | None]


def run_batch(upload_ids: list[str], processing_type: str, op_context) -> dict:
    """薄 claim → Argo 执行 → Iceberg 单写者 → PG 最终批量汇总。"""
    run_id = op_context.run_id
    definition = resolve_processing_type(processing_type, expected_kind="ingest")
    sessions = {
        row["upload_id"]: row
        for row in fetch_all(
            """
            SELECT * FROM upload_session
            WHERE upload_id = ANY(%s) AND manifest_op = 'append'
              AND processing_type = %s AND status <> 'done'
            """,
            (list(upload_ids), processing_type),
        )
    }
    not_pending = [uid for uid in upload_ids if uid not in sessions]
    if not_pending:
        logger.info("跳过不存在/非 append/已 done 的 upload：%s", not_pending)

    batch = ClaimBatch(SCOPE, list(sessions), run_id)
    with transaction() as conn:
        claimed = batch.acquire_many(conn=conn)
        if claimed:
            conn.execute(
                """
                UPDATE upload_session
                SET status = 'ingesting', last_dagster_run_id = %s,
                    last_execution_profile_id = %s,
                    last_error_code = NULL, last_error = NULL, updated_at = now()
                WHERE upload_id = ANY(%s)
                """,
                (run_id, definition.profile.profile_id, claimed),
            )
    skipped = [uid for uid in sessions if uid not in claimed]

    try:
        result = _execute_batch(sessions, claimed, run_id, batch, definition)
    except Exception as e:  # noqa: BLE001 - 整批级异常（典型：Iceberg commit / PG 故障）
        code = classify_exception(e, where="run")
        detail = f"{type(e).__name__}: {e}"
        _finalize_uploads(batch, [], {uid: (code, detail, None) for uid in claimed}, run_id)
        raise

    result["num_requested"] = len(upload_ids)
    result["skipped_uploads"] = skipped + not_pending
    return result


def _execute_batch(
    sessions: dict[str, dict],
    claimed: list[str],
    run_id: str,
    batch: ClaimBatch,
    definition: ProcessingDefinition,
) -> dict:
    if not definition.strategy_stage:
        raise ValueError(f"processing_type {definition.processing_type} 未配置清洗策略")
    strategy = resolve(definition.strategy_stage, definition.strategy_id)

    manifests, failures = _fan_out_parse(
        sessions, claimed, run_id, batch, definition, strategy.entrypoint
    )

    def _alive() -> list[dict]:
        ids = batch.heartbeat_many(list(manifests))
        return [manifests[uid] for uid in ids]

    # ---- INDEX：薄表（manifest 内联行）合并，每表一次 upsert commit ----
    ms = _alive()
    for table in (RAW_FILE, EPISODE, EPISODE_FILE):
        _upsert_thin(table, ms)

    # ---- BRONZE / SILVER：厚表从 staging 逐 row group 收回，事务内分块追加，
    # 每表仍是一次 commit（内存与批大小解耦，见 replace_where_chunked）----
    ms = _alive()
    _replace_thick(BRONZE_IMU, ms)
    ms = _alive()
    _replace_thick(SILVER_IMU, ms)

    # ---- SAMPLES：sample / gold_sample_index 薄表 upsert ----
    ms = _alive()
    _upsert_thin(SAMPLE, ms)
    _upsert_thin(GOLD_SAMPLE_INDEX, ms)

    succeeded = _finalize_uploads(
        batch, [m["upload_id"] for m in ms], failures, run_id
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
        "failures": {uid: format_error(code, message) for uid, (code, message, _) in failures.items()},
        "per_upload": per_upload,
        "num_samples": sum(len(p["sample_ids"]) for p in per_upload),
        "quarantined_files": sum(p["quarantined_files"] for p in per_upload),
        "silver_clean_strategy_id": strategy.strategy_id,
        "execution_profile_id": definition.profile.profile_id,
    }


def _finalize_uploads(
    batch: ClaimBatch,
    success_ids: list[str],
    failures: dict[str, Failure],
    run_id: str,
) -> list[str]:
    """按当前 run 的 fencing token，一次事务落业务终态并释放租约。"""
    requested = list(dict.fromkeys([*success_ids, *failures]))
    if not requested:
        return []
    failure_rows = [
        {"id": uid, "code": code.value, "error": message[:2000]}
        for uid, (code, message, _outcome) in failures.items()
    ]
    with transaction() as conn:
        owned_rows = conn.execute(
            """
            SELECT business_id FROM execution_claim
            WHERE scope = %s AND business_id = ANY(%s) AND run_id = %s
            FOR UPDATE
            """,
            (batch.scope, requested, run_id),
        ).fetchall()
        owned = {row["business_id"] for row in owned_rows}
        succeeded = [uid for uid in success_ids if uid in owned]
        owned_failures = [row for row in failure_rows if row["id"] in owned]
        if succeeded:
            conn.execute(
                """
                UPDATE upload_session
                SET status = 'done', last_error_code = NULL, last_error = NULL, updated_at = now()
                WHERE upload_id = ANY(%s) AND last_dagster_run_id = %s
                """,
                (succeeded, run_id),
            )
        if owned_failures:
            conn.execute(
                """
                UPDATE upload_session us
                SET status = 'failed', last_error_code = f.code,
                    last_error = f.error, updated_at = now()
                FROM jsonb_to_recordset(%s::jsonb) AS f(id text, code text, error text)
                WHERE us.upload_id = f.id AND us.last_dagster_run_id = %s
                """,
                (to_json(owned_failures), run_id),
            )
            conn.execute(
                """
                INSERT INTO alerts (severity, source, run_id, message, context)
                SELECT 'error', %s, %s, 'upload ' || f.id || ' 处理失败：[' || f.code || '] ' || f.error,
                       jsonb_build_object('upload_id', f.id, 'error_code', f.code, 'error', f.error)
                FROM jsonb_to_recordset(%s::jsonb) AS f(id text, code text, error text)
                """,
                (batch.scope, run_id, to_json(owned_failures)),
            )
        batch.release_many(owned, conn=conn)
    return succeeded


def _fan_out_parse(
    sessions: dict[str, dict],
    upload_ids: list[str],
    run_id: str,
    batch: ClaimBatch,
    definition: ProcessingDefinition,
    clean_entrypoint: str,
    mode: str = "append",
    extra_input: dict[str, dict] | None = None,
) -> tuple[dict[str, dict], dict[str, Failure]]:
    """每 upload 一个解析 worker；按 INGEST_BATCH_MAX 切 Workflow（默认一批
    ≤200），模板、镜像、并发、超时和内存档全部来自本 run 冻结的 Profile。

    返回 (成功的 {upload_id: manifest}, 失败的 {upload_id: "[CODE] msg"})。
    ingest_correct 复用（mode="correct"，extra_input 注入 episode 锚点）。
    """
    profile = definition.profile
    batch_max = max(1, get_int("INGEST_BATCH_MAX", 200))

    manifests: dict[str, dict] = {}
    failures: dict[str, Failure] = {}

    for i in range(0, len(upload_ids), batch_max):
        chunk = upload_ids[i : i + batch_max]
        specs: list[WorkerSpec] = []
        for uid in chunk:
            prefix = staging.prefix(run_id, uid)
            payload = {
                "mode": mode,
                "upload_id": uid,
                "run_id": run_id,
                "session": {
                    k: sessions[uid][k] for k in ("upload_id", "robot_id", "task_id", "operator", "manifest")
                },
                "clean_entrypoint": clean_entrypoint,
                "processing_type": definition.processing_type,
                "execution_profile_id": profile.profile_id,
                "chunk_rows": profile.chunk_rows,
                **({"episode": extra_input[uid]} if extra_input else {}),
            }
            staging.write_json(f"{prefix}/{staging.INPUT_JSON}", payload)
            specs.append(
                WorkerSpec(
                    upload_id=uid,
                    staging_prefix=prefix,
                    memory_tiers=list(profile.memory_tiers),
                    command=[
                        "python",
                        "-m",
                        definition.worker_module,
                        "--upload-id",
                        uid,
                        "--run-id",
                        run_id,
                        "--staging-prefix",
                        prefix,
                    ],
                )
            )

        outcomes = launch_wave(
            specs,
            run_id=run_id,
            timeout_seconds=profile.timeout_seconds,
            parallelism=profile.parallelism,
            workflow_template_name=profile.workflow_template_name,
            image_ref=profile.image_ref,
            processing_type=definition.processing_type,
            execution_profile_id=profile.profile_id,
            # 心跳防 stuck 误判；fencing 在后续 advance_many 边界统一做
            heartbeat=lambda ids=list(chunk): batch.heartbeat_many(ids),
        )

        for uid in chunk:
            m = staging.try_read_json(f"{staging.prefix(run_id, uid)}/{staging.MANIFEST_JSON}")
            outcome = outcomes.get(uid)
            if m is None:
                code, detail = (outcome.classify() if outcome else (ErrorCode.WORKER_LOST, "无 Argo 观测"))
                failures[uid] = (code, detail, outcome)
            elif m.get("status") != "ok":
                code = ErrorCode(m.get("error_code") or ErrorCode.INTERNAL.value)
                failures[uid] = (code, m.get("error", "worker 报告未知错误"), outcome)
            else:
                manifests[uid] = m
            if uid in failures:
                code, detail, observed = failures[uid]
                logger.warning(
                    "worker %s 最终失败：%s；Argo=%s",
                    uid,
                    format_error(code, detail),
                    observed.to_dict() if observed else None,
                )
    return manifests, failures


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
