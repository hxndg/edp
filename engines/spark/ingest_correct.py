"""`ingest_correct` job 的 run 侧逻辑（README 3.2.1 / 3.6.3）：修正已有 episode 的
某段时间范围，范围限定 backfill，并把受影响 sample 的 annotation/qc_result 置
pending，重新进入 3.2.2 节的标注流程。

pod fan-out 形态与 ingest_append 相同（复用 `_fan_out_parse`，讲解见
docs/pod-fanout-guide.md），差异只有三处：
- run pod 先从 Iceberg 读出目标 episode 的锚点（robot_id / start_ts）放进
  input.json——worker 不碰 catalog，切片锚点必须由单写者喂给它；
- bronze/silver 的删除条件不是"整个 episode"而是每个 upload 声明的受影响
  时间窗，本批所有时间窗 Or 起来 + 分块追加修正数据，每表仍是一次事务式 commit；
- 多一个 RESET_DOWNSTREAM 阶段：受影响 sample 的 annotation/qc_result 置 pending。
"""
from __future__ import annotations

import logging
from datetime import datetime
from functools import reduce

import pyarrow as pa
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual, Or

from common.audit import make_batch_id
from common.db import fetch_all, transaction
from common.errors import ErrorCode, classify_exception, format_error
from common.execution_claim import ClaimBatch
from common.iceberg import in_filter, load_table, replace_where_chunked, upsert
from common.processing_registry import ProcessingDefinition, resolve_processing_type
from common.strategy_registry import resolve
from engines.spark.ingest_append import Failure, _fan_out_parse, _finalize_uploads, _upsert_thin
from engines.worker import staging
from schemas.iceberg_tables import (
    ANNOTATION,
    BRONZE_IMU,
    EPISODE,
    GOLD_SAMPLE_INDEX,
    QC_RESULT,
    RAW_FILE,
    SAMPLE,
    SILVER_IMU,
)

logger = logging.getLogger(__name__)

SCOPE = "ingest_correct"


def run_batch(upload_ids: list[str], processing_type: str, op_context) -> dict:
    """薄 claim + Argo + 最终批量落态，与 ingest_append.run_batch 同构。
    同样以 PG 状态为起点：status = done 的 upload 廉价跳过，Re-execute 安全。"""
    run_id = op_context.run_id
    definition = resolve_processing_type(processing_type, expected_kind="ingest")
    sessions = {
        row["upload_id"]: row
        for row in fetch_all(
            """
            SELECT * FROM upload_session
            WHERE upload_id = ANY(%s) AND manifest_op = 'correct'
              AND processing_type = %s AND status <> 'done'
            """,
            (list(upload_ids), processing_type),
        )
    }
    not_pending = [uid for uid in upload_ids if uid not in sessions]
    if not_pending:
        logger.info("跳过不存在/非 correct/已 done 的 upload：%s", not_pending)

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
    except Exception as e:  # noqa: BLE001
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

    # ---- 锚点准备：worker 不碰 catalog，episode 的 robot_id/start_ts 由 run pod
    # 一次批量读出，喂进各自的 input.json；episode 不存在的 upload 逐条隔离 ----
    anchors, failures = _load_episode_anchors(sessions, claimed)

    # ---- PARSE：fan-out 到 worker pod ----
    manifests, parse_failures = _fan_out_parse(
        sessions,
        list(anchors),
        run_id,
        batch,
        definition,
        strategy.entrypoint,
        mode="correct",
        extra_input=anchors,
    )
    failures.update(parse_failures)

    def _alive() -> list[dict]:
        ids = batch.heartbeat_many(list(manifests))
        return [manifests[uid] for uid in ids]

    # ---- RAW_INDEX：一次 upsert commit ----
    ms = _alive()
    _upsert_thin(RAW_FILE, ms)

    # ---- BRONZE / SILVER：本批所有受影响时间窗 Or 起来删旧 + 分块追加修正数据，
    # 每表一次事务式 commit（README 4.6：读者看不到"旧的没了新的没来"的空洞）----
    ms = _alive()
    _replace_thick_ranged(BRONZE_IMU, ms)
    ms = _alive()
    _replace_thick_ranged(SILVER_IMU, ms)

    # ---- SAMPLES：重新切片的样本 upsert（确定性 sample_id 命中原样本）----
    ms = _alive()
    _upsert_thin(SAMPLE, ms)
    _upsert_thin(GOLD_SAMPLE_INDEX, ms)

    # ---- RESET_DOWNSTREAM：受影响 sample 的 annotation/qc_result 置 pending，
    # 本批合并成每表一次 upsert ----
    ms = _alive()
    affected_sample_ids = sorted({sid for m in ms for sid in m.get("affected_sample_ids", [])})
    shared_batch_id = make_batch_id(robot_id="ingest_correct", upload_id=f"batch-{run_id[:8]}")
    num_reset_annotations = _reset_to_pending(
        ANNOTATION, "target_id", affected_sample_ids, {"review_status": "pending"}, "anno_id", shared_batch_id, run_id
    )
    num_reset_qc = _reset_to_pending(
        QC_RESULT, "target_id", affected_sample_ids, {"verdict": "need_review"}, "qc_id", shared_batch_id, run_id
    )

    succeeded = _finalize_uploads(batch, [m["upload_id"] for m in ms], failures, run_id)

    per_upload = [
        {
            "upload_id": m["upload_id"],
            "episode_id": m["episode_id"],
            "sample_ids": m["affected_sample_ids"],
            "num_files": m["num_files"],
            "quarantined_files": 0,
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
        "quarantined_files": 0,
        "reset_annotations": num_reset_annotations,
        "reset_qc_results": num_reset_qc,
        "silver_clean_strategy_id": strategy.strategy_id,
        "execution_profile_id": definition.profile.profile_id,
    }


def _load_episode_anchors(
    sessions: dict[str, dict], upload_ids: list[str]
) -> tuple[dict[str, dict], dict[str, Failure]]:
    """批量读目标 episode 的锚点。返回 ({upload_id: {episode_id, robot_id, start_ts}}, 失败)。"""
    target_by_upload = {uid: sessions[uid]["manifest"]["episode_id"] for uid in upload_ids}
    rows = (
        load_table(EPISODE)
        .scan(row_filter=in_filter("episode_id", sorted(set(target_by_upload.values()))))
        .to_arrow()
        .to_pylist()
        if target_by_upload
        else []
    )
    episode_by_id = {r["episode_id"]: r for r in rows}

    anchors: dict[str, dict] = {}
    failures: dict[str, Failure] = {}
    for uid, episode_id in target_by_upload.items():
        ep = episode_by_id.get(episode_id)
        if ep is None:
            failures[uid] = (
                ErrorCode.DATA_PARSE_ERROR,
                f"要修正的 episode '{episode_id}' 不存在，correct 只能修正已有 episode",
                None,
            )
        else:
            anchors[uid] = {"episode_id": episode_id, "robot_id": ep["robot_id"], "start_ts": ep["start_ts"]}
    return anchors, failures


def _range_filter(m: dict):
    return And(
        EqualTo("episode_id", m["episode_id"]),
        GreaterThanOrEqual("ts", datetime.fromisoformat(m["affected_range"]["start"])),
        LessThanOrEqual("ts", datetime.fromisoformat(m["affected_range"]["end"])),
    )


def _replace_thick_ranged(table: str, manifests: list[dict]) -> None:
    if not manifests:
        return

    def _batches():
        for m in manifests:
            ref = m.get("thick_files", {}).get(table)
            if ref:
                yield from staging.iter_parquet_batches(ref["key"])

    replace_where_chunked(table, reduce(Or, [_range_filter(m) for m in manifests]), _batches())


def _reset_to_pending(
    table_name: str,
    target_col: str,
    target_ids: list[str],
    updates: dict,
    pk_col: str,
    batch_id: str,
    run_id: str,
) -> int:
    if not target_ids:
        return 0
    table = load_table(table_name)
    try:
        existing = table.scan(row_filter=in_filter(target_col, target_ids)).to_arrow().to_pandas()
    except Exception:  # noqa: BLE001 - 表可能还没有任何行
        return 0
    if existing.empty:
        return 0
    for k, v in updates.items():
        existing[k] = v
    existing["_batch_id"] = batch_id
    existing["_run_id"] = run_id
    existing["_source_uri"] = f"run:{run_id}"
    tbl = pa.Table.from_pandas(existing, preserve_index=False)
    upsert(table_name, tbl, join_cols=[pk_col])
    return len(existing)
