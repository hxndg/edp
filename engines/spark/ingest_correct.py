"""`ingest_correct` job 的核心逻辑（README 3.2.1）：修正已有 episode 的某段时间范围，
范围限定 backfill，只触达受影响的分区/样本，并把受影响 sample 的 annotation/qc_result
标记为 pending，重新进入 3.2.2 节的标注流程。
"""
from __future__ import annotations

import logging
from datetime import datetime

import pyarrow as pa
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

from common import object_store
from common.audit import make_batch_id
from common.db import execute, fetch_one, to_json
from common.iceberg import in_filter, load_table, replace_where, upsert, with_audit_columns
from common.saga import Saga, SagaOwnershipLostError
from common.strategy_registry import run_strategy
from engines.spark.ingest_common import (
    bucket_by_window,
    compute_quality_score,
    read_imu_messages,
    sha256_bytes,
    split_s3_uri,
    write_sample_to_lance,
)
from schemas.iceberg_tables import (
    ANNOTATION,
    EPISODE,
    GOLD_SAMPLE_INDEX,
    QC_RESULT,
    RAW_FILE,
    SAMPLE,
)

logger = logging.getLogger(__name__)


def run(upload_id: str, run_id: str) -> dict:
    """Saga 外壳与 ingest_append.run 相同（docs/saga-consistency-guide.md）：
    claim 互斥 → 分步 advance（心跳 + fencing）→ 显式 SUCCEEDED/FAILED 终态。
    """
    session = fetch_one("SELECT * FROM upload_session WHERE upload_id = %s", (upload_id,))
    if session is None:
        raise ValueError(f"upload_session '{upload_id}' 不存在")
    if session["manifest_op"] != "correct":
        raise ValueError(f"upload_session '{upload_id}' 的 manifest_op 不是 correct")

    saga = Saga("ingest_correct", upload_id, run_id)
    saga.claim()
    execute("UPDATE upload_session SET status = 'ingesting', updated_at = now() WHERE upload_id = %s", (upload_id,))

    try:
        result = _execute(session, upload_id, run_id, saga)
    except SagaOwnershipLostError:
        raise
    except Exception as e:  # noqa: BLE001
        if saga.fail(f"{type(e).__name__}: {e}"):
            execute("UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = %s", (upload_id,))
        raise

    saga.succeed()
    execute("UPDATE upload_session SET status = 'done', updated_at = now() WHERE upload_id = %s", (upload_id,))
    return result


def _execute(session: dict, upload_id: str, run_id: str, saga: Saga) -> dict:
    manifest = session["manifest"]
    episode_id = manifest["episode_id"]
    affected_start = datetime.fromisoformat(manifest["affected_start_ts"])
    affected_end = datetime.fromisoformat(manifest["affected_end_ts"])
    batch_id = make_batch_id(robot_id=session["robot_id"], upload_id=upload_id)

    episode_rows = (
        load_table(EPISODE)
        .scan(row_filter=EqualTo("episode_id", episode_id))
        .to_arrow()
        .to_pylist()
    )
    if not episode_rows:
        raise ValueError(f"要修正的 episode '{episode_id}' 不存在，correct 只能修正已有 episode")
    episode = episode_rows[0]
    robot_id = episode["robot_id"]
    episode_start_ts = episode["start_ts"]

    # 1) 重新解析这次提交的文件（只覆盖 affected 范围）
    saga.advance("PARSE")
    raw_file_rows: list[dict] = []
    bronze_payload_rows: list[dict] = []
    for entry in manifest["files"]:
        file_uri = entry["file_uri"]
        bucket, key = split_s3_uri(file_uri)
        data = object_store.get_bytes(key, bucket=bucket)
        sha256 = sha256_bytes(data)
        rows = read_imu_messages(data)
        ts_values = [r["ts"] for r in rows]
        raw_file_rows.append(
            {
                "file_uri": file_uri,
                "robot_id": robot_id,
                "task_id": session["task_id"],
                "start_ts": min(ts_values) if ts_values else affected_start,
                "end_ts": max(ts_values) if ts_values else affected_end,
                "sha256": sha256,
                "schema_version": entry.get("schema_version", "v1"),
                "upload_id": upload_id,
                "status": "ok",
            }
        )
        for r in rows:
            bronze_payload_rows.append({"payload": r["payload"], "ts": r["ts"]})

    def _prep(rows: list[dict]) -> pa.Table | None:
        if not rows:
            return None
        tbl = pa.Table.from_pylist(rows)
        return with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri=f"upload:{upload_id}")

    saga.advance("RAW_INDEX")
    tbl = _prep(raw_file_rows)
    if tbl is not None:
        upsert(RAW_FILE, tbl, join_cols=["file_uri"])

    # 2) 范围限定覆盖（README 4.6）：delete 受影响时间窗 + append 修正数据必须原子，
    #    否则并发读者会在两次 commit 之间看到"旧数据没了、新数据还没来"的空洞——
    #    这正是用户担心的"sensor 定时重试与读端并发"场景，replace_where 用
    #    pyiceberg transaction 把两步合成单次快照提交解决。
    range_filter = And(
        EqualTo("episode_id", episode_id),
        GreaterThanOrEqual("ts", affected_start),
        LessThanOrEqual("ts", affected_end),
    )
    saga.advance("BRONZE")
    replace_where(
        "bronze_imu",
        range_filter,
        _prep(
            [
                {
                    "episode_id": episode_id,
                    "robot_id": robot_id,
                    "source_file": manifest["files"][0]["file_uri"],
                    "seq": i,
                    "ts": r["ts"],
                    "payload_json": to_json(r["payload"]),
                }
                for i, r in enumerate(bronze_payload_rows)
            ]
        ),
    )

    strategy, silver_rows = run_strategy("silver_clean", None, bronze_payload_rows)

    saga.advance("SILVER")
    silver_table_rows = [{**r, "episode_id": episode_id, "robot_id": robot_id} for r in silver_rows]
    replace_where("silver_imu", range_filter, _prep(silver_table_rows))

    # 3) 重新切片受影响的窗口；window_index 用绝对时间锚点计算，天然命中原来的 sample_id
    saga.advance("SAMPLES")
    windows = bucket_by_window(silver_rows, episode_start_ts=episode_start_ts)
    sample_rows = []
    gold_rows = []
    affected_sample_ids: list[str] = []
    for idx, window in sorted(windows.items()):
        sample_id = f"{episode_id}-w{idx:04d}"
        affected_sample_ids.append(sample_id)
        score, tags = compute_quality_score(window)
        lance_uri = write_sample_to_lance(sample_id, window)
        sample_rows.append(
            {
                "sample_id": sample_id,
                "episode_id": episode_id,
                "robot_id": robot_id,
                "event_date": episode_start_ts,
                "slicer_version": "v1-fixed-window",
                "lance_uri": lance_uri,
                "quality_score": score,
                "quality_tags_json": to_json(tags),
            }
        )
        gold_rows.append(
            {"episode_id": episode_id, "sample_id": sample_id, "duration_s": 2.0, "num_points": len(window), "quality_score": score}
        )

    tbl = _prep(sample_rows)
    if tbl is not None:
        upsert(SAMPLE, tbl, join_cols=["sample_id"])
    tbl = _prep(gold_rows)
    if tbl is not None:
        upsert(GOLD_SAMPLE_INDEX, tbl, join_cols=["sample_id"])

    # 4) 受影响 sample 的 annotation/qc_result 置为 pending，重新进入 3.2.2 标注流程
    saga.advance("RESET_DOWNSTREAM")
    num_reset_annotations = _reset_to_pending(ANNOTATION, "target_id", affected_sample_ids, {"review_status": "pending"}, "anno_id", batch_id, run_id, upload_id)
    num_reset_qc = _reset_to_pending(QC_RESULT, "target_id", affected_sample_ids, {"verdict": "need_review"}, "qc_id", batch_id, run_id, upload_id)

    return {
        "episode_id": episode_id,
        "status": "done",
        "affected_samples": len(affected_sample_ids),
        "reset_annotations": num_reset_annotations,
        "reset_qc_results": num_reset_qc,
        "silver_clean_strategy_id": strategy.strategy_id,
    }


def _reset_to_pending(
    table_name: str,
    target_col: str,
    target_ids: list[str],
    updates: dict,
    pk_col: str,
    batch_id: str,
    run_id: str,
    upload_id: str,
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
    existing["_source_uri"] = f"upload:{upload_id}"
    tbl = pa.Table.from_pandas(existing, preserve_index=False)
    upsert(table_name, tbl, join_cols=[pk_col])
    return len(existing)
