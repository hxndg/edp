"""`ingest_append` job 的核心逻辑（README 3.2.1）：新增采集，只新建/追加。

由 `orchestration/assets/ingest.py` 里的 `ingest_append` asset 直接调用
`run(upload_id, run_id)`。之所以直接函数调用而不是 shell 子进程：本地模式下
Spark/Ray 的计算本来就跑在各自的 JVM/worker 进程里，Dagster 进程内的这层
Python 代码只做参数准备和落表，符合"编排器只做控制面"的精神，同时避免了
子进程 stdout 解析的额外复杂度。
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pyarrow as pa

from common import object_store
from common.audit import make_batch_id
from common.db import execute, fetch_one, to_json
from common.iceberg import append, upsert, with_audit_columns
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
    BRONZE_IMU,
    EPISODE,
    EPISODE_FILE,
    GOLD_SAMPLE_INDEX,
    RAW_FILE,
    SAMPLE,
    SILVER_IMU,
)

logger = logging.getLogger(__name__)


def run(upload_id: str, run_id: str) -> dict:
    session = fetch_one("SELECT * FROM upload_session WHERE upload_id = %s", (upload_id,))
    if session is None:
        raise ValueError(f"upload_session '{upload_id}' 不存在")
    if session["manifest_op"] != "append":
        raise ValueError(f"upload_session '{upload_id}' 的 manifest_op 不是 append")

    execute("UPDATE upload_session SET status = 'ingesting', updated_at = now() WHERE upload_id = %s", (upload_id,))

    manifest = session["manifest"]
    robot_id = session["robot_id"]
    task_id = session["task_id"]
    batch_id = make_batch_id(robot_id=robot_id, upload_id=upload_id)
    # 确定性 episode_id（而非随机 uuid）：同一个 upload_id 重跑会命中同一个 episode，
    # 配合下面的 upsert 写法，保证"至少一次 + 幂等 = 最终一致"（README 2.2 原则 8）。
    episode_id = f"ep-{upload_id}"

    raw_file_rows: list[dict] = []
    all_bronze_rows: list[dict] = []
    episode_file_rows: list[dict] = []
    quarantined = 0

    for ordinal, entry in enumerate(manifest["files"]):
        file_uri = entry["file_uri"]
        bucket, key = split_s3_uri(file_uri)
        data: bytes | None = None
        try:
            data = object_store.get_bytes(key, bucket=bucket)
            sha256 = sha256_bytes(data)
            bronze_rows = read_imu_messages(data)
            if not bronze_rows:
                raise ValueError("文件里没有解析出任何 imu 消息")
            status = "ok"
        except Exception:  # noqa: BLE001
            logger.exception("file parse failed, quarantining: %s", file_uri)
            if data is not None:
                object_store.put_bytes(f"{object_store.PREFIX_QUARANTINE}/{upload_id}/{key.split('/')[-1]}", data)
            execute(
                "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
                ("error", "ingest_append", run_id, f"quarantined file {file_uri}", to_json({"upload_id": upload_id, "file_uri": file_uri})),
            )
            quarantined += 1
            raw_file_rows.append(
                {
                    "file_uri": file_uri,
                    "robot_id": robot_id,
                    "task_id": task_id,
                    "start_ts": None,
                    "end_ts": None,
                    "sha256": entry.get("sha256"),
                    "schema_version": entry.get("schema_version", "v1"),
                    "upload_id": upload_id,
                    "status": "quarantined",
                }
            )
            continue

        ts_values = [r["ts"] for r in bronze_rows]
        raw_file_rows.append(
            {
                "file_uri": file_uri,
                "robot_id": robot_id,
                "task_id": task_id,
                "start_ts": min(ts_values),
                "end_ts": max(ts_values),
                "sha256": sha256,
                "schema_version": entry.get("schema_version", "v1"),
                "upload_id": upload_id,
                "status": status,
            }
        )
        episode_file_rows.append({"episode_id": episode_id, "file_uri": file_uri, "ordinal": ordinal})
        for row in bronze_rows:
            all_bronze_rows.append(
                {
                    "robot_id": robot_id,
                    "episode_id": episode_id,
                    "source_file": file_uri,
                    "ts": row["ts"],
                    "seq": row["seq"],
                    "payload_json": to_json(row["payload"]),
                }
            )

    if not raw_file_rows or all(r["status"] == "quarantined" for r in raw_file_rows):
        execute("UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = %s", (upload_id,))
        return {"episode_id": None, "status": "failed", "quarantined_files": quarantined}

    start_ts = min(r["start_ts"] for r in raw_file_rows if r["start_ts"])
    end_ts = max(r["end_ts"] for r in raw_file_rows if r["end_ts"])

    episode_row = {
        "episode_id": episode_id,
        "robot_id": robot_id,
        "task_id": task_id,
        "operator": session["operator"],
        "start_ts": start_ts,
        "end_ts": end_ts,
        "firmware_ver": "mock-1.0",
        "calib_ver": "mock-1.0",
        "agent_ver": "mock-1.0",
        "source": "declared",
    }

    def _prep(rows: list[dict]) -> pa.Table | None:
        if not rows:
            return None
        tbl = pa.Table.from_pylist(rows)
        return with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri=f"upload:{upload_id}")

    def _write_append(table_name: str, rows: list[dict]) -> None:
        tbl = _prep(rows)
        if tbl is not None:
            append(table_name, tbl)

    def _write_upsert(table_name: str, rows: list[dict], join_cols: list[str]) -> None:
        tbl = _prep(rows)
        if tbl is not None:
            upsert(table_name, tbl, join_cols=join_cols)

    # 索引/目录表按主键 upsert：重跑同一个 upload_id 幂等，不产生重复行。
    _write_upsert(RAW_FILE, raw_file_rows, ["file_uri"])
    _write_upsert(EPISODE, [episode_row], ["episode_id"])
    _write_upsert(EPISODE_FILE, episode_file_rows, ["episode_id", "file_uri"])
    # Bronze/Silver 是纯追加信号表（README 4.6），MVP 简化：不去重，失败重跑可能产生
    # 重复信号行，已知取舍，见 4.5 节"数据一致性原则"的讨论。
    _write_append(BRONZE_IMU, all_bronze_rows)

    # 行为性替换：silver 清洗策略从 pipeline_step_config 解析（README 4.3）
    bronze_payload_rows = [
        {"payload": _json_loads(r["payload_json"]), "ts": r["ts"]} for r in all_bronze_rows
    ]
    strategy, silver_rows = run_strategy("silver_clean", None, bronze_payload_rows)
    silver_table_rows = [{**r, "episode_id": episode_id, "robot_id": robot_id} for r in silver_rows]
    _write_append(SILVER_IMU, silver_table_rows)

    windows = bucket_by_window(silver_rows, episode_start_ts=start_ts)
    sample_rows = []
    gold_rows = []
    for idx, window in sorted(windows.items()):
        # 确定性 sample_id：同一 episode 同一窗口序号永远映射到同一个 sample_id，
        # 这样 ingest_correct 重新切片时能用 upsert"覆盖"旧样本，而不是产生新 ID
        # 让下游 annotation/qc_result 失去引用目标。
        sample_id = f"{episode_id}-w{idx:04d}"
        score, tags = compute_quality_score(window)
        lance_uri = write_sample_to_lance(sample_id, window)
        sample_rows.append(
            {
                "sample_id": sample_id,
                "episode_id": episode_id,
                "robot_id": robot_id,
                "event_date": start_ts,
                "slicer_version": "v1-fixed-window",
                "lance_uri": lance_uri,
                "quality_score": score,
                "quality_tags_json": to_json(tags),
            }
        )
        gold_rows.append(
            {
                "episode_id": episode_id,
                "sample_id": sample_id,
                "duration_s": 2.0,
                "num_points": len(window),
                "quality_score": score,
            }
        )
    _write_upsert(SAMPLE, sample_rows, ["sample_id"])
    _write_upsert(GOLD_SAMPLE_INDEX, gold_rows, ["sample_id"])

    execute("UPDATE upload_session SET status = 'done', updated_at = now() WHERE upload_id = %s", (upload_id,))

    return {
        "episode_id": episode_id,
        "status": "done",
        "num_samples": len(sample_rows),
        "num_files": len(raw_file_rows),
        "quarantined_files": quarantined,
        "silver_clean_strategy_id": strategy.strategy_id,
    }


def _json_loads(s: str) -> dict:
    import json

    return json.loads(s)
