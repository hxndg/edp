"""解析 worker 的入口（README 3.6.3 pod fan-out）：一个 pod 处理一个 upload。

被 run pod 以 K8s Job 形式拉起（common/k8s_jobs.py），干的是批内最重的活：
下载 MCAP → 解析 → 清洗 → 切片 → 写 Lance → 把待写行放到 staging。

职责边界（与 run pod 的契约见 engines/worker/staging.py）：
- 只读 input.json（run pod 预先写好：session 快照、清洗策略入口、correct 的
  episode 锚点），**不连 PG、不碰 Iceberg catalog**——worker 无状态可随便杀；
- 业务失败（文件解析不出来等）也要**正常退出**：把 error 写进 manifest.json，
  由 run pod 对该 upload 做 saga fail_one + 告警；只有 pod 级失败（OOM/超时）
  才表现为"没有 manifest"；
- 隔离区文件（quarantine/ 前缀）由 worker 直接写对象存储，但 alerts（PG）
  由 run pod 根据 manifest 里的 quarantined_files 统一落。
"""
from __future__ import annotations

import importlib
import logging
import sys

import click

from common import object_store
from common.audit import make_batch_id
from common.db import to_json  # 只是 json.dumps 包装，不建立 PG 连接
from common.iceberg import now_utc
from engines.spark.ingest_common import (
    bucket_by_window,
    compute_quality_score,
    read_imu_messages,
    sha256_bytes,
    split_s3_uri,
    write_sample_to_lance,
)
from engines.worker import staging

logger = logging.getLogger(__name__)


def _load_entrypoint(entrypoint: str):
    module_name, func_name = entrypoint.split(":")
    return getattr(importlib.import_module(module_name), func_name)


def _stamper(batch_id: str, run_id: str, upload_id: str):
    def _stamp(rows: list[dict]) -> list[dict]:
        ts = now_utc()
        return [
            {**r, "_batch_id": batch_id, "_run_id": run_id, "_ingested_at": ts, "_source_uri": f"upload:{upload_id}"}
            for r in rows
        ]

    return _stamp


def _parse_files(files: list[dict], *, upload_id: str, robot_id: str, task_id: str | None):
    """逐文件解析，坏文件进隔离区。返回 (raw_file_rows, bronze_msgs, episode_file_rows,
    quarantined_files)，bronze_msgs 是 [{file_uri, ts, seq, payload}]。"""
    raw_file_rows: list[dict] = []
    bronze_msgs: list[dict] = []
    episode_file_rows: list[dict] = []
    quarantined_files: list[str] = []

    for ordinal, entry in enumerate(files):
        file_uri = entry["file_uri"]
        bucket, key = split_s3_uri(file_uri)
        data: bytes | None = None
        try:
            data = object_store.get_bytes(key, bucket=bucket)
            sha256 = sha256_bytes(data)
            msgs = read_imu_messages(data)
            if not msgs:
                raise ValueError("文件里没有解析出任何 imu 消息")
        except Exception:  # noqa: BLE001
            logger.exception("file parse failed, quarantining: %s", file_uri)
            if data is not None:
                object_store.put_bytes(f"{object_store.PREFIX_QUARANTINE}/{upload_id}/{key.split('/')[-1]}", data)
            quarantined_files.append(file_uri)
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

        ts_values = [m["ts"] for m in msgs]
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
                "status": "ok",
            }
        )
        episode_file_rows.append({"file_uri": file_uri, "ordinal": ordinal})
        for m in msgs:
            bronze_msgs.append({"file_uri": file_uri, **m})

    return raw_file_rows, bronze_msgs, episode_file_rows, quarantined_files


def _slice_samples(silver_rows: list[dict], *, episode_id: str, robot_id: str, anchor_ts, event_date):
    """固定窗口切片 + 写 Lance。返回 (sample_rows, gold_rows, sample_ids)。"""
    windows = bucket_by_window(silver_rows, episode_start_ts=anchor_ts)
    sample_rows, gold_rows, sample_ids = [], [], []
    for idx, window in sorted(windows.items()):
        sample_id = f"{episode_id}-w{idx:04d}"
        sample_ids.append(sample_id)
        score, tags = compute_quality_score(window)
        lance_uri = write_sample_to_lance(sample_id, window)
        sample_rows.append(
            {
                "sample_id": sample_id,
                "episode_id": episode_id,
                "robot_id": robot_id,
                "event_date": event_date,
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
    return sample_rows, gold_rows, sample_ids


def _run_append(inp: dict, prefix: str) -> dict:
    upload_id = inp["upload_id"]
    run_id = inp["run_id"]
    session = inp["session"]
    manifest = session["manifest"]
    robot_id = session["robot_id"]
    task_id = session.get("task_id")
    episode_id = f"ep-{upload_id}"
    stamp = _stamper(make_batch_id(robot_id=robot_id, upload_id=upload_id), run_id, upload_id)
    clean_fn = _load_entrypoint(inp["clean_entrypoint"])

    raw_file_rows, bronze_msgs, episode_file_rows, quarantined_files = _parse_files(
        manifest["files"], upload_id=upload_id, robot_id=robot_id, task_id=task_id
    )
    if not raw_file_rows or all(r["status"] == "quarantined" for r in raw_file_rows):
        raise ValueError(f"upload '{upload_id}' 的全部 {len(quarantined_files)} 个文件解析失败并已隔离")

    start_ts = min(r["start_ts"] for r in raw_file_rows if r["start_ts"])
    end_ts = max(r["end_ts"] for r in raw_file_rows if r["end_ts"])
    episode_row = {
        "episode_id": episode_id,
        "robot_id": robot_id,
        "task_id": task_id,
        "operator": session.get("operator"),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "firmware_ver": "mock-1.0",
        "calib_ver": "mock-1.0",
        "agent_ver": "mock-1.0",
        "source": "declared",
    }
    bronze_rows = [
        {
            "robot_id": robot_id,
            "episode_id": episode_id,
            "source_file": m["file_uri"],
            "ts": m["ts"],
            "seq": m["seq"],
            "payload_json": to_json(m["payload"]),
        }
        for m in bronze_msgs
    ]
    silver_rows = clean_fn([{"payload": m["payload"], "ts": m["ts"]} for m in bronze_msgs])
    silver_table_rows = [{**r, "episode_id": episode_id, "robot_id": robot_id} for r in silver_rows]
    sample_rows, gold_rows, sample_ids = _slice_samples(
        silver_rows, episode_id=episode_id, robot_id=robot_id, anchor_ts=start_ts, event_date=start_ts
    )

    thick_files = {}
    for table, rows in (("bronze_imu", stamp(bronze_rows)), ("silver_imu", stamp(silver_table_rows))):
        key = f"{prefix}/{table}.parquet"
        n = staging.write_parquet(key, rows)
        if n:
            thick_files[table] = {"key": key, "rows": n}

    return {
        "upload_id": upload_id,
        "status": "ok",
        "episode_id": episode_id,
        "sample_ids": sample_ids,
        "num_files": len(raw_file_rows),
        "quarantined_files": quarantined_files,
        "thin_rows": {
            "raw_file": stamp(raw_file_rows),
            "episode": stamp([episode_row]),
            "episode_file": stamp([{**r, "episode_id": episode_id} for r in episode_file_rows]),
            "sample": stamp(sample_rows),
            "gold_sample_index": stamp(gold_rows),
        },
        "thick_files": thick_files,
    }


def _run_correct(inp: dict, prefix: str) -> dict:
    upload_id = inp["upload_id"]
    run_id = inp["run_id"]
    session = inp["session"]
    manifest = session["manifest"]
    episode = inp["episode"]  # run pod 从 Iceberg 读好的锚点：episode_id/robot_id/start_ts
    episode_id = episode["episode_id"]
    robot_id = episode["robot_id"]
    stamp = _stamper(make_batch_id(robot_id=session["robot_id"], upload_id=upload_id), run_id, upload_id)
    clean_fn = _load_entrypoint(inp["clean_entrypoint"])

    raw_file_rows, bronze_msgs, _, quarantined_files = _parse_files(
        manifest["files"], upload_id=upload_id, robot_id=robot_id, task_id=session.get("task_id")
    )
    if quarantined_files:
        # correct 是对已有 episode 的修正：输入文件坏了就整个 upload 失败，
        # 不能只写一半时间窗（append 的"隔离坏文件继续"语义在这里不成立）。
        raise ValueError(f"correct upload '{upload_id}' 的输入文件解析失败：{quarantined_files}")

    bronze_rows = [
        {
            "episode_id": episode_id,
            "robot_id": robot_id,
            "source_file": manifest["files"][0]["file_uri"],
            "seq": i,
            "ts": m["ts"],
            "payload_json": to_json(m["payload"]),
        }
        for i, m in enumerate(bronze_msgs)
    ]
    silver_rows = clean_fn([{"payload": m["payload"], "ts": m["ts"]} for m in bronze_msgs])
    silver_table_rows = [{**r, "episode_id": episode_id, "robot_id": robot_id} for r in silver_rows]
    # 切片锚点用 episode 原始 start_ts：绝对时间窗序号才能命中原 sample_id
    sample_rows, gold_rows, sample_ids = _slice_samples(
        silver_rows,
        episode_id=episode_id,
        robot_id=robot_id,
        anchor_ts=episode["start_ts"],
        event_date=episode["start_ts"],
    )

    thick_files = {}
    for table, rows in (("bronze_imu", stamp(bronze_rows)), ("silver_imu", stamp(silver_table_rows))):
        key = f"{prefix}/{table}.parquet"
        n = staging.write_parquet(key, rows)
        if n:
            thick_files[table] = {"key": key, "rows": n}

    return {
        "upload_id": upload_id,
        "status": "ok",
        "episode_id": episode_id,
        "sample_ids": sample_ids,
        "affected_sample_ids": sample_ids,
        "num_files": len(raw_file_rows),
        "quarantined_files": [],
        "affected_range": {
            "start": manifest["affected_start_ts"],
            "end": manifest["affected_end_ts"],
        },
        "thin_rows": {
            "raw_file": stamp(raw_file_rows),
            "sample": stamp(sample_rows),
            "gold_sample_index": stamp(gold_rows),
        },
        "thick_files": thick_files,
    }


@click.command()
@click.option("--upload-id", required=True)
@click.option("--run-id", required=True)
@click.option("--staging-prefix", required=True)
def main(upload_id: str, run_id: str, staging_prefix: str) -> None:
    logging.basicConfig(level=logging.INFO)
    inp = staging.read_json(f"{staging_prefix}/{staging.INPUT_JSON}")
    try:
        result = _run_append(inp, staging_prefix) if inp["mode"] == "append" else _run_correct(inp, staging_prefix)
    except Exception as e:  # noqa: BLE001 - 业务失败：写 error 清单后正常退出（契约见模块 docstring）
        logger.exception("worker failed for upload %s", upload_id)
        staging.write_json(
            f"{staging_prefix}/{staging.MANIFEST_JSON}",
            {"upload_id": upload_id, "status": "error", "error": f"{type(e).__name__}: {e}"},
        )
        sys.exit(0)
    staging.write_json(f"{staging_prefix}/{staging.MANIFEST_JSON}", result)
    logger.info("worker done for upload %s: %s samples", upload_id, len(result.get("sample_ids", [])))


if __name__ == "__main__":
    main()
