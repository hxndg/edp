"""解析 worker 的入口（README 3.6.3 pod fan-out）：一个 pod 处理一个 upload。

被 run pod 用 PipesK8sClient 拉起（engines/spark/ingest_append.py::_fan_out_parse），
干批内最重的活：下载 MCAP → 流式解析 → 清洗 → 切片 → 写 Lance → 待写行落 staging。

职责边界（与 run pod 的契约见 engines/worker/staging.py）：
- 只读 input.json（run pod 预先写好：session 快照、清洗策略入口、correct 的
  episode 锚点、chunk 大小），**不连 PG、不碰 Iceberg catalog、不调 K8s API**；
- 业务失败也要**正常退出**（exit 0）：把 error_code + error 写进 manifest.json
  （common/errors.py 的状态码），由 run pod 对该 upload 做 saga fail_one；
  只有 pod 级失败（OOM/超时）才表现为"没有清单"——死掉的进程自报不了，
  由 run pod 查 pod 终态推断码；
- Dagster Pipes 是**观测通道**（日志流回 run 的 compute log、完成时上报小结），
  数据契约仍以 staging 的 manifest.json 为准——不受日志行长度限制影响。

内存模型（固定预算，与文件大小解耦）：
- 原始文件下载到本地盘（不整块进内存），MCAP 从文件句柄流式迭代；
- 两遍扫描：pass 1 只统计每个文件的 imu min/max ts + 结构校验（决定隔离），
  pass 2 按时间序逐消息产出行——bronze/silver 走分块 ParquetWriter（每攒
  chunk_rows 行 flush 一个 row group 到本地盘），切片窗口按水位线 flush
  （下一文件的起始时间之前结束的窗口即可关闭写 Lance）；
- 峰值内存 ≈ chunk_rows 行 + 少量未关闭窗口，与 episode 时长无关。
"""
from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime

import click
import pyarrow as pa
import pyarrow.parquet as pq

from common import object_store
from common.audit import make_batch_id
from common.db import to_json  # 纯 json.dumps 包装，不建立 PG 连接
from common.errors import ErrorCode, WorkerError, classify_exception
from common.iceberg import now_utc
from engines.spark.ingest_common import (
    IMU_TOPIC,
    WINDOW_SECONDS,
    compute_quality_score,
    ns_to_datetime,
    split_s3_uri,
    write_sample_to_lance,
)
from engines.worker import staging

logger = logging.getLogger(__name__)

_TS = pa.timestamp("us", tz="UTC")
_AUDIT_FIELDS = [
    ("_batch_id", pa.string()),
    ("_run_id", pa.string()),
    ("_ingested_at", _TS),
    ("_source_uri", pa.string()),
]
BRONZE_SCHEMA = pa.schema(
    [
        ("robot_id", pa.string()),
        ("episode_id", pa.string()),
        ("source_file", pa.string()),
        ("ts", _TS),
        ("seq", pa.int64()),
        ("payload_json", pa.string()),
        *_AUDIT_FIELDS,
    ]
)
SILVER_SCHEMA = pa.schema(
    [
        ("ax", pa.float64()),
        ("ay", pa.float64()),
        ("az", pa.float64()),
        ("gx", pa.float64()),
        ("gy", pa.float64()),
        ("gz", pa.float64()),
        ("ts", _TS),
        ("quality_flag", pa.string()),
        ("episode_id", pa.string()),
        ("robot_id", pa.string()),
        *_AUDIT_FIELDS,
    ]
)


class _ChunkedWriter:
    """分块 parquet 写入：行攒到 chunk_rows 就 flush 一个 row group 到本地盘，
    内存里永远只有一个 chunk。schema 显式给定，跨 chunk 稳定。"""

    def __init__(self, local_path: str, schema: pa.Schema, chunk_rows: int, audit: dict):
        self.local_path = local_path
        self.schema = schema
        self.chunk_rows = chunk_rows
        self.audit = audit
        self._rows: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self.total = 0

    def add(self, row: dict) -> None:
        self._rows.append({**row, **self.audit, "_ingested_at": now_utc()})
        self.total += 1
        if len(self._rows) >= self.chunk_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._rows:
            return
        table = pa.Table.from_pylist(self._rows, schema=self.schema)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.local_path, self.schema)
        self._writer.write_table(table)
        self._rows = []

    def close_and_upload(self, staging_key: str) -> int:
        """flush 残余、关闭 writer、上传 staging。返回总行数（0 行不产生文件）。"""
        self._flush()
        if self._writer is not None:
            self._writer.close()
            object_store.put_file(staging_key, self.local_path)
        return self.total


@dataclass
class _WindowSlicer:
    """按绝对时间窗切片 + 水位线 flush（见模块 docstring 内存模型）。

    锚点 = episode 起始时间：窗口序号是"距锚点第几个 2s 窗"，与切片调用的
    时间范围无关，确定性 sample_id 在 correct 重切时命中原样本（README 2.2 原则 8）。
    """

    episode_id: str
    robot_id: str
    anchor_ts: datetime
    event_date: datetime
    buffers: dict[int, list[dict]] = field(default_factory=dict)
    flushed: set[int] = field(default_factory=set)
    sample_rows: list[dict] = field(default_factory=list)
    gold_rows: list[dict] = field(default_factory=list)
    sample_ids: list[str] = field(default_factory=list)
    late_rows: int = 0

    def add(self, silver_row: dict) -> None:
        idx = int((silver_row["ts"] - self.anchor_ts).total_seconds() // WINDOW_SECONDS)
        if idx in self.flushed:  # 不应发生（文件按起始时间排序 + 边界水位线），计数兜底
            self.late_rows += 1
            return
        self.buffers.setdefault(idx, []).append(silver_row)

    def flush_before(self, watermark: datetime) -> None:
        """关闭所有"窗口结束时间 <= watermark"的窗——后续消息不可能再落进它们。"""
        for idx in sorted(self.buffers):
            window_end_offset = (idx + 1) * WINDOW_SECONDS
            if (watermark - self.anchor_ts).total_seconds() >= window_end_offset:
                self._flush(idx)

    def finish(self) -> None:
        for idx in sorted(self.buffers):
            self._flush(idx)

    def _flush(self, idx: int) -> None:
        window = self.buffers.pop(idx)
        sample_id = f"{self.episode_id}-w{idx:04d}"
        score, tags = compute_quality_score(window)
        lance_uri = write_sample_to_lance(sample_id, window)
        self.sample_ids.append(sample_id)
        self.sample_rows.append(
            {
                "sample_id": sample_id,
                "episode_id": self.episode_id,
                "robot_id": self.robot_id,
                "event_date": self.event_date,
                "slicer_version": "v1-fixed-window",
                "lance_uri": lance_uri,
                "quality_score": score,
                "quality_tags_json": to_json(tags),
            }
        )
        self.gold_rows.append(
            {
                "episode_id": self.episode_id,
                "sample_id": sample_id,
                "duration_s": WINDOW_SECONDS,
                "num_points": len(window),
                "quality_score": score,
            }
        )
        self.flushed.add(idx)


@dataclass
class _FileScan:
    """pass 1 的产物：一个已下载文件的元信息（不含任何消息数据）。"""

    file_uri: str
    local_path: str
    sha256: str
    num_imu: int
    min_ts: datetime | None
    max_ts: datetime | None
    entry: dict


def _download_and_scan(entry: dict, workdir: str) -> _FileScan:
    """pass 1：下载到本地盘 + 流式 sha256 + 流式统计 imu min/max/count。
    结构性损坏在这里抛出（调用方决定隔离还是整单失败），不产出任何行。"""
    from mcap.reader import make_reader

    file_uri = entry["file_uri"]
    bucket, key = split_s3_uri(file_uri)
    local_path = os.path.join(workdir, hashlib.sha256(file_uri.encode()).hexdigest()[:16] + ".mcap")
    object_store.client().download_file(bucket, key, local_path)

    sha = hashlib.sha256()
    with open(local_path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            sha.update(block)

    num_imu, min_ts, max_ts = 0, None, None
    with open(local_path, "rb") as f:
        for _schema, _channel, message in make_reader(f).iter_messages(topics=[IMU_TOPIC]):
            ts = ns_to_datetime(message.log_time)
            num_imu += 1
            min_ts = ts if min_ts is None or ts < min_ts else min_ts
            max_ts = ts if max_ts is None or ts > max_ts else max_ts
    return _FileScan(file_uri, local_path, sha.hexdigest(), num_imu, min_ts, max_ts, entry)


def _emit_file_rows(
    scan: _FileScan,
    *,
    episode_id: str,
    robot_id: str,
    clean_fn,
    bronze: _ChunkedWriter,
    silver: _ChunkedWriter,
    slicer: _WindowSlicer,
    chunk_rows: int,
    seq_start: int,
) -> tuple[int, int]:
    """pass 2：流式重放一个文件的 imu 消息，逐 chunk 清洗后喂给 silver/切片器。
    返回 (处理的消息数, 跳过的坏消息数)。"""
    import json as _json

    from mcap.reader import make_reader

    skipped = 0
    seq = seq_start
    payload_chunk: list[dict] = []

    def _drain() -> None:
        nonlocal payload_chunk
        for srow in clean_fn(payload_chunk):
            row = {**srow, "episode_id": episode_id, "robot_id": robot_id}
            silver.add(row)
            slicer.add(row)
        payload_chunk = []

    with open(scan.local_path, "rb") as f:
        for _schema, _channel, message in make_reader(f).iter_messages(topics=[IMU_TOPIC]):
            try:
                payload = _json.loads(message.data.decode("utf-8"))
            except (UnicodeDecodeError, _json.JSONDecodeError):
                skipped += 1
                continue
            ts = ns_to_datetime(message.log_time)
            bronze.add(
                {
                    "robot_id": robot_id,
                    "episode_id": episode_id,
                    "source_file": scan.file_uri,
                    "ts": ts,
                    "seq": seq,
                    "payload_json": to_json(payload),
                }
            )
            payload_chunk.append({"payload": payload, "ts": ts})
            seq += 1
            if len(payload_chunk) >= chunk_rows:
                _drain()
    _drain()
    return seq - seq_start, skipped


def _quarantine(scan: _FileScan, upload_id: str) -> None:
    _, key = split_s3_uri(scan.file_uri)
    object_store.put_file(f"{object_store.PREFIX_QUARANTINE}/{upload_id}/{key.split('/')[-1]}", scan.local_path)


def _load_entrypoint(entrypoint: str):
    import importlib

    module_name, func_name = entrypoint.split(":")
    return getattr(importlib.import_module(module_name), func_name)


def _run(inp: dict, prefix: str, workdir: str) -> dict:
    """append / correct 共用主体，差异全部由 input.json 参数化：
    - append：episode_id = ep-{upload_id}，锚点 = 全部有效文件的最小 imu ts，
      坏文件隔离后继续（全坏才整单失败）；
    - correct：episode/锚点由 run pod 从 Iceberg 读好传入，任何坏文件整单失败
      （修正已有数据只写一半时间窗比不写更糟）。
    """
    mode = inp["mode"]
    upload_id = inp["upload_id"]
    run_id = inp["run_id"]
    session = inp["session"]
    manifest = session["manifest"]
    chunk_rows = int(inp.get("chunk_rows", 50000))
    clean_fn = _load_entrypoint(inp["clean_entrypoint"])

    if mode == "correct":
        episode = inp["episode"]
        episode_id, robot_id = episode["episode_id"], episode["robot_id"]
        anchor_ts = event_date = episode["start_ts"]
    else:
        episode_id, robot_id = f"ep-{upload_id}", session["robot_id"]
        anchor_ts = event_date = None  # pass 1 之后才知道

    audit = {
        "_batch_id": make_batch_id(robot_id=session["robot_id"], upload_id=upload_id),
        "_run_id": run_id,
        "_source_uri": f"upload:{upload_id}",
    }

    # ---- pass 1：下载 + 校验 + 统计，决定隔离；不产出任何行 ----
    scans: list[_FileScan] = []
    raw_file_rows: list[dict] = []
    quarantined_files: list[str] = []
    for entry in manifest["files"]:
        try:
            scan = _download_and_scan(entry, workdir)
            if scan.num_imu == 0:
                raise WorkerError(ErrorCode.DATA_EMPTY, f"文件里没有 imu 消息：{entry['file_uri']}")
        except Exception as e:  # noqa: BLE001
            if mode == "correct":
                raise WorkerError(
                    classify_exception(e), f"correct 输入文件不可用（整单失败）：{entry['file_uri']}: {e}"
                ) from e
            logger.exception("file failed in pass 1, quarantining: %s", entry["file_uri"])
            local = os.path.join(workdir, hashlib.sha256(entry["file_uri"].encode()).hexdigest()[:16] + ".mcap")
            if os.path.exists(local):
                _quarantine(_FileScan(entry["file_uri"], local, "", 0, None, None, entry), upload_id)
            quarantined_files.append(entry["file_uri"])
            raw_file_rows.append(
                {
                    "file_uri": entry["file_uri"],
                    "robot_id": robot_id,
                    "task_id": session.get("task_id"),
                    "start_ts": None,
                    "end_ts": None,
                    "sha256": entry.get("sha256"),
                    "schema_version": entry.get("schema_version", "v1"),
                    "upload_id": upload_id,
                    "status": "quarantined",
                }
            )
            continue
        scans.append(scan)
        raw_file_rows.append(
            {
                "file_uri": scan.file_uri,
                "robot_id": robot_id,
                "task_id": session.get("task_id"),
                "start_ts": scan.min_ts,
                "end_ts": scan.max_ts,
                "sha256": scan.sha256,
                "schema_version": entry.get("schema_version", "v1"),
                "upload_id": upload_id,
                "status": "ok",
            }
        )

    if not scans:
        raise WorkerError(
            ErrorCode.DATA_EMPTY, f"upload '{upload_id}' 的全部 {len(quarantined_files)} 个文件不可用并已隔离"
        )

    scans.sort(key=lambda s: s.min_ts)
    start_ts = min(s.min_ts for s in scans)
    end_ts = max(s.max_ts for s in scans)
    if anchor_ts is None:
        anchor_ts = event_date = start_ts

    # ---- pass 2：按起始时间序流式重放，行进分块 writer，窗口按水位线 flush ----
    bronze = _ChunkedWriter(os.path.join(workdir, "bronze.parquet"), BRONZE_SCHEMA, chunk_rows, audit)
    silver = _ChunkedWriter(os.path.join(workdir, "silver.parquet"), SILVER_SCHEMA, chunk_rows, audit)
    slicer = _WindowSlicer(episode_id=episode_id, robot_id=robot_id, anchor_ts=anchor_ts, event_date=event_date)
    skipped_messages = 0
    seq = 0
    for i, scan in enumerate(scans):
        n, skipped = _emit_file_rows(
            scan,
            episode_id=episode_id,
            robot_id=robot_id,
            clean_fn=clean_fn,
            bronze=bronze,
            silver=silver,
            slicer=slicer,
            chunk_rows=chunk_rows,
            seq_start=seq,
        )
        seq += n
        skipped_messages += skipped
        # 边界水位线：下一个文件的起始时间之前结束的窗口可以安全关闭
        if i + 1 < len(scans):
            slicer.flush_before(scans[i + 1].min_ts)
    slicer.finish()

    thick_files = {}
    for table, writer in (("bronze_imu", bronze), ("silver_imu", silver)):
        key = f"{prefix}/{table}.parquet"
        n = writer.close_and_upload(key)
        if n:
            thick_files[table] = {"key": key, "rows": n}

    def _stamp(rows: list[dict]) -> list[dict]:
        ts = now_utc()
        return [{**r, **audit, "_ingested_at": ts} for r in rows]

    result = {
        "upload_id": upload_id,
        "status": "ok",
        "error_code": None,
        "episode_id": episode_id,
        "sample_ids": slicer.sample_ids,
        "num_files": len(raw_file_rows),
        "num_messages": seq,
        "skipped_messages": skipped_messages,
        "late_rows": slicer.late_rows,
        "quarantined_files": quarantined_files,
        "thick_files": thick_files,
    }
    if mode == "correct":
        result["affected_sample_ids"] = slicer.sample_ids
        result["affected_range"] = {
            "start": manifest["affected_start_ts"],
            "end": manifest["affected_end_ts"],
        }
        result["thin_rows"] = {
            "raw_file": _stamp(raw_file_rows),
            "sample": _stamp(slicer.sample_rows),
            "gold_sample_index": _stamp(slicer.gold_rows),
        }
    else:
        episode_row = {
            "episode_id": episode_id,
            "robot_id": robot_id,
            "task_id": session.get("task_id"),
            "operator": session.get("operator"),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "firmware_ver": "mock-1.0",
            "calib_ver": "mock-1.0",
            "agent_ver": "mock-1.0",
            "source": "declared",
        }
        episode_file_rows = [
            {"episode_id": episode_id, "file_uri": s.file_uri, "ordinal": i} for i, s in enumerate(scans)
        ]
        result["thin_rows"] = {
            "raw_file": _stamp(raw_file_rows),
            "episode": _stamp([episode_row]),
            "episode_file": _stamp(episode_file_rows),
            "sample": _stamp(slicer.sample_rows),
            "gold_sample_index": _stamp(slicer.gold_rows),
        }
    return result


def _maybe_open_pipes():
    """在 PipesK8sClient 拉起的 pod 里打开 pipes 会话（日志/消息流回 Dagster run）；
    本地手工运行（无 bootstrap 环境变量）时退化为普通进程，功能不受影响。"""
    from dagster_pipes import DAGSTER_PIPES_CONTEXT_ENV_VAR

    if os.environ.get(DAGSTER_PIPES_CONTEXT_ENV_VAR):
        from dagster_pipes import open_dagster_pipes

        return open_dagster_pipes()
    return contextlib.nullcontext(None)


@click.command()
@click.option("--upload-id", required=True)
@click.option("--run-id", required=True)
@click.option("--staging-prefix", required=True)
def main(upload_id: str, run_id: str, staging_prefix: str) -> None:
    logging.basicConfig(level=logging.INFO)
    # 业务失败走 return 正常返回（exit 0）而不是 sys.exit：sys.exit 抛 SystemExit，
    # 会被 pipes 上下文的 __exit__ 当异常上报，在 run 日志里制造
    # "pipes closed with exception" 的假警报。
    with _maybe_open_pipes() as pipes:
        _main_inner(upload_id, staging_prefix, pipes)


def _main_inner(upload_id: str, staging_prefix: str, pipes) -> None:
    try:
        inp = staging.read_json(f"{staging_prefix}/{staging.INPUT_JSON}")
    except Exception as e:  # noqa: BLE001 - input.json 都读不到：多半是存储抖动
        code = classify_exception(e)
        code = ErrorCode.INPUT_MISSING if code == ErrorCode.STORAGE_IO_ERROR else code
        _write_error_manifest(staging_prefix, upload_id, code, f"读 input.json 失败: {type(e).__name__}: {e}", pipes)
        return

    with tempfile.TemporaryDirectory(prefix=f"ingest-{upload_id[:16]}-") as workdir:
        try:
            result = _run(inp, staging_prefix, workdir)
        except Exception as e:  # noqa: BLE001 - 业务失败：写带码清单后正常退出（契约见模块 docstring）
            logger.exception("worker failed for upload %s", upload_id)
            _write_error_manifest(
                staging_prefix, upload_id, classify_exception(e), f"{type(e).__name__}: {e}", pipes
            )
            return

    staging.write_json(f"{staging_prefix}/{staging.MANIFEST_JSON}", result)
    summary = {
        "upload_id": upload_id,
        "status": "ok",
        "num_samples": len(result["sample_ids"]),
        "num_messages": result["num_messages"],
        "quarantined_files": len(result["quarantined_files"]),
    }
    logger.info("worker done: %s", summary)
    if pipes is not None:
        pipes.report_custom_message(summary)


def _write_error_manifest(prefix: str, upload_id: str, code: ErrorCode, message: str, pipes) -> None:
    payload = {"upload_id": upload_id, "status": "error", "error_code": code.value, "error": message}
    staging.write_json(f"{prefix}/{staging.MANIFEST_JSON}", payload)
    if pipes is not None:
        pipes.report_custom_message(payload)


if __name__ == "__main__":
    main()
