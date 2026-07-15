"""run pod ↔ worker pod 的 staging 交接约定（README 3.6.3 pod fan-out）。

对象存储上的目录布局（每个 (run, upload) 一个前缀，天然隔离、可并发）：

    staging/{run_id}/{upload_id}/input.json      run pod 写：worker 的全部输入
    staging/{run_id}/{upload_id}/manifest.json   worker 写：结果清单（薄表行内联）
    staging/{run_id}/{upload_id}/bronze_imu.parquet   worker 写：厚表数据文件
    staging/{run_id}/{upload_id}/silver_imu.parquet

设计要点：
- **worker 无状态**：只读写 staging + 原始文件 + Lance，不连 PG、不碰 Iceberg
  catalog。策略入口（entrypoint 字符串）、session 内容、correct 模式的 episode
  锚点全部由 run pod 预先放进 input.json。
- **厚薄分工**（README 3.6.3）：bronze/silver 每 upload 成百上千行走 parquet
  文件；raw_file/episode/sample 等索引行（每 upload 几行）直接内联在
  manifest.json 里，省掉几次对象存储往返。
- **可见性**：staging 前缀不属于任何 Iceberg 表，写在这里的东西对读者不存在
  （4.5：不被快照引用即不存在）；孤儿残留由 retention job 按 mtime 清理。
- datetime 在 JSON 里用 {"__dt__": iso} 标记往返，双方无需知道哪些字段是时间。
"""
from __future__ import annotations

import io
import json
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from common import object_store

STAGING_ROOT = "staging"
INPUT_JSON = "input.json"
MANIFEST_JSON = "manifest.json"

# 厚表：走 parquet 文件；其余表的行内联在 manifest.json 的 thin_rows 里
THICK_TABLES = ("bronze_imu", "silver_imu")


def prefix(run_id: str, upload_id: str) -> str:
    return f"{STAGING_ROOT}/{run_id}/{upload_id}"


def _encode(obj):
    if isinstance(obj, datetime):
        return {"__dt__": obj.isoformat()}
    return str(obj)


def _decode(obj: dict):
    if set(obj.keys()) == {"__dt__"}:
        return datetime.fromisoformat(obj["__dt__"])
    return obj


def write_json(key: str, payload: dict) -> str:
    return object_store.put_bytes(key, json.dumps(payload, ensure_ascii=False, default=_encode).encode("utf-8"))


def read_json(key: str) -> dict:
    return json.loads(object_store.get_bytes(key).decode("utf-8"), object_hook=_decode)


def try_read_json(key: str) -> dict | None:
    """worker 崩溃/超时/没调度上时 manifest 不存在——返回 None 交给调用方 fail_one。"""
    try:
        return read_json(key)
    except Exception:  # noqa: BLE001 - NoSuchKey 及一切读取失败都视为"清单缺失"
        return None


def write_parquet(key: str, rows: list[dict]) -> int:
    """rows -> parquet 写到 staging，返回行数。空行列表不写文件，返回 0。"""
    if not rows:
        return 0
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    object_store.put_bytes(key, buf.getvalue())
    return len(rows)


def read_parquet(key: str) -> pa.Table:
    return pq.read_table(io.BytesIO(object_store.get_bytes(key)))


def iter_parquet_batches(key: str):
    """逐 row group 产出一个 staging parquet 的内容（run pod 收厚表数据用）。

    内存 = 一个文件的字节 + 一个 row group（worker 侧 _ChunkedWriter 保证
    row group ≈ chunk_rows 行），配合 `replace_where_chunked` 让 run pod
    收全批数据的峰值内存与批大小解耦。
    """
    pf = pq.ParquetFile(io.BytesIO(object_store.get_bytes(key)))
    for i in range(pf.num_row_groups):
        yield pf.read_row_group(i)
