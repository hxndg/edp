"""入湖公共逻辑：MCAP 解析、清洗策略、切片、Lance 写入。

被 `ingest_append.py` / `ingest_correct.py` 共用。`clean_default` /
`clean_strict` 是策略注册表 `silver_clean` stage 下可替换的行为性策略
（README 3.1.7 / 4.3），签名统一为 `(bronze_rows: list[dict]) -> list[dict]`。
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa

from common.config import settings

IMU_TOPIC = "imu"
SLICER_VERSION = "v1-fixed-window"
WINDOW_SECONDS = 2.0


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ns_to_datetime(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)


def read_imu_messages(file_bytes: bytes) -> list[dict]:
    """读一个 MCAP 文件的 imu topic，返回 bronze 行（不做清洗）。"""
    from mcap.reader import make_reader

    rows: list[dict] = []
    with io.BytesIO(file_bytes) as f:
        reader = make_reader(f)
        for _schema, channel, message in reader.iter_messages(topics=[IMU_TOPIC]):
            payload = json.loads(message.data.decode("utf-8"))
            rows.append(
                {
                    "ts": ns_to_datetime(message.log_time),
                    "seq": message.sequence,
                    "payload": payload,
                }
            )
    return rows


def clean_default(bronze_rows: list[dict]) -> list[dict]:
    """默认清洗策略（stage=silver_clean, strategy_id=default）：
    丢弃缺字段/明显越界（|value| > 50）的行，其余原样对齐成 silver 字段。
    """
    cleaned = []
    for row in bronze_rows:
        payload = row["payload"]
        fields = ["ax", "ay", "az", "gx", "gy", "gz"]
        if any(k not in payload for k in fields):
            continue
        if any(abs(float(payload[k])) > 50.0 for k in fields):
            continue
        cleaned.append({**{k: float(payload[k]) for k in fields}, "ts": row["ts"], "quality_flag": "ok"})
    return cleaned


def clean_strict(bronze_rows: list[dict]) -> list[dict]:
    """备用清洗策略示例：阈值更严格（|value| > 20 即丢弃），供科研团队试验用。"""
    cleaned = []
    for row in bronze_rows:
        payload = row["payload"]
        fields = ["ax", "ay", "az", "gx", "gy", "gz"]
        if any(k not in payload for k in fields):
            continue
        if any(abs(float(payload[k])) > 20.0 for k in fields):
            continue
        cleaned.append({**{k: float(payload[k]) for k in fields}, "ts": row["ts"], "quality_flag": "strict_ok"})
    return cleaned


def bucket_by_window(
    silver_rows: list[dict], *, episode_start_ts: datetime, window_seconds: float = WINDOW_SECONDS
) -> dict[int, list[dict]]:
    """按"距 episode 起始时间的第几个固定窗口"分桶，而不是按传入行的顺序分组。

    这样无论一次调用喂进来的是整个 episode 的信号（ingest_append）还是只有
    某个时间段的信号（ingest_correct 的范围限定 backfill），同一个绝对时间点
    永远落进同一个 window_index，`sample_id = f"{episode_id}-w{window_index}"`
    才能在修正时稳定命中原来的样本，而不是从 0 重新编号。
    """
    buckets: dict[int, list[dict]] = {}
    for row in silver_rows:
        idx = int((row["ts"] - episode_start_ts).total_seconds() // window_seconds)
        buckets.setdefault(idx, []).append(row)
    return buckets


def compute_quality_score(window_rows: list[dict]) -> tuple[float, dict[str, float]]:
    """简单质量分：点数越接近期望密度分越高，数值方差在合理区间加分。"""
    n = len(window_rows)
    density_score = min(n / 20.0, 1.0)  # 期望约 10Hz * 2s = 20 点
    df = pd.DataFrame(window_rows)
    numeric_cols = ["ax", "ay", "az", "gx", "gy", "gz"]
    variance = df[numeric_cols].var().mean() if n > 1 else 0.0
    stability_score = 1.0 if 0.0 < variance < 25.0 else 0.5
    overall = round(0.6 * density_score + 0.4 * stability_score, 4)
    return overall, {"density": round(density_score, 4), "stability": round(stability_score, 4)}


def write_sample_to_lance(sample_id: str, window_rows: list[dict]) -> str:
    """把一个样本窗口写成一个 Lance dataset，返回 lance_uri。

    MVP 默认本地磁盘后端（README 2.4 组件清单：Lance 本地/MinIO 后端二选一）。
    """
    import lance

    os.makedirs(settings.lance_root, exist_ok=True)
    uri = os.path.join(settings.lance_root, f"{sample_id}.lance")
    table = pa.Table.from_pandas(pd.DataFrame(window_rows))
    lance.write_dataset(table, uri, mode="overwrite")
    return uri


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def split_s3_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), f"expected s3:// uri, got {uri}"
    rest = uri[len("s3://") :]
    bucket, _, key = rest.partition("/")
    return bucket, key
