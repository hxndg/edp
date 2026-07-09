"""自动数据质检（README 3.2.2 / 4.8，stage=`qc` 的默认策略）。

对每个 sample（2 秒窗口）检查两条硬规则，产出 `qc_result`（check_type=data）：

1. **滑动窗口频率**：在 sample 窗口内以 1s 窗口、0.5s 步长滑动，imu / pose 两个
   topic 在每个滑动窗口内的消息频率都必须 >= 8Hz（标称 10Hz）。掉帧、断流、
   静默段都会让某个滑动窗口的频率跌破阈值。
2. **位姿连续性**：pose topic 相邻两条消息的位移距离不得超过 0.5m（10Hz 下相当
   于 5m/s 的瞬时速度上限）。定位跳变、坐标系错乱、时间戳错序都会表现为一步
   超限的"瞬移"。

数据源是 episode 的原始 MCAP 文件（raw 层）：质检针对"采集回来的数据本身"，
不受清洗策略影响；pose topic 不参与 bronze/silver 分层（分层表是 imu 专属），
只在这里消费。每个 episode 一个 Ray task 并行解析。
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import timedelta, timezone

import pyarrow as pa
import ray

from common.audit import make_batch_id
from common.iceberg import in_filter, load_table, with_audit_columns
from engines.ray.ray_utils import ensure_ray
from engines.spark.ingest_common import IMU_TOPIC, POSE_TOPIC, WINDOW_SECONDS, read_topic_messages, split_s3_uri

# 质检阈值（qc_default 策略的一部分；要换阈值/规则，按 README 3.1.2.2 在
# pipeline_step_config 注册一个新策略指向别的函数，不改编排图）
MIN_TOPIC_HZ = 8.0          # 每个滑动窗口内 imu/pose 的最低频率
FREQ_WINDOW_S = 1.0         # 滑动窗口宽度
FREQ_STEP_S = 0.5           # 滑动步长
MAX_POSE_STEP_M = 0.5       # pose 相邻两条消息允许的最大位移（10Hz 下 ≈ 5m/s）


def _min_sliding_freq(ts_list: list, window_start, window_end) -> float:
    """sample 窗口内滑动子窗口的最低频率（Hz）。ts_list 已按时间升序。"""
    lo = window_start
    min_freq = math.inf
    while lo + timedelta(seconds=FREQ_WINDOW_S) <= window_end:
        hi = lo + timedelta(seconds=FREQ_WINDOW_S)
        count = sum(1 for t in ts_list if lo <= t < hi)
        min_freq = min(min_freq, count / FREQ_WINDOW_S)
        lo = lo + timedelta(seconds=FREQ_STEP_S)
    return 0.0 if min_freq is math.inf else min_freq


def _max_pose_step(pose_rows: list[dict]) -> float:
    """窗口内 pose 相邻消息的最大位移距离（米）。"""
    max_step = 0.0
    for prev, cur in zip(pose_rows, pose_rows[1:]):
        p, q = prev["payload"], cur["payload"]
        step = math.sqrt(sum((float(q[k]) - float(p[k])) ** 2 for k in ("px", "py", "pz")))
        max_step = max(max_step, step)
    return max_step


@ray.remote
def _qc_episode(episode_id: str, start_ts, file_uris: list[str], sample_ids: list[str]) -> list[dict]:
    """解析一个 episode 的原始文件，对它名下每个 sample 窗口跑两条检查。"""
    from common import object_store

    if start_ts.tzinfo is None:
        # Iceberg timestamp 列不带时区，按约定就是 UTC；MCAP 解析出的 ts 是 UTC-aware
        start_ts = start_ts.replace(tzinfo=timezone.utc)

    topic_rows: dict[str, list[dict]] = {IMU_TOPIC: [], POSE_TOPIC: []}
    for uri in file_uris:
        bucket, key = split_s3_uri(uri)
        data = object_store.get_bytes(key, bucket=bucket)
        parsed = read_topic_messages(data, [IMU_TOPIC, POSE_TOPIC])
        for topic in topic_rows:
            topic_rows[topic].extend(parsed[topic])
    for rows in topic_rows.values():
        rows.sort(key=lambda r: r["ts"])

    results = []
    for sample_id in sample_ids:
        # sample_id = {episode_id}-w{idx:04d}，窗口与切片器（bucket_by_window）同一锚点
        idx = int(sample_id.rsplit("-w", 1)[1])
        w_start = start_ts + timedelta(seconds=idx * WINDOW_SECONDS)
        w_end = w_start + timedelta(seconds=WINDOW_SECONDS)

        freqs = {
            topic: _min_sliding_freq([r["ts"] for r in rows if w_start <= r["ts"] < w_end], w_start, w_end)
            for topic, rows in topic_rows.items()
        }
        # 连续性检查要带上窗口前的最后一条 pose：跳变如果恰好落在两个窗口的
        # 边界上（前窗口最后一条 → 本窗口第一条），只看窗口内相邻对会漏掉
        pose_in_window = [r for r in topic_rows[POSE_TOPIC] if w_start <= r["ts"] < w_end]
        before = [r for r in topic_rows[POSE_TOPIC] if r["ts"] < w_start]
        if before and pose_in_window:
            pose_in_window = [before[-1], *pose_in_window]
        max_step = _max_pose_step(pose_in_window)

        freq_ok = all(f >= MIN_TOPIC_HZ for f in freqs.values())
        pose_ok = max_step <= MAX_POSE_STEP_M
        # 分数：频率取"最差 topic 相对阈值的比例"，位姿取"阈值相对最大步长的比例"，都封顶 1
        freq_score = min(min(f / MIN_TOPIC_HZ for f in freqs.values()), 1.0)
        pose_score = 1.0 if max_step == 0.0 else min(MAX_POSE_STEP_M / max_step, 1.0)
        results.append(
            {
                "qc_id": f"{sample_id}-data-qc",
                "target_id": sample_id,
                "check_type": "data",
                "verdict": "pass" if (freq_ok and pose_ok) else "fail",
                "score": round(min(freq_score, pose_score), 4),
                "checked_by": "auto:qc_default",
            }
        )
    return results


def qc_default(target_ids: list[str], *, run_id: str) -> list[dict]:
    """默认质检策略：对 target_ids（sample_id 列表）跑滑动窗口频率 + 位姿连续性检查。"""
    if not target_ids:
        return []
    ensure_ray()

    # 按 episode 分组：{episode_id: [sample_id, ...]}
    by_episode: dict[str, list[str]] = defaultdict(list)
    for sid in target_ids:
        if "-w" in sid:
            by_episode[sid.rsplit("-w", 1)[0]].append(sid)

    episode_ids = sorted(by_episode)
    if not episode_ids:
        return []
    episodes = load_table("episode").scan(row_filter=in_filter("episode_id", episode_ids)).to_arrow().to_pylist()
    start_by_ep = {e["episode_id"]: e["start_ts"] for e in episodes}
    ep_files = (
        load_table("episode_file").scan(row_filter=in_filter("episode_id", episode_ids)).to_arrow().to_pylist()
    )
    files_by_ep: dict[str, list[str]] = defaultdict(list)
    for ef in sorted(ep_files, key=lambda r: r["ordinal"]):
        files_by_ep[ef["episode_id"]].append(ef["file_uri"])

    futures = [
        _qc_episode.remote(ep, start_by_ep[ep], files_by_ep[ep], sorted(by_episode[ep]))
        for ep in episode_ids
        if ep in start_by_ep and files_by_ep.get(ep)
    ]
    rows: list[dict] = []
    for result in ray.get(futures):
        rows.extend(result)
    return rows


def run(target_ids: list[str], *, run_id: str, strategy_id: str | None = None) -> dict:
    from common.strategy_registry import run_strategy
    from schemas.iceberg_tables import QC_RESULT

    strategy, rows = run_strategy("qc", strategy_id, target_ids, run_id=run_id)
    if rows:
        batch_id = make_batch_id(robot_id="qc", upload_id=run_id)
        tbl = pa.Table.from_pylist(rows)
        tbl = with_audit_columns(tbl, batch_id=batch_id, run_id=run_id, source_uri="asset:qc_result")
        from common.iceberg import upsert

        upsert(QC_RESULT, tbl, join_cols=["qc_id"])
    return {"num_qc": len(rows), "strategy_id": strategy.strategy_id}
