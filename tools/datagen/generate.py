"""合成数据生成器（README 2.4 / 3.1.2.2）：生成带 `imu` + `pose` 两个 topic 的
MCAP 文件，供 `tools/datagen/upload.py` 走网关上传，demo 整条入湖链路。也可以把
真实 MCAP 样本文件直接放进 `tools/datagen/fixtures/` 使用，不强制要求走这个脚本。

为验证数据质检（engines/ray/qc.py：滑动窗口频率 + 位姿跳变），提供两个故障注入开关：
  --dropout-seconds N   在 episode 中段丢掉 N 秒消息，制造"频率低于阈值"的坏段
  --pose-jump           在 episode 中段给 pose 注入一次瞬移，制造"位姿突然跳转"
"""
from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import click

IMU_SCHEMA = {
    "type": "object",
    "properties": {k: {"type": "number"} for k in ("ax", "ay", "az", "gx", "gy", "gz")},
}

POSE_SCHEMA = {
    "type": "object",
    "properties": {k: {"type": "number"} for k in ("px", "py", "pz")},
}


def _write_mcap(
    path: str,
    *,
    start_time: datetime,
    duration_s: float,
    hz: float,
    bad_ratio: float,
    dropout_seconds: float,
    pose_jump: bool,
) -> None:
    from mcap.writer import Writer

    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="edp-datagen", library="edp-datagen-0.1")
        imu_schema_id = writer.register_schema(
            name="imu", encoding="jsonschema", data=json.dumps(IMU_SCHEMA).encode("utf-8")
        )
        imu_channel = writer.register_channel(schema_id=imu_schema_id, topic="imu", message_encoding="json")
        pose_schema_id = writer.register_schema(
            name="pose", encoding="jsonschema", data=json.dumps(POSE_SCHEMA).encode("utf-8")
        )
        pose_channel = writer.register_channel(schema_id=pose_schema_id, topic="pose", message_encoding="json")

        n = int(duration_s * hz)
        imu_state = {k: 0.0 for k in ("ax", "ay", "az", "gx", "gy", "gz")}
        pos = {"px": 0.0, "py": 0.0, "pz": 0.0}
        vel = {"px": 0.05, "py": 0.02, "pz": 0.0}  # 每 tick 的平滑位移（10Hz 下约 0.5m/s）

        # 故障注入位置都放在中段，保证同一个 episode 里既有好窗口也有坏窗口
        dropout_start = int(n * 0.4)
        dropout_end = dropout_start + int(dropout_seconds * hz)
        jump_at = int(n * 0.6)

        for i in range(n):
            if dropout_start <= i < dropout_end:
                continue  # 整段静默：滑动窗口频率检查应该抓到这一段
            ts = start_time + timedelta(seconds=i / hz)
            log_time_ns = int(ts.timestamp() * 1e9)

            for k in imu_state:
                imu_state[k] += random.uniform(-0.3, 0.3)
                imu_state[k] = max(-5.0, min(5.0, imu_state[k]))
            imu_payload = dict(imu_state)
            if random.random() < bad_ratio:
                imu_payload["ax"] = 999.0  # 故意造一条越界数据，验证清洗策略会丢弃它
            writer.add_message(
                channel_id=imu_channel,
                log_time=log_time_ns,
                data=json.dumps(imu_payload).encode("utf-8"),
                publish_time=log_time_ns,
                sequence=i,
            )

            if pose_jump and i == jump_at:
                pos["px"] += 10.0  # 瞬移 10m：位姿连续性检查应该抓到这一步
            for k in pos:
                pos[k] += vel[k] + random.uniform(-0.01, 0.01)
            writer.add_message(
                channel_id=pose_channel,
                log_time=log_time_ns,
                data=json.dumps({k: round(v, 4) for k, v in pos.items()}).encode("utf-8"),
                publish_time=log_time_ns,
                sequence=i,
            )
        writer.finish()


@click.command()
@click.option("--robot", "robot_id", default="r-001", help="机器人 ID")
@click.option("--episodes", default=3, help="生成几个 episode（每个 episode 一个 mcap 文件）")
@click.option("--duration", "duration_s", default=20.0, help="每个 episode 的时长（秒）")
@click.option("--hz", default=10.0, help="imu/pose 采样频率")
@click.option("--bad-ratio", default=0.0, help="故意注入越界 imu 数据的比例，0~1，用来验证清洗/隔离逻辑")
@click.option("--dropout-seconds", default=0.0, help="在中段丢掉 N 秒消息，验证 QC 的滑动窗口频率检查")
@click.option("--pose-jump", is_flag=True, default=False, help="在中段注入一次位姿瞬移，验证 QC 的跳变检查")
@click.option("--out-dir", default="tools/datagen/fixtures", help="生成文件的输出目录")
def main(
    robot_id: str,
    episodes: int,
    duration_s: float,
    hz: float,
    bad_ratio: float,
    dropout_seconds: float,
    pose_jump: bool,
    out_dir: str,
) -> None:
    os.makedirs(out_dir, exist_ok=True)
    now = datetime.now(timezone.utc)
    paths = []
    for i in range(episodes):
        start_time = now + timedelta(seconds=i * (duration_s + 5))
        fname = f"{robot_id}-{int(time.time())}-{i:03d}.mcap"
        path = os.path.join(out_dir, fname)
        _write_mcap(
            path,
            start_time=start_time,
            duration_s=duration_s,
            hz=hz,
            bad_ratio=bad_ratio,
            dropout_seconds=dropout_seconds,
            pose_jump=pose_jump,
        )
        paths.append(path)
        click.echo(f"生成 {path}（start={start_time.isoformat()}, {int(duration_s * hz)} tick, imu+pose 两个 topic）")
    click.echo(f"\n共生成 {len(paths)} 个文件，接下来运行：\n  python -m tools.datagen.upload --robot {robot_id} --files {' '.join(paths)}")


if __name__ == "__main__":
    main()
