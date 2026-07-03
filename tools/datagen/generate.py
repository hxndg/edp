"""合成数据生成器（README 2.4 / 3.1.7）：生成带 `imu` topic 的 MCAP 文件，
供 `tools/datagen/upload.py` 走网关上传，демo 整条入湖链路。也可以把真实
MCAP 样本文件直接放进 `tools/datagen/fixtures/` 使用，不强制要求走这个脚本。
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
    "properties": {
        "ax": {"type": "number"},
        "ay": {"type": "number"},
        "az": {"type": "number"},
        "gx": {"type": "number"},
        "gy": {"type": "number"},
        "gz": {"type": "number"},
    },
}


def _write_mcap(path: str, *, start_time: datetime, duration_s: float, hz: float, bad_ratio: float) -> None:
    from mcap.writer import Writer

    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="edp-datagen", library="edp-datagen-0.1")
        schema_id = writer.register_schema(
            name="imu", encoding="jsonschema", data=json.dumps(IMU_SCHEMA).encode("utf-8")
        )
        channel_id = writer.register_channel(schema_id=schema_id, topic="imu", message_encoding="json")

        n = int(duration_s * hz)
        state = {k: 0.0 for k in ("ax", "ay", "az", "gx", "gy", "gz")}
        for i in range(n):
            ts = start_time + timedelta(seconds=i / hz)
            log_time_ns = int(ts.timestamp() * 1e9)
            for k in state:
                state[k] += random.uniform(-0.3, 0.3)
                state[k] = max(-5.0, min(5.0, state[k]))
            payload = dict(state)
            if random.random() < bad_ratio:
                payload["ax"] = 999.0  # 故意造一条越界数据，验证清洗策略会丢弃它
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time_ns,
                data=json.dumps(payload).encode("utf-8"),
                publish_time=log_time_ns,
                sequence=i,
            )
        writer.finish()


@click.command()
@click.option("--robot", "robot_id", default="r-001", help="机器人 ID")
@click.option("--episodes", default=3, help="生成几个 episode（每个 episode 一个 mcap 文件）")
@click.option("--duration", "duration_s", default=20.0, help="每个 episode 的时长（秒）")
@click.option("--hz", default=10.0, help="imu 采样频率")
@click.option("--bad-ratio", default=0.0, help="故意注入越界数据的比例，0~1，用来验证清洗/隔离逻辑")
@click.option("--out-dir", default="tools/datagen/fixtures", help="生成文件的输出目录")
def main(robot_id: str, episodes: int, duration_s: float, hz: float, bad_ratio: float, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    now = datetime.now(timezone.utc)
    paths = []
    for i in range(episodes):
        start_time = now + timedelta(seconds=i * (duration_s + 5))
        fname = f"{robot_id}-{int(time.time())}-{i:03d}.mcap"
        path = os.path.join(out_dir, fname)
        _write_mcap(path, start_time=start_time, duration_s=duration_s, hz=hz, bad_ratio=bad_ratio)
        paths.append(path)
        click.echo(f"生成 {path}（start={start_time.isoformat()}, {int(duration_s * hz)} 条 imu 消息）")
    click.echo(f"\n共生成 {len(paths)} 个文件，接下来运行：\n  python -m tools.datagen.upload --robot {robot_id} --files {' '.join(paths)}")


if __name__ == "__main__":
    main()
