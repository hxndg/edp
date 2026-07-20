"""模拟客户端走网关完整上传流程（README 5.3 快速开始）：
创建 session -> 逐个文件签发预签名 URL 直传 -> 提交 manifest。

`--manifest-op correct` 时必须额外指定 `--episode-id`/`--affected-start`/
`--affected-end`，对应 README 3.2.1 里"范围限定 backfill"的输入。
"""
from __future__ import annotations

import glob
import hashlib
import os

import click
import requests

from common.config import settings


def _gateway_base_url(override: str | None) -> str:
    if override:
        return override
    host = "localhost" if settings.gateway_host == "0.0.0.0" else settings.gateway_host
    return f"http://{host}:{settings.gateway_port}"


@click.command()
@click.option("--robot", "robot_id", required=True)
@click.option("--task-id", default=None)
@click.option("--operator", default="datagen")
@click.option("--manifest-op", type=click.Choice(["append", "correct"]), default="append")
@click.option("--pipeline-profile", type=click.Choice(["auto_only", "human_required"]), default="auto_only")
@click.option("--processing-type", default="mcap_imu", help="业务处理类型，由 PG 注册表解析执行 Profile")
@click.option("--files", multiple=True, help="要上传的 mcap 文件路径；不填则用 fixtures 目录下最新一个文件")
@click.option("--fixtures-dir", default="tools/datagen/fixtures")
@click.option("--episode-id", default=None, help="manifest_op=correct 时必填")
@click.option("--affected-start", default=None, help="manifest_op=correct 时必填，ISO8601")
@click.option("--affected-end", default=None, help="manifest_op=correct 时必填，ISO8601")
@click.option("--gateway-url", default=None)
def main(
    robot_id: str,
    task_id: str | None,
    operator: str,
    manifest_op: str,
    pipeline_profile: str,
    processing_type: str,
    files: tuple[str, ...],
    fixtures_dir: str,
    episode_id: str | None,
    affected_start: str | None,
    affected_end: str | None,
    gateway_url: str | None,
) -> None:
    if manifest_op == "correct" and not (episode_id and affected_start and affected_end):
        raise click.ClickException("manifest_op=correct 需要同时指定 --episode-id --affected-start --affected-end")

    base_url = _gateway_base_url(gateway_url)
    file_list = list(files) or sorted(glob.glob(os.path.join(fixtures_dir, f"{robot_id}-*.mcap")))
    if not file_list:
        raise click.ClickException(f"没找到要上传的文件，先跑 `python -m tools.datagen.generate --robot {robot_id}`")

    session_resp = requests.post(
        f"{base_url}/sessions",
        json={
            "robot_id": robot_id,
            "task_id": task_id,
            "operator": operator,
            "manifest_op": manifest_op,
            "pipeline_profile": pipeline_profile,
            "processing_type": processing_type,
        },
        timeout=10,
    )
    session_resp.raise_for_status()
    upload_id = session_resp.json()["upload_id"]
    click.echo(
        f"创建 upload session: {upload_id} "
        f"(manifest_op={manifest_op}, pipeline_profile={pipeline_profile}, processing_type={processing_type})"
    )

    manifest_files = []
    for path in file_list:
        fname = os.path.basename(path)
        presign = requests.post(f"{base_url}/sessions/{upload_id}/presign", json={"file_name": fname}, timeout=10).json()
        with open(path, "rb") as f:
            data = f.read()
        put_resp = requests.put(presign["upload_url"], data=data, timeout=30)
        put_resp.raise_for_status()
        manifest_files.append({"file_uri": presign["file_uri"], "sha256": hashlib.sha256(data).hexdigest()})
        click.echo(f"  已直传 {fname} -> {presign['file_uri']}")

    manifest_payload: dict = {"files": manifest_files}
    if manifest_op == "correct":
        manifest_payload.update(episode_id=episode_id, affected_start_ts=affected_start, affected_end_ts=affected_end)

    manifest_resp = requests.post(f"{base_url}/sessions/{upload_id}/manifest", json=manifest_payload, timeout=10)
    manifest_resp.raise_for_status()
    click.echo(f"manifest 已提交: {manifest_resp.json()}")
    click.echo(f"\n打开 Dagster UI 观察 sensor 拉起 job：http://localhost:3000")


if __name__ == "__main__":
    main()
