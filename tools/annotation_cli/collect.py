"""标注 CLI ——"收活"侧（README 3.2.2）：提交标注结果、调 webhook 唤醒
`annotation_collect` job-B。

默认 `--auto-mock` 模式自动生成标注结果（demo/CI 用，不需要真人参与）；
真实使用时可以先 `dispatch.py` 导出任务包、编辑后再跑这个脚本提交。
"""
from __future__ import annotations

import random

import click
import requests

from common.annotation_batches import load_package, upload_result
from common.config import settings


@click.command()
@click.option("--batch", "batch_id", required=True, help="annotation_batch.batch_id")
@click.option("--reviewer", default="mock-annotator", help="标注员/审核员标识")
@click.option("--auto-mock/--no-auto-mock", default=True, help="自动生成标注结果（demo 用），否则需要交互输入")
def main(batch_id: str, reviewer: str, auto_mock: bool) -> None:
    package = load_package(batch_id)
    results = []
    for item in package["samples"]:
        if auto_mock:
            caption = item.get("prelabel_caption") or "人工补充的描述"
            review_status = "passed" if random.random() > 0.1 else "rejected"
        else:
            caption = click.prompt(f"[{item['sample_id']}] 预标: {item.get('prelabel_caption')!r}，输入最终标注", default=item.get("prelabel_caption", ""))
            review_status = click.prompt("review_status", type=click.Choice(["passed", "rejected"]), default="passed")
        results.append({"sample_id": item["sample_id"], "caption": caption, "review_status": review_status})

    result_uri = upload_result(batch_id, reviewer, results)
    click.echo(f"结果包已上传: {result_uri}")

    resp = requests.post(
        f"http://{settings.gateway_host if settings.gateway_host != '0.0.0.0' else 'localhost'}:{settings.gateway_port}/webhooks/annotation-complete",
        json={"batch_id": batch_id, "package_result_uri": result_uri, "reviewer": reviewer},
        timeout=10,
    )
    resp.raise_for_status()
    click.echo(f"已调用 webhook 唤醒 annotation_collect：{resp.json()}")


if __name__ == "__main__":
    main()
