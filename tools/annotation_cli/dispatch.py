"""标注 CLI ——"派活"侧（README 3.2.2 / 4.9 backlog）：
拉取 Dagster `annotation_dispatch` 写好的任务包，展示给"标注员"看。

生产化时这一步会换成真实查询 Iceberg 并推给 Label Studio/CVAT；这里先用
CLI 模拟，接口边界（任务包 JSON 结构）现在就按未来要接的样子设计，见
`common/annotation_batches.py`。
"""
from __future__ import annotations

import json

import click

from common.annotation_batches import load_package
from common.db import fetch_one


@click.command()
@click.option("--batch", "batch_id", required=True, help="annotation_batch.batch_id（MVP 里等于 upload_id）")
def main(batch_id: str) -> None:
    batch = fetch_one("SELECT * FROM annotation_batch WHERE batch_id = %s", (batch_id,))
    if batch is None:
        raise click.ClickException(f"annotation_batch '{batch_id}' 不存在，确认 Dagster 的 annotation_dispatch 是否已经跑过")

    package = load_package(batch_id)
    click.echo(f"批次 {batch_id} 状态: {batch['status']}，共 {len(package['samples'])} 个样本待标注：\n")
    click.echo(json.dumps(package, indent=2, ensure_ascii=False))
    click.echo(
        f"\n（模拟人工标注中...）标注完成后运行：\n"
        f"  python -m tools.annotation_cli.collect --batch {batch_id}"
    )


if __name__ == "__main__":
    main()
