"""把 worker manifest 的业务结果翻译成 Argo 可执行的退出策略。"""
from __future__ import annotations

import click

from common.errors import ErrorCode, Retry, retry_policy
from engines.worker import staging

EXIT_BUSINESS = 10
EXIT_RETRYABLE = 20


def manifest_exit_code(staging_prefix: str) -> int:
    manifest = staging.try_read_json(f"{staging_prefix}/{staging.MANIFEST_JSON}")
    if not manifest:
        return EXIT_RETRYABLE
    if manifest.get("status") == "ok":
        return 0
    raw_code = manifest.get("error_code")
    try:
        code = ErrorCode(raw_code)
    except (TypeError, ValueError):
        return EXIT_RETRYABLE
    return EXIT_BUSINESS if retry_policy(code.value) == Retry.NOT_RETRYABLE else EXIT_RETRYABLE


@click.command()
@click.option("--staging-prefix", required=True)
@click.option("--clear", is_flag=True, help="worker attempt 开始前删除旧 manifest")
def main(staging_prefix: str, clear: bool) -> None:
    if clear:
        staging.clear_manifest(staging_prefix)
        return
    raise SystemExit(manifest_exit_code(staging_prefix))


if __name__ == "__main__":
    main()
