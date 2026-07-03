"""定期 compaction（README 4.6）：频繁的小批次 MERGE 会产生大量小文件，
拖垮分区裁剪效果，需要定期把小文件合并到 128-512MB 目标大小。这里只对
"需要 MERGE 的索引表"（对齐 2.2 原则 10）做 compaction，纯追加的 Bronze/
Silver 信号表不需要（它们的小文件问题靠上游攒批写入解决，不在 MVP 范围）。
"""
from __future__ import annotations

from dagster import job, op

from schemas.iceberg_tables import EPISODE, ENTITY_TAG, SAMPLE

TABLES_TO_COMPACT = [EPISODE, SAMPLE, ENTITY_TAG]


@op
def compact_tables(context) -> dict:
    from common.iceberg import NAMESPACE
    from engines.spark.spark_session import CATALOG, build_spark_session

    spark = build_spark_session("compaction")
    results = {}
    for table in TABLES_TO_COMPACT:
        try:
            df = spark.sql(
                f"CALL {CATALOG}.system.rewrite_data_files(table => '{NAMESPACE}.{table}')"
            )
            rows = df.collect()
            results[table] = rows[0].asDict() if rows else {}
            context.log.info(f"compacted {table}: {results[table]}")
        except Exception as e:  # noqa: BLE001
            context.log.exception(f"compaction failed for {table}")
            results[table] = {"error": str(e)}
    return results


@job(description="定期把 episode/sample/entity_tag 的小文件合并到目标大小（README 4.6）")
def compaction_job():
    compact_tables()
