"""Iceberg REST Catalog 访问帮助函数（pyiceberg）。

README 2.2 原则 3：Iceberg commit 是数据唯一真相源。这个模块是全平台唯一
应该建 Iceberg catalog 连接的地方——engines/orchestration/gateway 都通过它读写表，
不要在别处再拼一份 catalog 配置。
"""
from __future__ import annotations

import functools
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import Table

from common.config import settings

NAMESPACE = "edp"

# 每张表都加的四列审计列（README 3.1.3），建表 schema 里统一拼进去，
# 避免每个表定义各写一遍、漏掉某一列。
AUDIT_FIELDS_DDL = """
    _batch_id STRING,
    _run_id STRING,
    _ingested_at TIMESTAMP,
    _source_uri STRING
"""


@functools.lru_cache(maxsize=1)
def catalog() -> Catalog:
    return load_catalog(
        settings.iceberg_catalog_name,
        **{
            "type": "rest",
            "uri": settings.iceberg_rest_uri,
            "warehouse": settings.iceberg_warehouse,
            "s3.endpoint": settings.minio_endpoint,
            "s3.access-key-id": settings.minio_root_user,
            "s3.secret-access-key": settings.minio_root_password,
            "s3.path-style-access": "true",
        },
    )


def ensure_namespace() -> None:
    cat = catalog()
    if (NAMESPACE,) not in cat.list_namespaces():
        cat.create_namespace(NAMESPACE)


def table_identifier(table_name: str) -> str:
    return f"{NAMESPACE}.{table_name}"


def create_table_if_not_exists(
    table_name: str, schema: Schema, partition_spec: PartitionSpec | None = None
) -> Table:
    ensure_namespace()
    cat = catalog()
    ident = table_identifier(table_name)
    try:
        return cat.load_table(ident)
    except NoSuchTableError:
        kwargs: dict[str, Any] = {}
        if partition_spec is not None:
            kwargs["partition_spec"] = partition_spec
        return cat.create_table(ident, schema=schema, **kwargs)


def load_table(table_name: str) -> Table:
    return catalog().load_table(table_identifier(table_name))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


AUDIT_COLUMN_NAMES = ("_batch_id", "_run_id", "_ingested_at", "_source_uri")


def with_audit_columns(
    arrow_table: pa.Table, *, batch_id: str, run_id: str, source_uri: str
) -> pa.Table:
    """给一批准备写入 Iceberg 的 pyarrow Table 补上四列审计列。

    幂等：如果传入的 rows 是从 Iceberg 读回来再改几个字段写回去的（比如
    annotation_auto 把预标结果转正），本身已经带着上一次写入时打的审计列，
    这里先丢掉旧的四列再重新盖章，避免出现同名列出现两次。
    """
    existing = set(arrow_table.column_names)
    if existing.intersection(AUDIT_COLUMN_NAMES):
        arrow_table = arrow_table.drop_columns(
            [c for c in AUDIT_COLUMN_NAMES if c in existing]
        )
    n = arrow_table.num_rows
    ingested_at = now_utc()
    return arrow_table.append_column(
        "_batch_id", pa.array([batch_id] * n, type=pa.string())
    ).append_column(
        "_run_id", pa.array([run_id] * n, type=pa.string())
    ).append_column(
        "_ingested_at", pa.array([ingested_at] * n, type=pa.timestamp("us"))
    ).append_column(
        "_source_uri", pa.array([source_uri] * n, type=pa.string())
    )


def in_filter(col: str, values: list[str]):
    """构造 `col IN (values)` 等价的 pyiceberg 行过滤表达式（pyiceberg 无原生 IN）。"""
    from functools import reduce

    from pyiceberg.expressions import AlwaysFalse, EqualTo, Or

    if not values:
        return AlwaysFalse()
    return reduce(Or, [EqualTo(col, v) for v in values])


def _align_to_table_schema(arrow_table: pa.Table, tbl: Table) -> pa.Table:
    """把待写入的 pyarrow Table 的列类型/可空性对齐到 Iceberg 表 schema。

    典型触发场景一：某一批行里某个可空字段全是 None（比如没填 task_id），
    `pa.Table.from_pylist` 会把这一列推断成 `pa.null()` 类型，pyiceberg
    不认识这个类型，写入直接报 `TypeError: Unsupported type: null`。

    典型触发场景二：`pa.Table.from_pylist` 推出来的字段一律是 nullable=True，
    而某些主键列在 Iceberg schema 里是 `required`，pyiceberg 的兼容性检查
    会因为 nullable 对不上而拒绝写入（即便实际数据里没有一个 None）。

    这里按目标 schema 把类型和可空性都改过来，一次修复，全平台写入点受益。
    """
    target_schema = tbl.schema().as_arrow()
    target_fields = {f.name: f for f in target_schema}
    columns, fields = [], []
    for name in arrow_table.column_names:
        col = arrow_table.column(name)
        target_field = target_fields.get(name)
        if target_field is not None:
            if not col.type.equals(target_field.type):
                col = col.cast(target_field.type)
            field = pa.field(name, col.type, nullable=target_field.nullable)
        else:
            field = pa.field(name, col.type, nullable=True)
        columns.append(col)
        fields.append(field)
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))


def append(table_name: str, arrow_table: pa.Table) -> Table:
    tbl = load_table(table_name)
    tbl.append(_align_to_table_schema(arrow_table, tbl))
    return tbl


def upsert(table_name: str, arrow_table: pa.Table, join_cols: list[str]) -> Table:
    """行级 MERGE 语义：命中 join_cols 的行覆盖，其余追加。

    使用 pyiceberg >= 0.9 的原生 `Table.upsert`：内部在**单个 Iceberg 事务**里
    完成 matched-update + not-matched-insert，只产生一次 commit，读者要么看到
    整批新数据、要么看到整批旧数据，不存在"删了旧行、还没插新行"的中间态
    （旧版手写 delete+append 是两次 commit，存在这个窗口，已废弃）。
    """
    tbl = load_table(table_name)
    aligned = _align_to_table_schema(arrow_table, tbl)
    tbl.upsert(aligned, join_cols=join_cols)
    return tbl


def replace_where(table_name: str, delete_filter, arrow_table: pa.Table | None) -> Table:
    """事务式"范围覆盖"：删除命中 delete_filter 的旧行 + 追加整批新行，单次 commit。

    用 pyiceberg 的 `Table.transaction()` 把 delete 和 append 合并成一个原子
    快照提交，专门服务两类场景（见 docs/saga-consistency-guide.md）：
    - ingest_append 重跑：先清掉上一次半途而废的 bronze/silver 行再写，幂等；
    - ingest_correct 范围修正：delete 受影响时间窗 + append 修正数据必须原子，
      否则并发读者会看到"旧数据没了、新数据还没来"的空洞。

    arrow_table 传 None / 空表时退化为纯删除（同样是一次 commit）。
    """
    tbl = load_table(table_name)
    with tbl.transaction() as txn:
        txn.delete(delete_filter=delete_filter)
        if arrow_table is not None and arrow_table.num_rows > 0:
            txn.append(_align_to_table_schema(arrow_table, tbl))
    return tbl
