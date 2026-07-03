"""DuckDB 执行引擎的公共入口（README 2.4 / 4.2）。

实现取舍：DuckDB 官方 iceberg 扩展对 REST Catalog 的原生支持还在快速演进中，
MVP 为了稳定性，采用"pyiceberg 按需扫描出 Arrow（可以带 row_filter 做裁剪）
→ DuckDB 对着这块 Arrow 内存做零拷贝 SQL 分析"的组合：DuckDB 仍然是真正跑
SQL 的引擎，只是数据接入这一步没有用 DuckDB 自带的 iceberg_scan。后续切换到
原生 `ATTACH ... (TYPE ICEBERG)` 时，上层调用方式（`iceberg_arrow` 的返回值
是一张 Arrow Table）不需要变。
"""
from __future__ import annotations

import pyarrow as pa
import duckdb

from common.iceberg import load_table


def iceberg_arrow(table_name: str, row_filter=None) -> pa.Table:
    table = load_table(table_name)
    scan = table.scan(row_filter=row_filter) if row_filter is not None else table.scan()
    return scan.to_arrow()


def query(sql: str, **arrow_tables: pa.Table):
    """跑一段 SQL，`arrow_tables` 里的每个 Arrow Table 都能在 SQL 里直接按变量名引用。"""
    con = duckdb.connect()
    for name, tbl in arrow_tables.items():
        con.register(name, tbl)
    return con.sql(sql).to_df()
