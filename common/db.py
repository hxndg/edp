"""platform 库（业务瞬态状态）的连接与轻量 DAO 帮助函数。

原则（对应 README 2.2 第 6 条 / 4.7）：这里的表都是半派生态，只做"当前状态点查"，
出问题可以从 Iceberg 快照 + Kafka 事件重放重建，所以只用最简单的 psycopg，
不引入 ORM/迁移框架，保持这一层足够薄。
"""
from __future__ import annotations

import contextlib
import json
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from common.config import settings


@contextlib.contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(settings.platform_dsn, row_factory=dict_row, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def execute(sql: str, params: dict | tuple | None = None) -> None:
    with get_conn() as conn:
        conn.execute(sql, params)


def fetch_one(sql: str, params: dict | tuple | None = None) -> dict[str, Any] | None:
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchone()


def fetch_all(sql: str, params: dict | tuple | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)
