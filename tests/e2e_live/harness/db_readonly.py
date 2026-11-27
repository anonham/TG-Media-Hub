"""scratch 库只读直查 (; F8: 仅 SELECT/PRAGMA, 写操作一律走应用 API,
不绕过 submit_db_write)。"""

import sqlite3
import urllib.request
from pathlib import Path

_READONLY_HEADS = {"SELECT", "PRAGMA", "WITH", "EXPLAIN"}


def connect_ro(db_path: Path) -> sqlite3.Connection:
    """只读连接 scratch 库 (mode=ro URI + query_only 双保险)。"""
    if not db_path.exists():
        raise FileNotFoundError(f"scratch 库不存在: {db_path}")
    uri = "file:" + urllib.request.pathname2url(str(db_path)) + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=15.0)
    conn.execute("PRAGMA query_only = 1")
    return conn


def _assert_readonly(sql: str) -> None:
    head = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    if head not in _READONLY_HEADS:
        raise ValueError(f"live harness DB 通道仅允许只读查询 (F8), 收到: {head}")


def fetch_all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    _assert_readonly(sql)
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def fetch_one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict | None:
    rows = fetch_all(conn, sql, params)
    return rows[0] if rows else None
