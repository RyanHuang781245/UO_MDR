from __future__ import annotations

import os

from sqlalchemy import event
from sqlalchemy.engine import Engine


def resolve_mssql_query_timeout() -> int | None:
    raw = (os.environ.get("MSSQL_QUERY_TIMEOUT") or "").strip()
    if not raw:
        return None
    try:
        timeout = int(raw)
    except ValueError:
        return None
    return timeout if timeout > 0 else None


def apply_mssql_query_timeout(engine: Engine, timeout: int | None = None) -> None:
    resolved_timeout = timeout if timeout is not None else resolve_mssql_query_timeout()
    if not resolved_timeout:
        return
    if engine.dialect.name != "mssql" or engine.dialect.driver != "pyodbc":
        return

    @event.listens_for(engine, "connect")
    def set_pyodbc_query_timeout(dbapi_connection, _connection_record):
        dbapi_connection.timeout = resolved_timeout
