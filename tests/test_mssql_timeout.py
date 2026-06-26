from __future__ import annotations

from sqlalchemy import create_engine

from modules.mssql_timeout import apply_mssql_query_timeout, resolve_mssql_query_timeout


def test_resolve_mssql_query_timeout(monkeypatch):
    monkeypatch.setenv("MSSQL_QUERY_TIMEOUT", "30")

    assert resolve_mssql_query_timeout() == 30


def test_resolve_mssql_query_timeout_ignores_invalid_values(monkeypatch):
    monkeypatch.setenv("MSSQL_QUERY_TIMEOUT", "invalid")
    assert resolve_mssql_query_timeout() is None

    monkeypatch.setenv("MSSQL_QUERY_TIMEOUT", "0")
    assert resolve_mssql_query_timeout() is None


def test_apply_mssql_query_timeout_registers_connect_listener_for_pyodbc():
    engine = create_engine(
        "mssql+pyodbc://user:pass@example.invalid/db"
        "?driver=ODBC+Driver+18+for+SQL+Server"
    )
    before_count = len(engine.pool.dispatch.connect)

    apply_mssql_query_timeout(engine, 30)

    assert len(engine.pool.dispatch.connect) == before_count + 1


def test_apply_mssql_query_timeout_skips_non_mssql_engines():
    engine = create_engine("sqlite:///:memory:")
    before_count = len(engine.pool.dispatch.connect)

    apply_mssql_query_timeout(engine, 30)

    assert len(engine.pool.dispatch.connect) == before_count
