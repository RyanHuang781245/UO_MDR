from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from app.services.schema_control import SCHEMA_GROUPS

ROOT = Path(__file__).resolve().parents[1]


def test_alembic_upgrade_head_creates_baseline_schema(monkeypatch, tmp_path):
    db_path = tmp_path / "alembic.sqlite"
    db_url = f"sqlite:///{db_path}"

    monkeypatch.setenv("ALEMBIC_DATABASE_URL", db_url)
    monkeypatch.setenv("ALEMBIC_CONFIG_NAME", "testing")
    monkeypatch.setenv("AUTH_ENABLED", "1")

    cfg = Config(str(ROOT / "alembic.ini"))
    command.upgrade(cfg, "head")

    engine = create_engine(db_url)
    tables = set(inspect(engine).get_table_names())
    required_tables = {table for group in SCHEMA_GROUPS.values() for table in group}

    assert required_tables.issubset(tables)

    with engine.connect() as conn:
        revision = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()

    assert revision == "0001_baseline_schema"
