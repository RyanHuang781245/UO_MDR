from __future__ import annotations

from sqlalchemy import func, inspect, text

from modules.auth_models import db


class TaskRecord(db.Model):
    __tablename__ = "tasks"

    id = db.Column(db.String(40), primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    creator = db.Column(db.String(200))
    nas_path = db.Column(db.Text)
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    if "tasks" not in set(inspector.get_table_names()):
        return

    existing_columns = {col["name"].lower() for col in inspector.get_columns("tasks")}
    if engine.dialect.name == "mssql":
        with engine.begin() as conn:
            if "description" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD description NVARCHAR(MAX) NULL;"))
            if "creator" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD creator NVARCHAR(200) NULL;"))
            if "nas_path" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD nas_path NVARCHAR(MAX) NULL;"))
            if "created_at" not in existing_columns:
                conn.execute(
                    text(
                        """
                        ALTER TABLE tasks
                        ADD created_at DATETIME2 NOT NULL
                        CONSTRAINT DF_tasks_created_at DEFAULT(SYSDATETIME());
                        """
                    )
                )
    elif engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            if "description" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD COLUMN description TEXT;"))
            if "creator" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD COLUMN creator TEXT;"))
            if "nas_path" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD COLUMN nas_path TEXT;"))
            if "created_at" not in existing_columns:
                conn.execute(text("ALTER TABLE tasks ADD COLUMN created_at DATETIME;"))
