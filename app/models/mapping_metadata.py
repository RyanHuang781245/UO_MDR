from __future__ import annotations

from sqlalchemy import func, inspect, text

from app.extensions import db


class TaskFileState(db.Model):
    __tablename__ = "task_file_states"

    task_id = db.Column(db.String(40), primary_key=True)
    files_revision = db.Column(db.BigInteger, nullable=False, server_default="0")
    updated_at = db.Column(db.DateTime, nullable=False, server_default=func.now())


class MappingSchemeRecord(db.Model):
    __tablename__ = "mapping_schemes"

    scheme_id = db.Column(db.String(40), primary_key=True)
    task_id = db.Column(db.String(40), nullable=False, index=True)
    name = db.Column(db.String(200), nullable=False)
    mapping_file = db.Column(db.String(260))
    mapping_display_name = db.Column(db.String(260))
    source_path = db.Column(db.Text)
    reference_ok = db.Column(db.Boolean, nullable=False, server_default="0")
    extract_ok = db.Column(db.Boolean, nullable=False, server_default="0")
    validated_against_revision = db.Column(db.BigInteger, nullable=False, server_default="0")
    status_key = db.Column(db.String(50), nullable=False, server_default="error")
    saved_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    actor_work_id = db.Column(db.String(100))
    actor_label = db.Column(db.String(200))
    enable_figure_reference = db.Column(db.Boolean, nullable=False, server_default="1")

    __table_args__ = (
        db.Index("ix_mapping_schemes_task_updated", "task_id", "updated_at"),
        db.Index("ix_mapping_schemes_task_status_updated", "task_id", "status_key", "updated_at"),
    )


class MappingRunRecord(db.Model):
    __tablename__ = "mapping_runs"

    run_id = db.Column(db.String(40), primary_key=True)
    task_id = db.Column(db.String(40), nullable=False, index=True)
    scheme_id = db.Column(db.String(40), nullable=True, index=True)
    mapping_display_name = db.Column(db.String(260))
    status = db.Column(db.String(50), nullable=False, server_default="unknown")
    output_count = db.Column(db.Integer, nullable=False, server_default="0")
    zip_file = db.Column(db.String(500))
    log_file = db.Column(db.String(500))
    error = db.Column(db.Text)
    reference_ok = db.Column(db.Boolean, nullable=False, server_default="0")
    extract_ok = db.Column(db.Boolean, nullable=False, server_default="0")
    source = db.Column(db.String(50), nullable=False, server_default="manual")
    started_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    completed_at = db.Column(db.DateTime)

    __table_args__ = (
        db.Index("ix_mapping_runs_task_started", "task_id", "started_at"),
        db.Index("ix_mapping_runs_task_status_started", "task_id", "status", "started_at"),
    )


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    if "mapping_schemes" not in set(inspector.get_table_names()):
        return

    existing_columns = {col["name"].lower() for col in inspector.get_columns("mapping_schemes")}
    if engine.dialect.name == "mssql":
        with engine.begin() as conn:
            if "enable_figure_reference" not in existing_columns:
                conn.execute(
                    text(
                        "ALTER TABLE mapping_schemes "
                        "ADD enable_figure_reference BIT NOT NULL "
                        "CONSTRAINT DF_mapping_schemes_enable_figure_reference DEFAULT(1);"
                    )
                )
    elif engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            if "enable_figure_reference" not in existing_columns:
                conn.execute(
                    text(
                        "ALTER TABLE mapping_schemes "
                        "ADD COLUMN enable_figure_reference BOOLEAN NOT NULL DEFAULT 1;"
                    )
                )
