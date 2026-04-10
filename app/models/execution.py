from __future__ import annotations

from sqlalchemy import func, inspect, text

from app.extensions import db


class JobRecord(db.Model):
    __tablename__ = "job_records"

    job_id = db.Column(db.String(40), primary_key=True)
    parent_job_id = db.Column(db.String(40), index=True)
    job_type = db.Column(db.String(50), nullable=False, index=True)
    queue_name = db.Column(db.String(50), nullable=False, server_default="default", index=True)
    task_id = db.Column(db.String(40), nullable=True, index=True)
    target_name = db.Column(db.String(260))
    status = db.Column(db.String(50), nullable=False, server_default="queued", index=True)
    priority = db.Column(db.Integer, nullable=False, server_default="100")
    payload_json = db.Column(db.Text, nullable=False, server_default="{}")
    result_json = db.Column(db.Text)
    artifact_root = db.Column(db.String(500))
    attempt_count = db.Column(db.Integer, nullable=False, server_default="0")
    max_attempts = db.Column(db.Integer, nullable=False, server_default="1")
    worker_id = db.Column(db.String(120))
    error_summary = db.Column(db.Text)
    created_by_work_id = db.Column(db.String(100))
    created_by_label = db.Column(db.String(200))
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    claimed_at = db.Column(db.DateTime)
    started_at = db.Column(db.DateTime)
    heartbeat_at = db.Column(db.DateTime)
    cancel_requested_at = db.Column(db.DateTime)
    cancel_reason = db.Column(db.Text)
    completed_at = db.Column(db.DateTime)

    __table_args__ = (
        db.Index("ix_job_records_status_priority_created", "status", "priority", "created_at"),
        db.Index("ix_job_records_task_status_created", "task_id", "status", "created_at"),
        db.Index("ix_job_records_type_status_created", "job_type", "status", "created_at"),
    )


class JobArtifactRecord(db.Model):
    __tablename__ = "job_artifacts"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job_id = db.Column(db.String(40), nullable=False, index=True)
    artifact_type = db.Column(db.String(50), nullable=False)
    rel_path = db.Column(db.String(500), nullable=False)
    size_bytes = db.Column(db.BigInteger)
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())

    __table_args__ = (
        db.Index("ix_job_artifacts_job_type_created", "job_id", "artifact_type", "created_at"),
    )


class JobEventRecord(db.Model):
    __tablename__ = "job_events"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job_id = db.Column(db.String(40), nullable=False, index=True)
    event_type = db.Column(db.String(50), nullable=False)
    message = db.Column(db.Text)
    payload_json = db.Column(db.Text)
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())

    __table_args__ = (
        db.Index("ix_job_events_job_created", "job_id", "created_at"),
    )


class TaskExecutionLock(db.Model):
    __tablename__ = "task_execution_locks"

    task_id = db.Column(db.String(40), primary_key=True)
    lock_type = db.Column(db.String(50), nullable=False, server_default="write")
    job_id = db.Column(db.String(40), nullable=False, index=True)
    acquired_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    expires_at = db.Column(db.DateTime, nullable=False, index=True)


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    if "job_records" not in set(inspector.get_table_names()):
        return

    existing_columns = {col["name"].lower() for col in inspector.get_columns("job_records")}
    if engine.dialect.name == "mssql":
        with engine.begin() as conn:
            if "queue_name" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD queue_name NVARCHAR(50) NOT NULL CONSTRAINT DF_job_records_queue_name DEFAULT('default');"))
            if "parent_job_id" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD parent_job_id NVARCHAR(40) NULL;"))
            if "claimed_at" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD claimed_at DATETIME2 NULL;"))
            if "heartbeat_at" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD heartbeat_at DATETIME2 NULL;"))
            if "cancel_requested_at" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD cancel_requested_at DATETIME2 NULL;"))
            if "cancel_reason" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD cancel_reason NVARCHAR(MAX) NULL;"))
            if "result_json" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD result_json NVARCHAR(MAX) NULL;"))
            if "artifact_root" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD artifact_root NVARCHAR(500) NULL;"))
    elif engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            if "queue_name" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN queue_name TEXT NOT NULL DEFAULT 'default';"))
            if "parent_job_id" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN parent_job_id TEXT;"))
            if "claimed_at" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN claimed_at DATETIME;"))
            if "heartbeat_at" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN heartbeat_at DATETIME;"))
            if "cancel_requested_at" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN cancel_requested_at DATETIME;"))
            if "cancel_reason" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN cancel_reason TEXT;"))
            if "result_json" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN result_json TEXT;"))
            if "artifact_root" not in existing_columns:
                conn.execute(text("ALTER TABLE job_records ADD COLUMN artifact_root TEXT;"))
