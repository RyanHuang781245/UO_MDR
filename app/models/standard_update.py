from __future__ import annotations

from sqlalchemy import func, inspect, text

from app.extensions import db


class StandardUpdateRecord(db.Model):
    __tablename__ = "standard_update_tasks"

    id = db.Column(db.String(40), primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    creator_name = db.Column(db.String(200))
    creator_work_id = db.Column(db.String(120))
    status = db.Column(db.String(40), nullable=False, server_default="draft")
    word_file_path = db.Column(db.Text)
    standard_excel_path = db.Column(db.Text)
    harmonised_snapshot_path = db.Column(db.Text)
    harmonised_snapshot_version = db.Column(db.String(120))
    last_output_path = db.Column(db.Text)
    last_run_at = db.Column(db.DateTime)
    last_run_status = db.Column(db.String(40))
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    updated_at = db.Column(db.DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class HarmonisedReleaseRecord(db.Model):
    __tablename__ = "harmonised_releases"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    source_url = db.Column(db.Text)
    file_name = db.Column(db.String(255))
    nas_path = db.Column(db.Text)
    version_label = db.Column(db.String(120))
    checksum = db.Column(db.String(120))
    is_active = db.Column(db.Boolean, nullable=False, server_default=text("1"))
    download_status = db.Column(db.String(40), nullable=False, server_default="available")
    error_message = db.Column(db.Text)
    downloaded_at = db.Column(db.DateTime, nullable=False, server_default=func.now())


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "standard_update_tasks" in tables:
        _ensure_standard_update_columns(engine, inspector)
    if "harmonised_releases" in tables:
        _ensure_harmonised_release_columns(engine, inspector)


def _ensure_standard_update_columns(engine, inspector) -> None:
    existing = {col["name"].lower() for col in inspector.get_columns("standard_update_tasks")}
    ddl = {
        "description": {"mssql": "ALTER TABLE standard_update_tasks ADD description NVARCHAR(MAX) NULL;",
                        "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN description TEXT;"},
        "creator_name": {"mssql": "ALTER TABLE standard_update_tasks ADD creator_name NVARCHAR(200) NULL;",
                         "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN creator_name TEXT;"},
        "creator_work_id": {"mssql": "ALTER TABLE standard_update_tasks ADD creator_work_id NVARCHAR(120) NULL;",
                            "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN creator_work_id TEXT;"},
        "status": {"mssql": "ALTER TABLE standard_update_tasks ADD status NVARCHAR(40) NOT NULL CONSTRAINT DF_standard_update_tasks_status DEFAULT('draft');",
                   "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN status TEXT DEFAULT 'draft';"},
        "word_file_path": {"mssql": "ALTER TABLE standard_update_tasks ADD word_file_path NVARCHAR(MAX) NULL;",
                           "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN word_file_path TEXT;"},
        "standard_excel_path": {"mssql": "ALTER TABLE standard_update_tasks ADD standard_excel_path NVARCHAR(MAX) NULL;",
                                "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN standard_excel_path TEXT;"},
        "harmonised_snapshot_path": {"mssql": "ALTER TABLE standard_update_tasks ADD harmonised_snapshot_path NVARCHAR(MAX) NULL;",
                                     "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN harmonised_snapshot_path TEXT;"},
        "harmonised_snapshot_version": {"mssql": "ALTER TABLE standard_update_tasks ADD harmonised_snapshot_version NVARCHAR(120) NULL;",
                                        "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN harmonised_snapshot_version TEXT;"},
        "last_output_path": {"mssql": "ALTER TABLE standard_update_tasks ADD last_output_path NVARCHAR(MAX) NULL;",
                             "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN last_output_path TEXT;"},
        "last_run_at": {"mssql": "ALTER TABLE standard_update_tasks ADD last_run_at DATETIME2 NULL;",
                        "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN last_run_at DATETIME;"},
        "last_run_status": {"mssql": "ALTER TABLE standard_update_tasks ADD last_run_status NVARCHAR(40) NULL;",
                            "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN last_run_status TEXT;"},
        "created_at": {"mssql": "ALTER TABLE standard_update_tasks ADD created_at DATETIME2 NOT NULL CONSTRAINT DF_standard_update_tasks_created_at DEFAULT(SYSDATETIME());",
                       "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN created_at DATETIME;"},
        "updated_at": {"mssql": "ALTER TABLE standard_update_tasks ADD updated_at DATETIME2 NOT NULL CONSTRAINT DF_standard_update_tasks_updated_at DEFAULT(SYSDATETIME());",
                       "sqlite": "ALTER TABLE standard_update_tasks ADD COLUMN updated_at DATETIME;"},
    }
    dialect = engine.dialect.name
    with engine.begin() as conn:
        for column, statements in ddl.items():
            if column not in existing and dialect in statements:
                conn.execute(text(statements[dialect]))


def _ensure_harmonised_release_columns(engine, inspector) -> None:
    existing = {col["name"].lower() for col in inspector.get_columns("harmonised_releases")}
    ddl = {
        "source_url": {"mssql": "ALTER TABLE harmonised_releases ADD source_url NVARCHAR(MAX) NULL;",
                       "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN source_url TEXT;"},
        "file_name": {"mssql": "ALTER TABLE harmonised_releases ADD file_name NVARCHAR(255) NULL;",
                      "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN file_name TEXT;"},
        "nas_path": {"mssql": "ALTER TABLE harmonised_releases ADD nas_path NVARCHAR(MAX) NULL;",
                     "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN nas_path TEXT;"},
        "version_label": {"mssql": "ALTER TABLE harmonised_releases ADD version_label NVARCHAR(120) NULL;",
                          "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN version_label TEXT;"},
        "checksum": {"mssql": "ALTER TABLE harmonised_releases ADD checksum NVARCHAR(120) NULL;",
                     "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN checksum TEXT;"},
        "is_active": {"mssql": "ALTER TABLE harmonised_releases ADD is_active BIT NOT NULL CONSTRAINT DF_harmonised_releases_is_active DEFAULT(1);",
                      "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN is_active BOOLEAN DEFAULT 1;"},
        "download_status": {"mssql": "ALTER TABLE harmonised_releases ADD download_status NVARCHAR(40) NOT NULL CONSTRAINT DF_harmonised_releases_download_status DEFAULT('available');",
                            "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN download_status TEXT DEFAULT 'available';"},
        "error_message": {"mssql": "ALTER TABLE harmonised_releases ADD error_message NVARCHAR(MAX) NULL;",
                          "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN error_message TEXT;"},
        "downloaded_at": {"mssql": "ALTER TABLE harmonised_releases ADD downloaded_at DATETIME2 NOT NULL CONSTRAINT DF_harmonised_releases_downloaded_at DEFAULT(SYSDATETIME());",
                          "sqlite": "ALTER TABLE harmonised_releases ADD COLUMN downloaded_at DATETIME;"},
    }
    dialect = engine.dialect.name
    with engine.begin() as conn:
        for column, statements in ddl.items():
            if column not in existing and dialect in statements:
                conn.execute(text(statements[dialect]))
