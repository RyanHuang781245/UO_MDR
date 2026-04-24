from __future__ import annotations

from sqlalchemy import func, inspect, text
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.auth import commit_session

REGULATION_SYNC_SOURCE_KEY = "regulation_eu_2017_745"
_UNSET = object()


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    email_batch_notify_enabled = db.Column(db.Boolean, nullable=False, server_default="0")
    nas_max_copy_file_size_mb = db.Column(db.Integer, nullable=True)
    regulation_download_page_url = db.Column(db.Text, nullable=True)
    regulation_download_link_text = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class RegulationSyncState(db.Model):
    __tablename__ = "regulation_sync_states"

    id = db.Column(db.Integer, primary_key=True)
    source_key = db.Column(db.String(120), nullable=False, unique=True)
    last_filename = db.Column(db.String(255), nullable=True)
    last_uuid = db.Column(db.String(255), nullable=True)
    last_url = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


def get_regulation_sync_state(source_key: str = REGULATION_SYNC_SOURCE_KEY) -> RegulationSyncState | None:
    return RegulationSyncState.query.filter_by(source_key=source_key).first()


def upsert_regulation_sync_state(
    source_key: str = REGULATION_SYNC_SOURCE_KEY,
    *,
    last_filename=_UNSET,
    last_uuid=_UNSET,
    last_url=_UNSET,
) -> RegulationSyncState:
    def _apply(record: RegulationSyncState) -> None:
        if last_filename is not _UNSET:
            record.last_filename = last_filename
        if last_uuid is not _UNSET:
            record.last_uuid = last_uuid
        if last_url is not _UNSET:
            record.last_url = last_url

    record = get_regulation_sync_state(source_key)
    if not record:
        record = RegulationSyncState(source_key=source_key)
        db.session.add(record)
    _apply(record)
    caught_error: IntegrityError | None = None
    try:
        commit_session()
        return record
    except IntegrityError as exc:
        db.session.rollback()
        caught_error = exc

    record = get_regulation_sync_state(source_key)
    if not record:
        raise caught_error
    _apply(record)
    commit_session()
    return record


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "system_settings" not in tables:
        return
    if "regulation_sync_states" not in tables:
        db.create_all()

    existing_columns = {col["name"].lower() for col in inspector.get_columns("system_settings")}
    if engine.dialect.name == "mssql":
        with engine.begin() as conn:
            if "email_batch_notify_enabled" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE system_settings ADD email_batch_notify_enabled BIT NOT NULL DEFAULT(0);")
                )
            if "nas_max_copy_file_size_mb" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD nas_max_copy_file_size_mb INT NULL;"))
            if "regulation_download_page_url" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD regulation_download_page_url NVARCHAR(MAX) NULL;"))
            if "regulation_download_link_text" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD regulation_download_link_text NVARCHAR(255) NULL;"))
            if "updated_at" not in existing_columns:
                conn.execute(
                    text(
                        """
                        ALTER TABLE system_settings
                        ADD updated_at DATETIME2 NOT NULL
                        CONSTRAINT DF_system_settings_updated_at DEFAULT(SYSDATETIME());
                        """
                    )
                )
    elif engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            if "email_batch_notify_enabled" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD COLUMN email_batch_notify_enabled BOOLEAN;"))
            if "nas_max_copy_file_size_mb" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD COLUMN nas_max_copy_file_size_mb INTEGER;"))
            if "regulation_download_page_url" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD COLUMN regulation_download_page_url TEXT;"))
            if "regulation_download_link_text" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD COLUMN regulation_download_link_text TEXT;"))
            if "updated_at" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD COLUMN updated_at DATETIME;"))


def ensure_default_settings() -> None:
    existing = SystemSetting.query.order_by(SystemSetting.id).first()
    if existing:
        return
    db.session.add(
        SystemSetting(
            id=1,
            email_batch_notify_enabled=False,
            nas_max_copy_file_size_mb=None,
            regulation_download_page_url=None,
            regulation_download_link_text=None,
        )
    )
    commit_session()


def ensure_default_regulation_sync_state() -> None:
    upsert_regulation_sync_state()
