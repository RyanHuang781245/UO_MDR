from __future__ import annotations

from sqlalchemy import func, inspect, text

from modules.auth_models import db, commit_session


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    email_batch_notify_enabled = db.Column(db.Boolean, nullable=False, server_default="0")
    updated_at = db.Column(db.DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    if "system_settings" not in set(inspector.get_table_names()):
        return

    existing_columns = {col["name"].lower() for col in inspector.get_columns("system_settings")}
    if engine.dialect.name == "mssql":
        with engine.begin() as conn:
            if "email_batch_notify_enabled" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE system_settings ADD email_batch_notify_enabled BIT NOT NULL DEFAULT(0);")
                )
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
            if "updated_at" not in existing_columns:
                conn.execute(text("ALTER TABLE system_settings ADD COLUMN updated_at DATETIME;"))


def ensure_default_settings() -> None:
    existing = SystemSetting.query.order_by(SystemSetting.id).first()
    if existing:
        return
    db.session.add(SystemSetting(id=1, email_batch_notify_enabled=False))
    commit_session()
