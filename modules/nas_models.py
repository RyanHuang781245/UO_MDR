from __future__ import annotations

from sqlalchemy import func, inspect, text

from modules.auth_models import db


class NasRoot(db.Model):
    __tablename__ = "nas_roots"

    id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.String(500), nullable=False)
    env = db.Column(db.String(50), nullable=False, server_default="development")
    platform = db.Column(db.String(50), nullable=False, server_default="windows")
    active = db.Column(db.Boolean, nullable=False, server_default="1")
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())

    __table_args__ = (db.UniqueConstraint("env", "platform", "path", name="uq_nas_roots_env_platform_path"),)

    def __str__(self) -> str:
        return self.path


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    if "nas_roots" not in set(inspector.get_table_names()):
        return

    existing_columns = {col["name"].lower() for col in inspector.get_columns("nas_roots")}
    with engine.begin() as conn:
        if engine.dialect.name == "mssql":
            conn.execute(
                text(
                    """
                    IF EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE name = 'ix_nas_roots_path'
                          AND object_id = OBJECT_ID('nas_roots')
                    )
                    BEGIN
                        DROP INDEX ix_nas_roots_path ON nas_roots;
                    END
                    """
                )
            )
            conn.execute(
                text(
                    """
                    IF EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE name = 'uq_nas_roots_path'
                          AND object_id = OBJECT_ID('nas_roots')
                    )
                    BEGIN
                        DROP INDEX uq_nas_roots_path ON nas_roots;
                    END
                    """
                )
            )
            if "env" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD env NVARCHAR(50) NOT NULL CONSTRAINT DF_nas_roots_env DEFAULT('development');"))
            if "platform" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD platform NVARCHAR(50) NOT NULL CONSTRAINT DF_nas_roots_platform DEFAULT('windows');"))
            if "active" not in existing_columns:
                conn.execute(
                    text(
                        """
                        ALTER TABLE nas_roots
                        ADD active BIT NOT NULL
                        CONSTRAINT DF_nas_roots_active DEFAULT(1);
                        """
                    )
                )
            conn.execute(text("UPDATE nas_roots SET active = 1 WHERE active IS NULL;"))
            if "created_at" not in existing_columns:
                conn.execute(
                    text(
                        """
                        ALTER TABLE nas_roots
                        ADD created_at DATETIME2 NOT NULL
                        CONSTRAINT DF_nas_roots_created_at DEFAULT(SYSDATETIME());
                        """
                    )
                )
            conn.execute(
                text(
                    """
                    IF NOT EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE name = 'uq_nas_roots_env_platform_path'
                          AND object_id = OBJECT_ID('nas_roots')
                    )
                    BEGIN
                        CREATE UNIQUE INDEX uq_nas_roots_env_platform_path
                        ON nas_roots(env, platform, path);
                    END
                    """
                )
            )
        elif engine.dialect.name == "sqlite":
            if "env" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD COLUMN env VARCHAR(50);"))
            if "platform" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD COLUMN platform VARCHAR(50);"))
            if "active" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD COLUMN active BOOLEAN;"))
            conn.execute(text("UPDATE nas_roots SET active = 1 WHERE active IS NULL;"))
            if "created_at" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD COLUMN created_at DATETIME;"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_nas_roots_env_platform_path ON nas_roots(env, platform, path);"
                )
            )
