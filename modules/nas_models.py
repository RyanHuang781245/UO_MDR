from __future__ import annotations

from sqlalchemy import func, inspect, text

from modules.auth_models import db


class NasRoot(db.Model):
    __tablename__ = "nas_roots"

    id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.String(500), nullable=False, unique=True)
    active = db.Column(db.Boolean, nullable=False, server_default="1")
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())

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
        elif engine.dialect.name == "sqlite":
            if "active" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD COLUMN active BOOLEAN;"))
            conn.execute(text("UPDATE nas_roots SET active = 1 WHERE active IS NULL;"))
            if "created_at" not in existing_columns:
                conn.execute(text("ALTER TABLE nas_roots ADD COLUMN created_at DATETIME;"))
