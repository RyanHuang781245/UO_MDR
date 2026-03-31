from __future__ import annotations

from app.extensions import db
from app.models.settings import ensure_default_settings, ensure_schema


def init_system_settings(app) -> None:
    with app.app_context():
        try:
            ensure_schema()
            ensure_default_settings()
        except Exception:
            db.session.rollback()
            app.logger.exception("System settings initialization failed")
