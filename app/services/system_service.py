from __future__ import annotations

from modules.auth_models import db
from modules.settings_models import ensure_default_settings, ensure_schema


def init_system_settings(app) -> None:
    with app.app_context():
        try:
            ensure_schema()
            ensure_default_settings()
        except Exception:
            db.session.rollback()
            app.logger.exception("System settings initialization failed")
