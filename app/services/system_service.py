from __future__ import annotations

from app.extensions import db
from app.models.settings import ensure_default_regulation_sync_state, ensure_default_settings, ensure_schema
from app.services.audit_service import record_system_error
from app.services.schema_control import auto_schema_management_enabled, tables_exist


def init_system_settings(app) -> None:
    with app.app_context():
        try:
            if auto_schema_management_enabled(app):
                ensure_schema()
            elif not tables_exist("system_settings", "regulation_sync_states"):
                app.logger.info("Skipping system settings schema bootstrap because AUTO_SCHEMA_MANAGEMENT is disabled.")
                return
            ensure_default_settings()
            ensure_default_regulation_sync_state()
        except Exception as exc:
            db.session.rollback()
            record_system_error(
                "system_settings.init",
                "System settings initialization failed",
                exc=exc,
            )
            app.logger.exception("System settings initialization failed")
