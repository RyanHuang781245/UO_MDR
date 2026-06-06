from __future__ import annotations

from app.extensions import login_manager
from app.models.auth import db, ensure_schema, seed_roles
from app.services.audit_service import record_system_error
from app.services.auth_admin_service import init_admin
from app.services.auth_hooks_service import register_auth_context, register_login_enforcement
from app.services.schema_control import auto_schema_management_enabled, tables_exist
from app.services.authn_service import (
    LDAPUserInfo,
    apply_default_local_password,
    authenticate_local_user,
    bootstrap_admins,
    build_ldap_profile,
    get_auth_mode,
    is_allowed_group_member,
    register_ldap_handlers,
    search_ad_users,
    search_local_users,
    set_local_password,
)
from app.services.authz_service import sanitize_next_url, user_has_permission, user_is_admin


def bootstrap_auth(app) -> None:
    with app.app_context():
        try:
            if auto_schema_management_enabled(app):
                ensure_schema()
            elif not tables_exist("users", "roles", "user_roles"):
                app.logger.info("Skipping auth schema bootstrap because AUTO_SCHEMA_MANAGEMENT is disabled.")
                return
            seed_roles()
            bootstrap_admins()
        except Exception as exc:
            db.session.rollback()
            record_system_error(
                "auth.init",
                "Auth initialization failed",
                exc=exc,
            )
            app.logger.exception("Auth initialization failed")


def init_auth(app) -> None:
    login_manager.login_view = "auth_bp.login"
    register_ldap_handlers()
    register_auth_context(app)
    register_login_enforcement(app)
    init_admin(app)
    if not app.config.get("TESTING"):
        bootstrap_auth(app)


__all__ = [
    "LDAPUserInfo",
    "apply_default_local_password",
    "authenticate_local_user",
    "bootstrap_auth",
    "build_ldap_profile",
    "get_auth_mode",
    "init_admin",
    "init_auth",
    "is_allowed_group_member",
    "register_auth_context",
    "register_ldap_handlers",
    "register_login_enforcement",
    "sanitize_next_url",
    "search_ad_users",
    "search_local_users",
    "set_local_password",
    "user_has_permission",
    "user_is_admin",
]
