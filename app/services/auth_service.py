from __future__ import annotations

from app.extensions import login_manager
from app.models.auth import db, ensure_schema, seed_roles
from app.services.auth_admin_service import init_admin
from app.services.auth_hooks_service import register_auth_context, register_login_enforcement
from app.services.authn_service import (
    LDAPUserInfo,
    bootstrap_admins,
    build_ldap_profile,
    is_allowed_group_member,
    register_ldap_handlers,
    search_ad_users,
)
from app.services.authz_service import sanitize_next_url, user_has_permission, user_is_admin


def bootstrap_auth(app) -> None:
    with app.app_context():
        try:
            ensure_schema()
            seed_roles()
            bootstrap_admins()
        except Exception:
            db.session.rollback()
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
    "bootstrap_auth",
    "build_ldap_profile",
    "init_admin",
    "init_auth",
    "is_allowed_group_member",
    "register_auth_context",
    "register_ldap_handlers",
    "register_login_enforcement",
    "sanitize_next_url",
    "search_ad_users",
    "user_has_permission",
    "user_is_admin",
]
