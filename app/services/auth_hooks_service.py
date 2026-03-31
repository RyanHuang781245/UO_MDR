from __future__ import annotations

from typing import Optional

from flask import current_app, redirect, request, url_for
from flask_login import current_user

from app.models.auth import (
    PERM_USER_MANAGE,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_LABELS_ZH,
    get_user_role_names,
)
from app.services.authz_service import sanitize_next_url, user_has_permission


def register_auth_context(app) -> None:
    @app.context_processor
    def inject_auth_context():
        def _has_perm(perm: str) -> bool:
            if not app.config.get("AUTH_ENABLED", True):
                return True
            if not current_user.is_authenticated:
                return False
            return user_has_permission(current_user.id, perm)

        def _role_labels(role_names: list[str]) -> str:
            return ", ".join(ROLE_LABELS_ZH.get(name, name) for name in role_names)

        def _extract_chinese_name(display_name: Optional[str]) -> Optional[str]:
            if not display_name:
                return None
            chinese_text = "".join(c for c in display_name if ord(c) > 127)
            return chinese_text if chinese_text else display_name

        return {
            "auth_enabled": app.config.get("AUTH_ENABLED", True),
            "current_user": current_user if current_user.is_authenticated else None,
            "current_user_roles": get_user_role_names(current_user.id) if current_user.is_authenticated else [],
            "has_permission": _has_perm,
            "role_labels": _role_labels,
            "extract_chinese_name": _extract_chinese_name,
            "ROLE_ADMIN": ROLE_ADMIN,
            "ROLE_EDITOR": ROLE_EDITOR,
            "PERM_USER_MANAGE": PERM_USER_MANAGE,
        }


def register_login_enforcement(app) -> None:
    @app.before_request
    def enforce_login():
        if request.is_secure or request.headers.get("X-Forwarded-Proto", "").lower() == "https":
            current_app.config["SESSION_COOKIE_SECURE"] = True

        if not app.config.get("AUTH_ENABLED", True):
            return

        public_endpoints = {"auth_bp.login", "auth_bp.logout", "static"}
        if request.endpoint in public_endpoints or request.endpoint is None:
            return

        if current_user.is_authenticated:
            return

        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))
