from __future__ import annotations

from datetime import datetime

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_ldap3_login.forms import LDAPLoginForm
from flask_login import current_user, login_user, logout_user

from app.models.auth import (
    ROLE_EDITOR,
    Role,
    commit_session,
    db,
    get_user_by_work_id,
    upsert_user_role,
)
from app.services.audit_service import record_audit
from app.services.auth_service import (
    build_ldap_profile,
    is_allowed_group_member,
    sanitize_next_url,
)

from .blueprint import auth_bp


def _login_actor_payload(username: str = "", *, label: str = "") -> dict[str, str]:
    normalized_username = (username or "").strip()
    normalized_label = (label or "").strip()
    return {
        "work_id": normalized_username,
        "label": normalized_label or normalized_username,
    }


@auth_bp.route("/login", methods=["GET", "POST"], endpoint="login")
def login():
    if not current_app.config.get("AUTH_ENABLED", True):
        return redirect(url_for("tasks_bp.launcher"))

    if current_user.is_authenticated:
        return redirect(url_for("tasks_bp.launcher"))

    form = LDAPLoginForm()
    error = ""

    if request.method == "POST":
        attempted_username = (request.form.get("username") or "").strip()
        try:
            if not form.validate_on_submit():
                current_app.logger.debug("LDAP form errors: %s", form.errors)
                error = "憑證無效"
                record_audit(
                    action="auth_login_failed",
                    actor=_login_actor_payload(attempted_username),
                    detail={"reason": "form_invalid", "username": attempted_username},
                )
                return render_template("auth/login.html", error=error, form=form)

            ldap_user = form.user
            if not ldap_user:
                error = "憑證無效，請確認工號和密碼是否正確"
                record_audit(
                    action="auth_login_failed",
                    actor=_login_actor_payload(attempted_username),
                    detail={"reason": "invalid_credentials", "username": attempted_username},
                )
                return render_template("auth/login.html", error=error, form=form)

            if not is_allowed_group_member(ldap_user.dn):
                error = "您的帳號不在允許的登入群組中"
                record_audit(
                    action="auth_login_failed",
                    actor=_login_actor_payload(attempted_username),
                    detail={"reason": "group_not_allowed", "username": attempted_username},
                )
                return render_template("auth/login.html", error=error, form=form)

            profile = build_ldap_profile(ldap_user)
            user = get_user_by_work_id(profile.work_id)
            if not user:
                error = "您的帳號未獲得授權"
                record_audit(
                    action="auth_login_failed",
                    actor=_login_actor_payload(profile.work_id, label=profile.display_name),
                    detail={"reason": "user_not_authorized", "username": profile.work_id},
                )
                return render_template("auth/login.html", error=error, form=form)

            if profile.display_name and user.display_name != profile.display_name:
                user.display_name = profile.display_name
            if profile.email and user.email != profile.email:
                user.email = profile.email

            if not user.user_role:
                editor_role = Role.query.filter_by(name=ROLE_EDITOR).first()
                if not editor_role:
                    raise ValueError("Role editor is missing")
                upsert_user_role(user, editor_role)

            if not user.is_active:
                db.session.rollback()
                error = "您的帳號已被停用"
                record_audit(
                    action="auth_login_failed",
                    actor=_login_actor_payload(user.work_id, label=user.display_name),
                    detail={"reason": "user_inactive", "username": user.work_id},
                )
                return render_template("auth/login.html", error=error, form=form)

            user.last_login_at = datetime.utcnow()
            commit_session()
            login_user(user)
            record_audit(
                action="auth_login",
                actor=_login_actor_payload(user.work_id, label=user.display_name),
                detail={"username": user.work_id},
            )

            next_url = sanitize_next_url(request.args.get("next"))
            return redirect(next_url or url_for("tasks_bp.launcher"))

        except Exception:
            db.session.rollback()
            current_app.logger.exception("Login failed")
            record_audit(
                action="auth_login_failed",
                actor=_login_actor_payload(attempted_username),
                detail={"reason": "exception", "username": attempted_username},
            )
            error = "登入失敗。請聯絡管理員"

    return render_template("auth/login.html", error=error, form=form)


@auth_bp.get("/logout", endpoint="logout")
def logout():
    record_audit(
        action="auth_logout",
        actor=_login_actor_payload(
            getattr(current_user, "work_id", "") or "",
            label=getattr(current_user, "display_name", "") or "",
        ),
        detail={"username": getattr(current_user, "work_id", "") or ""},
    )
    logout_user()
    flash("已登出", "info")
    return redirect(url_for("auth_bp.login"))
