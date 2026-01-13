from __future__ import annotations

from datetime import datetime

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for
from flask_ldap3_login.forms import LDAPLoginForm
from flask_login import current_user, login_user, logout_user

from app.services.auth_service import build_ldap_profile, is_allowed_group_member, sanitize_next_url
from modules.auth_models import Role, ROLE_EDITOR, commit_session, db, sync_user_from_ldap, upsert_user_role

auth_bp = Blueprint("auth_bp", __name__, template_folder="templates")


@auth_bp.route("/login", methods=["GET", "POST"], endpoint="login")
def login():
    if not current_app.config.get("AUTH_ENABLED", True):
        return redirect(url_for("tasks"))

    if current_user.is_authenticated:
        return redirect(url_for("tasks"))

    error = ""
    form = LDAPLoginForm()
    if request.method == "POST":
        try:
            if not form.validate_on_submit():
                current_app.logger.debug("LDAP form errors: %s", form.errors)
                error = "Invalid credentials."
            else:
                ldap_user = form.user
                if not ldap_user:
                    error = "Invalid credentials."
                else:
                    if not is_allowed_group_member(ldap_user.dn):
                        error = "Your account is not in the allowed login group."
                    else:
                        profile = build_ldap_profile(ldap_user)
                        user = sync_user_from_ldap(profile)
                        if not user.user_role:
                            editor_role = Role.query.filter_by(name=ROLE_EDITOR).first()
                            if not editor_role:
                                raise ValueError("Role editor is missing")
                            upsert_user_role(user, editor_role)
                        if not user.is_active:
                            db.session.rollback()
                            error = "Your account is disabled."
                        else:
                            user.last_login_at = datetime.utcnow()
                            commit_session()
                            login_user(user)
                            next_url = sanitize_next_url(request.args.get("next"))
                            return redirect(next_url or url_for("tasks"))
        except Exception:
            db.session.rollback()
            current_app.logger.exception("Login failed")
            error = "Login failed. Please contact the administrator."

    return render_template("auth/login.html", error=error, form=form)


@auth_bp.get("/logout", endpoint="logout")
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("login"))
