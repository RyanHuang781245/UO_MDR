from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from flask import abort, current_app, flash, redirect, request, url_for
from flask_admin import Admin, AdminIndexView, BaseView, expose
from flask_admin.contrib.sqla import ModelView
from flask_login import current_user
from wtforms import SelectField
from ldap3 import BASE, SUBTREE, Connection, Server
from ldap3.utils.conv import escape_filter_chars

from app.extensions import ldap_manager, login_manager
from app.utils import TAIWAN_TZ, format_tw_datetime
from modules.auth_models import (
    LDAPProfile,
    PERM_USER_MANAGE,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_LABELS_ZH,
    Role,
    User,
    UserRole,
    commit_session,
    count_admins,
    db,
    ensure_schema,
    get_user_by_id,
    get_user_by_work_id,
    get_user_role_names,
    seed_roles,
    sync_user_from_ldap,
    upsert_user_role,
    user_has_role,
)
from modules.settings_models import SystemSetting


@dataclass(frozen=True)
class LDAPUserInfo:
    dn: str
    work_id: str
    data: dict
    memberships: list

    @property
    def username(self) -> str:
        return self.work_id


def register_ldap_handlers() -> None:
    @ldap_manager.save_user
    def save_ldap_user(dn, username, data, memberships):
        return LDAPUserInfo(dn=dn, work_id=username, data=data or {}, memberships=memberships or [])


    @login_manager.user_loader
    def load_user(user_id: str) -> Optional[User]:
        try:
            return get_user_by_id(int(user_id))
        except Exception:
            return None


def sanitize_next_url(raw_next: Optional[str]) -> Optional[str]:
    if not raw_next:
        return None
    candidate = raw_next.strip()
    if candidate.endswith("?"):
        candidate = candidate[:-1]
    if not candidate.startswith("/") or candidate.startswith("//"):
        return None
    parsed = urlparse(candidate)
    if parsed.scheme or parsed.netloc:
        return None
    return candidate


def user_has_permission(user_id: int, permission_name: str) -> bool:
    if permission_name == PERM_USER_MANAGE:
        return user_has_role(user_id, ROLE_ADMIN)
    return False


def user_is_admin(user: User) -> bool:
    return bool(user and user.is_authenticated and user_has_role(user.id, ROLE_ADMIN))


def _normalize_ldap_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _get_ldap_search_config() -> dict:
    host = current_app.config.get("LDAP_HOST")
    base_dn = current_app.config.get("LDAP_BASE_DN")
    bind_dn = current_app.config.get("LDAP_BIND_USER_DN")
    bind_pw = current_app.config.get("LDAP_BIND_USER_PASSWORD")
    login_attr = current_app.config.get("LDAP_USER_LOGIN_ATTR", "sAMAccountName")
    obj_filter = current_app.config.get(
        "LDAP_USER_OBJECT_FILTER", "(&(objectClass=user)(!(objectClass=computer)))"
    )
    scope = current_app.config.get("LDAP_USER_SEARCH_SCOPE")

    if not host or not base_dn or not bind_dn or not bind_pw:
        raise ValueError("LDAP search configuration is missing")

    return {
        "host": host,
        "base_dn": base_dn,
        "bind_dn": bind_dn,
        "bind_pw": bind_pw,
        "login_attr": login_attr,
        "obj_filter": obj_filter,
        "scope": scope,
    }


def search_ad_users(keyword: str) -> list[dict]:
    keyword = (keyword or "").strip()
    if not keyword:
        return []

    cfg = _get_ldap_search_config()
    escaped = escape_filter_chars(keyword)
    pattern = f"*{escaped}*"
    login_attr = cfg["login_attr"]
    search_filter = (
        f"(&{cfg['obj_filter']}(|({login_attr}={pattern})"
        f"(displayName={pattern})(mail={pattern})))"
    )
    attributes = [login_attr, "displayName", "mail", "distinguishedName"]

    server = Server(cfg["host"])
    conn = Connection(server, user=cfg["bind_dn"], password=cfg["bind_pw"], auto_bind=True)
    try:
        conn.search(
            search_base=cfg["base_dn"],
            search_filter=search_filter,
            search_scope=cfg["scope"] or SUBTREE,
            attributes=attributes,
        )
        results = []
        for entry in conn.entries:
            data = entry.entry_attributes_as_dict
            work_id = _normalize_ldap_value(data.get(login_attr))
            if not work_id:
                continue
            results.append(
                {
                    "work_id": work_id,
                    "display_name": _normalize_ldap_value(data.get("displayName")),
                    "email": _normalize_ldap_value(data.get("mail")),
                    "dn": entry.entry_dn,
                }
            )
        return results
    finally:
        conn.unbind()


def build_ldap_profile(ldap_user: LDAPUserInfo) -> LDAPProfile:
    data = ldap_user.data or {}
    display_name = _normalize_ldap_value(
        data.get("displayName")
        or data.get("cn")
        or data.get("name")
        or data.get("givenName")
    )
    email = _normalize_ldap_value(data.get("mail"))
    return LDAPProfile(work_id=ldap_user.work_id, display_name=display_name, email=email)


def is_allowed_group_member(user_dn: str) -> bool:
    if not current_app.config.get("LDAP_GROUP_GATE_ENABLED", True):
        return True
    allowed_group_dn = current_app.config.get("ALLOWED_GROUP_DN")
    if not allowed_group_dn:
        raise ValueError("ALLOWED_GROUP_DN is not configured")
    host = current_app.config.get("LDAP_HOST")
    bind_dn = current_app.config.get("LDAP_BIND_USER_DN")
    bind_pw = current_app.config.get("LDAP_BIND_USER_PASSWORD")
    if not host or not bind_dn or not bind_pw:
        raise ValueError("LDAP bind configuration is missing")

    server = Server(host)
    conn = Connection(server, user=bind_dn, password=bind_pw, auto_bind=True)
    try:
        escaped_user_dn = escape_filter_chars(user_dn)
        search_filter = (
            "(&(objectClass=group)(member:1.2.840.113556.1.4.1941:="
            + escaped_user_dn
            + "))"
        )
        conn.search(
            search_base=allowed_group_dn,
            search_filter=search_filter,
            search_scope=BASE,
            attributes=["distinguishedName"],
        )
        return bool(conn.entries)
    finally:
        conn.unbind()


def bootstrap_admins() -> None:
    raw = os.environ.get("BOOTSTRAP_ADMIN", "")
    work_ids = [entry.strip() for entry in raw.split(",") if entry.strip()]
    if not work_ids:
        return

    admin_role = Role.query.filter_by(name=ROLE_ADMIN).first()
    if not admin_role:
        admin_role = Role(name=ROLE_ADMIN)
        db.session.add(admin_role)
        db.session.flush()

    for work_id in work_ids:
        user = User.query.filter_by(work_id=work_id).first()
        if not user:
            user = User(work_id=work_id, active=True)
            db.session.add(user)
            db.session.flush()
        upsert_user_role(user, admin_role)

    commit_session()


ADMIN_CUSTOM_CSS = ["/static/admin-custom.css"]


class SecureAdminIndexView(AdminIndexView):
    extra_css = ADMIN_CUSTOM_CSS
    def is_accessible(self):
        return user_is_admin(current_user)

    def inaccessible_callback(self, name, **kwargs):
        if current_user.is_authenticated:
            abort(403)
        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))


class SecureModelView(ModelView):
    extra_css = ADMIN_CUSTOM_CSS
    def is_accessible(self):
        return user_is_admin(current_user)

    def inaccessible_callback(self, name, **kwargs):
        if current_user.is_authenticated:
            abort(403)
        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))


class UserAdminView(SecureModelView):
    can_create = False
    can_delete = True
    can_edit = True
    column_list = (
        # "id",
        "work_id",
        "display_name",
        "email",
        "active",
        "created_at",
        "last_login_at",
        "role_name",
    )
    column_labels = {
        # "id": "編號",
        "work_id": "工號",
        "display_name": "顯示名稱",
        "email": "Email",
        "active": "狀態",
        "created_at": "建立時間",
        "last_login_at": "最後登入",
        "role_name": "角色",
    }
    form_extra_fields = {"role": SelectField("角色", coerce=int)}
    form_columns = ("active", "role")
    column_formatters = {
        "last_login_at": lambda _view, _context, model, _name: format_tw_datetime(model.last_login_at),
        "created_at": lambda _view, _context, model, _name: format_tw_datetime(model.created_at, assume_tz=TAIWAN_TZ),
    }

    def _load_role_choices(self):
        roles = Role.query.order_by(Role.name).all()
        return [(role.id, role.name) for role in roles]

    def create_form(self, obj=None):
        form = super().create_form(obj)
        form.role.choices = self._load_role_choices()
        return form

    def edit_form(self, obj=None):
        form = super().edit_form(obj)
        form.role.choices = self._load_role_choices()
        # if obj and obj.user_role:
        if obj and obj.user_role and not form.role.raw_data:
            form.role.data = obj.user_role.role_id
        return form

    def _is_last_admin_change(self, user: User, new_role: Role) -> bool:
        admin_role = Role.query.filter_by(name=ROLE_ADMIN).first()
        if not admin_role or not user:
            return False
        if new_role and new_role.id == admin_role.id:
            return False
        if not user_has_role(user.id, ROLE_ADMIN):
            return False
        return count_admins() <= 1

    def update_model(self, form, model):
        try:
            model.active = bool(form.active.data)
            role_id = form.role.data
            role = Role.query.get(role_id) if role_id else None
            if not role:
                flash("角色不存在", "danger")
                return False
            if self._is_last_admin_change(model, role):
                flash("Cannot remove the last admin.", "danger")
                return False
            upsert_user_role(model, role)
            commit_session()
            return True
        except Exception as exc:
            db.session.rollback()
            flash(str(exc), "danger")
            return False


class UserRoleAdminView(SecureModelView):
    can_create = False
    can_delete = False
    column_list = ("user", "role")
    column_labels = {
        "user": "使用者",
        "role": "角色",
    }
    form_columns = ("role",)

    def _is_last_admin_change(self, user_id: int, new_role_id: Optional[int], deleting: bool) -> bool:
        admin_role = Role.query.filter_by(name=ROLE_ADMIN).first()
        if not admin_role:
            return False
        if count_admins() > 1:
            return False
        current = UserRole.query.filter_by(user_id=user_id, role_id=admin_role.id).first()
        if not current:
            return False
        if deleting:
            return True
        return new_role_id is not None and new_role_id != admin_role.id

    def create_model(self, form):
        try:
            user = form.user.data
            role = form.role.data
            existing = UserRole.query.filter_by(user_id=user.id).first()
            if existing:
                if self._is_last_admin_change(user.id, role.id, deleting=False):
                    flash("Cannot remove the last admin.", "danger")
                    return False
                existing.role_id = role.id
            else:
                db.session.add(UserRole(user_id=user.id, role_id=role.id))
            commit_session()
            return True
        except Exception as exc:
            db.session.rollback()
            flash(str(exc), "danger")
            return False

    def update_model(self, form, model):
        try:
            new_role = form.role.data
            if self._is_last_admin_change(model.user_id, new_role.id, deleting=False):
                flash("Cannot remove the last admin.", "danger")
                return False
            model.role_id = new_role.id
            commit_session()
            return True
        except Exception as exc:
            db.session.rollback()
            flash(str(exc), "danger")
            return False

    def delete_model(self, model):
        try:
            if self._is_last_admin_change(model.user_id, model.role_id, deleting=True):
                flash("Cannot remove the last admin.", "danger")
                return False
            db.session.delete(model)
            commit_session()
            return True
        except Exception as exc:
            db.session.rollback()
            flash(str(exc), "danger")
            return False


class ADSearchView(BaseView):
    extra_css = ADMIN_CUSTOM_CSS
    def is_accessible(self):
        return user_is_admin(current_user)

    def inaccessible_callback(self, name, **kwargs):
        if current_user.is_authenticated:
            abort(403)
        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))

    @expose("/", methods=["GET", "POST"])
    def index(self):
        error = ""
        results = []
        roles = Role.query.order_by(Role.name).all()

        if request.method == "POST":
            work_id = (request.form.get("work_id") or "").strip()
            display_name = (request.form.get("display_name") or "").strip()
            email = (request.form.get("email") or "").strip()
            role_name = (request.form.get("role") or "").strip()
            query = (request.form.get("q") or "").strip()

            if not work_id:
                flash("缺少工號", "danger")
                return redirect(url_for("ad_search.index", q=query))

            role = Role.query.filter_by(name=role_name).first()
            if not role:
                flash("角色不存在", "danger")
                return redirect(url_for("ad_search.index", q=query))

            profile = LDAPProfile(
                work_id=work_id,
                display_name=display_name or None,
                email=email or None,
            )
            user = sync_user_from_ldap(profile)
            upsert_user_role(user, role)
            try:
                commit_session()
                flash("已加入/更新使用者", "success")
            except Exception as exc:
                db.session.rollback()
                flash(str(exc), "danger")

            return redirect(url_for("ad_search.index", q=query))

        query = (request.args.get("q") or "").strip()
        if query:
            try:
                results = search_ad_users(query)
            except Exception as exc:
                current_app.logger.exception("AD search failed")
                error = str(exc)

        for item in results:
            existing = get_user_by_work_id(item["work_id"])
            item["exists"] = bool(existing)
            item["role_name"] = existing.role_name if existing else None

        return self.render(
            "admin/ad_search.html",
            query=query,
            results=results,
            roles=roles,
            error=error,
        )


class SystemSettingAdminView(SecureModelView):
    can_create = False
    can_delete = False
    can_edit = True
    column_list = ("email_batch_notify_enabled", "updated_at")
    column_labels = {
        "email_batch_notify_enabled": "批次完成通知 Email",
        "updated_at": "更新時間",
    }
    form_columns = ("email_batch_notify_enabled",)
    column_formatters = {
        "updated_at": lambda _view, _context, model, _name: format_tw_datetime(model.updated_at, assume_tz=TAIWAN_TZ),
    }


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

        return {
            "auth_enabled": app.config.get("AUTH_ENABLED", True),
            "current_user": current_user if current_user.is_authenticated else None,
            "current_user_roles": get_user_role_names(current_user.id) if current_user.is_authenticated else [],
            "has_permission": _has_perm,
            "role_labels": _role_labels,
            "ROLE_ADMIN": ROLE_ADMIN,
            "ROLE_EDITOR": ROLE_EDITOR,
            "PERM_USER_MANAGE": PERM_USER_MANAGE,
        }


def register_login_enforcement(app) -> None:
    @app.before_request
    def enforce_login():
        if request.is_secure or request.headers.get("X-Forwarded-Proto", "").lower() == "https":
            app.config["SESSION_COOKIE_SECURE"] = True

        if not app.config.get("AUTH_ENABLED", True):
            return

        public_endpoints = {"auth_bp.login", "auth_bp.logout", "static"}
        if request.endpoint in public_endpoints or request.endpoint is None:
            return

        if current_user.is_authenticated:
            return

        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))


def init_admin(app) -> Admin:
    admin = Admin(app, name="系統管理", url="/admin", index_view=SecureAdminIndexView())
    admin.add_view(UserAdminView(User, db.session, name="使用者列表"))
    admin.add_view(ADSearchView(name="帳號搜尋", endpoint="ad_search", url="ad-search"))
    admin.add_view(SystemSettingAdminView(SystemSetting, db.session, name="系統設定"))
    return admin


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
