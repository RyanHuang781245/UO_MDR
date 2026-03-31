from __future__ import annotations

import json
from datetime import datetime
from io import BytesIO
from typing import Optional

from flask import abort, current_app, flash, jsonify, redirect, request, send_file, url_for
from flask_admin import Admin, AdminIndexView, BaseView, expose
from flask_admin.contrib.sqla import ModelView
from flask_login import current_user
from markupsafe import Markup
from wtforms import SelectField

from app.models.auth import (
    LDAPProfile,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_LABELS_ZH,
    AuditLog,
    Role,
    User,
    UserRole,
    commit_session,
    count_admins,
    db,
    get_user_by_work_id,
    upsert_user_role,
    user_has_role,
)
from app.models.settings import SystemSetting
from app.services.authn_service import search_ad_users
from app.services.authz_service import sanitize_next_url, user_is_admin
from app.services.task_service import list_tasks
from app.utils import TAIWAN_TZ, format_tw_datetime

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


def _format_role_column(view, context, model, name):
    role = model.role_name
    if not role:
        return ""
    label = ROLE_LABELS_ZH.get(role, role)
    if role == ROLE_ADMIN:
        return Markup(f'<span class="badge badge-danger">{label}</span>')
    if role == ROLE_EDITOR:
        return Markup(f'<span class="badge badge-success">{label}</span>')
    return Markup(f'<span class="badge badge-secondary">{label}</span>')


def _format_active_column(view, context, model, name):
    if model.active:
        return Markup('<span class="badge badge-success">啟用</span>')
    return Markup('<span class="badge badge-secondary">停用</span>')


class UserAdminView(SecureModelView):
    can_create = False
    can_delete = True
    can_edit = True
    column_list = (
        "work_id",
        "display_name",
        "email",
        "active",
        "created_at",
        "last_login_at",
        "role_name",
    )
    column_labels = {
        "work_id": "工號",
        "display_name": "顯示名稱",
        "email": "Email",
        "active": "狀態",
        "created_at": "建立時間",
        "last_login_at": "最後登入",
        "role_name": "角色",
    }
    form_extra_fields = {"role": SelectField("角色", coerce=int)}
    form_columns = ("work_id", "display_name", "active", "role")
    form_widget_args = {
        "work_id": {"readonly": True},
        "display_name": {"readonly": True},
    }
    column_formatters = {
        "last_login_at": lambda _view, _context, model, _name: format_tw_datetime(model.last_login_at),
        "created_at": lambda _view, _context, model, _name: format_tw_datetime(model.created_at, assume_tz=TAIWAN_TZ),
        "role_name": _format_role_column,
        "active": _format_active_column,
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
            if str(model.id) == str(current_user.id) and not form.active.data:
                flash("無法停用自己的帳號", "error")
                return False
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
            is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

            def _error(message: str, status: int = 400):
                if is_ajax:
                    return jsonify({"ok": False, "error": message}), status
                flash(message, "danger")
                return redirect(url_for("ad_search.index", q=query))

            if not work_id:
                return _error("缺少工號", 400)

            role = Role.query.filter_by(name=role_name).first()
            if not role:
                return _error("角色不存在", 400)
            try:
                profile = LDAPProfile(
                    work_id=work_id,
                    display_name=display_name or None,
                    email=email or None,
                )
                user = get_user_by_work_id(work_id)
                if not user:
                    from app.models.auth import sync_user_from_ldap

                    user = sync_user_from_ldap(profile)
                else:
                    user.display_name = profile.display_name or user.display_name
                    user.email = profile.email or user.email
                upsert_user_role(user, role)
                commit_session()
            except Exception as exc:
                db.session.rollback()
                if is_ajax:
                    return jsonify({"ok": False, "error": str(exc)}), 500
                flash(str(exc), "danger")
                return redirect(url_for("ad_search.index", q=query))

            if is_ajax:
                return jsonify({"ok": True, "role_name": role.name, "message": "已加入/更新使用者"})
            flash("已加入/更新使用者", "success")
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


class SystemSettingView(BaseView):
    extra_css = ADMIN_CUSTOM_CSS

    def is_accessible(self):
        return user_is_admin(current_user)

    def inaccessible_callback(self, name, **kwargs):
        if current_user.is_authenticated:
            abort(403)
        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))

    @expose("/", methods=["GET", "POST"])
    def index(self):
        setting = SystemSetting.query.first()
        if not setting:
            setting = SystemSetting()
            db.session.add(setting)
            db.session.commit()

        if request.method == "POST":
            try:
                setting.email_batch_notify_enabled = request.form.get("email_batch_notify_enabled") == "on"
                nas_limit = request.form.get("nas_max_copy_file_size_mb")
                if nas_limit and nas_limit.strip():
                    setting.nas_max_copy_file_size_mb = int(nas_limit)
                else:
                    setting.nas_max_copy_file_size_mb = None

                commit_session()
                flash("系統設定已更新", "success")
            except ValueError:
                flash("數值格式錯誤", "danger")
            except Exception as exc:
                db.session.rollback()
                flash(f"更新失敗: {str(exc)}", "danger")
            return redirect(url_for("system_settings.index"))

        last_updated = format_tw_datetime(setting.updated_at, assume_tz=TAIWAN_TZ) if setting.updated_at else "-"
        return self.render("admin/system_settings.html", setting=setting, last_updated=last_updated)


class AuditLogView(BaseView):
    extra_css = ADMIN_CUSTOM_CSS

    def is_accessible(self):
        return user_is_admin(current_user)

    def inaccessible_callback(self, name, **kwargs):
        if current_user.is_authenticated:
            abort(403)
        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))

    def _get_db_logs(
        self,
        task_id: Optional[str] = None,
        page: int = 1,
        per_page: int = 50,
        q: Optional[str] = None,
        action: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> tuple[list[dict], dict]:
        query = AuditLog.query

        if task_id:
            query = query.filter_by(task_id=task_id)
        if action:
            query = query.filter(AuditLog.action.ilike(f"%{action}%"))
        if q:
            search = f"%{q}%"
            query = query.filter(
                (AuditLog.action.ilike(search))
                | (AuditLog.work_id.ilike(search))
                | (AuditLog.detail.ilike(search))
            )
        if start_date:
            try:
                dt_start = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
                query = query.filter(AuditLog.created_at >= dt_start)
            except ValueError:
                pass
        if end_date:
            try:
                dt_end = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
                query = query.filter(AuditLog.created_at <= dt_end)
            except ValueError:
                pass

        total_count = query.count()
        total_pages = (total_count + per_page - 1) // per_page
        page = max(1, min(page, total_pages)) if total_pages > 0 else 1

        logs = query.order_by(AuditLog.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()

        entries = []
        for log in logs:
            try:
                detail = json.loads(log.detail) if log.detail else {}
            except Exception:
                detail = {"raw": log.detail}

            entries.append(
                {
                    "ts": log.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "action": log.action,
                    "actor": {"work_id": log.work_id},
                    "detail": detail,
                    "task_id": log.task_id,
                }
            )

        pagination = {
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        }
        return entries, pagination

    @expose("/", methods=["GET"])
    def index(self):
        tasks = list_tasks()
        task_id = (request.args.get("task_id") or "").strip()
        q = (request.args.get("q") or "").strip()
        action = (request.args.get("action") or "").strip()
        start_date = (request.args.get("start_date") or "").strip()
        end_date = (request.args.get("end_date") or "").strip()

        try:
            page = int(request.args.get("page", 1))
        except (ValueError, TypeError):
            page = 1

        entries, pagination = self._get_db_logs(
            task_id=task_id if task_id else None,
            page=page,
            q=q if q else None,
            action=action if action else None,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else None,
        )

        return self.render(
            "admin/audit_logs.html",
            tasks=tasks,
            task_id=task_id,
            q=q,
            action=action,
            start_date=start_date,
            end_date=end_date,
            entries=entries,
            pagination=pagination,
            has_file=True,
        )

    @expose("/download", methods=["GET"])
    def download(self):
        task_id = (request.args.get("task_id") or "").strip()
        q = (request.args.get("q") or "").strip()
        action = (request.args.get("action") or "").strip()
        start_date = (request.args.get("start_date") or "").strip()
        end_date = (request.args.get("end_date") or "").strip()

        entries, _pagination = self._get_db_logs(
            task_id=task_id if task_id else None,
            per_page=2000,
            q=q if q else None,
            action=action if action else None,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else None,
        )

        output = BytesIO()
        for entry in reversed(entries):
            line = json.dumps(entry, ensure_ascii=False) + "\n"
            output.write(line.encode("utf-8"))

        output.seek(0)
        filename = f"audit_{task_id if task_id else 'global'}.jsonl"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/x-jsonlines",
        )


def init_admin(app) -> Admin:
    admin = Admin(app, name="系統管理", url="/admin", index_view=SecureAdminIndexView())
    admin.add_view(SystemSettingView(name="系統設定", endpoint="system_settings", url="system-settings"))
    admin.add_view(UserAdminView(User, db.session, name="使用者列表"))
    admin.add_view(ADSearchView(name="帳號搜尋", endpoint="ad_search", url="ad-search"))
    admin.add_view(AuditLogView(name="操作紀錄", endpoint="audit_logs", url="audit-logs"))
    return admin
