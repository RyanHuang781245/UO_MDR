from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from io import BytesIO
from typing import Optional

from flask import abort, current_app, flash, has_request_context, jsonify, redirect, request, send_file, url_for
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
    SystemErrorLog,
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
from app.services.audit_service import record_audit
from app.services.authn_service import search_ad_users
from app.services.authz_service import sanitize_next_url, user_is_admin
from app.services.execution_service import is_inline_execution_enabled
from app.services.frontend_error_service import frontend_error_message
from app.services.standard_update_service import (
    activate_harmonised_release,
    get_active_harmonised_release,
    get_latest_harmonised_release_in_dir,
    harmonised_reference_configured_root,
    harmonised_reference_fallback_root,
    harmonised_reference_root,
    harmonised_reference_storage_mode,
    harmonised_reference_status_message,
    test_harmonised_reference_storage,
)
from app.services.task_service import list_tasks
from app.services.user_context_service import get_actor_info
from app.utils import TAIWAN_TZ, format_tw_datetime

ADMIN_CUSTOM_CSS = ["/static/admin-custom.css"]

_AUDIT_ACTION_LABELS = {
    "task_create": "建立任務",
    "task_copy": "複製任務",
    "task_delete": "刪除任務",
    "task_rename": "重新命名任務",
    "task_update_description": "更新任務描述",
    "nas_sync": "同步 NAS 文件",
    "flow_run_single": "執行流程",
    "flow_run_single_completed": "流程執行完成",
    "flow_run_single_failed": "流程執行失敗",
    "flow_run_single_canceled": "流程執行取消",
    "flow_batch_completed": "批次流程執行完成",
    "flow_batch_completed_with_errors": "批次流程執行完成但有錯誤",
    "global_task_batch_queued": "加入全域批次佇列",
    "global_task_batch_completed": "全域批次執行完成",
    "global_task_batch_failed": "全域批次執行失敗",
    "global_batch_delete": "刪除全域批次紀錄",
    "flow_delete": "刪除流程",
    "flow_rename": "重新命名流程",
    "flow_export": "匯出流程",
    "flow_export_mapping": "匯出流程 Mapping",
    "flow_import": "匯入流程",
    "regulation_release_update_check": "手動檢查更新",
    "regulation_release_manual_download": "手動下載採認標準",
    "regulation_release_update_run": "排程偵測歐盟採認標準更新",
    "regulation_release_switch_to_primary": "備援同步至主要路徑",
    "standard_update_create": "建立標準更新任務",
    "standard_update_rename": "重新命名標準更新任務",
    "standard_update_update_description": "更新標準更新描述",
    "standard_update_delete": "刪除標準更新任務",
    "standard_update_use_latest_harmonised": "套用最新歐盟採用標準文件",
    "standard_update_upload_word": "上傳標準更新 Word",
    "standard_update_upload_standard_excel": "上傳標準總表",
    "standard_update_upload_regulation_excel": "上傳法規條文登記表",
    "standard_update_upload_harmonised_excel": "上傳歐盟採用標準文件",
    "standard_update_delete_input_file": "刪除標準更新輸入檔",
    "standard_update_mapping_inspect_headers": "標準更新欄位檢查",
    "standard_update_mapping_preview": "標準更新預覽",
    "standard_update_mapping_preview_failed": "標準更新預覽失敗",
    "standard_update_mapping_download": "標準更新下載結果",
    "standard_update_mapping_download_failed": "標準更新下載失敗",
    "standard_update_lock_release": "釋放標準更新鎖定",
    "standard_update_lock_takeover": "接管標準更新鎖定",
    "task_standard_mapping_inspect_headers": "標準對應欄位檢查",
    "task_standard_mapping_preview": "標準對應預覽",
    "task_standard_mapping_preview_failed": "標準對應預覽失敗",
    "task_standard_mapping_download": "標準對應下載結果",
    "task_standard_mapping_download_failed": "標準對應下載失敗",
    "task_mapping_check": "Mapping 引用文件檢查",
    "task_mapping_check_completed": "Mapping 引用文件檢查完成",
    "task_mapping_check_failed": "Mapping 引用文件檢查失敗",
    "task_mapping_check_canceled": "Mapping 引用文件檢查取消",
    "task_mapping_check_extract": "Mapping 擷取條件檢查",
    "task_mapping_check_extract_completed": "Mapping 擷取條件檢查完成",
    "task_mapping_check_extract_failed": "Mapping 擷取條件檢查失敗",
    "task_mapping_check_extract_canceled": "Mapping 擷取條件檢查取消",
    "task_mapping_run": "Mapping 方案執行",
    "task_mapping_run_completed": "Mapping 方案執行完成",
    "task_mapping_run_failed": "Mapping 方案執行失敗",
    "task_mapping_canceled": "Mapping 方案執行取消",
    "task_mapping_download_log": "下載 Mapping 記錄",
    "task_mapping_download_zip": "下載 Mapping 結果",
    "task_mapping_run_canceled": "Mapping 方案執行取消",
    "mapping_scheme_create": "建立 Mapping 方案",
    "mapping_scheme_rename": "重新命名 Mapping 方案",
    "mapping_scheme_delete": "刪除 Mapping 方案",
    "mapping_scheme_schedule_set": "設定排程 Mapping 方案",
    "mapping_scheme_run": "執行 Mapping 方案",
    "mapping_scheme_run_completed": "Mapping 方案執行完成",
    "mapping_scheme_run_failed": "Mapping 方案執行失敗",
    "mapping_scheme_download_source": "下載 Mapping 方案來源檔",
    "mapping_scheme_download_log": "下載 Mapping 方案記錄",
}

_AUDIT_SUMMARY_SUPPRESSED_ACTIONS = {"standard_update_mapping_download"}

_AUDIT_STORAGE_MODE_LABELS = {
    "primary": "主要路徑",
    "fallback": "備援路徑",
    "default": "本機預設路徑",
}


def _audit_pill(
    text: str,
    *,
    class_name: str = "",
    style: str = "",
    href: str = "",
) -> dict[str, str]:
    return {
        "text": str(text or "").strip(),
        "class_name": class_name,
        "style": style,
        "href": href,
    }


def _audit_summary_line(text: str, *, class_name: str = "", title: str = "") -> dict[str, str]:
    return {
        "text": str(text or "").strip(),
        "class_name": class_name,
        "title": title,
    }


def _build_audit_status_badge(status: str) -> dict[str, str] | None:
    normalized = str(status or "").strip().lower()
    if not normalized:
        return None
    if normalized == "completed":
        return {"text": normalized.upper(), "class_name": "badge-status-completed", "icon_class": "fa-check-circle"}
    if normalized == "failed":
        return {"text": normalized.upper(), "class_name": "badge-status-failed", "icon_class": "fa-times-circle"}
    return {"text": normalized.upper(), "class_name": "bg-light border text-muted", "icon_class": "fa-info-circle"}


def _build_audit_badges(*, task_id: str, action_name: str, detail: dict) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    flow = str(detail.get("flow") or "").strip()
    job_id = str(detail.get("job_id") or "").strip()
    run_id = str(detail.get("run_id") or "").strip()
    batch_id = str(detail.get("batch_id") or "").strip()
    scheme_name = str(detail.get("scheme_name") or "").strip()
    mapping_label = str(detail.get("mapping_display_name") or detail.get("mapping_file") or "").strip()
    file_name = str(detail.get("file_name") or "").strip()
    storage_mode = str(detail.get("storage_mode") or "").strip().lower()
    should_download = detail.get("should_download")
    downloaded = detail.get("downloaded")
    reference_ok = detail.get("reference_ok")
    extract_ok = detail.get("extract_ok")

    if flow:
        flow_url = ""
        if has_request_context():
            flow_url = url_for("flow_builder_bp.flow_builder", task_id=task_id, flow=flow)
        badges.append(_audit_pill(flow, class_name="shadow-sm", href=flow_url))
    if job_id:
        badges.append(_audit_pill(job_id, class_name="border-primary font-monospace"))
    if run_id and run_id != job_id:
        badges.append(_audit_pill(run_id, class_name="border-primary font-monospace"))
    if batch_id:
        badges.append(
            _audit_pill(
                batch_id,
                class_name="border-info font-monospace",
                style="color: #6f42c1; background: #f5f3ff;",
            )
        )
    if scheme_name:
        badges.append(_audit_pill(scheme_name, class_name="shadow-sm"))
    if mapping_label:
        badges.append(_audit_pill(mapping_label, class_name="shadow-sm"))
    if file_name:
        badges.append(_audit_pill(file_name, class_name="shadow-sm"))
    if should_download is True:
        badges.append(
            _audit_pill("有更新", class_name="border-success", style="background: #ecfdf5; color: #047857;")
        )
    elif should_download is False:
        badges.append(
            _audit_pill("無更新", class_name="border-secondary", style="background: #f8fafc; color: #475569;")
        )
    if downloaded is True:
        badges.append(_audit_pill("偵測到更新", style="background: #eff6ff; color: #1d4ed8;"))
        badges.append(
            _audit_pill("已下載", class_name="border-primary", style="background: #eff6ff; color: #1d4ed8;")
        )
    elif downloaded is False:
        badges.append(_audit_pill("未偵測到更新", style="background: #f8fafc; color: #475569;"))
        badges.append(
            _audit_pill("未下載", class_name="border-secondary", style="background: #f8fafc; color: #475569;")
        )
    storage_label = _AUDIT_STORAGE_MODE_LABELS.get(storage_mode, "")
    if storage_label:
        style = "background: #fff8e1; color: #8a5a00;"
        class_name = "border-warning"
        if storage_mode in {"fallback", "default"}:
            style = "background: #ffe1e1; color: #8a0000;"
            class_name = "border-danger"
        badges.append(_audit_pill(storage_label, class_name=class_name, style=style))
    if reference_ok is True:
        badges.append(
            _audit_pill("引用檢查通過", class_name="border-success", style="background: #ecfdf5; color: #047857;")
        )
    elif reference_ok is False:
        badges.append(
            _audit_pill("引用檢查未通過", class_name="border-secondary", style="background: #f8fafc; color: #475569;")
        )
    if extract_ok is True:
        badges.append(
            _audit_pill("擷取檢查通過", class_name="border-success", style="background: #ecfdf5; color: #047857;")
        )
    elif extract_ok is False:
        badges.append(
            _audit_pill("擷取檢查未通過", class_name="border-secondary", style="background: #f8fafc; color: #475569;")
        )
    return [badge for badge in badges if badge["text"]]


def _build_audit_summary_lines(*, action_name: str, detail: dict) -> list[dict[str, str]]:
    if action_name in _AUDIT_SUMMARY_SUPPRESSED_ACTIONS:
        return []

    task_name = str(detail.get("task_name") or "").strip()
    name = str(detail.get("name") or "").strip()
    old_name = str(detail.get("old_name") or "").strip()
    output_path = str(detail.get("output_path") or "").strip()
    target_chapter_ref = str(detail.get("target_chapter_ref") or "").strip()
    target_table_index = detail.get("target_table_index")
    updated_count = detail.get("updated_count")
    same_count = detail.get("same_count")
    missing_count = detail.get("missing_count")
    harmonised_fallback_count = detail.get("harmonised_fallback_count")

    lines: list[dict[str, str]] = []
    if task_name:
        lines.append(_audit_summary_line(f"任務：{task_name}"))
    if old_name or name:
        display_name = f"{old_name} → {name}" if old_name and name else (name or old_name)
        lines.append(_audit_summary_line(f"名稱：{display_name}"))
    if target_chapter_ref or target_table_index:
        scope = target_chapter_ref or "未指定章節"
        if target_table_index:
            scope = f"{scope} / 表格 {target_table_index}"
        lines.append(_audit_summary_line(f"範圍：{scope}"))
    if (
        updated_count is not None
        or same_count is not None
        or missing_count is not None
        or harmonised_fallback_count is not None
    ):
        lines.append(
            _audit_summary_line(
                f"統計：更新 {updated_count or 0}、相同 {same_count or 0}、缺漏 {missing_count or 0}、EU YES 退選更新 {harmonised_fallback_count or 0}"
            )
        )
    if output_path:
        lines.append(_audit_summary_line(f"輸出：{output_path}", title=output_path))
    return lines


def _build_audit_entry(log: AuditLog, detail: dict) -> dict:
    action_name = str(log.action or "").strip()
    status = str(detail.get("status") or "").strip()
    return {
        "ts": log.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "action": action_name,
        "action_label": _AUDIT_ACTION_LABELS.get(action_name, action_name),
        "actor": {
            "work_id": log.work_id,
            "label": (detail.get("_actor_label") or "").strip(),
        },
        "detail": detail,
        "task_id": log.task_id,
        "status_badge": _build_audit_status_badge(status),
        "badges": _build_audit_badges(task_id=str(log.task_id or "").strip(), action_name=action_name, detail=detail),
        "summary_lines": _build_audit_summary_lines(action_name=action_name, detail=detail),
    }


def _build_system_error_entry(log: SystemErrorLog, detail: dict) -> dict:
    return {
        "ts": log.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "level": (log.level or "ERROR").strip().upper(),
        "component": (log.component or "").strip(),
        "message": (log.message or "").strip(),
        "error_type": (log.error_type or "").strip(),
        "detail": detail,
        "task_id": (log.task_id or "").strip(),
    }


def _is_within_path(target: str, base: str) -> bool:
    target_path = (target or "").strip()
    base_path = (base or "").strip()
    if not target_path or not base_path:
        return False
    try:
        return os.path.commonpath(
            [os.path.normcase(os.path.abspath(target_path)), os.path.normcase(os.path.abspath(base_path))]
        ) == os.path.normcase(os.path.abspath(base_path))
    except Exception:
        return False


def _current_actor() -> dict[str, str]:
    work_id, label = get_actor_info()
    return {"work_id": work_id, "label": label}


def _record_user_admin_audit(action: str, detail: dict | None = None) -> None:
    payload = dict(detail or {})
    record_audit(
        action=action,
        actor=_current_actor(),
        detail=payload,
    )


def _record_regulation_primary_switch_audit(
    *,
    actor: dict[str, str],
    status: str,
    source_release: dict | None = None,
    source_path: str = "",
    target_path: str = "",
    error: str = "",
) -> None:
    release = source_release or {}
    detail = {
        "status": status,
        "source_path": source_path,
        "target_path": target_path,
        "file_name": release.get("file_name", ""),
        "version_label": release.get("version_label", ""),
        "downloaded_at": release.get("downloaded_at", ""),
        "source_url": release.get("source_url", ""),
    }
    if error:
        detail["error"] = error
    record_audit(
        action="regulation_release_switch_to_primary",
        actor=actor,
        detail=detail,
    )


class SecureAdminIndexView(AdminIndexView):
    extra_css = ADMIN_CUSTOM_CSS

    def is_visible(self):
        return False

    def is_accessible(self):
        return user_is_admin(current_user)

    def inaccessible_callback(self, name, **kwargs):
        if current_user.is_authenticated:
            abort(403)
        return redirect(url_for("auth_bp.login", next=sanitize_next_url(request.full_path)))

    @expose("/")
    def index(self):
        return redirect(url_for("system_settings.index"))


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
            previous_active = bool(model.active)
            previous_role_name = (
                getattr(getattr(model, "user_role", None), "role", None).name
                if getattr(getattr(model, "user_role", None), "role", None)
                else ""
            )
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
            _record_user_admin_audit(
                "user_admin_update",
                {
                    "user_id": model.id,
                    "work_id": model.work_id,
                    "display_name": model.display_name,
                    "active": bool(model.active),
                    "previous_active": previous_active,
                    "role": role.name,
                    "previous_role": previous_role_name,
                },
            )
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
            previous_role_name = ""
            existing = UserRole.query.filter_by(user_id=user.id).first()
            if existing:
                if self._is_last_admin_change(user.id, role.id, deleting=False):
                    flash("Cannot remove the last admin.", "danger")
                    return False
                previous_role = Role.query.get(existing.role_id)
                previous_role_name = previous_role.name if previous_role else ""
                existing.role_id = role.id
            else:
                db.session.add(UserRole(user_id=user.id, role_id=role.id))
            commit_session()
            _record_user_admin_audit(
                "user_admin_role_create",
                {
                    "user_id": user.id,
                    "work_id": user.work_id,
                    "display_name": user.display_name,
                    "role": role.name,
                    "previous_role": previous_role_name,
                    "mode": "update_existing" if existing else "create_new",
                },
            )
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
            previous_role = Role.query.get(model.role_id) if model.role_id else None
            model.role_id = new_role.id
            commit_session()
            _record_user_admin_audit(
                "user_admin_role_update",
                {
                    "user_id": model.user_id,
                    "work_id": getattr(getattr(model, "user", None), "work_id", ""),
                    "display_name": getattr(getattr(model, "user", None), "display_name", ""),
                    "role": new_role.name,
                    "previous_role": previous_role.name if previous_role else "",
                },
            )
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
            role = Role.query.get(model.role_id) if model.role_id else None
            user = getattr(model, "user", None)
            db.session.delete(model)
            commit_session()
            _record_user_admin_audit(
                "user_admin_role_delete",
                {
                    "user_id": model.user_id,
                    "work_id": getattr(user, "work_id", ""),
                    "display_name": getattr(user, "display_name", ""),
                    "role": role.name if role else "",
                },
            )
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
                message = frontend_error_message(exc)
                if is_ajax:
                    return jsonify({"ok": False, "error": message}), 500
                flash(message, "danger")
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
                error = frontend_error_message(exc)

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
            action = (request.form.get("action") or "save_settings").strip()
            try:
                if action == "test_regulation_primary_storage":
                    configured_reference_root = harmonised_reference_configured_root()
                    ok, message = test_harmonised_reference_storage(configured_reference_root)
                    flash(message, "success" if ok else "warning")
                elif action == "switch_regulation_release_to_primary":
                    configured_reference_root = harmonised_reference_configured_root()
                    ok, message = test_harmonised_reference_storage(configured_reference_root)
                    if not ok:
                        flash(message, "warning")
                    else:
                        actor = _current_actor()
                        active_release = get_active_harmonised_release()
                        fallback_reference_root = harmonised_reference_fallback_root()
                        fallback_release = get_latest_harmonised_release_in_dir(fallback_reference_root)
                        source_release = (
                            active_release
                            if _is_within_path(active_release.get("path", ""), fallback_reference_root)
                            else fallback_release
                        )
                        source_path = (source_release.get("path") or "").strip()
                        target_path = ""
                        try:
                            if not source_path or not os.path.isfile(source_path):
                                _record_regulation_primary_switch_audit(
                                    actor=actor,
                                    status="failed",
                                    source_release=source_release,
                                    error="找不到備援路徑中的可用檔案",
                                )
                                flash("找不到備援路徑中的可用檔案，無法同步至主要存取路徑", "warning")
                            elif _is_within_path(source_path, configured_reference_root):
                                target_path = source_path
                                _record_regulation_primary_switch_audit(
                                    actor=actor,
                                    status="skipped",
                                    source_release=source_release,
                                    source_path=source_path,
                                    target_path=target_path,
                                    error="目前 active 版本已經位於主要存取路徑",
                                )
                                flash("目前 active 版本已經位於主要存取路徑", "info")
                            else:
                                target_path = os.path.join(configured_reference_root, os.path.basename(source_path))
                                os.makedirs(configured_reference_root, exist_ok=True)
                                shutil.copy2(source_path, target_path)
                                downloaded_at = None
                                if source_release.get("downloaded_at"):
                                    try:
                                        downloaded_at = datetime.strptime(source_release["downloaded_at"], "%Y-%m-%d %H:%M")
                                    except ValueError:
                                        downloaded_at = None
                                result = activate_harmonised_release(
                                    target_path,
                                    source_url=source_release.get("source_url", ""),
                                    downloaded_at=downloaded_at,
                                    version_label=source_release.get("version_label", ""),
                                )
                                if result:
                                    _record_regulation_primary_switch_audit(
                                        actor=actor,
                                        status="completed",
                                        source_release=result,
                                        source_path=source_path,
                                        target_path=target_path,
                                    )
                                    flash("已切回主要存取路徑並更新 active 版本", "success")
                                else:
                                    _record_regulation_primary_switch_audit(
                                        actor=actor,
                                        status="failed",
                                        source_release=source_release,
                                        source_path=source_path,
                                        target_path=target_path,
                                        error="activate_harmonised_release 回傳空結果",
                                    )
                                    flash("切回主要存取路徑失敗", "danger")
                        except Exception as exc:
                            _record_regulation_primary_switch_audit(
                                actor=actor,
                                status="failed",
                                source_release=source_release,
                                source_path=source_path,
                                target_path=target_path,
                                error=str(exc),
                            )
                            raise
                elif action == "download_regulation_release_now":
                    from app.jobs.adoption_standard_update import enqueue_regulation_manual_download_job

                    page_url = (request.form.get("regulation_download_page_url") or "").strip() or (
                        (setting.regulation_download_page_url or "").strip() if setting else ""
                    )
                    link_text = (request.form.get("regulation_download_link_text") or "").strip() or (
                        (setting.regulation_download_link_text or "").strip() if setting else ""
                    )
                    job_id, created = enqueue_regulation_manual_download_job(
                        page_url=page_url or None,
                        link_text=link_text or None,
                        actor=_current_actor(),
                    )
                    if created:
                        flash(
                            f"已建立背景下載工作，job_id={job_id}。請由 worker 處理。"
                            if not is_inline_execution_enabled()
                            else f"已建立下載工作，job_id={job_id}。",
                            "success",
                        )
                    else:
                        flash(
                            f"已有進行中的採認標準下載工作，job_id={job_id}",
                            "info",
                        )
                elif action == "check_regulation_release_update":
                    from app.jobs.adoption_standard_update import check_for_update

                    page_url = (request.form.get("regulation_download_page_url") or "").strip() or (
                        (setting.regulation_download_page_url or "").strip() if setting else ""
                    )
                    link_text = (request.form.get("regulation_download_link_text") or "").strip() or (
                        (setting.regulation_download_link_text or "").strip() if setting else ""
                    )
                    result = check_for_update(
                        page_url=page_url or None,
                        link_text=link_text or None,
                        actor=_current_actor(),
                    )
                    current_name = ((result.get("current") or {}).get("filename") or "-").strip() or "-"
                    reasons = result.get("reasons") or []
                    if result.get("should_download"):
                        reason_text = "；".join(reasons) if reasons else "偵測到版本差異"
                        flash(f"偵測到可更新版本：{current_name}。原因：{reason_text}", "warning")
                    else:
                        flash(f"目前已是最新版本：{current_name}", "info")
                else:
                    setting.email_batch_notify_enabled = request.form.get("email_batch_notify_enabled") == "on"
                    nas_limit = request.form.get("nas_max_copy_file_size_mb")
                    if nas_limit and nas_limit.strip():
                        setting.nas_max_copy_file_size_mb = int(nas_limit)
                    else:
                        setting.nas_max_copy_file_size_mb = None
                    setting.regulation_download_page_url = (
                        (request.form.get("regulation_download_page_url") or "").strip() or None
                    )
                    setting.regulation_download_link_text = (
                        (request.form.get("regulation_download_link_text") or "").strip() or None
                    )

                    commit_session()
                    flash("系統設定已更新", "success")
            except ValueError:
                flash("數值格式錯誤", "danger")
            except Exception as exc:
                db.session.rollback()
                flash(f"更新失敗: {str(exc)}", "danger")
            return redirect(url_for("system_settings.index"))

        last_updated = format_tw_datetime(setting.updated_at, assume_tz=TAIWAN_TZ) if setting.updated_at else "-"
        active_release = get_active_harmonised_release()
        reference_root = harmonised_reference_root()
        configured_reference_root = harmonised_reference_configured_root()
        reference_storage_mode = harmonised_reference_storage_mode()
        effective_download_page_url = (
            (setting.regulation_download_page_url or "").strip()
            or (current_app.config.get("REGULATION_DOWNLOAD_PAGE_URL") or "").strip()
        )
        effective_download_link_text = (
            (setting.regulation_download_link_text or "").strip()
            or (current_app.config.get("REGULATION_DOWNLOAD_LINK_TEXT") or "").strip()
        )
        local_reference_root = harmonised_reference_fallback_root()
        fallback_release = get_latest_harmonised_release_in_dir(local_reference_root)
        primary_reference_root = configured_reference_root or local_reference_root
        primary_storage_ok = None
        primary_storage_message = harmonised_reference_status_message() or "尚未測試，請按「測試 NAS 連線」確認主要存取路徑是否可用"
        active_release_on_fallback = bool(
            configured_reference_root
            and _is_within_path(active_release.get("path", ""), local_reference_root)
            and not _is_within_path(active_release.get("path", ""), configured_reference_root)
        )
        using_fallback_reference = reference_storage_mode == "fallback"
        can_switch_to_primary = bool(
            configured_reference_root
            and fallback_release.get("path")
            and not _is_within_path(fallback_release.get("path", ""), configured_reference_root)
        )
        return self.render(
            "admin/system_settings.html",
            setting=setting,
            last_updated=last_updated,
            active_release=active_release,
            regulation_reference_root=reference_root,
            configured_reference_root=configured_reference_root,
            primary_reference_root=primary_reference_root,
            fallback_reference_root=local_reference_root,
            effective_download_page_url=effective_download_page_url,
            effective_download_link_text=effective_download_link_text,
            primary_storage_ok=primary_storage_ok,
            primary_storage_message=primary_storage_message,
            using_fallback_reference=using_fallback_reference,
            active_release_on_fallback=active_release_on_fallback,
            can_switch_to_primary=can_switch_to_primary,
        )


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

            entries.append(_build_audit_entry(log, detail))

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


class SystemErrorLogView(BaseView):
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
        component: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> tuple[list[dict], dict]:
        query = SystemErrorLog.query

        if task_id:
            query = query.filter_by(task_id=task_id)
        if component:
            query = query.filter(SystemErrorLog.component.ilike(f"%{component}%"))
        if q:
            search = f"%{q}%"
            query = query.filter(
                (SystemErrorLog.component.ilike(search))
                | (SystemErrorLog.message.ilike(search))
                | (SystemErrorLog.error_type.ilike(search))
                | (SystemErrorLog.detail.ilike(search))
            )
        if start_date:
            try:
                dt_start = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
                query = query.filter(SystemErrorLog.created_at >= dt_start)
            except ValueError:
                pass
        if end_date:
            try:
                dt_end = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
                query = query.filter(SystemErrorLog.created_at <= dt_end)
            except ValueError:
                pass

        total_count = query.count()
        total_pages = (total_count + per_page - 1) // per_page
        page = max(1, min(page, total_pages)) if total_pages > 0 else 1

        logs = query.order_by(SystemErrorLog.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()

        entries = []
        for log in logs:
            try:
                detail = json.loads(log.detail) if log.detail else {}
            except Exception:
                detail = {"raw": log.detail}
            entries.append(_build_system_error_entry(log, detail))

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
        task_id = (request.args.get("task_id") or "").strip()
        q = (request.args.get("q") or "").strip()
        component = (request.args.get("component") or "").strip()
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
            component=component if component else None,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else None,
        )

        return self.render(
            "admin/system_error_logs.html",
            task_id=task_id,
            q=q,
            component=component,
            start_date=start_date,
            end_date=end_date,
            entries=entries,
            pagination=pagination,
        )


def init_admin(app) -> Admin:
    admin = Admin(app, name="系統管理", url="/admin", index_view=SecureAdminIndexView())
    admin.add_view(SystemSettingView(name="系統設定", endpoint="system_settings", url="system-settings"))
    admin.add_view(UserAdminView(User, db.session, name="使用者列表"))
    admin.add_view(ADSearchView(name="帳號搜尋", endpoint="ad_search", url="ad-search"))
    admin.add_view(AuditLogView(name="操作紀錄", endpoint="audit_logs", url="audit-logs"))
    admin.add_view(SystemErrorLogView(name="系統錯誤", endpoint="system_error_logs", url="system-error-logs"))
    return admin
