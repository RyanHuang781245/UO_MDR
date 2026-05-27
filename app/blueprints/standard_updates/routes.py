from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from flask import abort, current_app, flash, jsonify, redirect, render_template, request, send_file, url_for
from flask_login import current_user

from app.blueprints.tasks.mapping_routes import _safe_uploaded_filename
from app.blueprints.tasks.standard_mapping_routes import (
    _MANUAL_HEADER_FIELD_ORDER,
    _STANDARD_PRIORITY_FIELDS,
    _build_stats,
    _parse_enabled_standard_levels,
    _parse_edit_map,
    _has_unresolved_manual_mapping,
    _parse_iso_priority,
    _parse_limit_to_chapter,
    _parse_manual_header_mappings,
    _parse_override_map,
    _parse_required_headers,
    _parse_target_chapter_ref,
    _parse_target_table_index,
    _format_target_table_index_display,
)
from app.services.standard_mapping_service import (
    DEFAULT_ENABLED_STANDARD_LEVELS,
    DEFAULT_ISO_PRIORITY,
    DEFAULT_REQUIRED_HEADERS,
    inspect_document_sections,
    inspect_document_tables,
    normalize_iso_priority,
    normalize_enabled_standard_levels,
    normalize_manual_header_mappings,
    normalize_required_headers,
    process_document,
)
from app.services.audit_service import record_audit
from app.services.standard_update_service import (
    ALLOWED_EXCEL_EXTENSIONS,
    ALLOWED_WORD_EXTENSIONS,
    HARMONISED_SOURCE_CUSTOM,
    HARMONISED_SOURCE_SYSTEM,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PREVIEWED,
    STATUS_READY,
    acquire_standard_update_lock,
    available_input_files,
    create_standard_update,
    delete_input_file,
    delete_standard_update,
    force_takeover_standard_update_lock,
    get_active_harmonised_release,
    get_latest_uploaded_input,
    get_locked_harmonised_release,
    get_standard_update_lock_info,
    get_task_harmonised_release,
    input_file_history,
    lock_standard_update_to_latest_harmonised,
    list_standard_updates,
    load_standard_update,
    normalize_harmonised_source_mode,
    refresh_standard_update_lock,
    release_standard_update_lock,
    safe_standard_update_file,
    save_standard_update,
    save_uploaded_input,
    standard_update_name_exists,
    standard_update_output_dir,
    sync_harmonised_release_snapshot,
)
from app.services.authz_service import user_is_admin
from app.services.user_context_service import get_actor_info

from .blueprint import standard_updates_bp

STANDARD_UPDATE_TEXT_LIMIT = 50


def _validate_limited_text(value: str, label: str, *, required: bool = False) -> str | None:
    normalized = (value or "").strip()
    if required and not normalized:
        return f"請輸入{label}"
    if len(normalized) > STANDARD_UPDATE_TEXT_LIMIT:
        return f"{label}最多 {STANDARD_UPDATE_TEXT_LIMIT} 字"
    return None


def _wants_json_response() -> bool:
    return (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or request.accept_mimetypes.best == "application/json"
    )


def _current_actor() -> dict[str, str]:
    work_id, label = get_actor_info()
    return {"work_id": work_id, "label": label}


def _record_standard_update_audit(action: str, task_id: str, detail: dict | None = None, *, actor: dict | None = None) -> None:
    payload = {"task_id": task_id}
    payload.update(detail or {})
    record_audit(
        action=action,
        actor=actor or _current_actor(),
        detail=payload,
        task_id=task_id,
    )


def _paginate(items: list[dict], page: int, per_page: int = 10) -> tuple[list[dict], dict]:
    total_count = len(items)
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], {
        "page": page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def _remove_stale_output_variants(output_dir: str, base_name: str) -> None:
    stem = Path(base_name).stem
    suffix = Path(base_name).suffix.lower()
    variant_prefix = f"{stem} ("
    for entry in os.listdir(output_dir):
        entry_path = os.path.join(output_dir, entry)
        if not os.path.isfile(entry_path):
            continue
        if Path(entry).suffix.lower() != suffix:
            continue
        entry_stem = Path(entry).stem
        if not (entry_stem.startswith(variant_prefix) and entry_stem.endswith(")")):
            continue
        variant_number = entry_stem[len(variant_prefix):-1]
        if not variant_number.isdigit():
            continue
        try:
            os.remove(entry_path)
        except OSError:
            current_app.logger.warning("Failed to remove stale standard update output: %s", entry_path)


def _load_chapter_options(task_id: str, selected_word: str) -> tuple[list[dict], str]:
    if not selected_word:
        return [], ""
    try:
        word_path = safe_standard_update_file(task_id, selected_word, ALLOWED_WORD_EXTENSIONS, kind="word")
    except (ValueError, FileNotFoundError):
        return [], ""
    try:
        options = inspect_document_sections(word_path)
    except Exception:
        current_app.logger.exception("Failed to inspect document sections")
        return [], "章節清單讀取失敗"
    if not options:
        return [], "找不到可辨識的章節清單"
    return options, ""


def _apply_ready_status(task_id: str, task: dict) -> dict:
    task_harmonised_release = get_task_harmonised_release(task_id, task)
    harmonised_required = normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_SYSTEM
    is_ready = bool(
        task.get("word_file_path")
        and task.get("standard_excel_path")
        and task.get("regulation_excel_path")
        and (task_harmonised_release.get("path") if harmonised_required else True)
    )
    if is_ready:
        task["status"] = STATUS_READY
    elif task.get("status") != STATUS_FAILED:
        task["status"] = "draft"
    return task


def _resolve_lock_actor() -> tuple[str, str, str]:
    work_id, actor_name = get_actor_info()
    actor_id = work_id or actor_name or f"ip:{request.remote_addr or 'unknown'}"
    return actor_id, work_id, actor_name


def _build_lock_state(task_id: str, task: dict | None = None) -> dict:
    actor_id, _, actor_name = _resolve_lock_actor()
    lock_info = get_standard_update_lock_info(task_id, meta=task)
    locked_display_name = lock_info.get("locked_by_name") or lock_info.get("locked_by_work_id") or "其他使用者"
    return {
        **lock_info,
        "held_by_current_user": bool(lock_info.get("is_locked")) and lock_info.get("locked_by_actor_id") == actor_id,
        "blocked_by_other": bool(lock_info.get("is_locked")) and lock_info.get("locked_by_actor_id") != actor_id,
        "lock_display_name": locked_display_name if lock_info.get("is_locked") else (actor_name or ""),
        "lock_expires_at_display": lock_info.get("lock_expires_at") or "",
    }


def _lock_block_message(lock_state: dict) -> str:
    if not lock_state.get("blocked_by_other"):
        return ""
    name = lock_state.get("lock_display_name") or "其他使用者"
    expires_at = lock_state.get("lock_expires_at_display") or "-"
    # return f"此任務目前由 {name} 使用中，鎖定至 {expires_at}"
    return f"此任務目前由 {name} 使用中"


def _acquire_task_lock_or_respond(task_id: str, task: dict | None = None):
    actor_id, work_id, actor_name = _resolve_lock_actor()
    ok, updated_task = acquire_standard_update_lock(
        task_id,
        actor_id,
        work_id=work_id,
        actor_name=actor_name,
        meta=task,
    )
    if ok:
        return updated_task, None

    lock_state = _build_lock_state(task_id, updated_task or task)
    message = _lock_block_message(lock_state) or "此任務目前無法取得操作鎖"
    if _wants_json_response():
        return updated_task or task or {}, jsonify({"ok": False, "error": message, "lock": lock_state}), 423
    flash(message, "warning")
    return updated_task or task or {}, redirect(url_for("standard_updates_bp.detail", task_id=task_id))


def _render_mapping_page(
    task_id: str,
    *,
    preview_result: dict | None = None,
    selected_word: str = "",
    selected_excel: str = "",
    selected_regulation_excel: str = "",
    selected_harmonised_excel: str = "",
    iso_priority: tuple[str, ...] | list[str] | None = None,
    enabled_standard_levels: tuple[str, ...] | list[str] | None = None,
    required_headers: tuple[str, ...] | list[str] | None = None,
    limit_to_chapter: bool = False,
    target_chapter_ref: str = "",
    target_table_index: tuple[int, ...] | list[int] | int | None = None,
    manual_target_chapter_ref: str = "",
    manual_header_mappings: dict[int, dict[str, str]] | None = None,
):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    task_lock = _build_lock_state(task_id, task)
    word_options, excel_options = available_input_files(task_id)
    regulation_excel_options = [item["name"] for item in input_file_history(task_id, kind="regulation", current_file=task.get("regulation_excel_path", ""))]
    harmonised_excel_options = [item["name"] for item in input_file_history(task_id, kind="harmonised", current_file="")]
    chapter_options, chapter_options_error = _load_chapter_options(task_id, selected_word)
    harmonised_release = get_task_harmonised_release(task_id, task)
    use_system_harmonised = normalize_harmonised_source_mode(task.get("harmonised_source_mode")) != HARMONISED_SOURCE_CUSTOM
    effective_harmonised_selection = (selected_harmonised_excel or harmonised_release.get("file_name", "")).strip()
    harmonised_source_message = (
        "目前使用任務鎖定的系統 Regulation (EU) 2017/745 採認標準"
        if use_system_harmonised
        else "可選擇任務已上傳的 Regulation (EU) 2017/745 採認標準；未上傳時會略過 harmonised 比對"
    )
    page_description = (
        "使用獨立標準更新任務的上傳檔案與任務鎖定的 Regulation (EU) 2017/745 snapshot 產生預覽或下載結果"
        if use_system_harmonised
        else "使用獨立標準更新任務的上傳檔案與任務自訂採認標準檔案產生預覽或下載結果"
    )
    reference_payload = (preview_result or {}).get("reference_payload", {})
    active_iso_priority = tuple((preview_result or {}).get("iso_priority") or normalize_iso_priority(iso_priority))
    preview_enabled_levels = (preview_result or {}).get("enabled_standard_levels")
    active_enabled_standard_levels = tuple(
        normalize_enabled_standard_levels(enabled_standard_levels)
        if preview_enabled_levels is None
        else preview_enabled_levels
    )
    active_required_headers = tuple((preview_result or {}).get("required_headers") or normalize_required_headers(required_headers))
    active_manual_header_mappings = normalize_manual_header_mappings(
        (preview_result or {}).get("manual_header_mappings") or manual_header_mappings
    )
    iso_priority_positions = {label: index + 1 for index, label in enumerate(active_iso_priority)}
    interactive_rows = len({item.get("row_key", "") for item in reference_payload.values() if item.get("row_key")})
    table_checks = (preview_result or {}).get("table_checks", [])
    return render_template(
        "tasks/standard_mapping.html",
        task_id=task_id,
        task={"id": task_id, "name": task.get("name", task_id)},
        page_title="標準更新",
        page_description=page_description,
        task_label="標準更新任務",
        missing_file_hint="請先上傳 Word、UOC 標準規範總表_現行標準與各國法規條文登記表",
        word_options=word_options,
        excel_options=excel_options,
        regulation_excel_options=regulation_excel_options,
        harmonised_excel_options=harmonised_excel_options,
        selected_word=selected_word,
        selected_excel=selected_excel,
        selected_regulation_excel=selected_regulation_excel,
        selected_harmonised_excel=effective_harmonised_selection,
        preview_tables=(preview_result or {}).get("preview_tables", []),
        table_checks=table_checks,
        reference_payload=reference_payload,
        stats=_build_stats((preview_result or {}).get("report", [])) if preview_result else {"updated": 0, "same": 0, "missing": 0, "total": 0},
        interactive_rows=interactive_rows,
        interactive_fields=len(reference_payload),
        has_preview=bool(preview_result and (preview_result.get("preview_tables") or [])),
        last_generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if preview_result else "",
        iso_priority=active_iso_priority,
        default_iso_priority=DEFAULT_ISO_PRIORITY,
        enabled_standard_levels=active_enabled_standard_levels,
        default_enabled_standard_levels=DEFAULT_ENABLED_STANDARD_LEVELS,
        iso_priority_positions=iso_priority_positions,
        standard_priority_fields=_STANDARD_PRIORITY_FIELDS,
        required_headers=active_required_headers,
        default_required_headers=DEFAULT_REQUIRED_HEADERS,
        limit_to_chapter=bool((preview_result or {}).get("target_chapter_ref") or limit_to_chapter),
        target_chapter_ref=(preview_result or {}).get("target_chapter_ref", target_chapter_ref),
        target_table_index=(preview_result or {}).get("target_table_index", target_table_index),
        target_table_index_display=_format_target_table_index_display((preview_result or {}).get("target_table_index", target_table_index)),
        manual_target_chapter_ref=manual_target_chapter_ref,
        scope_table_count=(preview_result or {}).get("scope_table_count", 0),
        chapter_options=chapter_options,
        chapter_options_error=chapter_options_error,
        manual_header_mappings=active_manual_header_mappings,
        manual_header_field_order=_MANUAL_HEADER_FIELD_ORDER,
        has_unresolved_manual_mapping=_has_unresolved_manual_mapping(table_checks),
        mapping_route_endpoint="standard_updates_bp.mapping",
        mapping_download_endpoint="standard_updates_bp.download_result",
        mapping_detail_endpoint="standard_updates_bp.detail",
        use_system_harmonised=use_system_harmonised,
        harmonised_system_release=harmonised_release,
        harmonised_source_message=harmonised_source_message,
        task_lock=task_lock,
    )


@standard_updates_bp.route("/standards", methods=["GET", "POST"], endpoint="list")
def list_page():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = (request.form.get("description") or "").strip()
        harmonised_source_mode = normalize_harmonised_source_mode(request.form.get("harmonised_source_mode"))
        name_error = _validate_limited_text(name, "標準更新任務名稱", required=True)
        if name_error:
            flash(name_error, "danger")
            return redirect(url_for("standard_updates_bp.list"))
        desc_error = _validate_limited_text(description, "任務描述")
        if desc_error:
            flash(desc_error, "danger")
            return redirect(url_for("standard_updates_bp.list"))
        if standard_update_name_exists(name):
            flash("標準更新任務名稱已存在", "danger")
            return redirect(url_for("standard_updates_bp.list"))
        task_id = create_standard_update(name, description, harmonised_source_mode=harmonised_source_mode)
        _record_standard_update_audit(
            "standard_update_create",
            task_id,
            {
                "name": name,
                "description": description,
                "harmonised_source_mode": harmonised_source_mode,
            },
        )
        flash("已建立標準更新任務", "success")
        return redirect(url_for("standard_updates_bp.detail", task_id=task_id))

    items = list_standard_updates()
    for item in items:
        item["harmonised_source_mode"] = normalize_harmonised_source_mode(item.get("harmonised_source_mode"))
        if item["harmonised_source_mode"] == HARMONISED_SOURCE_CUSTOM:
            item["custom_harmonised_version"] = get_task_harmonised_release(item.get("id", ""), item).get("version_label", "")
    page = request.args.get("page", 1, type=int)
    paged_items, pagination = _paginate(items, page)
    harmonised_release = sync_harmonised_release_snapshot()
    return render_template(
        "standard_updates/list.html",
        items=paged_items,
        pagination=pagination,
        harmonised_release=harmonised_release,
    )


@standard_updates_bp.get("/standards/<task_id>", endpoint="detail")
def detail(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    task["harmonised_source_mode"] = normalize_harmonised_source_mode(task.get("harmonised_source_mode"))
    word_options, excel_options = available_input_files(task_id)
    harmonised_release = sync_harmonised_release_snapshot()
    locked_harmonised_release = get_locked_harmonised_release(task)
    task_harmonised_release = get_task_harmonised_release(task_id, task)
    custom_harmonised_history = input_file_history(task_id, kind="harmonised", current_file="")
    using_custom_harmonised = task.get("harmonised_source_mode") == HARMONISED_SOURCE_CUSTOM
    has_newer_harmonised = bool(
        task.get("harmonised_source_mode") == HARMONISED_SOURCE_SYSTEM
        and harmonised_release.get("version_label")
        and harmonised_release.get("version_label") != locked_harmonised_release.get("version_label", "")
    )
    is_ready = bool(
        task.get("word_file_path")
        and task.get("standard_excel_path")
        and task.get("regulation_excel_path")
        and (task_harmonised_release.get("path") if task.get("harmonised_source_mode") == HARMONISED_SOURCE_SYSTEM else True)
    )
    return render_template(
        "standard_updates/detail.html",
        task=task,
        harmonised_source_mode=task.get("harmonised_source_mode"),
        word_options=word_options,
        excel_options=excel_options,
        word_history=input_file_history(task_id, kind="word", current_file=task.get("word_file_path", "")),
        standard_excel_history=input_file_history(task_id, kind="excel", current_file=task.get("standard_excel_path", "")),
        regulation_excel_history=input_file_history(task_id, kind="regulation", current_file=task.get("regulation_excel_path", "")),
        custom_harmonised_history=custom_harmonised_history,
        harmonised_release=harmonised_release,
        locked_harmonised_release=locked_harmonised_release,
        task_harmonised_release=task_harmonised_release,
        using_custom_harmonised=using_custom_harmonised,
        has_newer_harmonised=has_newer_harmonised,
        is_ready=is_ready,
        task_lock=_build_lock_state(task_id, task),
    )


@standard_updates_bp.post("/standards/<task_id>/rename", endpoint="rename")
def rename(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    next_url = (request.form.get("next") or request.referrer or "").strip()
    if not next_url:
        next_url = url_for("standard_updates_bp.list")
    name = (request.form.get("name") or "").strip()
    name_error = _validate_limited_text(name, "標準更新任務名稱", required=True)
    if name_error:
        if _wants_json_response():
            return jsonify({"ok": False, "error": name_error}), 400
        flash(name_error, "danger")
        return redirect(next_url)
    if standard_update_name_exists(name, exclude_id=task_id):
        if _wants_json_response():
            return jsonify({"ok": False, "error": "標準更新任務名稱已存在"}), 400
        flash("標準更新任務名稱已存在", "danger")
        return redirect(next_url)
    old_name = str(task.get("name") or "").strip()
    task["name"] = name
    save_standard_update(task_id, task)
    _record_standard_update_audit(
        "standard_update_rename",
        task_id,
        {"old_name": old_name, "name": name},
    )
    if _wants_json_response():
        return jsonify({"ok": True, "task_id": task_id, "name": name, "message": "已更新標準更新任務名稱"})
    flash("已更新標準更新任務名稱", "success")
    return redirect(next_url)


@standard_updates_bp.post("/standards/<task_id>/description", endpoint="update_description")
def update_description(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    next_url = (request.form.get("next") or request.referrer or "").strip()
    if not next_url:
        next_url = url_for("standard_updates_bp.list")
    description = (request.form.get("description") or "").strip()
    desc_error = _validate_limited_text(description, "任務描述")
    if desc_error:
        if _wants_json_response():
            return jsonify({"ok": False, "error": desc_error}), 400
        flash(desc_error, "danger")
        return redirect(next_url)
    old_description = str(task.get("description") or "")
    task["description"] = description
    save_standard_update(task_id, task)
    _record_standard_update_audit(
        "standard_update_update_description",
        task_id,
        {"old_description": old_description, "description": description},
    )
    if _wants_json_response():
        return jsonify({"ok": True, "task_id": task_id, "description": description, "message": "已更新標準更新任務描述"})
    flash("已更新標準更新任務描述", "success")
    return redirect(next_url)


@standard_updates_bp.post("/standards/<task_id>/use-latest-harmonised", endpoint="use_latest_harmonised")
def use_latest_harmonised(task_id: str):
    existing = load_standard_update(task_id)
    if not existing:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, existing)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    existing, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    if normalize_harmonised_source_mode(existing.get("harmonised_source_mode")) != HARMONISED_SOURCE_SYSTEM:
        flash("此任務為自行上傳模式，不能套用系統版本", "warning")
        return redirect(url_for("standard_updates_bp.detail", task_id=task_id))
    task = lock_standard_update_to_latest_harmonised(task_id)
    if not task:
        abort(404)
    task = _apply_ready_status(task_id, task)
    save_standard_update(task_id, task)
    task_release = get_task_harmonised_release(task_id, task)
    _record_standard_update_audit(
        "standard_update_use_latest_harmonised",
        task_id,
        {
            "version_label": task_release.get("version_label", ""),
            "path": task_release.get("path", ""),
            "status": task.get("status", ""),
        },
    )
    if task.get("harmonised_snapshot_path"):
        flash("已改用最新 harmonised 版本", "success")
    else:
        flash("目前找不到可用的 harmonised 版本", "warning")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/delete", endpoint="delete")
def delete(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    _record_standard_update_audit(
        "standard_update_delete",
        task_id,
        {
            "name": str(task.get("name") or "").strip(),
            "harmonised_source_mode": task.get("harmonised_source_mode", ""),
        },
    )
    delete_standard_update(task_id)
    flash("已刪除標準更新任務", "success")
    return redirect(url_for("standard_updates_bp.list"))


@standard_updates_bp.post("/standards/<task_id>/upload-word", endpoint="upload_word")
def upload_word(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    try:
        filename = save_uploaded_input(task_id, request.files.get("word_file"), kind="word")
        task["word_file_path"] = filename
        task = _apply_ready_status(task_id, task)
        save_standard_update(task_id, task)
        _record_standard_update_audit(
            "standard_update_upload_word",
            task_id,
            {"file_name": filename, "status": task.get("status", "")},
        )
        flash("Word 檔案已上傳", "success")
    except ValueError as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/upload-standard-excel", endpoint="upload_standard_excel")
def upload_standard_excel(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    try:
        filename = save_uploaded_input(task_id, request.files.get("standard_excel_file"), kind="excel")
        task["standard_excel_path"] = filename
        task = _apply_ready_status(task_id, task)
        save_standard_update(task_id, task)
        _record_standard_update_audit(
            "standard_update_upload_standard_excel",
            task_id,
            {"file_name": filename, "status": task.get("status", "")},
        )
        flash("Excel 標準總表已上傳", "success")
    except ValueError as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/upload-regulation-excel", endpoint="upload_regulation_excel")
def upload_regulation_excel(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    try:
        filename = save_uploaded_input(task_id, request.files.get("regulation_excel_file"), kind="regulation")
        task["regulation_excel_path"] = filename
        task = _apply_ready_status(task_id, task)
        save_standard_update(task_id, task)
        _record_standard_update_audit(
            "standard_update_upload_regulation_excel",
            task_id,
            {"file_name": filename, "status": task.get("status", "")},
        )
        flash("法規條文登記表已上傳", "success")
    except ValueError as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/upload-harmonised-excel", endpoint="upload_harmonised_excel")
def upload_harmonised_excel(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    if normalize_harmonised_source_mode(task.get("harmonised_source_mode")) != HARMONISED_SOURCE_CUSTOM:
        flash("此任務為系統檔案模式，不提供任務自訂採認標準上傳", "warning")
        return redirect(url_for("standard_updates_bp.detail", task_id=task_id))
    try:
        filename = save_uploaded_input(task_id, request.files.get("harmonised_excel_file"), kind="harmonised")
        task["custom_harmonised_path"] = ""
        task["custom_harmonised_version"] = ""
        task = _apply_ready_status(task_id, task)
        save_standard_update(task_id, task)
        _record_standard_update_audit(
            "standard_update_upload_harmonised_excel",
            task_id,
            {"file_name": filename, "status": task.get("status", "")},
        )
        flash("任務自訂採認標準已上傳", "success")
    except ValueError as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/files/delete", endpoint="delete_input_file")
def delete_uploaded_input_file(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    kind = (request.form.get("kind") or "").strip().lower()
    rel_path = (request.form.get("file_name") or "").strip()
    if kind not in {"word", "excel", "regulation", "harmonised"} or not rel_path:
        flash("缺少要刪除的檔案資訊", "danger")
        return redirect(url_for("standard_updates_bp.detail", task_id=task_id))
    try:
        delete_input_file(task_id, kind=kind, rel_path=rel_path)
        _record_standard_update_audit(
            "standard_update_delete_input_file",
            task_id,
            {"kind": kind, "file_name": rel_path},
        )
        flash("已移除檔案", "success")
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.route("/standards/<task_id>/mapping", methods=["GET", "POST"], endpoint="mapping")
def mapping(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    selected_word = (request.values.get("word_path") or task.get("word_file_path") or "").strip()
    selected_excel = (request.values.get("excel_path") or task.get("standard_excel_path") or "").strip()
    selected_regulation_excel = (request.values.get("regulation_excel_path") or task.get("regulation_excel_path") or "").strip()
    selected_harmonised_excel = (request.values.get("harmonised_excel_path") or "").strip()
    if normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_CUSTOM and not selected_harmonised_excel:
        selected_harmonised_excel = str(get_latest_uploaded_input(task_id, kind="harmonised").get("name") or "").strip()

    if request.method == "GET":
        try:
            iso_priority = _parse_iso_priority(request.values)
            enabled_standard_levels = _parse_enabled_standard_levels(request.values)
            required_headers = _parse_required_headers(request.values)
            manual_header_mappings = _parse_manual_header_mappings(request.values)
            limit_to_chapter = _parse_limit_to_chapter(request.values)
            manual_target_chapter_ref = str(request.values.get("manual_target_chapter_ref") or "").strip()
            target_chapter_ref = _parse_target_chapter_ref(request.values, limit_to_chapter=limit_to_chapter)
            target_table_index = _parse_target_table_index(request.values, limit_to_chapter=limit_to_chapter)
        except ValueError as exc:
            flash(str(exc), "danger")
            iso_priority = DEFAULT_ISO_PRIORITY
            enabled_standard_levels = DEFAULT_ENABLED_STANDARD_LEVELS
            required_headers = DEFAULT_REQUIRED_HEADERS
            limit_to_chapter = False
            target_chapter_ref = ""
            target_table_index = None
            manual_target_chapter_ref = ""
            manual_header_mappings = {}
        return _render_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_regulation_excel=selected_regulation_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )

    action = (request.form.get("action") or "preview").strip().lower()
    if action not in {"preview", "inspect_headers"}:
        return redirect(url_for("standard_updates_bp.mapping", task_id=task_id))

    try:
        iso_priority = _parse_iso_priority(request.form)
        enabled_standard_levels = _parse_enabled_standard_levels(request.form)
        required_headers = _parse_required_headers(request.form)
        manual_header_mappings = _parse_manual_header_mappings(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_chapter_ref = _parse_target_chapter_ref(request.form, limit_to_chapter=limit_to_chapter)
        target_table_index = _parse_target_table_index(request.form, limit_to_chapter=limit_to_chapter)
    except ValueError as exc:
        flash(str(exc), "danger")
        return _render_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_regulation_excel=selected_regulation_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=DEFAULT_ISO_PRIORITY,
            enabled_standard_levels=DEFAULT_ENABLED_STANDARD_LEVELS,
            required_headers=DEFAULT_REQUIRED_HEADERS,
        )

    try:
        word_path = safe_standard_update_file(task_id, selected_word, ALLOWED_WORD_EXTENSIONS, kind="word")
        if normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_CUSTOM:
            harmonised_reference_path = (
                safe_standard_update_file(task_id, selected_harmonised_excel, ALLOWED_EXCEL_EXTENSIONS, kind="harmonised")
                if selected_harmonised_excel
                else ""
            )
        else:
            task_harmonised_release = get_task_harmonised_release(task_id, task)
            harmonised_reference_path = task_harmonised_release.get("path")
        if not harmonised_reference_path and normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_SYSTEM:
            raise FileNotFoundError("目前任務沒有可用的 harmonised Excel，請先改用最新版本或上傳任務自訂檔案")
        if action == "inspect_headers":
            result = inspect_document_tables(
                word_path,
                target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
                target_table_index=target_table_index if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            _record_standard_update_audit(
                "standard_update_mapping_inspect_headers",
                task_id,
                {
                    "word_path": selected_word,
                    "target_chapter_ref": target_chapter_ref,
                    "target_table_index": _format_target_table_index_display(target_table_index),
                    "manual_header_mapping_count": len(manual_header_mappings),
                    "table_count": len(result.get("table_checks") or []),
                },
            )
            flash("欄位檢查完成，已列出目前偵測到的表格標頭", "info")
        else:
            inspection_result = inspect_document_tables(
                word_path,
                target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
                target_table_index=target_table_index if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            if _has_unresolved_manual_mapping(inspection_result.get("table_checks")):
                flash("尚有表格未符合預設四欄格式，請先完成手動對應欄位設定後再更新標準清單", "warning")
                return _render_mapping_page(
                    task_id,
                    preview_result=inspection_result,
                    selected_word=selected_word,
                    selected_excel=selected_excel,
                    selected_regulation_excel=selected_regulation_excel,
                    selected_harmonised_excel=selected_harmonised_excel,
                    iso_priority=iso_priority,
                    enabled_standard_levels=enabled_standard_levels,
                    required_headers=required_headers,
                    limit_to_chapter=limit_to_chapter,
                    target_chapter_ref=target_chapter_ref,
                    target_table_index=target_table_index,
                    manual_target_chapter_ref=manual_target_chapter_ref,
                    manual_header_mappings=manual_header_mappings,
                )
            excel_path = safe_standard_update_file(task_id, selected_excel, ALLOWED_EXCEL_EXTENSIONS, kind="standard_excel")
            if not selected_regulation_excel:
                raise FileNotFoundError("請先上傳並選擇各國法規條文登記表")
            regulation_reference_path = safe_standard_update_file(
                task_id,
                selected_regulation_excel,
                ALLOWED_EXCEL_EXTENSIONS,
                kind="regulation",
            )
            result = process_document(
                word_path,
                excel_path,
                harmonised_reference_path=harmonised_reference_path,
                regulation_reference_path=regulation_reference_path,
                iso_priority=iso_priority,
                enabled_standard_levels=enabled_standard_levels,
                required_headers=required_headers,
                target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
                target_table_index=target_table_index if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            task["status"] = STATUS_PREVIEWED
            save_standard_update(task_id, task)
            preview_stats = _build_stats(result.get("report", []))
            _record_standard_update_audit(
                "standard_update_mapping_preview",
                task_id,
                {
                    "word_path": selected_word,
                    "excel_path": selected_excel,
                    "regulation_excel_path": selected_regulation_excel,
                    "harmonised_excel_path": selected_harmonised_excel,
                    "target_chapter_ref": target_chapter_ref,
                    "target_table_index": _format_target_table_index_display(target_table_index),
                    "manual_header_mapping_count": len(manual_header_mappings),
                    "updated_count": preview_stats.get("updated", 0),
                    "same_count": preview_stats.get("same", 0),
                    "missing_count": preview_stats.get("missing", 0),
                },
            )
        if limit_to_chapter and not result.get("table_checks"):
            flash("指定章節下找不到可辨識的表格", "warning")
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return _render_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_regulation_excel=selected_regulation_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )
    except Exception as exc:
        current_app.logger.exception("Standard update preview failed")
        _record_standard_update_audit(
            "standard_update_mapping_preview_failed",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "regulation_excel_path": selected_regulation_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "manual_header_mapping_count": len(manual_header_mappings),
                "error": str(exc),
            },
        )
        flash(f"預覽失敗：{exc}", "danger")
        task["status"] = STATUS_FAILED
        save_standard_update(task_id, task)
        return _render_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_regulation_excel=selected_regulation_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )

    return _render_mapping_page(
        task_id,
        preview_result=result,
        selected_word=selected_word,
        selected_excel=selected_excel,
        selected_regulation_excel=selected_regulation_excel,
        selected_harmonised_excel=selected_harmonised_excel,
        iso_priority=iso_priority,
        enabled_standard_levels=enabled_standard_levels,
        required_headers=required_headers,
        limit_to_chapter=limit_to_chapter,
        target_chapter_ref=target_chapter_ref,
        target_table_index=target_table_index,
        manual_target_chapter_ref=manual_target_chapter_ref,
        manual_header_mappings=manual_header_mappings,
    )


@standard_updates_bp.post("/standards/<task_id>/download", endpoint="download_result")
def download_result(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    lock_result = _acquire_task_lock_or_respond(task_id, task)
    if len(lock_result) == 3:
        return lock_result[1], lock_result[2]
    task, blocked_response = lock_result
    if blocked_response is not None:
        return blocked_response
    selected_word = (request.form.get("word_path") or task.get("word_file_path") or "").strip()
    selected_excel = (request.form.get("excel_path") or task.get("standard_excel_path") or "").strip()
    selected_regulation_excel = (request.form.get("regulation_excel_path") or task.get("regulation_excel_path") or "").strip()
    selected_harmonised_excel = (request.form.get("harmonised_excel_path") or "").strip()
    if normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_CUSTOM and not selected_harmonised_excel:
        selected_harmonised_excel = str(get_latest_uploaded_input(task_id, kind="harmonised").get("name") or "").strip()

    try:
        iso_priority = _parse_iso_priority(request.form)
        enabled_standard_levels = _parse_enabled_standard_levels(request.form)
        required_headers = _parse_required_headers(request.form)
        manual_header_mappings = _parse_manual_header_mappings(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_chapter_ref = _parse_target_chapter_ref(request.form, limit_to_chapter=limit_to_chapter)
        target_table_index = _parse_target_table_index(request.form, limit_to_chapter=limit_to_chapter)
        override_map = _parse_override_map(request.form.get("overrides_json", ""))
        edit_map = _parse_edit_map(request.form.get("edits_json", ""))
        word_path = safe_standard_update_file(task_id, selected_word, ALLOWED_WORD_EXTENSIONS, kind="word")
        excel_path = safe_standard_update_file(task_id, selected_excel, ALLOWED_EXCEL_EXTENSIONS, kind="standard_excel")
        if normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_CUSTOM:
            harmonised_reference_path = (
                safe_standard_update_file(task_id, selected_harmonised_excel, ALLOWED_EXCEL_EXTENSIONS, kind="harmonised")
                if selected_harmonised_excel
                else ""
            )
        else:
            task_harmonised_release = get_task_harmonised_release(task_id, task)
            harmonised_reference_path = task_harmonised_release.get("path")
        if not selected_regulation_excel:
            raise FileNotFoundError("請先上傳並選擇各國法規條文登記表")
        regulation_reference_path = safe_standard_update_file(
            task_id,
            selected_regulation_excel,
            ALLOWED_EXCEL_EXTENSIONS,
            kind="regulation",
        )
        if not harmonised_reference_path and normalize_harmonised_source_mode(task.get("harmonised_source_mode")) == HARMONISED_SOURCE_SYSTEM:
            raise FileNotFoundError("目前任務沒有可用的 harmonised Excel，請先改用最新版本或上傳任務自訂檔案")
        inspection_result = inspect_document_tables(
            word_path,
            target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
            target_table_index=target_table_index if limit_to_chapter else None,
            manual_header_mappings=manual_header_mappings,
        )
        if _has_unresolved_manual_mapping(inspection_result.get("table_checks")):
            flash("尚有表格未符合預設四欄格式，請先完成手動對應欄位設定後再下載結果", "warning")
            return _render_mapping_page(
                task_id,
                preview_result=inspection_result,
                selected_word=selected_word,
                selected_excel=selected_excel,
                selected_regulation_excel=selected_regulation_excel,
                selected_harmonised_excel=selected_harmonised_excel,
                iso_priority=iso_priority,
                enabled_standard_levels=enabled_standard_levels,
                required_headers=required_headers,
                limit_to_chapter=limit_to_chapter,
                target_chapter_ref=target_chapter_ref,
                target_table_index=target_table_index,
                manual_target_chapter_ref=manual_target_chapter_ref,
                manual_header_mappings=manual_header_mappings,
            )

        output_dir = standard_update_output_dir(task_id)
        os.makedirs(output_dir, exist_ok=True)
        base_name = _safe_uploaded_filename(f"{Path(selected_word).stem}_updated.docx", default_stem="standard_mapping_updated")
        _remove_stale_output_variants(output_dir, base_name)
        output_path = os.path.join(output_dir, base_name)

        process_document(
            word_path=word_path,
            excel_path=excel_path,
            harmonised_reference_path=harmonised_reference_path,
            regulation_reference_path=regulation_reference_path,
            override_map=override_map,
            edit_map=edit_map,
            output_path=output_path,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
            target_table_index=target_table_index if limit_to_chapter else None,
            manual_header_mappings=manual_header_mappings,
        )
        task["status"] = STATUS_COMPLETED
        task["last_output_path"] = output_path
        task["last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        task["last_run_status"] = STATUS_COMPLETED
        save_standard_update(task_id, task)
        download_stats = _build_stats(inspection_result.get("report", []))
        _record_standard_update_audit(
            "standard_update_mapping_download",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "regulation_excel_path": selected_regulation_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "manual_header_mapping_count": len(manual_header_mappings),
                "updated_count": download_stats.get("updated", 0),
                "same_count": download_stats.get("same", 0),
                "missing_count": download_stats.get("missing", 0),
                "output_path": output_path,
            },
        )
    except (ValueError, FileNotFoundError) as exc:
        _record_standard_update_audit(
            "standard_update_mapping_download_failed",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "regulation_excel_path": selected_regulation_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "manual_header_mapping_count": len(manual_header_mappings),
                "error": str(exc),
            },
        )
        flash(str(exc), "danger")
        return redirect(url_for("standard_updates_bp.mapping", task_id=task_id))
    except Exception as exc:
        current_app.logger.exception("Standard update download failed")
        _record_standard_update_audit(
            "standard_update_mapping_download_failed",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "regulation_excel_path": selected_regulation_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "manual_header_mapping_count": len(manual_header_mappings),
                "error": str(exc),
            },
        )
        task["status"] = STATUS_FAILED
        task["last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        task["last_run_status"] = STATUS_FAILED
        save_standard_update(task_id, task)
        flash(f"下載失敗：{exc}", "danger")
        return redirect(url_for("standard_updates_bp.mapping", task_id=task_id))

    return send_file(output_path, as_attachment=True, download_name=base_name)


@standard_updates_bp.post("/standards/<task_id>/lock/refresh", endpoint="refresh_lock")
def refresh_lock(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    actor_id, work_id, actor_name = _resolve_lock_actor()
    ok, updated_task = refresh_standard_update_lock(
        task_id,
        actor_id,
        work_id=work_id,
        actor_name=actor_name,
    )
    if not ok:
        lock_state = _build_lock_state(task_id, updated_task or task)
        return jsonify({"ok": False, "error": _lock_block_message(lock_state) or "無法續鎖", "lock": lock_state}), 423
    return jsonify({"ok": True, "lock": _build_lock_state(task_id, updated_task)})


@standard_updates_bp.post("/standards/<task_id>/lock/release", endpoint="release_lock")
def release_lock(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    actor_id, _, _ = _resolve_lock_actor()
    ok, updated_task = release_standard_update_lock(task_id, actor_id)
    if ok:
        _record_standard_update_audit("standard_update_lock_release", task_id, {"actor_id": actor_id})
    if not _wants_json_response():
        next_url = (request.form.get("next") or request.referrer or "").strip()
        if not next_url:
            next_url = url_for("standard_updates_bp.detail", task_id=task_id)
        if ok:
            flash("已釋放任務鎖定", "success")
        else:
            flash("你目前未持有此任務鎖，無法釋放", "warning")
        return redirect(next_url)
    if not ok:
        return jsonify({"ok": False, "lock": _build_lock_state(task_id, updated_task or task)}), 409
    return jsonify({"ok": True, "lock": _build_lock_state(task_id, updated_task)})


@standard_updates_bp.post("/standards/<task_id>/lock/takeover", endpoint="takeover_lock")
def takeover_lock(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    if not user_is_admin(current_user):
        abort(403)

    actor_id, work_id, actor_name = _resolve_lock_actor()
    ok, updated_task = force_takeover_standard_update_lock(
        task_id,
        actor_id,
        work_id=work_id,
        actor_name=actor_name,
    )
    if not ok:
        abort(404)
    _record_standard_update_audit(
        "standard_update_lock_takeover",
        task_id,
        {"actor_id": actor_id, "work_id": work_id, "actor_name": actor_name},
    )

    if not _wants_json_response():
        next_url = (request.form.get("next") or request.referrer or "").strip()
        if not next_url:
            next_url = url_for("standard_updates_bp.mapping", task_id=task_id)
        flash("已強制接管此任務鎖定", "success")
        return redirect(next_url)
    return jsonify({"ok": True, "lock": _build_lock_state(task_id, updated_task)})
