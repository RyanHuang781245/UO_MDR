from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from flask import abort, current_app, flash, redirect, render_template, request, send_file, url_for

from app.blueprints.tasks.mapping_routes import _safe_uploaded_filename
from app.blueprints.tasks.standard_mapping_routes import (
    _MANUAL_HEADER_FIELD_ORDER,
    _STANDARD_PRIORITY_FIELDS,
    _build_stats,
    _has_unresolved_manual_mapping,
    _parse_iso_priority,
    _parse_limit_to_chapter,
    _parse_manual_header_mappings,
    _parse_override_map,
    _parse_required_headers,
    _parse_target_chapter_ref,
    _parse_target_table_index,
)
from app.services.standard_mapping_service import (
    DEFAULT_ISO_PRIORITY,
    DEFAULT_REQUIRED_HEADERS,
    inspect_document_sections,
    inspect_document_tables,
    normalize_iso_priority,
    normalize_manual_header_mappings,
    normalize_required_headers,
    process_document,
)
from app.services.standard_update_service import (
    ALLOWED_EXCEL_EXTENSIONS,
    ALLOWED_WORD_EXTENSIONS,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PREVIEWED,
    STATUS_READY,
    available_input_files,
    create_standard_update,
    delete_standard_update,
    get_active_harmonised_release,
    get_locked_harmonised_release,
    input_file_history,
    lock_standard_update_to_latest_harmonised,
    list_standard_updates,
    load_standard_update,
    safe_standard_update_file,
    save_standard_update,
    save_uploaded_input,
    standard_update_name_exists,
    standard_update_output_dir,
    sync_harmonised_release_snapshot,
)

from .blueprint import standard_updates_bp


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


def _load_chapter_options(task_id: str, selected_word: str) -> tuple[list[dict], str]:
    if not selected_word:
        return [], ""
    try:
        word_path = safe_standard_update_file(task_id, selected_word, ALLOWED_WORD_EXTENSIONS)
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


def _render_mapping_page(
    task_id: str,
    *,
    preview_result: dict | None = None,
    selected_word: str = "",
    selected_excel: str = "",
    iso_priority: tuple[str, ...] | list[str] | None = None,
    required_headers: tuple[str, ...] | list[str] | None = None,
    limit_to_chapter: bool = False,
    target_chapter_ref: str = "",
    target_table_index: int | None = None,
    manual_target_chapter_ref: str = "",
    manual_header_mappings: dict[int, dict[str, str]] | None = None,
):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    word_options, excel_options = available_input_files(task_id)
    chapter_options, chapter_options_error = _load_chapter_options(task_id, selected_word)
    harmonised_release = get_locked_harmonised_release(task)
    reference_payload = (preview_result or {}).get("reference_payload", {})
    active_iso_priority = tuple((preview_result or {}).get("iso_priority") or normalize_iso_priority(iso_priority))
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
        page_title="標準更新預覽",
        page_description="使用獨立標準更新任務的上傳檔案與系統 harmonised 參照表產生預覽或下載結果。",
        task_label="標準更新任務",
        missing_file_hint="請先上傳 Word 與 Excel 檔案。",
        word_options=word_options,
        excel_options=excel_options,
        selected_word=selected_word,
        selected_excel=selected_excel,
        selected_harmonised_excel=harmonised_release.get("file_name", ""),
        preview_tables=(preview_result or {}).get("preview_tables", []),
        table_checks=table_checks,
        reference_payload=reference_payload,
        stats=_build_stats((preview_result or {}).get("report", [])) if preview_result else {"updated": 0, "same": 0, "missing": 0, "total": 0},
        interactive_rows=interactive_rows,
        interactive_fields=len(reference_payload),
        has_preview=bool(preview_result and (preview_result.get("preview_tables") or [])),
        last_generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if preview_result else "",
        iso_priority=active_iso_priority,
        iso_priority_positions=iso_priority_positions,
        standard_priority_fields=_STANDARD_PRIORITY_FIELDS,
        required_headers=active_required_headers,
        default_required_headers=DEFAULT_REQUIRED_HEADERS,
        limit_to_chapter=bool((preview_result or {}).get("target_chapter_ref") or limit_to_chapter),
        target_chapter_ref=(preview_result or {}).get("target_chapter_ref", target_chapter_ref),
        target_table_index=(preview_result or {}).get("target_table_index", target_table_index),
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
        use_system_harmonised=True,
        harmonised_system_release=harmonised_release,
    )


@standard_updates_bp.route("/standards", methods=["GET", "POST"], endpoint="list")
def list_page():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = (request.form.get("description") or "").strip()
        if not name:
            flash("請輸入標準更新任務名稱", "danger")
            return redirect(url_for("standard_updates_bp.list"))
        if standard_update_name_exists(name):
            flash("標準更新任務名稱已存在", "danger")
            return redirect(url_for("standard_updates_bp.list"))
        task_id = create_standard_update(name, description)
        flash("已建立標準更新任務", "success")
        return redirect(url_for("standard_updates_bp.detail", task_id=task_id))

    items = list_standard_updates()
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
    word_options, excel_options = available_input_files(task_id)
    harmonised_release = sync_harmonised_release_snapshot()
    locked_harmonised_release = get_locked_harmonised_release(task)
    has_newer_harmonised = bool(
        harmonised_release.get("path")
        and harmonised_release.get("path") != task.get("harmonised_snapshot_path", "")
    )
    is_ready = bool(task.get("word_file_path") and task.get("standard_excel_path") and locked_harmonised_release.get("path"))
    return render_template(
        "standard_updates/detail.html",
        task=task,
        word_options=word_options,
        excel_options=excel_options,
        word_history=input_file_history(task_id, kind="word", current_file=task.get("word_file_path", "")),
        standard_excel_history=input_file_history(task_id, kind="excel", current_file=task.get("standard_excel_path", "")),
        harmonised_release=harmonised_release,
        locked_harmonised_release=locked_harmonised_release,
        has_newer_harmonised=has_newer_harmonised,
        is_ready=is_ready,
    )


@standard_updates_bp.post("/standards/<task_id>/use-latest-harmonised", endpoint="use_latest_harmonised")
def use_latest_harmonised(task_id: str):
    task = lock_standard_update_to_latest_harmonised(task_id)
    if not task:
        abort(404)
    if task.get("harmonised_snapshot_path"):
        flash("已改用最新 harmonised 版本", "success")
    else:
        flash("目前找不到可用的 harmonised 版本", "warning")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/delete", endpoint="delete")
def delete(task_id: str):
    if not load_standard_update(task_id):
        abort(404)
    delete_standard_update(task_id)
    flash("已刪除標準更新任務", "success")
    return redirect(url_for("standard_updates_bp.list"))


@standard_updates_bp.post("/standards/<task_id>/upload-word", endpoint="upload_word")
def upload_word(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    try:
        filename = save_uploaded_input(task_id, request.files.get("word_file"), kind="word")
        task["word_file_path"] = filename
        task["status"] = STATUS_READY if task.get("standard_excel_path") else task.get("status", STATUS_READY)
        save_standard_update(task_id, task)
        flash("Word 檔案已上傳", "success")
    except ValueError as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.post("/standards/<task_id>/upload-standard-excel", endpoint="upload_standard_excel")
def upload_standard_excel(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    try:
        filename = save_uploaded_input(task_id, request.files.get("standard_excel_file"), kind="excel")
        task["standard_excel_path"] = filename
        task["status"] = STATUS_READY if task.get("word_file_path") else task.get("status", STATUS_READY)
        save_standard_update(task_id, task)
        flash("Excel 標準總表已上傳", "success")
    except ValueError as exc:
        flash(str(exc), "danger")
    return redirect(url_for("standard_updates_bp.detail", task_id=task_id))


@standard_updates_bp.route("/standards/<task_id>/mapping", methods=["GET", "POST"], endpoint="mapping")
def mapping(task_id: str):
    task = load_standard_update(task_id)
    if not task:
        abort(404)
    selected_word = (request.values.get("word_path") or task.get("word_file_path") or "").strip()
    selected_excel = (request.values.get("excel_path") or task.get("standard_excel_path") or "").strip()

    if request.method == "GET":
        try:
            iso_priority = _parse_iso_priority(request.values)
            required_headers = _parse_required_headers(request.values)
            manual_header_mappings = _parse_manual_header_mappings(request.values)
            limit_to_chapter = _parse_limit_to_chapter(request.values)
            manual_target_chapter_ref = str(request.values.get("manual_target_chapter_ref") or "").strip()
            target_chapter_ref = _parse_target_chapter_ref(request.values, limit_to_chapter=limit_to_chapter)
            target_table_index = _parse_target_table_index(request.values, limit_to_chapter=limit_to_chapter)
        except ValueError as exc:
            flash(str(exc), "danger")
            iso_priority = DEFAULT_ISO_PRIORITY
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
            iso_priority=iso_priority,
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
            iso_priority=DEFAULT_ISO_PRIORITY,
            required_headers=DEFAULT_REQUIRED_HEADERS,
        )

    try:
        word_path = safe_standard_update_file(task_id, selected_word, ALLOWED_WORD_EXTENSIONS)
        locked_harmonised_release = get_locked_harmonised_release(task)
        harmonised_reference_path = locked_harmonised_release.get("path")
        if not harmonised_reference_path:
            raise FileNotFoundError("目前任務鎖定的 harmonised Excel 不存在，請先改用最新版本")
        if action == "inspect_headers":
            result = inspect_document_tables(
                word_path,
                target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
                target_table_index=target_table_index if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            flash("欄位檢查完成，已列出目前偵測到的表格標頭。", "info")
        else:
            inspection_result = inspect_document_tables(
                word_path,
                target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
                target_table_index=target_table_index if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            if _has_unresolved_manual_mapping(inspection_result.get("table_checks")):
                flash("尚有表格未符合預設四欄格式，請先完成手動對應欄位設定後再更新標準清單。", "warning")
                return _render_mapping_page(
                    task_id,
                    preview_result=inspection_result,
                    selected_word=selected_word,
                    selected_excel=selected_excel,
                    iso_priority=iso_priority,
                    required_headers=required_headers,
                    limit_to_chapter=limit_to_chapter,
                    target_chapter_ref=target_chapter_ref,
                    target_table_index=target_table_index,
                    manual_target_chapter_ref=manual_target_chapter_ref,
                    manual_header_mappings=manual_header_mappings,
                )
            excel_path = safe_standard_update_file(task_id, selected_excel, ALLOWED_EXCEL_EXTENSIONS)
            result = process_document(
                word_path,
                excel_path,
                harmonised_reference_path=harmonised_reference_path,
                iso_priority=iso_priority,
                required_headers=required_headers,
                target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
                target_table_index=target_table_index if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            task["status"] = STATUS_PREVIEWED
            save_standard_update(task_id, task)
        if limit_to_chapter and not result.get("table_checks"):
            flash("指定章節下找不到可辨識的表格", "warning")
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return _render_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            iso_priority=iso_priority,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )
    except Exception as exc:
        current_app.logger.exception("Standard update preview failed")
        flash(f"預覽失敗：{exc}", "danger")
        task["status"] = STATUS_FAILED
        save_standard_update(task_id, task)
        return _render_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            iso_priority=iso_priority,
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
        iso_priority=iso_priority,
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
    selected_word = (request.form.get("word_path") or task.get("word_file_path") or "").strip()
    selected_excel = (request.form.get("excel_path") or task.get("standard_excel_path") or "").strip()

    try:
        iso_priority = _parse_iso_priority(request.form)
        required_headers = _parse_required_headers(request.form)
        manual_header_mappings = _parse_manual_header_mappings(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_chapter_ref = _parse_target_chapter_ref(request.form, limit_to_chapter=limit_to_chapter)
        target_table_index = _parse_target_table_index(request.form, limit_to_chapter=limit_to_chapter)
        override_map = _parse_override_map(request.form.get("overrides_json", ""))
        word_path = safe_standard_update_file(task_id, selected_word, ALLOWED_WORD_EXTENSIONS)
        excel_path = safe_standard_update_file(task_id, selected_excel, ALLOWED_EXCEL_EXTENSIONS)
        locked_harmonised_release = get_locked_harmonised_release(task)
        harmonised_reference_path = locked_harmonised_release.get("path")
        if not harmonised_reference_path:
            raise FileNotFoundError("目前任務鎖定的 harmonised Excel 不存在，請先改用最新版本")
        inspection_result = inspect_document_tables(
            word_path,
            target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
            target_table_index=target_table_index if limit_to_chapter else None,
            manual_header_mappings=manual_header_mappings,
        )
        if _has_unresolved_manual_mapping(inspection_result.get("table_checks")):
            flash("尚有表格未符合預設四欄格式，請先完成手動對應欄位設定後再下載結果。", "warning")
            return _render_mapping_page(
                task_id,
                preview_result=inspection_result,
                selected_word=selected_word,
                selected_excel=selected_excel,
                iso_priority=iso_priority,
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
        output_name = base_name
        counter = 1
        while os.path.exists(os.path.join(output_dir, output_name)):
            output_name = f"{Path(base_name).stem} ({counter}){Path(base_name).suffix}"
            counter += 1
        output_path = os.path.join(output_dir, output_name)

        process_document(
            word_path=word_path,
            excel_path=excel_path,
            harmonised_reference_path=harmonised_reference_path,
            override_map=override_map,
            output_path=output_path,
            iso_priority=iso_priority,
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
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("standard_updates_bp.mapping", task_id=task_id))
    except Exception as exc:
        current_app.logger.exception("Standard update download failed")
        task["status"] = STATUS_FAILED
        task["last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        task["last_run_status"] = STATUS_FAILED
        save_standard_update(task_id, task)
        flash(f"下載失敗：{exc}", "danger")
        return redirect(url_for("standard_updates_bp.mapping", task_id=task_id))

    return send_file(output_path, as_attachment=True, download_name=output_name)
