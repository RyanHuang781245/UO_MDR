from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

from flask import abort, current_app, flash, redirect, render_template, request, send_file, url_for

from app.services.audit_service import record_audit
from app.services.standard_mapping_service import (
    DEFAULT_ENABLED_STANDARD_LEVELS,
    DEFAULT_REQUIRED_HEADERS,
    DEFAULT_ISO_PRIORITY,
    HEADER_FIELD_IDS,
    inspect_document_tables,
    inspect_document_sections,
    normalize_iso_priority,
    normalize_enabled_standard_levels,
    normalize_manual_header_mappings,
    normalize_required_headers,
    process_document,
)
from app.services.task_service import deduplicate_name, list_files, load_task_context as _load_task_context
from app.services.user_context_service import get_actor_info
from app.blueprints.task_mapping.upload_helpers import _safe_uploaded_filename
from app.blueprints.tasks.blueprint import tasks_bp

_ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xltx", ".xltm"}
_MANUAL_HEADER_FIELD_ORDER = [
    ("Standards", HEADER_FIELD_IDS["Standards"]),
    ("Issued Year", HEADER_FIELD_IDS["Issued Year"]),
    ("EU Harmonised Standards under MDR 2017/745 (YES/NO)", HEADER_FIELD_IDS["EU Harmonised Standards under MDR 2017/745 (YES/NO)"]),
    ("Title", HEADER_FIELD_IDS["Title"]),
]
_STANDARD_PRIORITY_FIELDS = {
    "BS EN ISO": "priority_bs_en_iso",
    "BS EN": "priority_bs_en",
    "EN": "priority_en",
    "EN ISO": "priority_en_iso",
    "BS ISO": "priority_bs_iso",
    "ISO": "priority_iso",
    "BS": "priority_bs",
}


def _current_actor() -> dict[str, str]:
    work_id, label = get_actor_info()
    return {"work_id": work_id, "label": label}


def _record_task_standard_mapping_audit(action: str, task_id: str, detail: dict | None = None) -> None:
    payload = {"task_id": task_id}
    payload.update(detail or {})
    record_audit(
        action=action,
        actor=_current_actor(),
        detail=payload,
        task_id=task_id,
    )


def _resolve_regulation_reference_path(files_dir: str | None = None) -> str | None:
    configured = str(current_app.config.get("REGULATION_REFERENCE_PATH") or "").strip()
    if configured and os.path.isfile(configured):
        return configured
    if files_dir:
        candidate = os.path.join(files_dir, "各國法規條文登記表_20250801.xlsx")
        if os.path.isfile(candidate):
            return candidate
    return None


def _task_files_dir(task_id: str) -> str:
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(task_dir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    return files_dir


def _safe_task_file(files_dir: str, rel_path: str, allowed_exts: set[str]) -> str:
    normalized = os.path.normpath((rel_path or "").replace("/", os.sep))
    if not normalized or normalized.startswith("..") or os.path.isabs(normalized):
        raise ValueError("檔案路徑不合法")
    abs_path = os.path.abspath(os.path.join(files_dir, normalized))
    base_dir = os.path.abspath(files_dir)
    try:
        if os.path.commonpath([base_dir, abs_path]) != base_dir:
            raise ValueError("檔案路徑不合法")
    except ValueError as exc:
        raise ValueError("檔案路徑不合法") from exc
    ext = Path(abs_path).suffix.lower()
    if ext not in allowed_exts:
        raise ValueError("檔案類型不支援")
    if not os.path.isfile(abs_path):
        raise FileNotFoundError("找不到指定檔案")
    return abs_path


def _list_standard_mapping_files(files_dir: str) -> tuple[list[str], list[str]]:
    all_files = list_files(files_dir)
    word_options = [rel for rel in all_files if rel.lower().endswith(".docx")]
    excel_options = [rel for rel in all_files if Path(rel).suffix.lower() in _ALLOWED_EXCEL_EXTENSIONS]
    return word_options, excel_options


def _parse_override_map(raw_value: str) -> dict[str, str]:
    if not (raw_value or "").strip():
        return {}
    payload = json.loads(raw_value)
    if isinstance(payload, dict) and isinstance(payload.get("overrides"), dict):
        return {str(key): str(value) for key, value in payload["overrides"].items()}
    if isinstance(payload, dict):
        return {str(key): str(value) for key, value in payload.items()}
    return {}


def _parse_edit_map(raw_value: str) -> dict[str, dict[str, str]]:
    if not (raw_value or "").strip():
        return {}
    payload = json.loads(raw_value)
    if isinstance(payload, dict) and isinstance(payload.get("edits"), dict):
        payload = payload["edits"]
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, str]] = {}
    allowed_fields = {"standards", "issued_year", "harmonised", "title"}
    for row_key, value in payload.items():
        if not isinstance(value, dict):
            continue
        fields = {
            str(field): str(field_value)
            for field, field_value in value.items()
            if str(field) in allowed_fields
        }
        if fields:
            normalized[str(row_key)] = fields
    return normalized


def _parse_iso_priority(values) -> tuple[str, ...]:
    raw_positions = {
        label: (values.get(field_name) or "").strip()
        for label, field_name in _STANDARD_PRIORITY_FIELDS.items()
    }
    if not any(raw_positions.values()):
        return DEFAULT_ISO_PRIORITY

    try:
        positions = {label: int(position) for label, position in raw_positions.items()}
    except ValueError as exc:
        raise ValueError("優先級設定格式不正確") from exc

    if sorted(positions.values()) != list(range(1, len(_STANDARD_PRIORITY_FIELDS) + 1)):
        raise ValueError("優先級設定不可重複")

    ordered = [label for label, _ in sorted(positions.items(), key=lambda item: item[1])]
    return normalize_iso_priority(ordered)


def _parse_enabled_standard_levels(values) -> tuple[str, ...]:
    if hasattr(values, "getlist"):
        raw_levels = [str(item).strip() for item in values.getlist("enabled_standard_levels") if str(item).strip()]
    else:
        single_value = str(values.get("enabled_standard_levels") or "").strip()
        raw_levels = [single_value] if single_value else []
    has_marker = bool(str(values.get("enabled_standard_levels_present") or "").strip())
    if not raw_levels and not has_marker:
        return DEFAULT_ENABLED_STANDARD_LEVELS
    if not raw_levels:
        return ()
    return normalize_enabled_standard_levels(raw_levels)


def _parse_limit_to_chapter(values) -> bool:
    if hasattr(values, "getlist"):
        raw_values = [str(item).strip().lower() for item in values.getlist("limit_to_chapter") if str(item).strip()]
        if raw_values:
            return raw_values[-1] in {"1", "true", "on", "yes"}
        return False
    raw_value = str(values.get("limit_to_chapter") or "").strip().lower()
    return raw_value in {"1", "true", "on", "yes"}


def _parse_target_chapter_ref(values, *, limit_to_chapter: bool) -> str:
    manual_target_chapter_ref = str(values.get("manual_target_chapter_ref") or "").strip()
    if manual_target_chapter_ref:
        return manual_target_chapter_ref
    target_chapter_ref = str(values.get("target_chapter_ref") or "").strip()
    if limit_to_chapter and not target_chapter_ref:
        raise ValueError("請選擇或輸入指定章節")
    return target_chapter_ref


def _parse_table_index_expression(raw_value: str) -> tuple[int, ...] | None:
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return None
    resolved: list[int] = []
    seen: set[int] = set()
    for chunk in re.split(r"\s*,\s*", raw_value):
        part = chunk.strip()
        if not part:
            continue
        range_match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", part)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if start <= 0 or end <= 0:
                raise ValueError("表格索引必須大於 0")
            if start > end:
                raise ValueError("表格索引範圍格式不正確")
            for value in range(start, end + 1):
                if value not in seen:
                    seen.add(value)
                    resolved.append(value)
            continue
        try:
            value = int(part)
        except ValueError as exc:
            raise ValueError("表格索引只支援正整數、逗號清單或範圍格式，例如 1,3,5 或 2-4") from exc
        if value <= 0:
            raise ValueError("表格索引必須大於 0")
        if value not in seen:
            seen.add(value)
            resolved.append(value)
    if not resolved:
        return None
    return tuple(resolved)


def _parse_target_table_index(values, *, limit_to_chapter: bool) -> tuple[int, ...] | None:
    if not limit_to_chapter:
        return None
    return _parse_table_index_expression(str(values.get("target_table_index") or "").strip())


def _get_form_list(values, name: str) -> list[str]:
    if hasattr(values, "getlist"):
        return [str(item).strip() for item in values.getlist(name)]
    value = str(values.get(name) or "").strip()
    return [value] if value else []


def _parse_target_scopes(values, *, limit_to_chapter: bool) -> tuple[dict, ...]:
    if not limit_to_chapter:
        return ()

    selected_refs = _get_form_list(values, "target_scope_chapter_ref")
    manual_refs = _get_form_list(values, "manual_target_scope_chapter_ref")
    table_indexes = _get_form_list(values, "target_scope_table_index")
    has_scope_rows = bool(selected_refs or manual_refs or table_indexes)

    if has_scope_rows:
        scopes: list[dict] = []
        row_count = max(len(selected_refs), len(manual_refs), len(table_indexes))
        for index in range(row_count):
            selected_ref = selected_refs[index] if index < len(selected_refs) else ""
            manual_ref = manual_refs[index] if index < len(manual_refs) else ""
            table_index_text = table_indexes[index] if index < len(table_indexes) else ""
            chapter_ref = manual_ref or selected_ref
            if not chapter_ref and table_index_text:
                raise ValueError("已輸入表格索引時，請同時選擇或輸入指定章節")
            if not chapter_ref:
                continue
            scopes.append({
                "chapter_ref": chapter_ref,
                "table_indexes": _parse_table_index_expression(table_index_text),
            })
        if not scopes:
            raise ValueError("請選擇或輸入指定章節")
        return tuple(scopes)

    chapter_ref = _parse_target_chapter_ref(values, limit_to_chapter=limit_to_chapter)
    table_index = _parse_target_table_index(values, limit_to_chapter=limit_to_chapter)
    return ({"chapter_ref": chapter_ref, "table_indexes": table_index},) if chapter_ref else ()


def _format_target_table_index_display(value) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        parts = []
        for item in value:
            text = str(item).strip()
            if text:
                parts.append(text)
        return ",".join(parts)
    return str(value).strip()


def _format_target_scopes_display(scopes) -> str:
    parts: list[str] = []
    for scope in scopes or []:
        if not isinstance(scope, dict):
            continue
        chapter_ref = str(scope.get("chapter_ref") or scope.get("target_chapter_ref") or "").strip()
        if not chapter_ref:
            continue
        table_text = _format_target_table_index_display(scope.get("table_indexes") or scope.get("target_table_index"))
        parts.append(f"{chapter_ref}（表格 {table_text}）" if table_text else chapter_ref)
    return "；".join(parts)


def _normalize_target_scope_rows(scopes) -> list[dict]:
    rows: list[dict] = []
    for scope in scopes or []:
        if not isinstance(scope, dict):
            continue
        chapter_ref = str(scope.get("chapter_ref") or scope.get("target_chapter_ref") or "").strip()
        if not chapter_ref:
            continue
        table_indexes = scope.get("table_indexes") if "table_indexes" in scope else scope.get("target_table_index")
        rows.append({
            "chapter_ref": chapter_ref,
            "table_indexes": table_indexes,
            "table_index_display": _format_target_table_index_display(table_indexes),
        })
    return rows


def _load_chapter_options(files_dir: str, selected_word: str) -> tuple[list[dict], str]:
    if not selected_word:
        return [], ""
    try:
        word_path = _safe_task_file(files_dir, selected_word, {".docx"})
    except (ValueError, FileNotFoundError):
        return [], ""
    try:
        options = inspect_document_sections(word_path)
    except Exception:
        current_app.logger.exception("Failed to inspect document sections for standard mapping")
        return [], "章節清單讀取失敗"
    if not options:
        return [], "找不到可辨識的章節清單"
    return options, ""


def _parse_required_headers(values) -> tuple[str, ...]:
    raw_headers = []
    if hasattr(values, "getlist"):
        raw_headers = [str(item).strip() for item in values.getlist("required_headers") if str(item).strip()]
    elif values.get("required_headers"):
        raw_headers = [str(values.get("required_headers")).strip()]

    required_headers = normalize_required_headers(raw_headers)
    if "Standards" not in required_headers or "Issued Year" not in required_headers:
        raise ValueError("表格辨識欄位至少必須包含 Standards 與 Issued Year")
    return required_headers


def _parse_manual_header_mappings(values) -> dict[int, dict[str, str]]:
    parsed: dict[int, dict[str, str]] = {}
    keys = values.keys() if hasattr(values, "keys") else []
    prefix = "manual_header_map__"
    for key in keys:
        if not str(key).startswith(prefix):
            continue
        parts = str(key).split("__", 2)
        if len(parts) != 3:
            continue
        _, raw_table_index, field_id = parts
        try:
            table_index = int(raw_table_index)
        except ValueError:
            continue
        value = str(values.get(key) or "").strip()
        if not value:
            continue
        parsed.setdefault(table_index, {})[field_id] = value
    return normalize_manual_header_mappings(parsed)


def _build_stats(report: list[dict]) -> dict[str, int]:
    stats = {"updated": 0, "same": 0, "missing": 0}
    for item in report:
        status = item.get("status")
        if status == "UPDATED":
            stats["updated"] += 1
        elif status == "SAME_NO_UPDATE":
            stats["same"] += 1
        elif status == "NOT_FOUND":
            stats["missing"] += 1
    stats["total"] = len(report)
    return stats


def _has_unresolved_manual_mapping(table_checks: list[dict] | None) -> bool:
    for item in table_checks or []:
        if item.get("needs_manual_mapping") and not item.get("manual_mapping_ready") and not item.get("is_full_match"):
            return True
    return False


def _render_standard_mapping_page(
    task_id: str,
    *,
    preview_result: dict | None = None,
    selected_word: str = "",
    selected_excel: str = "",
    selected_harmonised_excel: str = "",
    iso_priority: tuple[str, ...] | list[str] | None = None,
    enabled_standard_levels: tuple[str, ...] | list[str] | None = None,
    required_headers: tuple[str, ...] | list[str] | None = None,
    limit_to_chapter: bool = False,
    target_chapter_ref: str = "",
    target_table_index: tuple[int, ...] | list[int] | int | None = None,
    target_scopes: tuple[dict, ...] | list[dict] | None = None,
    manual_target_chapter_ref: str = "",
    manual_header_mappings: dict[int, dict[str, str]] | None = None,
):
    files_dir = _task_files_dir(task_id)
    word_options, excel_options = _list_standard_mapping_files(files_dir)
    chapter_options, chapter_options_error = _load_chapter_options(files_dir, selected_word)
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
    active_target_scopes = list((preview_result or {}).get("target_scopes") or target_scopes or [])
    active_target_scope_display = (preview_result or {}).get("target_scope_display") or _format_target_scopes_display(active_target_scopes)
    active_target_chapter_ref = (preview_result or {}).get("target_chapter_ref", target_chapter_ref)
    active_target_table_index = (preview_result or {}).get("target_table_index", target_table_index)
    if not active_target_scopes and active_target_chapter_ref:
        active_target_scopes = [{
            "chapter_ref": active_target_chapter_ref,
            "table_indexes": active_target_table_index,
        }]
    active_target_scope_rows = _normalize_target_scope_rows(active_target_scopes)
    return render_template(
        "standard_mapping/standard_mapping.html",
        task_id=task_id,
        task=_load_task_context(task_id),
        word_options=word_options,
        excel_options=excel_options,
        selected_word=selected_word,
        selected_excel=selected_excel,
        selected_harmonised_excel=(preview_result or {}).get("harmonised_reference_path", selected_harmonised_excel),
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
        limit_to_chapter=bool(active_target_scope_rows or active_target_chapter_ref or limit_to_chapter),
        target_chapter_ref=active_target_chapter_ref,
        target_table_index=active_target_table_index,
        target_table_index_display=_format_target_table_index_display(active_target_table_index),
        target_scopes=active_target_scope_rows,
        target_scope_display=active_target_scope_display,
        manual_target_chapter_ref=manual_target_chapter_ref,
        scope_table_count=(preview_result or {}).get("scope_table_count", 0),
        chapter_options=chapter_options,
        chapter_options_error=chapter_options_error,
        manual_header_mappings=active_manual_header_mappings,
        manual_header_field_order=_MANUAL_HEADER_FIELD_ORDER,
        has_unresolved_manual_mapping=_has_unresolved_manual_mapping(table_checks),
    )


@tasks_bp.route("/tasks/<task_id>/standard-mapping", methods=["GET", "POST"], endpoint="task_standard_mapping")
def task_standard_mapping(task_id):
    files_dir = _task_files_dir(task_id)
    selected_word = (request.values.get("word_path") or "").strip()
    selected_excel = (request.values.get("excel_path") or "").strip()
    selected_harmonised_excel = (request.values.get("harmonised_excel_path") or "").strip()

    if request.method == "GET":
        try:
            iso_priority = _parse_iso_priority(request.values)
            enabled_standard_levels = _parse_enabled_standard_levels(request.values)
            required_headers = _parse_required_headers(request.values)
            manual_header_mappings = _parse_manual_header_mappings(request.values)
            limit_to_chapter = _parse_limit_to_chapter(request.values)
            manual_target_chapter_ref = str(request.values.get("manual_target_chapter_ref") or "").strip()
            target_scopes = _parse_target_scopes(request.values, limit_to_chapter=limit_to_chapter)
            target_chapter_ref = _format_target_scopes_display(target_scopes)
            target_table_index = None
        except ValueError as exc:
            flash(str(exc), "danger")
            iso_priority = DEFAULT_ISO_PRIORITY
            enabled_standard_levels = DEFAULT_ENABLED_STANDARD_LEVELS
            required_headers = DEFAULT_REQUIRED_HEADERS
            limit_to_chapter = False
            target_chapter_ref = ""
            target_table_index = None
            target_scopes = ()
            manual_target_chapter_ref = ""
            manual_header_mappings = {}
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            target_scopes=target_scopes,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )

    action = (request.form.get("action") or "preview").strip().lower()
    if action not in {"preview", "inspect_headers"}:
        return redirect(url_for("tasks_bp.task_standard_mapping", task_id=task_id))

    try:
        iso_priority = _parse_iso_priority(request.form)
        enabled_standard_levels = _parse_enabled_standard_levels(request.form)
        required_headers = _parse_required_headers(request.form)
        manual_header_mappings = _parse_manual_header_mappings(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_scopes = _parse_target_scopes(request.form, limit_to_chapter=limit_to_chapter)
        target_chapter_ref = _format_target_scopes_display(target_scopes)
        target_table_index = None
    except ValueError as exc:
        flash(str(exc), "danger")
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=DEFAULT_ISO_PRIORITY,
            enabled_standard_levels=DEFAULT_ENABLED_STANDARD_LEVELS,
            required_headers=DEFAULT_REQUIRED_HEADERS,
            limit_to_chapter=False,
            target_chapter_ref="",
            target_table_index=None,
            target_scopes=(),
            manual_target_chapter_ref="",
            manual_header_mappings={},
        )

    try:
        word_path = _safe_task_file(files_dir, selected_word, {".docx"})
        if action == "inspect_headers":
            result = inspect_document_tables(
                word_path,
                target_scopes=target_scopes if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            _record_task_standard_mapping_audit(
                "task_standard_mapping_inspect_headers",
                task_id,
                {
                    "word_path": selected_word,
                    "target_chapter_ref": target_chapter_ref,
                    "target_table_index": _format_target_table_index_display(target_table_index),
                    "target_scopes": list(target_scopes),
                    "manual_header_mapping_count": len(manual_header_mappings),
                    "table_count": len(result.get("table_checks") or []),
                },
            )
            flash("欄位檢查完成，已列出目前偵測到的表格標頭。", "info")
        else:
            inspection_result = inspect_document_tables(
                word_path,
                target_scopes=target_scopes if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            if _has_unresolved_manual_mapping(inspection_result.get("table_checks")):
                flash("尚有表格未符合預設四欄格式，請先完成手動對應欄位設定後再更新標準清單。", "warning")
                return _render_standard_mapping_page(
                    task_id,
                    preview_result=inspection_result,
                    selected_word=selected_word,
                    selected_excel=selected_excel,
                    selected_harmonised_excel=selected_harmonised_excel,
                    iso_priority=iso_priority,
                    enabled_standard_levels=enabled_standard_levels,
                    required_headers=required_headers,
                    limit_to_chapter=limit_to_chapter,
                    target_chapter_ref=target_chapter_ref,
                    target_table_index=target_table_index,
                    target_scopes=target_scopes,
                    manual_target_chapter_ref=manual_target_chapter_ref,
                    manual_header_mappings=manual_header_mappings,
                )
            excel_path = _safe_task_file(files_dir, selected_excel, _ALLOWED_EXCEL_EXTENSIONS)
            harmonised_reference_path = _safe_task_file(files_dir, selected_harmonised_excel, _ALLOWED_EXCEL_EXTENSIONS) if selected_harmonised_excel else None
            regulation_reference_path = _resolve_regulation_reference_path(files_dir)
            result = process_document(
                word_path,
                excel_path,
                harmonised_reference_path=harmonised_reference_path,
                regulation_reference_path=regulation_reference_path,
                iso_priority=iso_priority,
                enabled_standard_levels=enabled_standard_levels,
                required_headers=required_headers,
                target_scopes=target_scopes if limit_to_chapter else None,
                manual_header_mappings=manual_header_mappings,
            )
            preview_stats = _build_stats(result.get("report", []))
            _record_task_standard_mapping_audit(
                "task_standard_mapping_preview",
                task_id,
                {
                    "word_path": selected_word,
                    "excel_path": selected_excel,
                    "harmonised_excel_path": selected_harmonised_excel,
                    "target_chapter_ref": target_chapter_ref,
                    "target_table_index": _format_target_table_index_display(target_table_index),
                    "target_scopes": list(target_scopes),
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
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            target_scopes=target_scopes,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )
    except Exception as exc:
        current_app.logger.exception("Standard mapping preview failed")
        _record_task_standard_mapping_audit(
            "task_standard_mapping_preview_failed",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "target_scopes": list(target_scopes),
                "manual_header_mapping_count": len(manual_header_mappings),
                "error": str(exc),
            },
        )
        flash(f"預覽失敗：{exc}", "danger")
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            selected_harmonised_excel=selected_harmonised_excel,
            iso_priority=iso_priority,
            enabled_standard_levels=enabled_standard_levels,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            target_scopes=target_scopes,
            manual_target_chapter_ref=manual_target_chapter_ref,
            manual_header_mappings=manual_header_mappings,
        )

    return _render_standard_mapping_page(
        task_id,
        preview_result=result,
        selected_word=selected_word,
        selected_excel=selected_excel,
        selected_harmonised_excel=selected_harmonised_excel,
        iso_priority=iso_priority,
        enabled_standard_levels=enabled_standard_levels,
        required_headers=required_headers,
        limit_to_chapter=limit_to_chapter,
        target_chapter_ref=target_chapter_ref,
        target_table_index=target_table_index,
        target_scopes=target_scopes,
        manual_target_chapter_ref=manual_target_chapter_ref,
        manual_header_mappings=manual_header_mappings,
    )


@tasks_bp.post("/tasks/<task_id>/standard-mapping/download", endpoint="task_standard_mapping_download")
def task_standard_mapping_download(task_id):
    files_dir = _task_files_dir(task_id)
    selected_word = (request.form.get("word_path") or "").strip()
    selected_excel = (request.form.get("excel_path") or "").strip()
    selected_harmonised_excel = (request.form.get("harmonised_excel_path") or "").strip()
    manual_header_mappings: dict[int, dict[str, str]] = {}
    limit_to_chapter = False
    manual_target_chapter_ref = ""
    target_chapter_ref = ""
    target_table_index = None
    target_scopes: tuple[dict, ...] = ()

    try:
        iso_priority = _parse_iso_priority(request.form)
        enabled_standard_levels = _parse_enabled_standard_levels(request.form)
        required_headers = _parse_required_headers(request.form)
        manual_header_mappings = _parse_manual_header_mappings(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_scopes = _parse_target_scopes(request.form, limit_to_chapter=limit_to_chapter)
        target_chapter_ref = _format_target_scopes_display(target_scopes)
        word_path = _safe_task_file(files_dir, selected_word, {".docx"})
        excel_path = _safe_task_file(files_dir, selected_excel, _ALLOWED_EXCEL_EXTENSIONS)
        harmonised_reference_path = _safe_task_file(files_dir, selected_harmonised_excel, _ALLOWED_EXCEL_EXTENSIONS) if selected_harmonised_excel else None
        regulation_reference_path = _resolve_regulation_reference_path(files_dir)
        override_map = _parse_override_map(request.form.get("overrides_json", ""))
        edit_map = _parse_edit_map(request.form.get("edits_json", ""))
        inspection_result = inspect_document_tables(
            word_path,
            target_scopes=target_scopes if limit_to_chapter else None,
            manual_header_mappings=manual_header_mappings,
        )
        if _has_unresolved_manual_mapping(inspection_result.get("table_checks")):
            flash("尚有表格未符合預設四欄格式，請先完成手動對應欄位設定後再下載結果。", "warning")
            return _render_standard_mapping_page(
                task_id,
                preview_result=inspection_result,
                selected_word=selected_word,
                selected_excel=selected_excel,
                selected_harmonised_excel=selected_harmonised_excel,
                iso_priority=iso_priority,
                enabled_standard_levels=enabled_standard_levels,
                required_headers=required_headers,
                limit_to_chapter=limit_to_chapter,
                target_chapter_ref=target_chapter_ref,
                target_table_index=target_table_index,
                target_scopes=target_scopes,
                manual_target_chapter_ref=manual_target_chapter_ref,
                manual_header_mappings=manual_header_mappings,
            )

        output_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id, "standard_mapping")
        os.makedirs(output_dir, exist_ok=True)
        base_name = _safe_uploaded_filename(f"{Path(selected_word).stem}_updated.docx", default_stem="standard_mapping_updated")
        output_name = deduplicate_name(output_dir, base_name)
        output_path = os.path.join(output_dir, output_name)

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
            target_scopes=target_scopes if limit_to_chapter else None,
            manual_header_mappings=manual_header_mappings,
        )
        download_stats = _build_stats(inspection_result.get("report", []))
        _record_task_standard_mapping_audit(
            "task_standard_mapping_download",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "target_scopes": list(target_scopes),
                "manual_header_mapping_count": len(manual_header_mappings),
                "updated_count": download_stats.get("updated", 0),
                "same_count": download_stats.get("same", 0),
                "missing_count": download_stats.get("missing", 0),
                "output_path": output_path,
            },
        )
    except (ValueError, FileNotFoundError) as exc:
        _record_task_standard_mapping_audit(
            "task_standard_mapping_download_failed",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "target_scopes": list(target_scopes),
                "manual_header_mapping_count": len(manual_header_mappings),
                "error": str(exc),
            },
        )
        flash(str(exc), "danger")
        return redirect(
            url_for(
                "tasks_bp.task_standard_mapping",
                task_id=task_id,
                word_path=selected_word,
                excel_path=selected_excel,
                harmonised_excel_path=selected_harmonised_excel,
                limit_to_chapter=1 if limit_to_chapter else 0,
                target_chapter_ref=target_chapter_ref,
                target_table_index=_format_target_table_index_display(target_table_index),
                manual_target_chapter_ref=manual_target_chapter_ref,
            )
        )
    except Exception as exc:
        current_app.logger.exception("Standard mapping download failed")
        _record_task_standard_mapping_audit(
            "task_standard_mapping_download_failed",
            task_id,
            {
                "word_path": selected_word,
                "excel_path": selected_excel,
                "harmonised_excel_path": selected_harmonised_excel,
                "target_chapter_ref": target_chapter_ref,
                "target_table_index": _format_target_table_index_display(target_table_index),
                "target_scopes": list(target_scopes),
                "manual_header_mapping_count": len(manual_header_mappings),
                "error": str(exc),
            },
        )
        flash(f"下載失敗：{exc}", "danger")
        return redirect(
            url_for(
                "tasks_bp.task_standard_mapping",
                task_id=task_id,
                word_path=selected_word,
                excel_path=selected_excel,
                harmonised_excel_path=selected_harmonised_excel,
                limit_to_chapter=1 if limit_to_chapter else 0,
                target_chapter_ref=target_chapter_ref,
                target_table_index=_format_target_table_index_display(target_table_index),
                manual_target_chapter_ref=manual_target_chapter_ref,
            )
        )

    return send_file(output_path, as_attachment=True, download_name=output_name)
