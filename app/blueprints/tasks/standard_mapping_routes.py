from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from flask import abort, current_app, flash, redirect, render_template, request, send_file, url_for

from app.services.standard_mapping_service import (
    DEFAULT_REQUIRED_HEADERS,
    DEFAULT_ISO_PRIORITY,
    DEFAULT_PREFER_LATEST_EN_VARIANTS,
    inspect_document_sections,
    normalize_iso_priority,
    normalize_required_headers,
    process_document,
)
from app.services.task_service import deduplicate_name, list_files, load_task_context as _load_task_context
from .blueprint import tasks_bp
from .mapping_routes import _safe_uploaded_filename

_ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xltx", ".xltm"}


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


def _parse_iso_priority(values) -> tuple[str, ...]:
    raw_positions = {
        "BS EN ISO": (values.get("priority_bs_en_iso") or "").strip(),
        "EN ISO": (values.get("priority_en_iso") or "").strip(),
        "ISO": (values.get("priority_iso") or "").strip(),
    }
    if not any(raw_positions.values()):
        return DEFAULT_ISO_PRIORITY

    try:
        positions = {label: int(position) for label, position in raw_positions.items()}
    except ValueError as exc:
        raise ValueError("優先級設定格式不正確") from exc

    if sorted(positions.values()) != [1, 2, 3]:
        raise ValueError("優先級設定不可重複")

    ordered = [label for label, _ in sorted(positions.items(), key=lambda item: item[1])]
    return normalize_iso_priority(ordered)


def _parse_prefer_latest_en_variants(values) -> bool:
    if hasattr(values, "getlist"):
        raw_values = [str(item).strip().lower() for item in values.getlist("prefer_latest_en_variants") if str(item).strip()]
        if raw_values:
            return raw_values[-1] in {"1", "true", "on", "yes"}
        return DEFAULT_PREFER_LATEST_EN_VARIANTS
    raw_value = str(values.get("prefer_latest_en_variants") or "").strip().lower()
    if not raw_value:
        return DEFAULT_PREFER_LATEST_EN_VARIANTS
    return raw_value in {"1", "true", "on", "yes"}


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


def _parse_target_table_index(values, *, limit_to_chapter: bool) -> int | None:
    raw_value = str(values.get("target_table_index") or "").strip()
    if not limit_to_chapter or not raw_value:
        return None
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError("表格索引必須是正整數") from exc
    if value <= 0:
        raise ValueError("表格索引必須大於 0")
    return value


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


def _render_standard_mapping_page(
    task_id: str,
    *,
    preview_result: dict | None = None,
    selected_word: str = "",
    selected_excel: str = "",
    iso_priority: tuple[str, ...] | list[str] | None = None,
    prefer_latest_en_variants: bool = DEFAULT_PREFER_LATEST_EN_VARIANTS,
    required_headers: tuple[str, ...] | list[str] | None = None,
    limit_to_chapter: bool = False,
    target_chapter_ref: str = "",
    target_table_index: int | None = None,
    manual_target_chapter_ref: str = "",
):
    files_dir = _task_files_dir(task_id)
    word_options, excel_options = _list_standard_mapping_files(files_dir)
    chapter_options, chapter_options_error = _load_chapter_options(files_dir, selected_word)
    reference_payload = (preview_result or {}).get("reference_payload", {})
    active_iso_priority = tuple((preview_result or {}).get("iso_priority") or normalize_iso_priority(iso_priority))
    active_required_headers = tuple((preview_result or {}).get("required_headers") or normalize_required_headers(required_headers))
    iso_priority_positions = {label: index + 1 for index, label in enumerate(active_iso_priority)}
    interactive_rows = len({item.get("row_key", "") for item in reference_payload.values() if item.get("row_key")})
    return render_template(
        "tasks/standard_mapping.html",
        task_id=task_id,
        task=_load_task_context(task_id),
        word_options=word_options,
        excel_options=excel_options,
        selected_word=selected_word,
        selected_excel=selected_excel,
        preview_tables=(preview_result or {}).get("preview_tables", []),
        table_checks=(preview_result or {}).get("table_checks", []),
        reference_payload=reference_payload,
        stats=_build_stats((preview_result or {}).get("report", [])) if preview_result else {"updated": 0, "same": 0, "missing": 0, "total": 0},
        interactive_rows=interactive_rows,
        interactive_fields=len(reference_payload),
        has_preview=bool(preview_result and (preview_result.get("preview_tables") or [])),
        last_generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if preview_result else "",
        iso_priority=active_iso_priority,
        iso_priority_positions=iso_priority_positions,
        prefer_latest_en_variants=(preview_result or {}).get("prefer_latest_en_variants", prefer_latest_en_variants),
        required_headers=active_required_headers,
        default_required_headers=DEFAULT_REQUIRED_HEADERS,
        limit_to_chapter=bool((preview_result or {}).get("target_chapter_ref") or limit_to_chapter),
        target_chapter_ref=(preview_result or {}).get("target_chapter_ref", target_chapter_ref),
        target_table_index=(preview_result or {}).get("target_table_index", target_table_index),
        manual_target_chapter_ref=manual_target_chapter_ref,
        scope_table_count=(preview_result or {}).get("scope_table_count", 0),
        chapter_options=chapter_options,
        chapter_options_error=chapter_options_error,
    )


@tasks_bp.route("/tasks/<task_id>/standard-mapping", methods=["GET", "POST"], endpoint="task_standard_mapping")
def task_standard_mapping(task_id):
    files_dir = _task_files_dir(task_id)
    selected_word = (request.values.get("word_path") or "").strip()
    selected_excel = (request.values.get("excel_path") or "").strip()

    if request.method == "GET":
        try:
            iso_priority = _parse_iso_priority(request.values)
            prefer_latest_en_variants = _parse_prefer_latest_en_variants(request.values)
            required_headers = _parse_required_headers(request.values)
            limit_to_chapter = _parse_limit_to_chapter(request.values)
            manual_target_chapter_ref = str(request.values.get("manual_target_chapter_ref") or "").strip()
            target_chapter_ref = _parse_target_chapter_ref(request.values, limit_to_chapter=limit_to_chapter)
            target_table_index = _parse_target_table_index(request.values, limit_to_chapter=limit_to_chapter)
        except ValueError as exc:
            flash(str(exc), "danger")
            iso_priority = DEFAULT_ISO_PRIORITY
            prefer_latest_en_variants = DEFAULT_PREFER_LATEST_EN_VARIANTS
            required_headers = DEFAULT_REQUIRED_HEADERS
            limit_to_chapter = False
            target_chapter_ref = ""
            target_table_index = None
            manual_target_chapter_ref = ""
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            iso_priority=iso_priority,
            prefer_latest_en_variants=prefer_latest_en_variants,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
        )

    action = (request.form.get("action") or "preview").strip().lower()
    if action != "preview":
        return redirect(url_for("tasks_bp.task_standard_mapping", task_id=task_id))

    try:
        iso_priority = _parse_iso_priority(request.form)
        prefer_latest_en_variants = _parse_prefer_latest_en_variants(request.form)
        required_headers = _parse_required_headers(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_chapter_ref = _parse_target_chapter_ref(request.form, limit_to_chapter=limit_to_chapter)
        target_table_index = _parse_target_table_index(request.form, limit_to_chapter=limit_to_chapter)
    except ValueError as exc:
        flash(str(exc), "danger")
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            iso_priority=DEFAULT_ISO_PRIORITY,
            prefer_latest_en_variants=DEFAULT_PREFER_LATEST_EN_VARIANTS,
            required_headers=DEFAULT_REQUIRED_HEADERS,
            limit_to_chapter=False,
            target_chapter_ref="",
            target_table_index=None,
            manual_target_chapter_ref="",
        )

    try:
        word_path = _safe_task_file(files_dir, selected_word, {".docx"})
        excel_path = _safe_task_file(files_dir, selected_excel, _ALLOWED_EXCEL_EXTENSIONS)
        result = process_document(
            word_path,
            excel_path,
            iso_priority=iso_priority,
            prefer_latest_en_variants=prefer_latest_en_variants,
            required_headers=required_headers,
            target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
            target_table_index=target_table_index if limit_to_chapter else None,
        )
        if limit_to_chapter and not result.get("table_checks"):
            flash("指定章節下找不到可辨識的表格", "warning")
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            iso_priority=iso_priority,
            prefer_latest_en_variants=prefer_latest_en_variants,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
        )
    except Exception as exc:
        current_app.logger.exception("Standard mapping preview failed")
        flash(f"預覽失敗：{exc}", "danger")
        return _render_standard_mapping_page(
            task_id,
            selected_word=selected_word,
            selected_excel=selected_excel,
            iso_priority=iso_priority,
            prefer_latest_en_variants=prefer_latest_en_variants,
            required_headers=required_headers,
            limit_to_chapter=limit_to_chapter,
            target_chapter_ref=target_chapter_ref,
            target_table_index=target_table_index,
            manual_target_chapter_ref=manual_target_chapter_ref,
        )

    return _render_standard_mapping_page(
        task_id,
        preview_result=result,
        selected_word=selected_word,
        selected_excel=selected_excel,
        iso_priority=iso_priority,
        prefer_latest_en_variants=prefer_latest_en_variants,
        required_headers=required_headers,
        limit_to_chapter=limit_to_chapter,
        target_chapter_ref=target_chapter_ref,
        target_table_index=target_table_index,
        manual_target_chapter_ref=manual_target_chapter_ref,
    )


@tasks_bp.post("/tasks/<task_id>/standard-mapping/download", endpoint="task_standard_mapping_download")
def task_standard_mapping_download(task_id):
    files_dir = _task_files_dir(task_id)
    selected_word = (request.form.get("word_path") or "").strip()
    selected_excel = (request.form.get("excel_path") or "").strip()

    try:
        iso_priority = _parse_iso_priority(request.form)
        prefer_latest_en_variants = _parse_prefer_latest_en_variants(request.form)
        required_headers = _parse_required_headers(request.form)
        limit_to_chapter = _parse_limit_to_chapter(request.form)
        manual_target_chapter_ref = str(request.form.get("manual_target_chapter_ref") or "").strip()
        target_chapter_ref = _parse_target_chapter_ref(request.form, limit_to_chapter=limit_to_chapter)
        target_table_index = _parse_target_table_index(request.form, limit_to_chapter=limit_to_chapter)
        word_path = _safe_task_file(files_dir, selected_word, {".docx"})
        excel_path = _safe_task_file(files_dir, selected_excel, _ALLOWED_EXCEL_EXTENSIONS)
        override_map = _parse_override_map(request.form.get("overrides_json", ""))

        output_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id, "standard_mapping")
        os.makedirs(output_dir, exist_ok=True)
        base_name = _safe_uploaded_filename(f"{Path(selected_word).stem}_updated.docx", default_stem="standard_mapping_updated")
        output_name = deduplicate_name(output_dir, base_name)
        output_path = os.path.join(output_dir, output_name)

        process_document(
            word_path=word_path,
            excel_path=excel_path,
            override_map=override_map,
            output_path=output_path,
            iso_priority=iso_priority,
            prefer_latest_en_variants=prefer_latest_en_variants,
            required_headers=required_headers,
            target_chapter_ref=target_chapter_ref if limit_to_chapter else "",
            target_table_index=target_table_index if limit_to_chapter else None,
        )
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return redirect(
            url_for(
                "tasks_bp.task_standard_mapping",
                task_id=task_id,
                word_path=selected_word,
                excel_path=selected_excel,
                limit_to_chapter=1 if limit_to_chapter else 0,
                target_chapter_ref=target_chapter_ref,
                target_table_index=target_table_index or "",
                manual_target_chapter_ref=manual_target_chapter_ref,
            )
        )
    except Exception as exc:
        current_app.logger.exception("Standard mapping download failed")
        flash(f"下載失敗：{exc}", "danger")
        return redirect(
            url_for(
                "tasks_bp.task_standard_mapping",
                task_id=task_id,
                word_path=selected_word,
                excel_path=selected_excel,
                limit_to_chapter=1 if limit_to_chapter else 0,
                target_chapter_ref=target_chapter_ref,
                target_table_index=target_table_index or "",
                manual_target_chapter_ref=manual_target_chapter_ref,
            )
        )

    return send_file(output_path, as_attachment=True, download_name=output_name)
