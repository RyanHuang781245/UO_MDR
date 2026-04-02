from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from flask import abort, current_app, flash, redirect, render_template, request, send_file, url_for

from app.services.standard_mapping_service import process_document
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


def _render_standard_mapping_page(task_id: str, *, preview_result: dict | None = None, selected_word: str = "", selected_excel: str = ""):
    files_dir = _task_files_dir(task_id)
    word_options, excel_options = _list_standard_mapping_files(files_dir)
    return render_template(
        "tasks/standard_mapping.html",
        task_id=task_id,
        task=_load_task_context(task_id),
        word_options=word_options,
        excel_options=excel_options,
        selected_word=selected_word,
        selected_excel=selected_excel,
        preview_tables=(preview_result or {}).get("preview_tables", []),
        reference_payload=(preview_result or {}).get("reference_payload", {}),
        stats=_build_stats((preview_result or {}).get("report", [])) if preview_result else {"updated": 0, "same": 0, "missing": 0, "total": 0},
        candidate_rows=len((preview_result or {}).get("reference_payload", {})),
        last_generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if preview_result else "",
    )


@tasks_bp.route("/tasks/<task_id>/standard-mapping", methods=["GET", "POST"], endpoint="task_standard_mapping")
def task_standard_mapping(task_id):
    files_dir = _task_files_dir(task_id)
    selected_word = (request.values.get("word_path") or "").strip()
    selected_excel = (request.values.get("excel_path") or "").strip()

    if request.method == "GET":
        return _render_standard_mapping_page(task_id, selected_word=selected_word, selected_excel=selected_excel)

    action = (request.form.get("action") or "preview").strip().lower()
    if action != "preview":
        return redirect(url_for("tasks_bp.task_standard_mapping", task_id=task_id))

    try:
        word_path = _safe_task_file(files_dir, selected_word, {".docx"})
        excel_path = _safe_task_file(files_dir, selected_excel, _ALLOWED_EXCEL_EXTENSIONS)
        result = process_document(word_path, excel_path)
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return _render_standard_mapping_page(task_id, selected_word=selected_word, selected_excel=selected_excel)
    except Exception as exc:
        current_app.logger.exception("Standard mapping preview failed")
        flash(f"預覽失敗：{exc}", "danger")
        return _render_standard_mapping_page(task_id, selected_word=selected_word, selected_excel=selected_excel)

    return _render_standard_mapping_page(
        task_id,
        preview_result=result,
        selected_word=selected_word,
        selected_excel=selected_excel,
    )


@tasks_bp.post("/tasks/<task_id>/standard-mapping/download", endpoint="task_standard_mapping_download")
def task_standard_mapping_download(task_id):
    files_dir = _task_files_dir(task_id)
    selected_word = (request.form.get("word_path") or "").strip()
    selected_excel = (request.form.get("excel_path") or "").strip()

    try:
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
        )
    except (ValueError, FileNotFoundError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("tasks_bp.task_standard_mapping", task_id=task_id, word_path=selected_word, excel_path=selected_excel))
    except Exception as exc:
        current_app.logger.exception("Standard mapping download failed")
        flash(f"下載失敗：{exc}", "danger")
        return redirect(url_for("tasks_bp.task_standard_mapping", task_id=task_id, word_path=selected_word, excel_path=selected_excel))

    return send_file(output_path, as_attachment=True, download_name=output_name)
