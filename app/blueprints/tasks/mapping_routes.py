from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

from flask import abort, current_app, redirect, render_template, request, send_file, send_from_directory, session, url_for
from werkzeug.utils import secure_filename

from app.services.task_service import load_task_context as _load_task_context
from app.services.mapping_metadata_service import (
    list_mapping_run_payloads,
    list_mapping_scheme_payloads,
    sync_job_payload,
    sync_run_payload,
    sync_scheme_payload,
)
from app.services.user_context_service import get_actor_info as _get_actor_info
from app.jobs.thread_queue import start_daemon_job
from .blueprint import tasks_bp
from .mapping_scheme_helpers import (
    delete_mapping_scheme,
    execute_saved_mapping_scheme,
    list_mapping_schemes,
    load_mapping_scheme,
    load_scheduled_mapping_scheme,
    rename_mapping_scheme,
    save_mapping_scheme,
    set_scheduled_mapping_scheme,
    task_files_last_updated,
    write_mapping_run_meta,
)

_INVALID_UPLOAD_FILENAME_CHARS = '\\/:*?"<>|'
_WINDOWS_RESERVED_FILE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}
_MAPPING_SESSION_KEY = "mapping_client_id"
_MAPPING_UI_STATE_FILE = "mapping_ui_state.json"
_MAPPING_OPS_DIR = "_ops"


def _paginate_saved_mapping_schemes(schemes_all: list[dict], page: int, per_page: int = 10) -> tuple[list[dict], dict]:
    total_count = len(schemes_all)
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * per_page
    schemes = schemes_all[start : start + per_page]
    return schemes, {
        "page": page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def _safe_uploaded_filename(filename: str, default_stem: str = "upload") -> str:
    raw_name = os.path.basename((filename or "").replace("\\", "/")).strip()
    secured = secure_filename(raw_name)
    raw_stem, raw_ext = os.path.splitext(raw_name)
    secured_raw_stem = secure_filename(raw_stem) if raw_stem else ""
    if secured and (not raw_stem or secured_raw_stem):
        return secured

    cleaned = "".join(
        "_" if (ord(ch) < 32 or ch in _INVALID_UPLOAD_FILENAME_CHARS) else ch
        for ch in raw_name
    ).strip().strip(".")
    if cleaned in {"", ".", ".."}:
        cleaned = default_stem

    stem, ext = os.path.splitext(cleaned)
    if not stem:
        stem = default_stem
    if stem.upper() in _WINDOWS_RESERVED_FILE_NAMES:
        stem = f"_{stem}"
    return f"{stem}{ext}" if ext else stem


def _get_mapping_client_id() -> str:
    client_id = str(session.get(_MAPPING_SESSION_KEY) or "").strip()
    if not re.fullmatch(r"[0-9a-f]{32}", client_id):
        client_id = uuid.uuid4().hex
        session[_MAPPING_SESSION_KEY] = client_id
        session.modified = True
    return client_id


def _get_mapping_owner_key() -> str:
    work_id, _actor_label = _get_actor_info()
    owner_key = secure_filename((work_id or "").strip())
    return owner_key or "anonymous"


def _mapping_workspace_dir(task_dir: str) -> str:
    workspace_dir = os.path.join(
        task_dir,
        "_mapping_sessions",
        _get_mapping_owner_key(),
        _get_mapping_client_id(),
    )
    os.makedirs(workspace_dir, exist_ok=True)
    return workspace_dir


def _reset_mapping_workspace(workspace_dir: str) -> None:
    shutil.rmtree(workspace_dir, ignore_errors=True)
    os.makedirs(workspace_dir, exist_ok=True)


def _load_mapping_workspace_cache(last_mapping_marker: str, validation_state_path: str) -> tuple[str | None, dict, str]:
    last_mapping_file = None
    validation_state = {
        "mapping_file": "",
        "mapping_display_name": "",
        "reference_ok": False,
        "extract_ok": False,
        "run_id": "",
    }
    current_mapping_display_name = ""

    if os.path.isfile(last_mapping_marker):
        try:
            cached_name = Path(last_mapping_marker).read_text(encoding="utf-8").strip()
            cached_path = os.path.join(os.path.dirname(last_mapping_marker), cached_name)
            if cached_name and os.path.isfile(cached_path):
                last_mapping_file = cached_name
        except Exception:
            last_mapping_file = None

    if os.path.isfile(validation_state_path):
        try:
            loaded_state = json.loads(Path(validation_state_path).read_text(encoding="utf-8"))
            if isinstance(loaded_state, dict):
                validation_state.update(
                    {
                        "mapping_file": str(loaded_state.get("mapping_file") or ""),
                        "mapping_display_name": str(loaded_state.get("mapping_display_name") or ""),
                        "reference_ok": bool(loaded_state.get("reference_ok")),
                        "extract_ok": bool(loaded_state.get("extract_ok")),
                        "run_id": str(loaded_state.get("run_id") or ""),
                    }
                )
                current_mapping_display_name = validation_state.get("mapping_display_name") or ""
        except Exception:
            pass

    return last_mapping_file, validation_state, current_mapping_display_name


def _load_mapping_ui_state(ui_state_path: str) -> dict:
    if not os.path.isfile(ui_state_path):
        return {}
    try:
        payload = json.loads(Path(ui_state_path).read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_mapping_ui_state(ui_state_path: str, payload: dict) -> None:
    Path(ui_state_path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _mapping_ops_dir(workspace_dir: str) -> str:
    path = os.path.join(workspace_dir, _MAPPING_OPS_DIR)
    os.makedirs(path, exist_ok=True)
    return path


def _mapping_op_path(workspace_dir: str, op_id: str) -> str:
    return os.path.join(_mapping_ops_dir(workspace_dir), f"{op_id}.json")


def _read_mapping_op(workspace_dir: str, op_id: str) -> dict:
    path = _mapping_op_path(workspace_dir, op_id)
    if not os.path.isfile(path):
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_mapping_op(workspace_dir: str, op_id: str, payload: dict) -> None:
    Path(_mapping_op_path(workspace_dir, op_id)).write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    task_id = os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(workspace_dir))))
    sync_job_payload(task_id, op_id, payload)


def _update_mapping_op(workspace_dir: str, op_id: str, **fields) -> dict:
    payload = _read_mapping_op(workspace_dir, op_id)
    payload.update(fields)
    _write_mapping_op(workspace_dir, op_id, payload)
    return payload


def _list_mapping_ops(workspace_dir: str, statuses: set[str] | None = None) -> list[dict]:
    ops_dir = _mapping_ops_dir(workspace_dir)
    results: list[dict] = []
    for name in os.listdir(ops_dir):
        if not name.endswith(".json"):
            continue
        payload = _read_mapping_op(workspace_dir, os.path.splitext(name)[0])
        if not payload:
            continue
        status = str(payload.get("status") or "").strip().lower()
        if statuses and status not in statuses:
            continue
        results.append(payload)
    results.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return results


def _workspace_has_active_mapping_ops(workspace_dir: str) -> bool:
    return bool(_list_mapping_ops(workspace_dir, {"queued", "running"}))


def _mapping_op_resume_url(task_id: str, op_id: str) -> str:
    query = urlencode({"mapping_tab": "create", "mapping_job": op_id})
    return f"/tasks/{task_id}/mapping?{query}"


def _first_mapping_error(messages: list[str]) -> str:
    for message in messages:
        text = str(message or "").strip()
        if "ERROR" in text:
            return text
    return ""


def _run_mapping_operation_job(
    app,
    task_id: str,
    workspace_dir: str,
    op_id: str,
    action: str,
    mapping_path: str,
    current_mapping_display_name: str,
    validation_state_snapshot: dict,
    actor: dict,
) -> None:
    with app.app_context():
        tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
        files_dir = os.path.join(tdir, "files")
        out_dir = os.path.join(tdir, "mapping_job")
        validation_state_path = os.path.join(workspace_dir, "mapping_validation_state.json")
        ui_state_path = os.path.join(workspace_dir, _MAPPING_UI_STATE_FILE)
        current_run_id = op_id
        _update_mapping_op(
            workspace_dir,
            op_id,
            status="running",
            started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        try:
            from modules.mapping_processor import process_mapping_excel

            run_out_dir = os.path.join(out_dir, current_run_id)
            result = process_mapping_excel(
                mapping_path,
                files_dir,
                run_out_dir,
                log_dir=run_out_dir,
                validate_only=(action == "check"),
                validate_extract_only=(action == "check_extract"),
            )
            messages = [str(item) for item in (result.get("logs") or [])]
            outputs = [str(item) for item in (result.get("outputs") or [])]
            log_file_raw = str(result.get("log_file") or "").strip()
            zip_file_raw = str(result.get("zip_file") or "").strip()
            log_file = f"{current_run_id}/{log_file_raw}" if log_file_raw else ""
            zip_file = f"{current_run_id}/{zip_file_raw}" if zip_file_raw else ""
            current_has_error = any("ERROR" in message for message in messages)
            current_mapping_name = os.path.basename(mapping_path)
            next_validation_state = {
                "mapping_file": current_mapping_name,
                "mapping_display_name": current_mapping_display_name or current_mapping_name,
                "reference_ok": bool(validation_state_snapshot.get("reference_ok")),
                "extract_ok": bool(validation_state_snapshot.get("extract_ok")),
                "run_id": current_run_id,
            }
            if action == "check":
                next_validation_state["reference_ok"] = not current_has_error
                next_validation_state["extract_ok"] = False
            elif action == "check_extract":
                next_validation_state["extract_ok"] = not current_has_error
            Path(validation_state_path).write_text(
                json.dumps(next_validation_state, ensure_ascii=False),
                encoding="utf-8",
            )

            rel_outputs = []
            for output_path in outputs:
                rel = os.path.relpath(output_path, out_dir) if os.path.isabs(output_path) else str(output_path)
                rel_outputs.append(rel.replace("\\", "/"))

            if action == "run_cached":
                run_outputs = []
                run_prefix = f"{current_run_id}/"
                for rel in rel_outputs:
                    run_outputs.append(rel[len(run_prefix):] if rel.startswith(run_prefix) else rel)
                write_mapping_run_meta(
                    os.path.join(out_dir, current_run_id),
                    {
                        "record_type": "mapping_run",
                        "run_id": current_run_id,
                        "mapping_file": next_validation_state.get("mapping_file") or "",
                        "mapping_display_name": next_validation_state.get("mapping_display_name") or "",
                        "status": "failed" if current_has_error else "completed",
                        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "reference_ok": bool(next_validation_state.get("reference_ok")),
                        "extract_ok": bool(next_validation_state.get("extract_ok")),
                        "outputs": run_outputs,
                        "output_count": len(run_outputs),
                        "zip_file": zip_file_raw,
                        "log_file": log_file_raw,
                        "error": _first_mapping_error(messages),
                        "actor_work_id": actor.get("work_id", ""),
                        "actor_label": actor.get("label", ""),
                        "source": "manual",
                    },
                )

            ui_payload = {
                "current_action": action,
                "current_run_id": current_run_id,
                "current_mapping_display_name": next_validation_state.get("mapping_display_name") or "",
                "messages": messages,
                "outputs": rel_outputs,
                "log_file": log_file,
                "zip_file": zip_file,
                "log_file_name": log_file_raw,
                "zip_file_name": zip_file_raw,
            }
            if action == "check_extract" and not current_has_error:
                try:
                    auto_saved_scheme = save_mapping_scheme(
                        task_id,
                        mapping_path,
                        "",
                        next_validation_state,
                        actor=actor,
                    )
                    ui_payload["auto_saved_scheme_name"] = str(
                        auto_saved_scheme.get("display_name") or auto_saved_scheme.get("id") or ""
                    ).strip()
                except Exception:
                    current_app.logger.exception("Failed to auto-save validated mapping scheme")
            _write_mapping_ui_state(ui_state_path, ui_payload)
            _update_mapping_op(
                workspace_dir,
                op_id,
                status="failed" if current_has_error else "completed",
                completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                mapping_display_name=next_validation_state.get("mapping_display_name") or "",
                current_action=action,
                current_run_id=current_run_id,
                messages=messages,
                outputs=rel_outputs,
                log_file=log_file,
                zip_file=zip_file,
                log_file_name=log_file_raw,
                zip_file_name=zip_file_raw,
                error=_first_mapping_error(messages),
                resume_url=_mapping_op_resume_url(task_id, op_id),
                auto_saved_scheme_name=str(ui_payload.get("auto_saved_scheme_name") or "").strip(),
            )
        except Exception as exc:
            current_app.logger.exception("Mapping operation failed")
            messages = [f"ERROR: {exc}"]
            ui_payload = {
                "current_action": action,
                "current_run_id": current_run_id,
                "current_mapping_display_name": current_mapping_display_name or os.path.basename(mapping_path),
                "messages": messages,
                "outputs": [],
                "log_file": "",
                "zip_file": "",
                "log_file_name": "",
                "zip_file_name": "",
                "auto_saved_scheme_name": "",
            }
            _write_mapping_ui_state(ui_state_path, ui_payload)
            if action == "run_cached":
                write_mapping_run_meta(
                    os.path.join(out_dir, current_run_id),
                    {
                        "record_type": "mapping_run",
                        "run_id": current_run_id,
                        "mapping_file": os.path.basename(mapping_path),
                        "mapping_display_name": current_mapping_display_name or os.path.basename(mapping_path),
                        "status": "failed",
                        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "reference_ok": bool(validation_state_snapshot.get("reference_ok")),
                        "extract_ok": bool(validation_state_snapshot.get("extract_ok")),
                        "outputs": [],
                        "output_count": 0,
                        "zip_file": "",
                        "log_file": "",
                        "error": str(exc),
                        "actor_work_id": actor.get("work_id", ""),
                        "actor_label": actor.get("label", ""),
                        "source": "manual",
                    },
                )
            _update_mapping_op(
                workspace_dir,
                op_id,
                status="failed",
                completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                mapping_display_name=current_mapping_display_name or os.path.basename(mapping_path),
                current_action=action,
                current_run_id=current_run_id,
                messages=messages,
                outputs=[],
                log_file="",
                zip_file="",
                log_file_name="",
                zip_file_name="",
                error=str(exc),
                resume_url=_mapping_op_resume_url(task_id, op_id),
            )

@tasks_bp.route("/tasks/<task_id>/mapping", methods=["GET", "POST"], endpoint="task_mapping")
def task_mapping(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    files_dir = os.path.join(tdir, "files")
    out_dir = os.path.join(tdir, "mapping_job")
    messages = []
    outputs = []
    log_file = None
    zip_file = None
    log_file_name = None
    zip_file_name = None
    step_runs = []
    workspace_dir = _mapping_workspace_dir(tdir)
    last_mapping_marker = os.path.join(workspace_dir, "mapping_last.txt")
    validation_state_path = os.path.join(workspace_dir, "mapping_validation_state.json")
    ui_state_path = os.path.join(workspace_dir, _MAPPING_UI_STATE_FILE)
    last_mapping_file = None
    validation_state = {
        "mapping_file": "",
        "mapping_display_name": "",
        "reference_ok": False,
        "extract_ok": False,
        "run_id": "",
    }
    current_run_id = None
    current_mapping_display_name = ""
    current_action = ""
    auto_saved_scheme_name = ""
    try:
        mapping_page = int(request.values.get("mpage", "1"))
    except (TypeError, ValueError):
        mapping_page = 1
    mapping_results_page = max(request.values.get("mrpage", 1, type=int), 1)
    mapping_results_q = (request.values.get("mq") or "").strip()
    mapping_results_status = (request.values.get("mstatus") or "").strip().lower()
    mapping_results_start_date = (request.values.get("mstart_date") or "").strip()
    mapping_results_end_date = (request.values.get("mend_date") or "").strip()
    active_mapping_tab = (request.values.get("mapping_tab") or "").strip().lower()
    if active_mapping_tab not in {"create", "saved", "results"}:
        if request.values.get("mpage"):
            active_mapping_tab = "saved"
        elif any(
            (
                request.values.get("mq"),
                request.values.get("mstatus"),
                request.values.get("mstart_date"),
                request.values.get("mend_date"),
            )
        ):
            active_mapping_tab = "results"
        elif mapping_results_page > 1:
            active_mapping_tab = "results"
        else:
            active_mapping_tab = "create"
    resume_mapping_state = (
        request.method == "GET"
        and active_mapping_tab == "create"
        and (request.args.get("resume_mapping") or "").strip() == "1"
    )
    current_mapping_job_id = (request.args.get("mapping_job") or "").strip()
    current_mapping_op = _read_mapping_op(workspace_dir, current_mapping_job_id) if current_mapping_job_id else {}
    current_mapping_job_status = str(current_mapping_op.get("status") or "").strip().lower() if current_mapping_op else ""
    has_active_mapping_ops = _workspace_has_active_mapping_ops(workspace_dir)

    # 如果是頁面跳轉/重新整理 (GET)，則清掉之前的暫存紀錄與檔案
    if request.method == "GET":
        if resume_mapping_state or current_mapping_op or has_active_mapping_ops:
            last_mapping_file, validation_state, current_mapping_display_name = _load_mapping_workspace_cache(
                last_mapping_marker,
                validation_state_path,
            )
            if current_mapping_op and str(current_mapping_op.get("status") or "").strip().lower() in {"completed", "failed"}:
                current_action = str(current_mapping_op.get("current_action") or "").strip()
                current_run_id = (
                    str(current_mapping_op.get("current_run_id") or validation_state.get("run_id") or "").strip() or None
                )
                current_mapping_display_name = (
                    str(current_mapping_op.get("mapping_display_name") or "").strip()
                    or current_mapping_display_name
                )
                messages = [str(item) for item in (current_mapping_op.get("messages") or [])]
                outputs = [str(item) for item in (current_mapping_op.get("outputs") or [])]
                log_file = str(current_mapping_op.get("log_file") or "").strip() or None
                zip_file = str(current_mapping_op.get("zip_file") or "").strip() or None
                log_file_name = str(current_mapping_op.get("log_file_name") or "").strip() or None
                zip_file_name = str(current_mapping_op.get("zip_file_name") or "").strip() or None
                auto_saved_scheme_name = str(current_mapping_op.get("auto_saved_scheme_name") or "").strip()
            elif current_mapping_op and current_mapping_job_status in {"queued", "running"}:
                current_action = str(current_mapping_op.get("action") or "").strip()
                current_run_id = current_mapping_job_id or None
                current_mapping_display_name = (
                    str(current_mapping_op.get("mapping_display_name") or "").strip()
                    or current_mapping_display_name
                )
            elif resume_mapping_state:
                ui_state = _load_mapping_ui_state(ui_state_path)
                current_action = str(ui_state.get("current_action") or "").strip()
                current_run_id = str(ui_state.get("current_run_id") or validation_state.get("run_id") or "").strip() or None
                current_mapping_display_name = (
                    str(ui_state.get("current_mapping_display_name") or "").strip()
                    or current_mapping_display_name
                )
                messages = [str(item) for item in (ui_state.get("messages") or [])]
                outputs = [str(item) for item in (ui_state.get("outputs") or [])]
                log_file = str(ui_state.get("log_file") or "").strip() or None
                zip_file = str(ui_state.get("zip_file") or "").strip() or None
                log_file_name = str(ui_state.get("log_file_name") or "").strip() or None
                zip_file_name = str(ui_state.get("zip_file_name") or "").strip() or None
                auto_saved_scheme_name = str(ui_state.get("auto_saved_scheme_name") or "").strip()
        else:
            _reset_mapping_workspace(workspace_dir)
    else:
        last_mapping_file, validation_state, current_mapping_display_name = _load_mapping_workspace_cache(
            last_mapping_marker,
            validation_state_path,
        )

    def _format_step_label(entry: dict) -> tuple[str, str]:
        stype = entry.get("type") or ""
        params = entry.get("params") or {}

        def _boolish(value, default: bool = False) -> bool:
            if value in (None, ""):
                return default
            return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

        def _base(path: str) -> str:
            if not path: return "?"
            name = os.path.basename(path)
            # 移除 "Section 1_", "Section 2_" 等前綴
            import re
            name = re.sub(r"^Section\s+\d+_", "", name)
            return name

        row_no = params.get("mapping_row")
        row_prefix = f"(Row {row_no}) " if row_no not in (None, "", "None") else ""
        preset_action = (params.get("mapping_action_label") or "").strip()
        preset_detail = (params.get("mapping_detail_label") or "").strip()
        if preset_action:
            return f"{row_prefix}{preset_action}", preset_detail
        if stype == "extract_word_chapter":
            src = _base(params.get("input_file", ""))
            chapter_start = (params.get("target_chapter_section") or "").strip()
            chapter_end = (params.get("explicit_end_number") or "").strip()
            chapter = f"{chapter_start}-{chapter_end}" if chapter_start and chapter_end else chapter_start
            title = (params.get("target_chapter_title") or params.get("target_title_section") or "").strip()
            sub = (params.get("target_subtitle") or params.get("subheading_text") or "").strip()
            
            # 組合章節與標題: "1.1.1 General description"
            main_header = f"{chapter} {title}".strip()
            
            parts = [src]
            if main_header:
                parts.append(main_header)
            if sub:
                parts.append(sub)
            if _boolish(params.get("hide_chapter_title"), default=False):
                parts.append("不含標題")
            
            return f"{row_prefix}擷取章節", " | ".join(parts)
        if stype == "extract_word_all_content":
            src = _base(params.get("input_file", ""))
            return f"{row_prefix}擷取全文", src.strip()
        if stype == "extract_pdf_pages_as_images":
            src = _base(params.get("input_file", ""))
            pages = params.get("pages")
            parts = [src.strip()]
            if pages:
                parts.append(f"pages={pages}")
            return f"{row_prefix}擷取 PDF 圖片", " | ".join(p for p in parts if p)
        if stype == "extract_specific_table_from_word":
            src = _base(params.get("input_file", ""))
            section = (params.get("target_chapter_section") or "").strip()
            title = (params.get("target_chapter_title") or params.get("target_title_section") or "").strip()
            main_header = f"{section} {title}".strip()
            label = (
                params.get("target_caption_label")
                or params.get("target_table_label", "")
                or params.get("target_figure_label", "")
            ).strip()
            table_title = (params.get("target_table_title") or "").strip()
            table_index = str(params.get("target_table_index") or "").strip()
            parts = [src]
            if main_header:
                parts.append(main_header)
            if label:
                parts.append(label)
            if table_title:
                parts.append(f"title={table_title}")
            if table_index:
                parts.append(f"index={table_index}")
            if not _boolish(params.get("include_caption"), default=True):
                parts.append("不含標題")
            return f"{row_prefix}擷取表格", " | ".join(parts)
        if stype == "extract_specific_figure_from_word":
            src = _base(params.get("input_file", ""))
            section = (params.get("target_chapter_section") or "").strip()
            title = (params.get("target_chapter_title") or params.get("target_title_section") or "").strip()
            main_header = f"{section} {title}".strip()
            label = (
                params.get("target_caption_label")
                or params.get("target_figure_label", "")
                or params.get("target_table_label", "")
            ).strip()
            figure_title = (params.get("target_figure_title") or "").strip()
            figure_index = str(params.get("target_figure_index") or "").strip()
            parts = [src]
            if main_header:
                parts.append(main_header)
            if label:
                parts.append(label)
            if figure_title:
                parts.append(f"title={figure_title}")
            if figure_index:
                parts.append(f"index={figure_index}")
            if not _boolish(params.get("include_caption"), default=True):
                parts.append("不含標題")
            return f"{row_prefix}擷取圖片", " | ".join(parts)
        if stype == "insert_text":
            text_val = (params.get("text") or "").strip()
            return f"{row_prefix}插入文字", text_val
        if stype == "copy_file":
            src = _base(params.get("source", ""))
            dest = (params.get("destination") or "").strip().replace("\\", "/")
            target_name = (params.get("target_name") or "").strip()
            parts = [src]
            if target_name:
                parts.append(f"目標名稱={target_name}")
            if dest:
                parts.append(dest)
            return f"{row_prefix}複製檔案", " | ".join(p for p in parts if p)
        if stype == "copy_folder":
            src = _base(params.get("source", ""))
            dest = (params.get("destination") or "").strip().replace("\\", "/")
            target_name = (params.get("target_name") or "").strip()
            parts = [src]
            if target_name:
                parts.append(f"目標名稱={target_name}")
            if dest:
                parts.append(dest)
            return f"{row_prefix}複製資料夾", " | ".join(p for p in parts if p)
        if stype == "template_merge":
            tpl = _base(entry.get("template_file", ""))
            return f"{row_prefix}模版合併", tpl.strip()
        return f"{row_prefix}{stype or '步驟'}", ""

    def _truncate_detail(text: str, limit: int = 160) -> tuple[str, bool]:
        if len(text) <= limit:
            return text, False
        trimmed = text[: max(0, limit - 1)].rstrip()
        return f"{trimmed}…", True
    if request.method == "POST":
        action = request.form.get("action") or "run"
        current_action = action
        mapping_path = None
        uploaded_new_mapping = False
        active_scheme = None

        if action == "run_scheme":
            scheme_id = (request.form.get("scheme_id") or "").strip()
            active_scheme = load_mapping_scheme(task_id, scheme_id)
            if not active_scheme:
                messages.append("找不到指定的 Mapping 方案。")
            elif not active_scheme.get("is_runnable"):
                messages.append(f"方案「{active_scheme.get('display_name') or scheme_id}」目前不可執行，請先重新檢查。")
            else:
                try:
                    work_id, actor_label = _get_actor_info()
                    run_result = execute_saved_mapping_scheme(
                        task_id,
                        scheme_id,
                        actor={"work_id": work_id, "label": actor_label},
                        source="manual",
                    )
                    current_run_id = run_result["run_id"]
                    messages = list(run_result.get("messages") or [])
                    outputs = [f"{current_run_id}/{name}" for name in (run_result.get("outputs") or [])]
                    log_file_name = run_result.get("log_file") or ""
                    zip_file_name = run_result.get("zip_file") or ""
                    log_file = run_result.get("log_relpath") or None
                    zip_file = run_result.get("zip_relpath") or None
                    current_mapping_display_name = active_scheme.get("display_name") or active_scheme.get("mapping_display_name") or ""
                    active_mapping_tab = "results"
                except Exception as e:
                    messages = [str(e)]
        elif action == "schedule_scheme":
            scheme_id = (request.form.get("scheme_id") or "").strip()
            active_scheme = load_mapping_scheme(task_id, scheme_id)
            if not active_scheme:
                messages.append("找不到指定的 Mapping 方案。")
            elif not active_scheme.get("is_runnable"):
                messages.append(f"方案「{active_scheme.get('display_name') or scheme_id}」目前不可設為排程，請先重新檢查。")
            else:
                try:
                    set_scheduled_mapping_scheme(task_id, scheme_id)
                    messages.append(f"已設為排程方案：{active_scheme.get('display_name') or scheme_id}")
                except Exception as e:
                    messages = [str(e)]
        elif action == "save_scheme":
            if not last_mapping_file or validation_state.get("mapping_file") != last_mapping_file:
                messages.append("請先上傳並檢查 Mapping 檔案後再儲存方案。")
            elif not validation_state.get("extract_ok"):
                messages.append("請先通過檢查擷取參數，再儲存方案。")
            else:
                mapping_path = os.path.join(workspace_dir, last_mapping_file)
                scheme_name = (request.form.get("scheme_name") or "").strip()
                try:
                    work_id, actor_label = _get_actor_info()
                    saved_scheme = save_mapping_scheme(
                        task_id,
                        mapping_path,
                        scheme_name,
                        validation_state,
                        actor={"work_id": work_id, "label": actor_label},
                    )
                    messages.append(f"已儲存方案：{saved_scheme.get('display_name') or saved_scheme.get('id')}")
                except Exception as e:
                    messages = [str(e)]
        elif action == "delete_scheme":
            scheme_id = (request.form.get("scheme_id") or "").strip()
            active_scheme = load_mapping_scheme(task_id, scheme_id)
            if not active_scheme:
                messages.append("找不到指定的 Mapping 方案。")
            else:
                try:
                    deleted = delete_mapping_scheme(task_id, scheme_id)
                    if deleted:
                        messages.append(f"已刪除方案：{active_scheme.get('display_name') or scheme_id}")
                    else:
                        messages.append("刪除失敗，請稍後再試。")
                except Exception as e:
                    messages = [str(e)]
        elif action == "rename_scheme":
            scheme_id = (request.form.get("scheme_id") or "").strip()
            active_scheme = load_mapping_scheme(task_id, scheme_id)
            if not active_scheme:
                messages.append("找不到指定的 Mapping 方案。")
            else:
                try:
                    renamed_scheme = rename_mapping_scheme(
                        task_id,
                        scheme_id,
                        request.form.get("scheme_name") or "",
                    )
                    messages.append(f"已重新命名方案：{renamed_scheme.get('display_name') or scheme_id}")
                except Exception as e:
                    messages = [str(e)]
        else:
            active_mapping_tab = "create"
            if action == "run_cached":
                if not last_mapping_file:
                    messages.append("找不到上次檢查的檔案，請重新上傳。")
                else:
                    mapping_path = os.path.join(workspace_dir, last_mapping_file)
            else:
                f = request.files.get("mapping_file")
                if f and f.filename:
                    _reset_mapping_workspace(workspace_dir)
                    display_name = os.path.basename((f.filename or "").replace("\\", "/")).strip()
                    filename = _safe_uploaded_filename(
                        f.filename,
                        default_stem=f"mapping_{uuid.uuid4().hex[:8]}",
                    )
                    mapping_path = os.path.join(workspace_dir, filename)
                    f.save(mapping_path)
                    uploaded_new_mapping = True
                    current_mapping_display_name = display_name or filename
                    try:
                        Path(last_mapping_marker).write_text(filename, encoding="utf-8")
                        last_mapping_file = filename
                        validation_state = {
                            "mapping_file": filename,
                            "mapping_display_name": current_mapping_display_name,
                            "reference_ok": False,
                            "extract_ok": False,
                            "run_id": "",
                        }
                    except Exception:
                        pass
                elif last_mapping_file:
                    mapping_path = os.path.join(workspace_dir, last_mapping_file)
                    current_mapping_display_name = current_mapping_display_name or last_mapping_file
                else:
                    messages.append("請選擇檔案")

            if action == "check_extract" and not validation_state.get("reference_ok"):
                messages.append("請先通過檢查引用文件。")
                mapping_path = None
            if action == "run_cached" and not validation_state.get("extract_ok"):
                messages.append("請先通過檢查擷取參數。")
                mapping_path = None
            if mapping_path:
                try:
                    current_mapping_name = os.path.basename(mapping_path)
                    current_mapping_display_name = current_mapping_display_name or validation_state.get("mapping_display_name") or current_mapping_name
                    current_run_id = uuid.uuid4().hex[:8]
                    if action == "check":
                        validation_state = {
                            "mapping_file": current_mapping_name,
                            "mapping_display_name": current_mapping_display_name,
                            "reference_ok": False,
                            "extract_ok": False,
                            "run_id": current_run_id,
                        }
                    elif action == "check_extract":
                        validation_state = {
                            "mapping_file": current_mapping_name,
                            "mapping_display_name": current_mapping_display_name,
                            "reference_ok": bool(validation_state.get("reference_ok")),
                            "extract_ok": False,
                            "run_id": current_run_id,
                        }
                    else:
                        validation_state = {
                            "mapping_file": current_mapping_name,
                            "mapping_display_name": current_mapping_display_name,
                            "reference_ok": bool(validation_state.get("reference_ok")),
                            "extract_ok": bool(validation_state.get("extract_ok")),
                            "run_id": current_run_id,
                        }
                    Path(validation_state_path).write_text(
                        json.dumps(validation_state, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    actor_work_id, actor_label = _get_actor_info()
                    _write_mapping_op(
                        workspace_dir,
                        current_run_id,
                        {
                            "op_id": current_run_id,
                            "status": "queued",
                            "action": action,
                            "mapping_display_name": current_mapping_display_name,
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "resume_url": _mapping_op_resume_url(task_id, current_run_id),
                        },
                    )
                    start_daemon_job(
                        _run_mapping_operation_job,
                        current_app._get_current_object(),
                        task_id,
                        workspace_dir,
                        current_run_id,
                        action,
                        mapping_path,
                        current_mapping_display_name,
                        dict(validation_state),
                        {"work_id": actor_work_id, "label": actor_label},
                    )
                    return redirect(
                        url_for(
                            "tasks_bp.task_mapping",
                            task_id=task_id,
                            mapping_tab="create",
                            mapping_job=current_run_id,
                        )
                    )
                except Exception as e:
                    messages = [str(e)]
        if action in {"save_scheme", "schedule_scheme"} and not log_file:
            preserved_run_id = str(validation_state.get("run_id") or "").strip()
            if preserved_run_id:
                preserved_run_dir = os.path.join(out_dir, preserved_run_id)
                if os.path.isdir(preserved_run_dir):
                    current_run_id = preserved_run_id
                    preserved_log_name = ""
                    preserved_zip_name = ""
                    for candidate in os.listdir(preserved_run_dir):
                        lower_name = candidate.lower()
                        if not preserved_log_name and lower_name.endswith(".json"):
                            preserved_log_name = candidate
                        if not preserved_zip_name and lower_name.endswith(".zip"):
                            preserved_zip_name = candidate
                    if preserved_log_name:
                        log_file_name = preserved_log_name
                        log_file = f"{preserved_run_id}/{preserved_log_name}"
                    if preserved_zip_name:
                        zip_file_name = preserved_zip_name
                        zip_file = f"{preserved_run_id}/{preserved_zip_name}"
                    for root, _dirs, files in os.walk(preserved_run_dir):
                        for name in files:
                            if not name.lower().endswith(".docx"):
                                continue
                            rel = os.path.relpath(os.path.join(root, name), out_dir).replace("\\", "/")
                            outputs.append(rel)
    if log_file:
        log_candidates = [
            os.path.join(out_dir, log_file),
        ]
        log_path = next((p for p in log_candidates if os.path.isfile(p)), None)
        if log_path:
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    log_data = json.load(f)
                for run in log_data.get("runs", []):
                    for entry in run.get("workflow_log", []):
                        if "step" not in entry:
                            continue
                        action, detail = _format_step_label(entry)
                        row_no = (entry.get("params") or {}).get("mapping_row")
                        detail_short, detail_long = _truncate_detail(detail) if detail else ("", False)
                        step_runs.append(
                            {
                                "action": action,
                                "detail": detail,
                                "detail_short": detail_short,
                                "detail_long": detail_long,
                                "row_no": row_no,
                                "status": entry.get("status") or "ok",
                                "error": entry.get("error") or "",
                            }
                        )
                if step_runs:
                    messages = [m for m in messages if not (m or "").startswith("WF_ERROR:")]
            except Exception as e:
                messages.append(f"ERROR: failed to read log file ({e})")
    has_error = any("ERROR" in (m or "") for m in messages) or any(
        step.get("status") == "error" for step in step_runs
    )
    warning_messages = [m for m in messages if (m or "").startswith("WARN:") or (m or "").startswith("WARNING:")]
    has_warning = bool(warning_messages)
    warning_confirm = None
    if has_warning:
        trimmed = []
        for m in warning_messages[:3]:
            trimmed.append(m.replace("WARN:", "").replace("WARNING:", "").strip())
        warning_confirm = "Warnings found. Run anyway?\n" + "\n".join(trimmed)

    error_messages = [m for m in messages if (m or "").startswith("ERROR:")]
    if error_messages:
        def _norm_error_text(text: str) -> str:
            return re.sub(r"\s+", " ", (text or "").strip())

        existing_row_errors: dict[int | None, set[str]] = {}
        for step in step_runs:
            if step.get("status") != "error":
                continue
            row_no = step.get("row_no")
            bucket = existing_row_errors.setdefault(row_no, set())
            for candidate in (step.get("error"), step.get("detail")):
                normalized = _norm_error_text(str(candidate or ""))
                if normalized:
                    bucket.add(normalized)

        error_steps = []
        for msg in error_messages:
            raw = (msg or "").replace("ERROR:", "", 1).strip()
            raw = re.sub(r"^Row\s+\d+\s*:\s*", "", raw, flags=re.IGNORECASE)
            action = raw
            detail = ""
            error_text = raw
            row_match = re.search(r"Row\s+(\d+)", msg or "", re.IGNORECASE)
            row_prefix = f"(Row {row_match.group(1)}) " if row_match else ""
            if "::" in raw:
                parts = [p.strip() for p in raw.split("::", 2)]
                if len(parts) >= 2:
                    base_action = parts[0] or action
                    if base_action.startswith("(Row "):
                        action = base_action
                    else:
                        action = f"{row_prefix}{base_action}".strip()
                    detail = parts[1]
                if len(parts) == 3:
                    error_text = parts[2]
            elif ":" in raw:
                head, tail = raw.split(":", 1)
                base_action = head.strip() or raw
                if base_action.startswith("(Row "):
                    action = base_action
                else:
                    action = f"{row_prefix}{base_action}".strip()
                detail = tail.strip()
            display_detail = detail or error_text
            parsed_row_no = int(row_match.group(1)) if row_match else None
            norm_error_text = _norm_error_text(error_text)
            norm_display_detail = _norm_error_text(display_detail)
            existing_bucket = existing_row_errors.get(parsed_row_no, set())
            if norm_error_text in existing_bucket or norm_display_detail in existing_bucket:
                continue

            detail_short, detail_long = _truncate_detail(display_detail)
            error_steps.append(
                {
                    "action": action,
                    "detail": display_detail,
                    "detail_short": detail_short,
                    "detail_long": detail_long,
                    "row_no": parsed_row_no,
                    "status": "error",
                    "error": error_text,
                }
            )
        if error_steps:
            step_runs = error_steps + step_runs
        error_messages = []
    if step_runs:
        step_runs = sorted(
            step_runs,
            key=lambda s: (s.get("row_no") is None, s.get("row_no") or 10**9),
        )
        row_has_error = {}
        for step in step_runs:
            row_no = step.get("row_no")
            if row_no is None:
                continue
            if step.get("status") == "error":
                row_has_error[row_no] = True
            else:
                row_has_error.setdefault(row_no, False)
        if row_has_error:
            step_runs = [
                step
                for step in step_runs
                if step.get("row_no") is None
                or not row_has_error.get(step.get("row_no"))
                or step.get("status") == "error"
            ]
    step_ok_count = sum(1 for step in step_runs if step.get("status") != "error")
    step_error_count = sum(1 for step in step_runs if step.get("status") == "error")
    rel_outputs = []
    for p in outputs:
        rel = os.path.relpath(p, out_dir) if os.path.isabs(p) else str(p)
        rel_outputs.append(rel.replace("\\", "/"))
    if request.method == "POST" and (request.form.get("action") == "run_cached") and current_run_id:
        run_dir = os.path.join(out_dir, current_run_id)
        run_outputs = []
        run_prefix = f"{current_run_id}/"
        for rel in rel_outputs:
            run_outputs.append(rel[len(run_prefix):] if rel.startswith(run_prefix) else rel)
        first_error = ""
        for step in step_runs:
            if step.get("status") == "error":
                first_error = str(step.get("error") or step.get("detail") or "").strip()
                if first_error:
                    break
        if not first_error:
            for msg in messages:
                if "ERROR" in (msg or ""):
                    first_error = str(msg).strip()
                    break
        status_text = "failed" if has_error else "completed"
        work_id, actor_label = _get_actor_info()
        write_mapping_run_meta(
            run_dir,
            {
                "record_type": "mapping_run",
                "run_id": current_run_id,
                "mapping_file": validation_state.get("mapping_file") or last_mapping_file or "",
                "mapping_display_name": current_mapping_display_name or validation_state.get("mapping_display_name") or validation_state.get("mapping_file") or last_mapping_file or "",
                "status": status_text,
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reference_ok": bool(validation_state.get("reference_ok")),
                "extract_ok": bool(validation_state.get("extract_ok")),
                "outputs": run_outputs,
                "output_count": len(run_outputs),
                "zip_file": zip_file_name or "",
                "log_file": log_file_name or "",
                "error": first_error,
                "actor_work_id": work_id,
                "actor_label": actor_label,
                "source": "manual",
            },
        )
    scheduled_scheme = None
    saved_schemes_all: list[dict] = []
    saved_schemes: list[dict] = []
    saved_schemes_pagination = {
        "page": mapping_page,
        "total_count": 0,
        "total_pages": 1,
        "has_prev": False,
        "has_next": False,
    }
    if active_mapping_tab == "saved":
        scheduled_scheme = load_scheduled_mapping_scheme(task_id)
        scheduled_scheme_id = (scheduled_scheme or {}).get("id") or (scheduled_scheme or {}).get("scheme_id") or ""
        current_files_revision = int(task_files_last_updated(task_id))
        saved_scheme_result = list_mapping_scheme_payloads(
            task_id,
            page=mapping_page,
            per_page=10,
            scheduled_scheme_id=scheduled_scheme_id,
            current_revision=current_files_revision,
        )
        saved_schemes = saved_scheme_result["items"]
        saved_schemes_pagination = saved_scheme_result["pagination"]
        if not saved_schemes_pagination.get("total_count"):
            saved_schemes_all = list_mapping_schemes(task_id)
            for scheme in saved_schemes_all:
                sync_scheme_payload(task_id, scheme)
            saved_scheme_result = list_mapping_scheme_payloads(
                task_id,
                page=mapping_page,
                per_page=10,
                scheduled_scheme_id=scheduled_scheme_id,
                current_revision=current_files_revision,
            )
            saved_schemes = saved_scheme_result["items"]
            saved_schemes_pagination = saved_scheme_result["pagination"]

    mapping_results = {
        "runs": [],
        "pagination": {
            "page": mapping_results_page,
            "per_page": 10,
            "total_count": 0,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
        },
        "filters": {
            "q": mapping_results_q,
            "status": mapping_results_status,
            "start_date": mapping_results_start_date,
            "end_date": mapping_results_end_date,
        },
    }
    if active_mapping_tab == "results":
        mapping_results = list_mapping_run_payloads(
            task_id,
            page=mapping_results_page,
            per_page=10,
            q=mapping_results_q,
            status=mapping_results_status,
            start_date=mapping_results_start_date,
            end_date=mapping_results_end_date,
        )
        if not mapping_results["pagination"].get("total_count"):
            tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
            mapping_dir = os.path.join(tdir, "mapping_job")
            if os.path.isdir(mapping_dir):
                for run_id in os.listdir(mapping_dir):
                    run_dir = os.path.join(mapping_dir, run_id)
                    meta_path = os.path.join(run_dir, "meta.json")
                    if not os.path.isdir(run_dir) or not os.path.isfile(meta_path):
                        continue
                    try:
                        payload = json.loads(Path(meta_path).read_text(encoding="utf-8"))
                    except Exception:
                        continue
                    if isinstance(payload, dict) and payload.get("record_type") == "mapping_run":
                        sync_run_payload(task_id, payload)
            mapping_results = list_mapping_run_payloads(
                task_id,
                page=mapping_results_page,
                per_page=10,
                q=mapping_results_q,
                status=mapping_results_status,
                start_date=mapping_results_start_date,
                end_date=mapping_results_end_date,
            )
    current_job_in_progress = bool(
        current_mapping_job_id
        and current_mapping_job_status in {"queued", "running"}
    )
    return render_template(
        "tasks/mapping.html",
        task_id=task_id,
        task=_load_task_context(task_id),
        messages=messages,
        outputs=rel_outputs,
        log_file=log_file,
        zip_file=zip_file,
        has_error=has_error,
        has_warning=has_warning,
        warning_confirm=warning_confirm,
        step_runs=step_runs,
        step_ok_count=step_ok_count,
        step_error_count=step_error_count,
        error_messages=error_messages,
        last_mapping_file=last_mapping_file,
        current_mapping_display_name=current_mapping_display_name or last_mapping_file,
        saved_schemes=saved_schemes,
        saved_schemes_pagination=saved_schemes_pagination,
        active_mapping_tab=active_mapping_tab,
        mapping_job_id=current_mapping_job_id,
        current_mapping_job_status=current_mapping_job_status,
        current_action=current_action,
        auto_saved_scheme_name=auto_saved_scheme_name,
        mapping_results_runs=mapping_results["runs"],
        mapping_results_pagination=mapping_results["pagination"],
        mapping_results_filters=mapping_results["filters"],
        scheduled_scheme=scheduled_scheme,
        show_processing_status=current_action in {"check", "check_extract"} or (
            current_job_in_progress and current_action in {"check", "check_extract"}
        ),
        show_generated_results=current_action == "run_cached" and not current_job_in_progress,
        current_job_in_progress=current_job_in_progress,
        allow_check_extract=bool(
            last_mapping_file
            and validation_state.get("mapping_file") == last_mapping_file
            and validation_state.get("reference_ok")
        ),
        allow_save_scheme=bool(
            last_mapping_file
            and validation_state.get("mapping_file") == last_mapping_file
            and validation_state.get("extract_ok")
            and current_action == "check_extract"
            and not has_error
            and not auto_saved_scheme_name
        ),
        allow_direct_run=bool(
            last_mapping_file
            and validation_state.get("mapping_file") == last_mapping_file
            and validation_state.get("extract_ok")
            and not has_error
        ),
    )


@tasks_bp.get("/tasks/<task_id>/mapping/example", endpoint="task_download_mapping_example")
def task_download_mapping_example(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)

    static_dir = current_app.static_folder or ""
    rel_sample = "samples/mapping_example.xlsx"
    sample_path = os.path.join(static_dir, rel_sample)
    checked_paths: list[str] = [sample_path]

    if os.path.isfile(sample_path):
        return send_from_directory(
            static_dir,
            rel_sample,
            as_attachment=True,
            download_name="mapping_example.xlsx",
        )

    project_root = Path(current_app.root_path).parent
    fallback_candidates = [
        project_root / "Mapping.xlsx",
        project_root / "mapping.xlsx",
        project_root / "MAPPING.xlsx",
    ]
    for candidate in fallback_candidates:
        checked_paths.append(str(candidate))
        if candidate.is_file():
            return send_file(
                str(candidate),
                as_attachment=True,
                download_name="mapping_example.xlsx",
            )

    current_app.logger.error(
        "Mapping example file not found. checked_paths=%s",
        checked_paths,
    )
    return (
        "找不到 Mapping 範例檔案。請確認 static/samples/mapping_example.xlsx 或專案根目錄 Mapping.xlsx 是否存在。",
        404,
    )


@tasks_bp.get("/tasks/<task_id>/mapping/ops/<op_id>/status", endpoint="task_mapping_op_status")
def task_mapping_op_status(task_id, op_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    workspace_dir = _mapping_workspace_dir(tdir)
    payload = _read_mapping_op(workspace_dir, op_id)
    if not payload:
        return {"ok": False, "error": "Mapping operation not found"}, 404
    return {
        "ok": True,
        "op_id": op_id,
        "status": str(payload.get("status") or "unknown").strip().lower(),
        "action": str(payload.get("action") or payload.get("current_action") or "").strip(),
        "mapping_display_name": str(payload.get("mapping_display_name") or "").strip(),
        "resume_url": str(payload.get("resume_url") or "").strip(),
        "error": str(payload.get("error") or "").strip(),
    }


@tasks_bp.get("/tasks/<task_id>/mapping/ops/active", endpoint="task_mapping_op_active")
def task_mapping_op_active(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    workspace_dir = _mapping_workspace_dir(tdir)
    active_ops = _list_mapping_ops(workspace_dir, {"queued", "running"})
    return {
        "ok": True,
        "runs": [
            {
                "job_id": str(item.get("op_id") or "").strip(),
                "status": str(item.get("status") or "").strip().lower(),
                "action": str(item.get("action") or "").strip(),
                "mapping_display_name": str(item.get("mapping_display_name") or "").strip(),
            }
            for item in active_ops
            if str(item.get("op_id") or "").strip()
        ],
    }


@tasks_bp.get("/tasks/<task_id>/mapping/schemes/<scheme_id>/download", endpoint="task_download_mapping_scheme")
def task_download_mapping_scheme(task_id, scheme_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)

    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme or not scheme.get("source_exists"):
        abort(404)

    download_name = (
        str(scheme.get("mapping_display_name") or "").strip()
        or str(scheme.get("mapping_file") or "").strip()
        or os.path.basename(str(scheme.get("source_path") or ""))
        or f"{scheme_id}.xlsx"
    )
    return send_file(
        scheme["source_path"],
        as_attachment=True,
        download_name=download_name,
    )


def _send_mapping_output_file(task_id: str, filename: str):
    safe_name = (filename or "").replace("\\", "/").strip("/")
    if not safe_name:
        abort(404)

    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_job_dir = os.path.join(tdir, "mapping_job")
    legacy_out_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id)

    for base_dir in (mapping_job_dir, legacy_out_dir):
        file_path = os.path.join(base_dir, safe_name)
        if os.path.isfile(file_path):
            return send_from_directory(base_dir, safe_name, as_attachment=True)
    abort(404)

@tasks_bp.get("/tasks/<task_id>/output/download", endpoint="task_download_output_query")
def task_download_output_query(task_id):
    return _send_mapping_output_file(task_id, request.args.get("filename", ""))


@tasks_bp.get("/tasks/<task_id>/output/<path:filename>", endpoint="task_download_output")
def task_download_output(task_id, filename):
    return _send_mapping_output_file(task_id, filename)
