from __future__ import annotations

import inspect
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

from flask import abort, current_app, redirect, render_template, request, send_file, send_from_directory, session, url_for
from werkzeug.utils import secure_filename

from app.services.execution_service import (
    JobCanceledError,
    MAPPING_OPERATION_JOB,
    MAPPING_SCHEME_RUN_JOB,
    delete_job_record,
    enqueue_job,
    ensure_job_not_canceled,
    find_active_job,
    get_job_payload,
)
from app.models.execution import JobRecord
from app.services.task_service import load_task_context as _load_task_context
from app.services.mapping_metadata_service import (
    list_mapping_run_payloads,
    list_mapping_scheme_payloads,
    sync_run_payload,
    sync_scheme_payload,
)
from app.services.user_context_service import get_actor_info as _get_actor_info
from .blueprint import tasks_bp
from .mapping_scheme_helpers import (
    delete_mapping_scheme,
    enqueue_saved_mapping_scheme_run,
    list_mapping_schemes,
    load_mapping_scheme,
    mapping_scheme_dir,
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
_MAPPING_VALIDATION_DIR = "_validation"
_MAPPING_WORKSPACE_TTL_DAYS = 7


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
    cleaned = "".join(
        "_" if (ord(ch) < 32 or ch in _INVALID_UPLOAD_FILENAME_CHARS) else ch
        for ch in raw_name
    ).strip().strip(".")
    if cleaned in {"", ".", ".."}:
        cleaned = default_stem

    stem, ext = os.path.splitext(cleaned)
    stem = stem.rstrip(" .")
    ext = ext.rstrip(" .")
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
    return os.path.join(
        task_dir,
        "_mapping_sessions",
        _get_mapping_owner_key(),
        _get_mapping_client_id(),
    )


def _reset_mapping_workspace(workspace_dir: str) -> None:
    shutil.rmtree(workspace_dir, ignore_errors=True)
    os.makedirs(workspace_dir, exist_ok=True)


def _delete_mapping_workspace(workspace_dir: str) -> None:
    shutil.rmtree(workspace_dir, ignore_errors=True)


def _mapping_workspace_last_updated(workspace_dir: str) -> float:
    latest = 0.0
    try:
        latest = os.path.getmtime(workspace_dir)
    except OSError:
        return 0.0
    for root, dirs, files in os.walk(workspace_dir):
        for name in dirs:
            try:
                latest = max(latest, os.path.getmtime(os.path.join(root, name)))
            except OSError:
                continue
        for name in files:
            try:
                latest = max(latest, os.path.getmtime(os.path.join(root, name)))
            except OSError:
                continue
    return latest


def _cleanup_stale_mapping_workspaces(task_dir: str, keep_workspace_dir: str = "") -> None:
    owner_dir = os.path.join(task_dir, "_mapping_sessions", _get_mapping_owner_key())
    if not os.path.isdir(owner_dir):
        return

    keep_path = os.path.abspath(keep_workspace_dir) if keep_workspace_dir else ""
    cutoff_ts = (datetime.now() - timedelta(days=_MAPPING_WORKSPACE_TTL_DAYS)).timestamp()
    for client_name in os.listdir(owner_dir):
        workspace_dir = os.path.join(owner_dir, client_name)
        if not os.path.isdir(workspace_dir):
            continue
        abs_workspace_dir = os.path.abspath(workspace_dir)
        if keep_path and abs_workspace_dir == keep_path:
            continue
        if _workspace_has_active_mapping_ops(workspace_dir):
            continue
        last_updated = _mapping_workspace_last_updated(workspace_dir)
        if not last_updated or last_updated >= cutoff_ts:
            continue
        try:
            shutil.rmtree(workspace_dir, ignore_errors=True)
        except OSError:
            current_app.logger.warning("Failed to delete stale mapping workspace: %s", workspace_dir, exc_info=True)


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
    return os.path.join(workspace_dir, _MAPPING_OPS_DIR)


def _mapping_validation_root_dir(workspace_dir: str) -> str:
    return os.path.join(workspace_dir, _MAPPING_VALIDATION_DIR)


def _mapping_validation_run_dir(workspace_dir: str, run_id: str) -> str:
    return os.path.join(_mapping_validation_root_dir(workspace_dir), str(run_id or "").strip())


def _task_relative_path(path: str) -> str:
    rel = os.path.relpath(path, current_app.config["TASK_FOLDER"])
    return rel.replace("\\", "/")


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
    ops_dir = _mapping_ops_dir(workspace_dir)
    os.makedirs(ops_dir, exist_ok=True)
    Path(_mapping_op_path(workspace_dir, op_id)).write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def _update_mapping_op(workspace_dir: str, op_id: str, **fields) -> dict:
    payload = _read_mapping_op(workspace_dir, op_id)
    payload.update(fields)
    _write_mapping_op(workspace_dir, op_id, payload)
    return payload


def _list_mapping_ops(workspace_dir: str, statuses: set[str] | None = None) -> list[dict]:
    ops_dir = os.path.join(workspace_dir, _MAPPING_OPS_DIR)
    if not os.path.isdir(ops_dir):
        return []
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


def _latest_active_mapping_op(workspace_dir: str) -> dict:
    active_ops = _list_mapping_ops(workspace_dir, {"queued", "running"})
    return dict(active_ops[0]) if active_ops else {}


def _mapping_op_redirect_url(task_id: str, op_id: str, *, notice: str = "") -> str:
    params = {"mapping_tab": "create", "mapping_job": op_id}
    if notice:
        params["mapping_notice"] = notice
    return url_for("tasks_bp.task_mapping", task_id=task_id, **params)


def _mapping_op_resume_url(task_id: str, op_id: str) -> str:
    query = urlencode({"mapping_tab": "create", "mapping_job": op_id})
    return f"/tasks/{task_id}/mapping?{query}"


def _localize_mapping_message(message: str) -> str:
    text = str(message or "").strip()
    if not text:
        return ""
    localized = text
    localized = re.sub(r"^ERROR:\s*", "錯誤：", localized, flags=re.IGNORECASE)
    localized = re.sub(r"^(WARN(?:ING)?):\s*", "警告：", localized, flags=re.IGNORECASE)
    localized = re.sub(r"^CANCELED:\s*", "已取消：", localized, flags=re.IGNORECASE)
    localized = re.sub(r"^WF_ERROR:\s*", "工作流程錯誤：", localized, flags=re.IGNORECASE)
    localized = re.sub(r"^\(Row\s+(\d+)\)\s*", lambda m: f"(第 {m.group(1)} 列) ", localized, flags=re.IGNORECASE)
    localized = re.sub(r"\bRow\s+(\d+)\b", lambda m: f"第 {m.group(1)} 列", localized, flags=re.IGNORECASE)

    replacements = [
        ("Invalid mapping operation payload", "Mapping 操作參數無效"),
        ("failed to read log file", "讀取記錄檔失敗"),
        ("Mapping operation not found", "找不到 Mapping 處理記錄"),
        ("Mapping scheme metadata is invalid", "Mapping 文件資料格式無效"),
        ("Mapping scheme source file not found", "找不到 Mapping 文件來源檔案"),
        ("Mapping scheme requires revalidation", "Mapping 文件需要重新檢查"),
        ("Mapping scheme is not validated", "Mapping 文件尚未通過檢查"),
        ("Mapping scheme not found", "找不到 Mapping 文件"),
        ("Canceled during execution", "執行期間已取消"),
        ("empty input name", "未填寫輸入名稱"),
        ("unsupported operation:", "不支援的操作："),
        ("no Word file found in directory:", "資料夾中找不到 Word 檔案："),
        ("file not found:", "找不到檔案："),
        ("directory not found:", "找不到資料夾："),
        ("folder not found", "找不到資料夾"),
        ("missing source filename", "缺少來源檔名"),
        ("unknown error", "未知錯誤"),
        ("Copy File", "複製檔案"),
        ("Copy Folder", "複製資料夾"),
        ("Add Text", "插入純文字段落"),
        ("Add Image", "插入圖片"),
        ("extract_specific_table_from_word", "插入 Word 指定章節/標題的特定表格"),
        ("extract_specific_figure_from_word", "插入 Word 指定章節/標題的特定圖片"),
        ("extract_word_all_content", "擷取 Word 全文"),
        ("extract_word_chapter", "擷取 Word 指定章節/標題"),
        ("extract_pdf_pages_as_images", "擷取 PDF 標籤圖片"),
        ("copy_file", "複製檔案"),
        ("copy_folder", "複製資料夾"),
        ("template_merge", "模版合併"),
        ("insert_text", "插入純文字段落"),
        # ("title=", "標題="),
        # ("index=", "編號="),
        # ("pages=", "頁碼="),
    ]
    for src, dst in replacements:
        localized = localized.replace(src, dst)
    return localized


def _localize_mapping_messages(messages: list[str]) -> list[str]:
    return [_localize_mapping_message(item) for item in (messages or []) if str(item or "").strip()]


def _mapping_message_is_error(message: str) -> bool:
    text = str(message or "").strip()
    return text.startswith(("ERROR:", "錯誤：", "WF_ERROR:", "工作流程錯誤："))


def _mapping_message_is_warning(message: str) -> bool:
    text = str(message or "").strip()
    return text.startswith(("WARN:", "WARNING:", "警告："))


def _mapping_message_is_workflow_error(message: str) -> bool:
    text = str(message or "").strip()
    return text.startswith(("WF_ERROR:", "工作流程錯誤："))


def _first_mapping_error(messages: list[str]) -> str:
    for message in messages:
        text = str(message or "").strip()
        if _mapping_message_is_error(text):
            return text
    return ""


def _can_reuse_mapping_run_id(
    action: str,
    current_mapping_name: str,
    validation_state: dict,
    uploaded_new_mapping: bool,
) -> bool:
    if uploaded_new_mapping:
        return False
    cached_mapping_name = str(validation_state.get("mapping_file") or "").strip()
    cached_run_id = str(validation_state.get("run_id") or "").strip()
    if not current_mapping_name or not cached_run_id or cached_mapping_name != current_mapping_name:
        return False
    if action == "run_cached":
        return True
    return not bool(validation_state.get("extract_ok"))


def _mapping_log_filename_for_action(action: str, default_name: str = "mapping_log.json") -> str:
    action_key = str(action or "").strip().lower()
    if action_key == "check":
        return "mapping_check_log.json"
    if action_key == "check_extract":
        return "mapping_check_extract_log.json"
    if action_key == "run_cached":
        return "mapping_run_log.json"
    return default_name


def _normalize_mapping_log_file(run_out_dir: str, log_file_name: str, action: str) -> str:
    raw_name = str(log_file_name or "").strip()
    if not raw_name:
        return ""
    src_path = os.path.join(run_out_dir, raw_name)
    if not os.path.isfile(src_path):
        return raw_name
    target_name = _mapping_log_filename_for_action(action, default_name=raw_name)
    if target_name == raw_name:
        return raw_name
    target_path = os.path.join(run_out_dir, target_name)
    os.replace(src_path, target_path)
    return target_name


def _preferred_mapping_log_name(action: str, validation_state: dict) -> str:
    action_key = str(action or "").strip().lower()
    if action_key in {"check", "check_extract", "run_cached"}:
        return _mapping_log_filename_for_action(action_key)
    if bool(validation_state.get("extract_ok")):
        return _mapping_log_filename_for_action("check_extract")
    if bool(validation_state.get("reference_ok")):
        return _mapping_log_filename_for_action("check")
    return ""


def _current_mapping_log_path(
    *,
    out_dir: str,
    workspace_dir: str,
    current_action: str,
    current_run_id: str | None,
    log_file_name: str | None,
    log_file: str | None,
) -> str:
    action_key = str(current_action or "").strip().lower()
    run_id = str(current_run_id or "").strip()
    file_name = str(log_file_name or "").strip()
    file_ref = str(log_file or "").strip()
    if action_key in {"check", "check_extract", "save_scheme", "schedule_scheme"} and run_id:
        preferred_name = file_name or (
            _mapping_log_filename_for_action(action_key)
            if action_key in {"check", "check_extract"}
            else ""
        )
        validation_log_path = os.path.join(_mapping_validation_run_dir(workspace_dir, run_id), preferred_name)
        if preferred_name and os.path.isfile(validation_log_path):
            return validation_log_path
    if file_ref:
        output_log_path = os.path.join(out_dir, file_ref)
        if os.path.isfile(output_log_path):
            return output_log_path
    return ""


def _load_mapping_run_ui_snapshot(out_dir: str, run_id: str) -> dict:
    current_run_id = str(run_id or "").strip()
    if not current_run_id:
        return {}
    run_dir = os.path.join(out_dir, current_run_id)
    meta_path = os.path.join(run_dir, "meta.json")
    if not os.path.isfile(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        current_app.logger.exception("Failed to load mapping run ui snapshot: %s", meta_path)
        return {}
    if not isinstance(payload, dict):
        return {}

    outputs = []
    for item in payload.get("outputs") or []:
        rel = str(item or "").strip().replace("\\", "/").lstrip("/")
        if rel:
            outputs.append(f"{current_run_id}/{rel}")
    log_file_name = str(payload.get("log_file") or "").strip()
    zip_file_name = str(payload.get("zip_file") or "").strip()
    return {
        "current_action": "run_cached",
        "current_run_id": current_run_id,
        "current_mapping_display_name": str(payload.get("mapping_display_name") or payload.get("mapping_file") or "").strip(),
        "status": str(payload.get("status") or "").strip().lower(),
        "messages": [],
        "outputs": outputs,
        "log_file": f"{current_run_id}/{log_file_name}" if log_file_name else "",
        "zip_file": f"{current_run_id}/{zip_file_name}" if zip_file_name else "",
        "log_file_name": log_file_name,
        "zip_file_name": zip_file_name,
        "auto_saved_scheme_name": "",
    }


def _list_check_log_downloads(task_id: str, workspace_dir: str, run_id: str | None) -> list[dict[str, str]]:
    current_run_id = str(run_id or "").strip()
    if not current_run_id:
        return []
    run_dir = _mapping_validation_run_dir(workspace_dir, current_run_id)
    if not os.path.isdir(run_dir):
        return []
    candidates = [
        ("引用檢查 Log", _mapping_log_filename_for_action("check")),
        ("擷取檢查 Log", _mapping_log_filename_for_action("check_extract")),
    ]
    downloads: list[dict[str, str]] = []
    for label, filename in candidates:
        if os.path.isfile(os.path.join(run_dir, filename)):
            downloads.append(
                {
                    "label": label,
                    "url": url_for(
                        "tasks_bp.task_download_mapping_validation_log",
                        task_id=task_id,
                        run_id=current_run_id,
                        kind="check" if filename == _mapping_log_filename_for_action("check") else "check_extract",
                    ),
                }
            )
    return downloads


def _can_run_saved_scheme_via_current_validation(
    scheme: dict | None,
    *,
    requested_action: str,
    last_mapping_file: str | None,
    validation_state: dict,
) -> bool:
    if str(requested_action or "").strip() != "run_scheme":
        return False
    if not scheme:
        return False
    current_mapping_file = str(last_mapping_file or "").strip()
    validated_mapping_file = str(validation_state.get("mapping_file") or "").strip()
    scheme_mapping_file = str(scheme.get("mapping_file") or "").strip()
    if not current_mapping_file or not validated_mapping_file or not scheme_mapping_file:
        return False
    if current_mapping_file != validated_mapping_file:
        return False
    if scheme_mapping_file != validated_mapping_file:
        return False
    return bool(validation_state.get("reference_ok")) and bool(validation_state.get("extract_ok"))


def _find_reusable_validated_mapping_workspace(task_dir: str, mapping_file: str) -> tuple[str, str, dict] | None:
    target_mapping_file = str(mapping_file or "").strip()
    if not target_mapping_file:
        return None
    owner_dir = os.path.join(task_dir, "_mapping_sessions", _get_mapping_owner_key())
    if not os.path.isdir(owner_dir):
        return None

    candidates: list[tuple[float, str, str, dict]] = []
    for client_name in os.listdir(owner_dir):
        workspace_dir = os.path.join(owner_dir, client_name)
        if not os.path.isdir(workspace_dir):
            continue
        last_mapping_marker = os.path.join(workspace_dir, "mapping_last.txt")
        validation_state_path = os.path.join(workspace_dir, "mapping_validation_state.json")
        last_mapping_file, validation_state, _display_name = _load_mapping_workspace_cache(
            last_mapping_marker,
            validation_state_path,
        )
        if not last_mapping_file:
            continue
        validated_mapping_file = str(validation_state.get("mapping_file") or "").strip()
        if validated_mapping_file != target_mapping_file:
            continue
        if not bool(validation_state.get("reference_ok")) or not bool(validation_state.get("extract_ok")):
            continue
        mapping_path = os.path.join(workspace_dir, last_mapping_file)
        if not os.path.isfile(mapping_path):
            continue
        try:
            score = os.path.getmtime(validation_state_path) if os.path.isfile(validation_state_path) else os.path.getmtime(mapping_path)
        except OSError:
            score = 0.0
        candidates.append((score, workspace_dir, last_mapping_file, dict(validation_state)))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    _score, workspace_dir, last_mapping_file, validation_state = candidates[0]
    return workspace_dir, last_mapping_file, validation_state


def _get_saved_scheme_reusable_run_id(scheme: dict | None, *, requested_action: str) -> str:
    if str(requested_action or "").strip() != "run_scheme":
        return ""
    if not scheme:
        return ""
    return str(scheme.get("validated_run_id") or "").strip()


def _mapping_job_source_copy_path(run_out_dir: str, mapping_path: str) -> str:
    ext = os.path.splitext(os.path.basename(str(mapping_path or "").strip()))[1] or ".xlsx"
    return os.path.join(run_out_dir, f"source{ext}")


def _prepare_mapping_job_source(mapping_path: str, run_out_dir: str) -> str:
    source_path = _mapping_job_source_copy_path(run_out_dir, mapping_path)
    os.makedirs(run_out_dir, exist_ok=True)
    if os.path.abspath(source_path) != os.path.abspath(mapping_path):
        shutil.copy2(mapping_path, source_path)
    return source_path


def _run_mapping_operation_job(op_id: str, payload: dict) -> dict:
    task_id = str(payload.get("task_id") or "").strip()
    workspace_dir = str(payload.get("workspace_dir") or "").strip()
    action = str(payload.get("action") or "").strip()
    mapping_path = str(payload.get("mapping_path") or "").strip()
    current_mapping_display_name = str(payload.get("current_mapping_display_name") or "").strip()
    validation_state_snapshot = dict(payload.get("validation_state_snapshot") or {})
    actor = dict(payload.get("actor") or {})
    enable_figure_reference = bool(payload.get("enable_figure_reference"))
    manage_workspace_state = bool(workspace_dir)
    if not task_id or not action or not mapping_path:
        raise RuntimeError("Mapping 操作參數無效")
    if not manage_workspace_state and action != "run_cached":
        raise RuntimeError("Mapping 操作參數無效")

    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    out_dir = os.path.join(tdir, "mapping_job")
    validation_state_path = os.path.join(workspace_dir, "mapping_validation_state.json") if manage_workspace_state else ""
    ui_state_path = os.path.join(workspace_dir, _MAPPING_UI_STATE_FILE) if manage_workspace_state else ""
    current_run_id = op_id
    validation_out_dir = _mapping_validation_run_dir(workspace_dir, current_run_id) if manage_workspace_state else ""

    def _check_canceled() -> None:
        ensure_job_not_canceled(op_id)

    if manage_workspace_state:
        _update_mapping_op(
            workspace_dir,
            op_id,
            status="running",
            started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
    try:
        from modules.mapping_processor import process_mapping_excel
        run_result_payload: dict | None = None

        run_out_dir = os.path.join(out_dir, current_run_id) if action == "run_cached" else validation_out_dir
        process_kwargs = {
            "log_dir": run_out_dir,
            "validate_only": (action == "check"),
            "validate_extract_only": (action == "check_extract"),
        }
        try:
            supported_params = inspect.signature(process_mapping_excel).parameters
            if "cancel_check" in supported_params:
                process_kwargs["cancel_check"] = _check_canceled
            if action == "run_cached" and "enable_figure_reference" in supported_params:
                process_kwargs["enable_figure_reference"] = enable_figure_reference
        except (TypeError, ValueError):
            pass

        _check_canceled()
        result = process_mapping_excel(
            mapping_path,
            files_dir,
            run_out_dir,
            **process_kwargs,
        )
        raw_messages = [str(item) for item in (result.get("logs") or [])]
        messages = _localize_mapping_messages(raw_messages)
        outputs = [str(item) for item in (result.get("outputs") or [])]
        log_file_raw = _normalize_mapping_log_file(
            run_out_dir,
            str(result.get("log_file") or "").strip(),
            action,
        )
        zip_file_raw = str(result.get("zip_file") or "").strip()
        log_file = f"{current_run_id}/{log_file_raw}" if log_file_raw else ""
        zip_file = f"{current_run_id}/{zip_file_raw}" if zip_file_raw else ""
        current_has_error = any(_mapping_message_is_error(message) for message in raw_messages + messages)
        current_mapping_name = (
            str(validation_state_snapshot.get("mapping_file") or "").strip()
            or os.path.basename(mapping_path)
        )
        preserved_validation_run_id = str(validation_state_snapshot.get("run_id") or "").strip()
        next_validation_state = {
            "mapping_file": current_mapping_name,
            "mapping_display_name": current_mapping_display_name or current_mapping_name,
            "reference_ok": bool(validation_state_snapshot.get("reference_ok")),
            "extract_ok": bool(validation_state_snapshot.get("extract_ok")),
            "run_id": preserved_validation_run_id or current_run_id,
        }
        if action == "check":
            next_validation_state["reference_ok"] = not current_has_error
            next_validation_state["extract_ok"] = False
        elif action == "check_extract":
            next_validation_state["extract_ok"] = not current_has_error
        if manage_workspace_state:
            Path(validation_state_path).write_text(
                json.dumps(next_validation_state, ensure_ascii=False),
                encoding="utf-8",
            )

        rel_outputs = []
        for output_path in outputs:
            rel = os.path.relpath(output_path, out_dir) if os.path.isabs(output_path) else str(output_path)
            rel_outputs.append(rel.replace("\\", "/"))

        if action == "run_cached":
            completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            run_outputs = []
            run_prefix = f"{current_run_id}/"
            for rel in rel_outputs:
                run_outputs.append(rel[len(run_prefix):] if rel.startswith(run_prefix) else rel)
            run_result_payload = {
                "record_type": "mapping_run",
                "run_id": current_run_id,
                "mapping_file": next_validation_state.get("mapping_file") or "",
                "mapping_display_name": next_validation_state.get("mapping_display_name") or "",
                "status": "failed" if current_has_error else "completed",
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "completed_at": completed_at,
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
            }
            write_mapping_run_meta(
                os.path.join(out_dir, current_run_id),
                run_result_payload,
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
                    validation_log_dir=run_out_dir,
                )
                ui_payload["auto_saved_scheme_name"] = str(
                    auto_saved_scheme.get("display_name") or auto_saved_scheme.get("id") or ""
                ).strip()
            except Exception:
                current_app.logger.exception("Failed to auto-save validated mapping scheme")
        if manage_workspace_state:
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
        if manage_workspace_state and action == "run_cached" and not current_has_error:
            _delete_mapping_workspace(workspace_dir)
        artifacts = []
        for artifact_type, filename in (("log_json", log_file_raw), ("result_zip", zip_file_raw)):
            if not filename:
                continue
            path = os.path.join(run_out_dir, filename)
            if os.path.isfile(path):
                artifacts.append(
                    {
                        "artifact_type": artifact_type,
                        "rel_path": _task_relative_path(path),
                        "size_bytes": os.path.getsize(path),
                    }
                )
        return {
            "artifact_root": _task_relative_path(run_out_dir),
            "artifacts": artifacts,
            "result_payload": run_result_payload,
        }
    except JobCanceledError as exc:
        messages = [_localize_mapping_message(f"CANCELED: {exc}")]
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
        if manage_workspace_state:
            _write_mapping_ui_state(ui_state_path, ui_payload)
        if action == "run_cached":
            write_mapping_run_meta(
                os.path.join(out_dir, current_run_id),
                {
                    "record_type": "mapping_run",
                    "run_id": current_run_id,
                    "mapping_file": (
                        str(validation_state_snapshot.get("mapping_file") or "").strip()
                        or os.path.basename(mapping_path)
                    ),
                    "mapping_display_name": (
                        current_mapping_display_name
                        or str(validation_state_snapshot.get("mapping_display_name") or "").strip()
                        or os.path.basename(mapping_path)
                    ),
                    "status": "canceled",
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
        if manage_workspace_state:
            _update_mapping_op(
                workspace_dir,
                op_id,
                status="canceled",
                completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                mapping_display_name=(
                    current_mapping_display_name
                    or str(validation_state_snapshot.get("mapping_display_name") or "").strip()
                    or os.path.basename(mapping_path)
                ),
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
        raise
    except Exception as exc:
        current_app.logger.exception("Mapping operation failed")
        messages = [_localize_mapping_message(f"ERROR: {exc}")]
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
        if manage_workspace_state:
            _write_mapping_ui_state(ui_state_path, ui_payload)
        if action == "run_cached":
            write_mapping_run_meta(
                os.path.join(out_dir, current_run_id),
                {
                    "record_type": "mapping_run",
                    "run_id": current_run_id,
                    "mapping_file": (
                        str(validation_state_snapshot.get("mapping_file") or "").strip()
                        or os.path.basename(mapping_path)
                    ),
                    "mapping_display_name": (
                        current_mapping_display_name
                        or str(validation_state_snapshot.get("mapping_display_name") or "").strip()
                        or os.path.basename(mapping_path)
                    ),
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
        if manage_workspace_state:
            _update_mapping_op(
                workspace_dir,
                op_id,
                status="failed",
                completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                mapping_display_name=(
                    current_mapping_display_name
                    or str(validation_state_snapshot.get("mapping_display_name") or "").strip()
                    or os.path.basename(mapping_path)
                ),
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
        raise

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
    _cleanup_stale_mapping_workspaces(tdir, keep_workspace_dir=workspace_dir)
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
    mapping_notice = (request.values.get("mapping_notice") or "").strip().lower()
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
    if mapping_notice == "active_op":
        messages.append("目前已有 Mapping 處理進行中，可先前往其他頁面，完成後再回來查看。")
    resume_mapping_state = (
        request.method == "GET"
        and active_mapping_tab == "create"
        and (request.args.get("resume_mapping") or "").strip() == "1"
    )
    current_mapping_job_id = (request.args.get("mapping_job") or "").strip()
    current_mapping_op = _read_mapping_op(workspace_dir, current_mapping_job_id) if current_mapping_job_id else {}
    current_mapping_job_status = str(current_mapping_op.get("status") or "").strip().lower() if current_mapping_op else ""
    if current_mapping_job_id and not current_mapping_job_status:
        job_row = JobRecord.query.filter_by(job_id=current_mapping_job_id, task_id=task_id).first()
        if job_row:
            current_mapping_job_status = str(job_row.status or "").strip().lower()
    has_active_mapping_ops = _workspace_has_active_mapping_ops(workspace_dir)

    # 如果是頁面跳轉/重新整理 (GET)，則清掉之前的暫存紀錄與檔案
    if request.method == "GET":
        if resume_mapping_state or current_mapping_op or has_active_mapping_ops or current_mapping_job_id:
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
            elif current_mapping_job_id:
                snapshot = _load_mapping_run_ui_snapshot(out_dir, current_mapping_job_id)
                if snapshot:
                    snapshot_status = str(snapshot.get("status") or "").strip().lower()
                    if snapshot_status in {"completed", "failed", "canceled", "timeout"}:
                        current_mapping_job_status = snapshot_status
                    current_action = str(snapshot.get("current_action") or "").strip()
                    current_run_id = str(snapshot.get("current_run_id") or "").strip() or None
                    current_mapping_display_name = (
                        str(snapshot.get("current_mapping_display_name") or "").strip()
                        or current_mapping_display_name
                    )
                    messages = [str(item) for item in (snapshot.get("messages") or [])]
                    outputs = [str(item) for item in (snapshot.get("outputs") or [])]
                    log_file = str(snapshot.get("log_file") or "").strip() or None
                    zip_file = str(snapshot.get("zip_file") or "").strip() or None
                    log_file_name = str(snapshot.get("log_file_name") or "").strip() or None
                    zip_file_name = str(snapshot.get("zip_file_name") or "").strip() or None
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
        row_prefix = f"(第 {row_no} 列) " if row_no not in (None, "", "None") else ""
        preset_action = (params.get("mapping_action_label") or "").strip()
        preset_detail = (params.get("mapping_detail_label") or "").strip()
        if preset_action:
            return _localize_mapping_message(f"{row_prefix}{preset_action}"), _localize_mapping_message(preset_detail)
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
        return _localize_mapping_message(f"{row_prefix}{stype or '步驟'}"), ""

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

        if action in {"run_scheme", "run_scheme_figure_reference"}:
            scheme_id = (request.form.get("scheme_id") or "").strip()
            active_scheme = load_mapping_scheme(task_id, scheme_id)
            if not active_scheme:
                messages.append("找不到指定的 Mapping 方案。")
            elif not active_scheme.get("is_runnable"):
                messages.append(f"方案「{active_scheme.get('display_name') or scheme_id}」目前不可執行，請先重新檢查。")
            else:
                try:
                    work_id, actor_label = _get_actor_info()
                    if action == "run_scheme":
                        mapping_path = str(active_scheme.get("source_path") or "").strip()
                        current_mapping_name = os.path.basename(mapping_path)
                        current_mapping_display_name = (
                            active_scheme.get("mapping_display_name")
                            or active_scheme.get("display_name")
                            or current_mapping_name
                        )
                        current_run_id = uuid.uuid4().hex[:8]
                        run_artifact_dir = os.path.join(out_dir, current_run_id)
                        job_mapping_path = _prepare_mapping_job_source(mapping_path, run_artifact_dir)
                        validation_snapshot = {
                            "mapping_file": str(active_scheme.get("mapping_file") or current_mapping_name or "").strip(),
                            "mapping_display_name": current_mapping_display_name,
                            "reference_ok": bool(active_scheme.get("reference_ok")),
                            "extract_ok": bool(active_scheme.get("extract_ok")),
                            "run_id": str(active_scheme.get("validated_run_id") or "").strip(),
                        }
                        enqueue_job(
                            MAPPING_OPERATION_JOB,
                            {
                                "task_id": task_id,
                                "action": "run_cached",
                                "mapping_path": job_mapping_path,
                                "current_mapping_display_name": current_mapping_display_name,
                                "validation_state_snapshot": validation_snapshot,
                                "enable_figure_reference": False,
                                "actor": {"work_id": work_id, "label": actor_label},
                                "source": "saved_scheme",
                                "scheme_id": str(active_scheme.get("id") or "").strip(),
                            },
                            task_id=task_id,
                            target_name=current_mapping_display_name or os.path.basename(mapping_path),
                            actor={"work_id": work_id, "label": actor_label},
                            queue_name="heavy",
                            job_id=current_run_id,
                            artifact_root=os.path.join(task_id, "mapping_job", current_run_id).replace("\\", "/"),
                        )
                    else:
                        current_run_id = enqueue_saved_mapping_scheme_run(
                            task_id,
                            scheme_id,
                            actor={"work_id": work_id, "label": actor_label},
                            source="manual",
                            job_id=None,
                            enable_figure_reference=(action == "run_scheme_figure_reference"),
                        )
                        current_mapping_display_name = active_scheme.get("display_name") or active_scheme.get("mapping_display_name") or ""
                    active_mapping_tab = "results"
                    messages = []
                except Exception as e:
                    messages = [_localize_mapping_message(str(e))]
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
                    messages = [_localize_mapping_message(str(e))]
        elif action == "save_scheme":
            active_workspace_op = _latest_active_mapping_op(workspace_dir)
            active_workspace_op_id = str(active_workspace_op.get("op_id") or "").strip()
            if active_workspace_op_id:
                return redirect(_mapping_op_redirect_url(task_id, active_workspace_op_id, notice="active_op"))
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
                        validation_log_dir=_mapping_validation_run_dir(
                            workspace_dir,
                            str(validation_state.get("run_id") or "").strip(),
                        ),
                    )
                    messages.append(f"已儲存方案：{saved_scheme.get('display_name') or saved_scheme.get('id')}")
                    _delete_mapping_workspace(workspace_dir)
                    last_mapping_file = None
                    validation_state = {
                        "mapping_file": "",
                        "mapping_display_name": "",
                        "reference_ok": False,
                        "extract_ok": False,
                        "run_id": "",
                    }
                    current_mapping_display_name = ""
                    current_run_id = None
                    current_action = ""
                    outputs = []
                    log_file = None
                    zip_file = None
                    log_file_name = None
                    zip_file_name = None
                    step_runs = []
                except Exception as e:
                    messages = [_localize_mapping_message(str(e))]
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
                    messages = [_localize_mapping_message(str(e))]
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
                    messages = [_localize_mapping_message(str(e))]
        else:
            active_mapping_tab = "create"
            active_workspace_op = _latest_active_mapping_op(workspace_dir)
            active_workspace_op_id = str(active_workspace_op.get("op_id") or "").strip()
            if active_workspace_op_id:
                return redirect(_mapping_op_redirect_url(task_id, active_workspace_op_id, notice="active_op"))
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
                    existing = find_active_job(
                        MAPPING_OPERATION_JOB,
                        task_id=task_id,
                        payload_matcher=lambda data: (
                            str(data.get("workspace_dir") or "").strip() == str(workspace_dir or "").strip()
                            and str(data.get("action") or "").strip() == str(action or "").strip()
                            and str(data.get("workspace_mapping_path") or data.get("mapping_path") or "").strip()
                            == str(mapping_path or "").strip()
                        ),
                    )
                    if existing:
                        return redirect(
                            url_for(
                                "tasks_bp.task_mapping",
                                task_id=task_id,
                                mapping_tab="create",
                                mapping_job=str(existing.job_id),
                            )
                        )
                    if _can_reuse_mapping_run_id(action, current_mapping_name, validation_state, uploaded_new_mapping):
                        current_run_id = str(validation_state.get("run_id") or "").strip() or uuid.uuid4().hex[:8]
                        delete_job_record(current_run_id)
                    else:
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
                    run_artifact_dir = (
                        os.path.join(out_dir, current_run_id)
                        if action == "run_cached"
                        else _mapping_validation_run_dir(workspace_dir, current_run_id)
                    )
                    job_mapping_path = _prepare_mapping_job_source(mapping_path, run_artifact_dir)
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
                    enqueue_job(
                        MAPPING_OPERATION_JOB,
                        {
                            "task_id": task_id,
                            "workspace_dir": workspace_dir,
                            "action": action,
                            "mapping_path": job_mapping_path,
                            "workspace_mapping_path": mapping_path,
                            "current_mapping_display_name": current_mapping_display_name,
                            "validation_state_snapshot": dict(validation_state),
                            "actor": {"work_id": actor_work_id, "label": actor_label},
                        },
                        task_id=task_id,
                        target_name=current_mapping_display_name or os.path.basename(mapping_path),
                        actor={"work_id": actor_work_id, "label": actor_label},
                        queue_name="light" if action in {"check", "check_extract"} else "heavy",
                        job_id=current_run_id,
                        artifact_root=_task_relative_path(run_artifact_dir),
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
                    messages = [_localize_mapping_message(str(e))]
        if action in {"save_scheme", "schedule_scheme"} and not log_file:
            preserved_run_id = str(validation_state.get("run_id") or "").strip()
            if preserved_run_id:
                preserved_run_dir = _mapping_validation_run_dir(workspace_dir, preserved_run_id)
                if os.path.isdir(preserved_run_dir):
                    current_run_id = preserved_run_id
                    preserved_log_name = ""
                    preserved_zip_name = ""
                    preferred_log_name = _preferred_mapping_log_name(action, validation_state)
                    candidates = list(os.listdir(preserved_run_dir))
                    if preferred_log_name and preferred_log_name in candidates:
                        preserved_log_name = preferred_log_name
                    for candidate in candidates:
                        lower_name = candidate.lower()
                        if not preserved_log_name and lower_name.endswith(".json"):
                            preserved_log_name = candidate
                        if not preserved_zip_name and lower_name.endswith(".zip"):
                            preserved_zip_name = candidate
                    if preserved_log_name:
                        log_file_name = preserved_log_name
                        log_file = preserved_log_name
                    if preserved_zip_name:
                        zip_file_name = preserved_zip_name
                        zip_file = f"{preserved_run_id}/{preserved_zip_name}"
                    for root, _dirs, files in os.walk(preserved_run_dir):
                        for name in files:
                            if not name.lower().endswith(".docx"):
                                continue
                            rel = os.path.relpath(os.path.join(root, name), out_dir).replace("\\", "/")
                            outputs.append(rel)
    log_path = _current_mapping_log_path(
        out_dir=out_dir,
        workspace_dir=workspace_dir,
        current_action=current_action,
        current_run_id=current_run_id,
        log_file_name=log_file_name,
        log_file=log_file,
    )
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
                        localized_action = _localize_mapping_message(action)
                        localized_detail = _localize_mapping_message(detail)
                        localized_error = _localize_mapping_message(entry.get("error") or "")
                        detail_short, detail_long = _truncate_detail(localized_detail) if localized_detail else ("", False)
                        step_runs.append(
                            {
                                "action": localized_action,
                                "detail": localized_detail,
                                "detail_short": detail_short,
                                "detail_long": detail_long,
                                "row_no": row_no,
                                "status": entry.get("status") or "ok",
                                "error": localized_error,
                            }
                        )
                if step_runs:
                    messages = [m for m in messages if not _mapping_message_is_workflow_error(m)]
            except Exception as e:
                messages.append(_localize_mapping_message(f"ERROR: failed to read log file ({e})"))
    messages = _localize_mapping_messages(messages)
    has_error = any(_mapping_message_is_error(m) for m in messages) or any(
        step.get("status") == "error" for step in step_runs
    )
    warning_messages = [m for m in messages if _mapping_message_is_warning(m)]
    has_warning = bool(warning_messages)
    warning_confirm = None
    if has_warning:
        trimmed = []
        for m in warning_messages[:3]:
            trimmed.append(re.sub(r"^(警告：|WARN:|WARNING:)\s*", "", m, flags=re.IGNORECASE).strip())
        warning_confirm = "發現警告，是否仍要繼續？\n" + "\n".join(trimmed)

    error_messages = [m for m in messages if _mapping_message_is_error(m)]
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
            raw = re.sub(r"^(錯誤：|ERROR:)\s*", "", msg or "", flags=re.IGNORECASE).strip()
            raw = re.sub(r"^(第\s*\d+\s*列|Row\s+\d+)\s*:\s*", "", raw, flags=re.IGNORECASE)
            action = raw
            detail = ""
            error_text = raw
            row_match = re.search(r"(?:第\s*(\d+)\s*列|Row\s+(\d+))", msg or "", re.IGNORECASE)
            row_no_text = ""
            if row_match:
                row_no_text = row_match.group(1) or row_match.group(2) or ""
            row_prefix = f"(第 {row_no_text} 列) " if row_no_text else ""
            if "::" in raw:
                parts = [p.strip() for p in raw.split("::", 2)]
                if len(parts) >= 2:
                    base_action = parts[0] or action
                    if base_action.startswith("(第 "):
                        action = base_action
                    else:
                        action = f"{row_prefix}{base_action}".strip()
                    detail = parts[1]
                if len(parts) == 3:
                    error_text = parts[2]
            elif ":" in raw:
                head, tail = raw.split(":", 1)
                base_action = head.strip() or raw
                if base_action.startswith("(第 "):
                    action = base_action
                else:
                    action = f"{row_prefix}{base_action}".strip()
                detail = tail.strip()
            display_detail = detail or error_text
            parsed_row_no = int(row_no_text) if row_no_text else None
            norm_error_text = _norm_error_text(error_text)
            norm_display_detail = _norm_error_text(display_detail)
            existing_bucket = existing_row_errors.get(parsed_row_no, set())
            if norm_error_text in existing_bucket or norm_display_detail in existing_bucket:
                continue

            localized_display_detail = _localize_mapping_message(display_detail)
            detail_short, detail_long = _truncate_detail(localized_display_detail)
            error_steps.append(
                {
                    "action": _localize_mapping_message(action),
                    "detail": localized_display_detail,
                    "detail_short": detail_short,
                    "detail_long": detail_long,
                    "row_no": parsed_row_no,
                    "status": "error",
                    "error": _localize_mapping_message(error_text),
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
    # 始終獲取所有分頁所需的資料，以支援無刷新切換
    scheduled_scheme = load_scheduled_mapping_scheme(task_id)
    scheduled_scheme_id = (scheduled_scheme or {}).get("id") or (scheduled_scheme or {}).get("scheme_id") or ""
    
    saved_scheme_result = list_mapping_scheme_payloads(
        task_id,
        page=mapping_page,
        per_page=10,
        scheduled_scheme_id=scheduled_scheme_id,
        current_revision=None,
    )
    saved_schemes = saved_scheme_result["items"]
    saved_schemes_pagination = saved_scheme_result["pagination"]

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
    check_log_downloads = _list_check_log_downloads(task_id, workspace_dir, current_run_id)
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
        check_log_downloads=check_log_downloads,
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
        out_dir = os.path.join(tdir, "mapping_job")
        snapshot = _load_mapping_run_ui_snapshot(out_dir, op_id)
        if snapshot:
            snapshot_status = str(snapshot.get("status") or "").strip().lower() or "unknown"
            return {
                "ok": True,
                "op_id": op_id,
                "status": snapshot_status,
                "action": str(snapshot.get("current_action") or "run_cached").strip(),
                "mapping_display_name": str(snapshot.get("current_mapping_display_name") or "").strip(),
                "resume_url": url_for(
                    "tasks_bp.task_mapping",
                    task_id=task_id,
                    mapping_tab="create",
                    mapping_job=op_id,
                ),
                "error": "",
            }
        return {"ok": False, "error": _localize_mapping_message("Mapping operation not found")}, 404
    return {
        "ok": True,
        "op_id": op_id,
        "status": str(payload.get("status") or "unknown").strip().lower(),
        "action": str(payload.get("action") or payload.get("current_action") or "").strip(),
        "mapping_display_name": str(payload.get("mapping_display_name") or "").strip(),
        "resume_url": str(payload.get("resume_url") or "").strip(),
        "error": _localize_mapping_message(str(payload.get("error") or "").strip()),
    }


@tasks_bp.get(
    "/tasks/<task_id>/mapping/validation/<run_id>/logs/<kind>",
    endpoint="task_download_mapping_validation_log",
)
def task_download_mapping_validation_log(task_id, run_id, kind):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)

    workspace_dir = _mapping_workspace_dir(tdir)
    kind_key = str(kind or "").strip().lower()
    if kind_key not in {"check", "check_extract"}:
        abort(404)

    filename = _mapping_log_filename_for_action(kind_key)
    file_path = os.path.join(_mapping_validation_run_dir(workspace_dir, run_id), filename)
    if not os.path.isfile(file_path):
        abort(404)

    return send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
    )


@tasks_bp.get("/tasks/<task_id>/mapping/ops/active", endpoint="task_mapping_op_active")
def task_mapping_op_active(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    workspace_dir = _mapping_workspace_dir(tdir)
    active_ops = _list_mapping_ops(workspace_dir, {"queued", "running"})
    active_scheme_jobs = (
        JobRecord.query.filter(
            JobRecord.task_id == task_id,
            JobRecord.job_type == MAPPING_SCHEME_RUN_JOB,
            JobRecord.status.in_(["queued", "running"]),
        )
        .order_by(JobRecord.created_at.desc(), JobRecord.job_id.desc())
        .all()
    )
    scheme_runs = []
    for job in active_scheme_jobs:
        payload = get_job_payload(job)
        mapping_name = (
            str(payload.get("mapping_display_name") or "").strip()
            or str(payload.get("scheme_name") or "").strip()
            or str(job.target_name or "").strip()
            or "未命名 Mapping"
        )
        scheme_runs.append(
            {
                "job_id": str(job.job_id or "").strip(),
                "status": str(job.status or "").strip().lower(),
                "action": "run_scheme",
                "mapping_display_name": mapping_name,
                "status_url": url_for("mapping_run_bp.mapping_run_status", task_id=task_id, run_id=job.job_id),
            }
        )

    op_runs = [
        {
            "job_id": str(item.get("op_id") or "").strip(),
            "status": str(item.get("status") or "").strip().lower(),
            "action": str(item.get("action") or "").strip(),
            "mapping_display_name": str(item.get("mapping_display_name") or "").strip(),
            "status_url": url_for("tasks_bp.task_mapping_op_status", task_id=task_id, op_id=str(item.get("op_id") or "").strip()),
        }
        for item in active_ops
        if str(item.get("op_id") or "").strip()
    ]
    return {
        "ok": True,
        "runs": op_runs + scheme_runs,
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


@tasks_bp.get(
    "/tasks/<task_id>/mapping/schemes/<scheme_id>/logs/<kind>",
    endpoint="task_download_mapping_scheme_log",
)
def task_download_mapping_scheme_log(task_id, scheme_id, kind):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)

    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme:
        abort(404)

    kind_key = str(kind or "").strip().lower()
    if kind_key == "check":
        log_file = str(scheme.get("check_log_file") or "").strip()
    elif kind_key == "check_extract":
        log_file = str(scheme.get("check_extract_log_file") or "").strip()
    else:
        abort(404)

    if not log_file:
        abort(404)

    scheme_dir = mapping_scheme_dir(task_id, scheme_id)
    file_path = os.path.join(scheme_dir, log_file)
    if not os.path.isfile(file_path):
        abort(404)

    return send_file(
        file_path,
        as_attachment=True,
        download_name=log_file,
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
