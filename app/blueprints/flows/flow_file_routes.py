from __future__ import annotations

import io
import os
import shutil
import zipfile
from datetime import datetime

from flask import current_app, request, send_file

from app.services.task_service import build_task_output_path, load_task_context as _load_task_context

from .flow_file_blueprint import flow_file_bp
from .flow_file_helpers import (
    _validate_new_entry_name,
    _normalize_task_file_rel_path,
    _resolve_task_file_path,
    _validate_new_folder_name,
)

_HIDDEN_FLOW_OUTPUT_FILES = {".uo_flow_copy_registry.json"}


def _resolve_browser_root(task_id: str, scope_raw: str) -> tuple[str, str]:
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    scope = (scope_raw or "files").strip().lower()
    if scope == "output":
        task_meta = _load_task_context(task_id) or {}
        root_dir = str(task_meta.get("output_path") or build_task_output_path(task_id)).strip()
        if not root_dir:
            root_dir = build_task_output_path(task_id)
        os.makedirs(root_dir, exist_ok=True)
        return os.path.abspath(root_dir), "output"

    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        raise FileNotFoundError("Task files not found")
    return files_dir, "files"


@flow_file_bp.get("", endpoint="api_flow_list_task_files")
def api_flow_list_task_files(task_id):
    try:
        root_dir, scope = _resolve_browser_root(task_id, request.args.get("scope"))
    except FileNotFoundError as exc:
        return {"ok": False, "error": str(exc)}, 404

    rel_path_raw = (request.args.get("path") or "").strip()
    try:
        rel_path = _normalize_task_file_rel_path(rel_path_raw)
        abs_dir = _resolve_task_file_path(root_dir, rel_path, expect_dir=True)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    dirs = []
    files = []
    for name in sorted(os.listdir(abs_dir), key=str.lower):
        if scope == "output" and name in _HIDDEN_FLOW_OUTPUT_FILES:
            continue
        full = os.path.join(abs_dir, name)
        child_rel = f"{rel_path}/{name}" if rel_path else name
        child_rel = child_rel.replace("\\", "/")
        if os.path.isdir(full):
            dirs.append({"name": name, "path": child_rel})
        elif os.path.isfile(full):
            files.append({"name": name, "path": child_rel})

    parent = None
    if rel_path:
        parts = rel_path.split("/")
        parent = "/".join(parts[:-1]) if len(parts) > 1 else ""

    return {
        "ok": True,
        "scope": scope,
        "root": root_dir,
        "path": rel_path,
        "parent": parent,
        "dirs": dirs,
        "files": files,
    }


@flow_file_bp.post("/folders", endpoint="api_flow_create_task_folder")
def api_flow_create_task_folder(task_id):
    payload = request.get_json(silent=True) if request.is_json else None
    scope_raw = ((payload or {}).get("scope") or request.form.get("scope") or "").strip()
    parent_raw = ((payload or {}).get("parent") or request.form.get("parent") or "").strip()
    name_raw = ((payload or {}).get("name") or request.form.get("name") or "").strip()

    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
        parent_rel = _normalize_task_file_rel_path(parent_raw)
        _resolve_task_file_path(root_dir, parent_rel, expect_dir=True)
        folder_name = _validate_new_folder_name(name_raw)
        target_rel = _normalize_task_file_rel_path(
            f"{parent_rel}/{folder_name}" if parent_rel else folder_name
        )
        target_abs = _resolve_task_file_path(root_dir, target_rel, expect_dir=None)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    if os.path.exists(target_abs):
        return {"ok": False, "error": "資料夾已存在"}, 409

    os.makedirs(target_abs, exist_ok=False)
    return {"ok": True, "scope": scope, "path": target_rel, "name": folder_name}


@flow_file_bp.post("/folders/rename", endpoint="api_flow_rename_task_folder")
def api_flow_rename_task_folder(task_id):
    payload = request.get_json(silent=True) if request.is_json else None
    scope_raw = ((payload or {}).get("scope") or request.form.get("scope") or "").strip()
    path_raw = ((payload or {}).get("path") or request.form.get("path") or "").strip()
    name_raw = ((payload or {}).get("name") or request.form.get("name") or "").strip()

    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
        target_rel = _normalize_task_file_rel_path(path_raw)
        if not target_rel:
            raise ValueError("根目錄不可重新命名")
        target_abs = _resolve_task_file_path(root_dir, target_rel, expect_dir=True)
        folder_name = _validate_new_folder_name(name_raw)
        parent_rel = os.path.dirname(target_rel).replace("\\", "/")
        renamed_rel = _normalize_task_file_rel_path(
            f"{parent_rel}/{folder_name}" if parent_rel else folder_name
        )
        renamed_abs = _resolve_task_file_path(root_dir, renamed_rel, expect_dir=None)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    if os.path.abspath(target_abs) == os.path.abspath(renamed_abs):
        return {"ok": True, "scope": scope, "path": target_rel, "name": os.path.basename(target_rel)}
    if os.path.exists(renamed_abs):
        return {"ok": False, "error": "資料夾已存在"}, 409

    os.rename(target_abs, renamed_abs)
    return {"ok": True, "scope": scope, "path": renamed_rel, "name": folder_name}


@flow_file_bp.post("/folders/delete", endpoint="api_flow_delete_task_folder")
def api_flow_delete_task_folder(task_id):
    payload = request.get_json(silent=True) if request.is_json else None
    scope_raw = ((payload or {}).get("scope") or request.form.get("scope") or "").strip()
    path_raw = ((payload or {}).get("path") or request.form.get("path") or "").strip()

    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
        target_rel = _normalize_task_file_rel_path(path_raw)
        if not target_rel:
            raise ValueError("根目錄不可刪除")
        target_abs = _resolve_task_file_path(root_dir, target_rel, expect_dir=True)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    shutil.rmtree(target_abs)
    return {"ok": True, "scope": scope, "deleted": target_rel}


@flow_file_bp.get("/download", endpoint="api_flow_download_task_file")
def api_flow_download_task_file(task_id):
    scope_raw = (request.args.get("scope") or "").strip()
    path_raw = (request.args.get("path") or "").strip()
    try:
        root_dir, _scope = _resolve_browser_root(task_id, scope_raw)
        rel_path = _normalize_task_file_rel_path(path_raw)
        if not rel_path:
            return {"ok": False, "error": "缺少檔案路徑"}, 400
        file_abs = _resolve_task_file_path(root_dir, rel_path, expect_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    return send_file(file_abs, as_attachment=True, download_name=os.path.basename(file_abs))


@flow_file_bp.get("/download-zip", endpoint="api_flow_download_task_scope_zip")
def api_flow_download_task_scope_zip(task_id):
    scope_raw = (request.args.get("scope") or "").strip()
    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
    except FileNotFoundError as exc:
        return {"ok": False, "error": str(exc)}, 404

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for current_root, _dirs, files in os.walk(root_dir):
            for filename in files:
                if scope == "output" and filename in _HIDDEN_FLOW_OUTPUT_FILES:
                    continue
                file_abs = os.path.join(current_root, filename)
                rel = os.path.relpath(file_abs, root_dir).replace("\\", "/")
                zf.write(file_abs, rel)
    buffer.seek(0)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"{task_id}_{scope}_{stamp}.zip"
    return send_file(buffer, as_attachment=True, download_name=zip_name, mimetype="application/zip")


@flow_file_bp.post("/entries/rename", endpoint="api_flow_rename_task_entry")
def api_flow_rename_task_entry(task_id):
    payload = request.get_json(silent=True) if request.is_json else None
    scope_raw = ((payload or {}).get("scope") or request.form.get("scope") or "").strip()
    path_raw = ((payload or {}).get("path") or request.form.get("path") or "").strip()
    name_raw = ((payload or {}).get("name") or request.form.get("name") or "").strip()

    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
        target_rel = _normalize_task_file_rel_path(path_raw)
        if not target_rel:
            raise ValueError("根目錄不可重新命名")
        target_abs = _resolve_task_file_path(root_dir, target_rel, expect_dir=None)
        entry_name = _validate_new_entry_name(name_raw)
        parent_rel = os.path.dirname(target_rel).replace("\\", "/")
        renamed_rel = _normalize_task_file_rel_path(
            f"{parent_rel}/{entry_name}" if parent_rel else entry_name
        )
        renamed_abs = _resolve_task_file_path(root_dir, renamed_rel, expect_dir=None)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    if os.path.abspath(target_abs) == os.path.abspath(renamed_abs):
        return {"ok": True, "scope": scope, "path": target_rel, "name": os.path.basename(target_rel)}
    if os.path.exists(renamed_abs):
        return {"ok": False, "error": "名稱已存在"}, 409

    os.rename(target_abs, renamed_abs)
    return {"ok": True, "scope": scope, "path": renamed_rel, "name": entry_name}


@flow_file_bp.post("/entries/delete", endpoint="api_flow_delete_task_entry")
def api_flow_delete_task_entry(task_id):
    payload = request.get_json(silent=True) if request.is_json else None
    scope_raw = ((payload or {}).get("scope") or request.form.get("scope") or "").strip()
    path_raw = ((payload or {}).get("path") or request.form.get("path") or "").strip()

    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
        target_rel = _normalize_task_file_rel_path(path_raw)
        if not target_rel:
            raise ValueError("根目錄不可刪除")
        target_abs = _resolve_task_file_path(root_dir, target_rel, expect_dir=None)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    if os.path.isdir(target_abs):
        shutil.rmtree(target_abs)
    else:
        os.remove(target_abs)
    return {"ok": True, "scope": scope, "deleted": target_rel}


@flow_file_bp.post("/clear", endpoint="api_flow_clear_task_scope")
def api_flow_clear_task_scope(task_id):
    payload = request.get_json(silent=True) if request.is_json else None
    scope_raw = ((payload or {}).get("scope") or request.form.get("scope") or "").strip()
    try:
        root_dir, scope = _resolve_browser_root(task_id, scope_raw)
    except FileNotFoundError as exc:
        return {"ok": False, "error": str(exc)}, 404

    for entry in os.listdir(root_dir):
        target = os.path.join(root_dir, entry)
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)
    return {"ok": True, "scope": scope, "cleared": True}
