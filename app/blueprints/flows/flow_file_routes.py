from __future__ import annotations

import os
import shutil

from flask import current_app, request

from .flow_file_blueprint import flow_file_bp
from .flow_file_helpers import (
    _normalize_task_file_rel_path,
    _resolve_task_file_path,
    _validate_new_folder_name,
)


@flow_file_bp.get("", endpoint="api_flow_list_task_files")
def api_flow_list_task_files(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        return {"ok": False, "error": "Task files not found"}, 404

    rel_path_raw = (request.args.get("path") or "").strip()
    try:
        rel_path = _normalize_task_file_rel_path(rel_path_raw)
        abs_dir = _resolve_task_file_path(files_dir, rel_path, expect_dir=True)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    dirs = []
    files = []
    for name in sorted(os.listdir(abs_dir), key=str.lower):
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
        "path": rel_path,
        "parent": parent,
        "dirs": dirs,
        "files": files,
    }


@flow_file_bp.post("/folders", endpoint="api_flow_create_task_folder")
def api_flow_create_task_folder(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        return {"ok": False, "error": "Task files not found"}, 404

    payload = request.get_json(silent=True) if request.is_json else None
    parent_raw = ((payload or {}).get("parent") or request.form.get("parent") or "").strip()
    name_raw = ((payload or {}).get("name") or request.form.get("name") or "").strip()

    try:
        parent_rel = _normalize_task_file_rel_path(parent_raw)
        _resolve_task_file_path(files_dir, parent_rel, expect_dir=True)
        folder_name = _validate_new_folder_name(name_raw)
        target_rel = _normalize_task_file_rel_path(
            f"{parent_rel}/{folder_name}" if parent_rel else folder_name
        )
        target_abs = _resolve_task_file_path(files_dir, target_rel, expect_dir=None)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    if os.path.exists(target_abs):
        return {"ok": False, "error": "資料夾已存在"}, 409

    os.makedirs(target_abs, exist_ok=False)
    return {"ok": True, "path": target_rel, "name": folder_name}


@flow_file_bp.post("/folders/rename", endpoint="api_flow_rename_task_folder")
def api_flow_rename_task_folder(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        return {"ok": False, "error": "Task files not found"}, 404

    payload = request.get_json(silent=True) if request.is_json else None
    path_raw = ((payload or {}).get("path") or request.form.get("path") or "").strip()
    name_raw = ((payload or {}).get("name") or request.form.get("name") or "").strip()

    try:
        target_rel = _normalize_task_file_rel_path(path_raw)
        if not target_rel:
            raise ValueError("根目錄不可重新命名")
        target_abs = _resolve_task_file_path(files_dir, target_rel, expect_dir=True)
        folder_name = _validate_new_folder_name(name_raw)
        parent_rel = os.path.dirname(target_rel).replace("\\", "/")
        renamed_rel = _normalize_task_file_rel_path(
            f"{parent_rel}/{folder_name}" if parent_rel else folder_name
        )
        renamed_abs = _resolve_task_file_path(files_dir, renamed_rel, expect_dir=None)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    if os.path.abspath(target_abs) == os.path.abspath(renamed_abs):
        return {"ok": True, "path": target_rel, "name": os.path.basename(target_rel)}
    if os.path.exists(renamed_abs):
        return {"ok": False, "error": "資料夾已存在"}, 409

    os.rename(target_abs, renamed_abs)
    return {"ok": True, "path": renamed_rel, "name": folder_name}


@flow_file_bp.post("/folders/delete", endpoint="api_flow_delete_task_folder")
def api_flow_delete_task_folder(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        return {"ok": False, "error": "Task files not found"}, 404

    payload = request.get_json(silent=True) if request.is_json else None
    path_raw = ((payload or {}).get("path") or request.form.get("path") or "").strip()

    try:
        target_rel = _normalize_task_file_rel_path(path_raw)
        if not target_rel:
            raise ValueError("根目錄不可刪除")
        target_abs = _resolve_task_file_path(files_dir, target_rel, expect_dir=True)
    except (ValueError, FileNotFoundError) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except PermissionError:
        return {"ok": False, "error": "Permission denied"}, 403

    shutil.rmtree(target_abs)
    return {"ok": True, "deleted": target_rel}
