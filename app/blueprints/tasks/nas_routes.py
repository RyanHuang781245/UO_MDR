from __future__ import annotations

import json
import os
import shutil

from flask import abort, current_app, flash, jsonify, redirect, url_for

from app.services.audit_service import record_audit
from app.services.nas_service import get_configured_nas_roots
from app.services.task_service import ensure_windows_long_path, enforce_max_copy_size, list_files
from app.services.user_context_service import get_actor_info as _get_actor_info
from .blueprint import tasks_bp
from .task_meta_helpers import _apply_last_edit


def _list_empty_dirs(base: str) -> set[str]:
    empties: set[str] = set()
    for root, dirs, files in os.walk(base):
        if dirs or files:
            continue
        rel = os.path.relpath(root, base)
        if rel == ".":
            continue
        empties.add(rel.replace("\\", "/") + "/")
    return empties


@tasks_bp.get("/tasks/<task_id>/nas-diff", endpoint="task_nas_diff")
def task_nas_diff(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    meta_path = os.path.join(tdir, "meta.json")
    if not os.path.isdir(files_dir) or not os.path.exists(meta_path):
        return jsonify({"ok": False, "error": "Task not found"}), 404

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    nas_path = (meta.get("nas_path") or "").strip()
    if not nas_path:
        return jsonify({"ok": True, "diff": None, "message": "尚未設定 NAS 路徑"}), 200
    if not os.path.isdir(nas_path):
        return jsonify({"ok": True, "diff": None, "message": "NAS 路徑不存在或不是資料夾"}), 200

    try:
        task_files_map = {p.replace("\\", "/"): os.path.join(files_dir, p) for p in list_files(files_dir)}
        nas_files_map = {p.replace("\\", "/"): os.path.join(nas_path, p) for p in list_files(nas_path)}
        task_entries = set(task_files_map.keys()) | _list_empty_dirs(files_dir)
        nas_entries = set(nas_files_map.keys()) | _list_empty_dirs(nas_path)

        added = sorted(nas_entries - task_entries)
        removed = sorted(task_entries - nas_entries)
        updated = []

        for rel in set(task_files_map.keys()) & set(nas_files_map.keys()):
            try:
                t_stat = os.stat(task_files_map[rel])
                n_stat = os.stat(nas_files_map[rel])
                if n_stat.st_size != t_stat.st_size or int(n_stat.st_mtime) > int(t_stat.st_mtime):
                    updated.append(rel)
            except Exception:
                continue
        updated.sort()

        if not added and not removed and not updated:
            return jsonify({"ok": True, "diff": None, "message": "未偵測到變更"}), 200

        limit = 5
        diff = {
            "added": added[:limit],
            "removed": removed[:limit],
            "updated": updated[:limit],
            "added_count": len(added),
            "removed_count": len(removed),
            "updated_count": len(updated),
            "limit": limit,
        }
        return jsonify({"ok": True, "diff": diff}), 200
    except Exception:
        current_app.logger.exception("Failed to compare NAS files")
        return jsonify({"ok": False, "error": "Failed to compare NAS files"}), 500


@tasks_bp.post("/tasks/<task_id>/sync-nas", endpoint="sync_task_nas")
def sync_task_nas(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    meta_path = os.path.join(tdir, "meta.json")
    if not os.path.exists(meta_path):
        abort(404)

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    nas_path = (meta.get("nas_path") or "").strip()
    if not nas_path:
        flash("尚未設定 NAS 路徑，無法更新。", "warning")
        return redirect(url_for("tasks_bp.task_detail", task_id=task_id))

    abs_path = os.path.abspath(nas_path)
    roots = get_configured_nas_roots()
    if roots:
        allowed = False
        for root in roots:
            root_abs = os.path.abspath(root)
            try:
                if os.path.commonpath([root_abs, abs_path]) == root_abs:
                    allowed = True
                    break
            except ValueError:
                continue
        if not allowed:
            flash("NAS 路徑不在允許的根目錄內。", "danger")
            return redirect(url_for("tasks_bp.task_detail", task_id=task_id))

    if not os.path.isdir(abs_path):
        flash("NAS 路徑不存在或不是資料夾。", "danger")
        return redirect(url_for("tasks_bp.task_detail", task_id=task_id))

    try:
        enforce_max_copy_size(abs_path)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("tasks_bp.task_detail", task_id=task_id))

    try:
        src_dir = ensure_windows_long_path(abs_path)
        dst_dir = ensure_windows_long_path(files_dir)
        os.makedirs(dst_dir, exist_ok=True)
        copied = 0
        updated = 0
        deleted = 0
        created_dirs = 0
        deleted_dirs = 0
        for root, dirs, files in os.walk(src_dir):
            rel = os.path.relpath(root, src_dir)
            dest_root = dst_dir if rel == "." else os.path.join(dst_dir, rel)
            if not os.path.exists(dest_root):
                os.makedirs(dest_root, exist_ok=True)
                if rel != ".":
                    created_dirs += 1
            else:
                os.makedirs(dest_root, exist_ok=True)
            for fname in files:
                src_file = os.path.join(root, fname)
                dst_file = os.path.join(dest_root, fname)
                try:
                    if not os.path.exists(dst_file):
                        shutil.copy2(src_file, dst_file)
                        copied += 1
                        continue
                    src_stat = os.stat(src_file)
                    dst_stat = os.stat(dst_file)
                    if src_stat.st_size != dst_stat.st_size or int(src_stat.st_mtime) > int(dst_stat.st_mtime):
                        shutil.copy2(src_file, dst_file)
                        updated += 1
                except FileNotFoundError:
                    continue
        for root, dirs, files in os.walk(dst_dir, topdown=False):
            rel = os.path.relpath(root, dst_dir)
            src_root = src_dir if rel == "." else os.path.join(src_dir, rel)
            for fname in files:
                dst_file = os.path.join(root, fname)
                src_file = os.path.join(src_root, fname)
                if not os.path.exists(src_file):
                    try:
                        os.remove(dst_file)
                        deleted += 1
                    except FileNotFoundError:
                        continue
            if rel != "." and not os.path.exists(src_root):
                try:
                    shutil.rmtree(root)
                    deleted_dirs += 1
                except FileNotFoundError:
                    pass
        _apply_last_edit(meta)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        total_added = copied + created_dirs
        total_deleted = deleted + deleted_dirs
        flash(f"已更新 NAS 內容（新增 {total_added}、更新 {updated}、刪除 {total_deleted}）。", "success")
        work_id, label = _get_actor_info()
        record_audit(
            action="nas_sync",
            actor={"work_id": work_id, "label": label},
            detail={
                "task_id": task_id,
                "nas_path": nas_path,
                "copied": copied,
                "updated": updated,
                "deleted": deleted,
                "created_dirs": created_dirs,
                "deleted_dirs": deleted_dirs,
            },
            task_id=task_id,
        )
    except PermissionError:
        flash("沒有足夠的權限讀取或複製指定路徑。", "danger")
    except Exception:
        current_app.logger.exception("更新 NAS 文件失敗")
        flash("更新 NAS 文件失敗，請稍後再試。", "danger")

    return redirect(url_for("tasks_bp.task_detail", task_id=task_id))
