from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from datetime import datetime

from flask import abort, current_app, flash, jsonify, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from app.services.audit_service import record_audit
from app.services.flow_service import parse_template_paragraphs
from app.services.nas_service import get_configured_nas_roots, resolve_nas_path
from app.services.task_service import (
    build_file_tree,
    can_delete_task as _can_delete_task,
    deduplicate_name,
    delete_task_record,
    enforce_max_copy_size,
    ensure_windows_long_path,
    list_dirs,
    list_files,
    list_tasks,
    load_task_context as _load_task_context,
    record_task_in_db,
    task_name_exists,
)
from app.services.user_context_service import get_actor_info as _get_actor_info
from modules.file_copier import copy_files
from .blueprint import tasks_bp


@tasks_bp.route("/tasks/<task_id>/copy-files", methods=["GET", "POST"], endpoint="task_copy_files")
def task_copy_files(task_id):
    base = os.path.join(current_app.config["TASK_FOLDER"], task_id, "files")
    if not os.path.isdir(base):
        abort(404)

    def _safe_path(rel: str) -> str:
        norm = os.path.normpath(rel)
        if not rel or os.path.isabs(norm) or norm.startswith(".."):
            raise ValueError("資料夾名稱不合法")
        return os.path.join(base, norm)

    message = ""
    if request.method == "POST":
        action = request.form.get("action")
        if action == "create_dir":
            new_rel = request.form.get("new_dir", "").strip()
            try:
                os.makedirs(_safe_path(new_rel), exist_ok=True)
                message = f"已建立資料夾 {os.path.normpath(new_rel)}"
            except ValueError:
                message = "資料夾名稱不合法"
        else:
            source_rel = request.form.get("source_dir", "").strip()
            dest_rel = request.form.get("dest_dir", "").strip()
            keywords_raw = request.form.get("keywords", "")
            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
            if not source_rel or not dest_rel or not keywords:
                message = "請完整輸入資料"
            else:
                try:
                    src = _safe_path(source_rel)
                    dest = _safe_path(dest_rel)
                    copied = copy_files(src, dest, keywords)
                    message = f"已複製 {len(copied)} 個檔案"
                except ValueError:
                    message = "資料夾名稱不合法"
                except Exception as e:
                    message = str(e)
    dirs = list_dirs(base)
    dirs.insert(0, ".")
    return render_template(
        "tasks/copy_files.html",
        dirs=dirs,
        message=message,
        task_id=task_id,
        task=_load_task_context(task_id),
    )


@tasks_bp.get("/", endpoint="tasks")
def tasks():
    task_list_all = list_tasks()
    pin_scope_key, _ = _get_actor_info()
    
    # Pagination
    page = request.args.get("page", 1, type=int)
    per_page = 10
    total_count = len(task_list_all)
    total_pages = (total_count + per_page - 1) // per_page
    start = (page - 1) * per_page
    task_list = task_list_all[start : start + per_page]
    
    pagination = {
        "page": page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages
    }
    
    for t in task_list:
        meta = {
            "creator_work_id": t.get("creator_work_id", ""),
            "creator": t.get("creator", ""),
        }
        t["can_delete"] = _can_delete_task(meta)
    return render_template(
        "tasks/tasks.html",
        tasks=task_list,
        pagination=pagination,
        all_task_ids=[(t.get("id") or "").strip() for t in task_list_all if (t.get("id") or "").strip()],
        pin_scope_key=pin_scope_key or "anonymous",
        allowed_nas_roots=get_configured_nas_roots(),
    )

@tasks_bp.post("/tasks", endpoint="create_task")
def create_task():
    def _fail(message: str):
        flash(message, "danger")
        return redirect(url_for("tasks_bp.tasks"))

    nas_path = request.form.get("nas_path", "")
    try:
        nas_root_index = request.form.get("nas_root_index", "").strip()
        resolved_path = resolve_nas_path(
            nas_path,
            allow_recursive=current_app.config.get("NAS_ALLOW_RECURSIVE", True),
            root_index=nas_root_index or None,
        )
        if not os.path.isdir(resolved_path):
            return _fail("指定的 NAS 路徑不是資料夾")
        enforce_max_copy_size(resolved_path)
    except ValueError as exc:
        return _fail(str(exc))
    except FileNotFoundError as exc:
        return _fail(str(exc))
    task_name = request.form.get("task_name", "").strip() or "未命名任務"
    task_desc = request.form.get("task_desc", "").strip()
    if task_name_exists(task_name):
        return _fail("任務名稱已存在")
    tid = str(uuid.uuid4())[:8]
    tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
    files_dir = os.path.join(tdir, "files")
    os.makedirs(files_dir, exist_ok=True)
    src_dir = ensure_windows_long_path(resolved_path)
    dest_dir = ensure_windows_long_path(files_dir)
    try:
        shutil.copytree(src_dir, dest_dir, dirs_exist_ok=True)
    except PermissionError:
        shutil.rmtree(tdir, ignore_errors=True)
        return _fail("沒有足夠的權限讀取或複製指定路徑")
    except shutil.Error as exc:
        current_app.logger.exception("複製 NAS 目錄失敗")
        shutil.rmtree(tdir, ignore_errors=True)
        detail = ""
        if exc.args and isinstance(exc.args[0], list) and exc.args[0]:
            first_error = exc.args[0][0]
            if len(first_error) >= 3:
                detail = f"：{first_error[2]}"
        return _fail(f"複製 NAS 目錄時發生錯誤{detail or ''}，請稍後再試")
    except Exception:
        current_app.logger.exception("複製 NAS 目錄失敗")
        shutil.rmtree(tdir, ignore_errors=True)
        return _fail("複製 NAS 目錄時發生錯誤，請稍後再試")
    work_id, creator = _get_actor_info()
    display_nas_path = resolved_path
    if nas_root_index:
        try:
            roots = get_configured_nas_roots()
            idx = int(nas_root_index)
            if 0 <= idx < len(roots) and not os.path.isabs(nas_path):
                root = roots[idx]
                sep = "\\" if "\\" in root else "/"
                root_clean = re.sub(r"[\\/]+$", "", root)
                rel = re.sub(r"^[./\\\\]+", "", nas_path).replace("/", sep)
                display_nas_path = f"{root_clean}{sep}{rel}" if rel else root_clean
        except (ValueError, TypeError):
            pass

    created_at = datetime.now()
    meta_payload = {
        "name": task_name,
        "description": task_desc,
        "created": created_at.strftime("%Y-%m-%d %H:%M"),
        "nas_path": display_nas_path,
    }
    if creator:
        meta_payload["creator"] = creator
    if work_id:
        meta_payload["creator_work_id"] = work_id
    if creator:
        meta_payload["last_editor"] = creator
    if work_id:
        meta_payload["last_editor_work_id"] = work_id
    meta_payload["last_edited"] = created_at.strftime("%Y-%m-%d %H:%M")
    with open(os.path.join(tdir, "meta.json"), "w", encoding="utf-8") as meta:
        json.dump(
            meta_payload,
            meta,
            ensure_ascii=False,
            indent=2,
        )
    record_task_in_db(
        tid,
        name=task_name,
        description=task_desc,
        creator=creator or None,
        nas_path=display_nas_path or None,
        created_at=created_at,
    )
    record_audit(
        action="task_create",
        actor={"work_id": work_id, "label": creator},
        detail={"task_id": tid, "task_name": task_name, "nas_path": display_nas_path},
        task_id=tid,
    )
    return redirect(url_for("tasks_bp.tasks"))

@tasks_bp.post("/tasks/<task_id>/copy", endpoint="copy_task")
def copy_task(task_id):
    def _fail(message: str):
        flash(message, "danger")
        return redirect(url_for("tasks_bp.tasks"))

    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        return _fail("找不到任務資料夾")

    new_name = request.form.get("name", "").strip()
    if not new_name:
        return _fail("缺少任務名稱")
    if task_name_exists(new_name):
        return _fail("任務名稱已存在")

    meta_path = os.path.join(tdir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    source_nas_path = (meta.get("nas_path", "") or "").strip()

    requested_nas_path = request.form.get("nas_path")
    requested_root_index = request.form.get("nas_root_index", "").strip()
    target_nas_path = source_nas_path
    if requested_nas_path is not None:
        raw_nas_path = requested_nas_path.strip()
        if not raw_nas_path:
            target_nas_path = ""
        elif raw_nas_path == source_nas_path:
            target_nas_path = source_nas_path
        else:
            try:
                if os.path.isabs(raw_nas_path):
                    resolved_path = os.path.abspath(raw_nas_path)
                    roots = get_configured_nas_roots()
                    if roots and not requested_root_index:
                        for idx, root in enumerate(roots):
                            root_abs = os.path.abspath(root)
                            try:
                                if os.path.commonpath([root_abs, resolved_path]) == root_abs:
                                    rel = os.path.relpath(resolved_path, root_abs).replace("\\", "/")
                                    raw_nas_path = "." if rel == "." else rel
                                    requested_root_index = str(idx)
                                    break
                            except ValueError:
                                continue
                    if roots:
                        allowed = False
                        for root in roots:
                            root_abs = os.path.abspath(root)
                            try:
                                if os.path.commonpath([root_abs, resolved_path]) == root_abs:
                                    allowed = True
                                    break
                            except ValueError:
                                continue
                        if not allowed:
                            return _fail("NAS 路徑不在允許的根目錄內。")
                    if not os.path.isdir(resolved_path):
                        return _fail("NAS 路徑不存在或不是資料夾。")
                    target_nas_path = resolved_path
                else:
                    resolved_path = resolve_nas_path(
                        raw_nas_path,
                        allow_recursive=current_app.config.get("NAS_ALLOW_RECURSIVE", True),
                        root_index=requested_root_index or None,
                    )
                    if not os.path.isdir(resolved_path):
                        return _fail("NAS 路徑不存在或不是資料夾。")
                    target_nas_path = resolved_path
                    if requested_root_index:
                        roots = get_configured_nas_roots()
                        try:
                            idx = int(requested_root_index)
                            if 0 <= idx < len(roots):
                                root_clean = roots[idx].rstrip("/\\")
                                sep = "\\" if "\\" in root_clean else "/"
                                rel = re.sub(r"^[./\\]+", "", raw_nas_path).replace("/", sep)
                                target_nas_path = f"{root_clean}{sep}{rel}" if rel else root_clean
                        except ValueError:
                            pass
            except ValueError as exc:
                return _fail(str(exc))
            except FileNotFoundError as exc:
                return _fail(str(exc))

    created_at = datetime.now()
    work_id, creator = _get_actor_info()

    new_id = str(uuid.uuid4())[:8]
    new_dir = os.path.join(current_app.config["TASK_FOLDER"], new_id)
    os.makedirs(new_dir, exist_ok=False)
    try:
        for subdir in ("files", "flows"):
            src = os.path.join(tdir, subdir)
            dest = os.path.join(new_dir, subdir)
            if os.path.isdir(src):
                shutil.copytree(ensure_windows_long_path(src), ensure_windows_long_path(dest))
            elif subdir == "files":
                os.makedirs(dest, exist_ok=True)
    except Exception:
        current_app.logger.exception("複製任務資料夾失敗")
        shutil.rmtree(new_dir, ignore_errors=True)
        return _fail("複製任務資料夾失敗，請稍後再試")

    new_meta = {
        "name": new_name,
        "description": meta.get("description", ""),
        "nas_path": target_nas_path,
        "created": created_at.strftime("%Y-%m-%d %H:%M"),
        "last_edited": created_at.strftime("%Y-%m-%d %H:%M"),
    }
    if creator:
        new_meta["creator"] = creator
        new_meta["last_editor"] = creator
    if work_id:
        new_meta["creator_work_id"] = work_id
        new_meta["last_editor_work_id"] = work_id
    with open(os.path.join(new_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(new_meta, f, ensure_ascii=False, indent=2)

    record_task_in_db(
        new_id,
        name=new_name,
        description=new_meta.get("description") or None,
        creator=creator or None,
        nas_path=new_meta.get("nas_path") or None,
        created_at=created_at,
    )
    record_audit(
        action="task_copy",
        actor={"work_id": work_id, "label": creator},
        detail={
            "task_id": new_id,
            "task_name": new_name,
            "nas_path": new_meta.get("nas_path"),
            "source_nas_path": source_nas_path,
            "source_task_id": task_id
        },
        task_id=new_id,
    )
    flash("已複製任務", "success")
    return redirect(url_for("tasks_bp.tasks"))

@tasks_bp.post("/tasks/<task_id>/delete", endpoint="delete_task")
def delete_task(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    meta_path = os.path.join(tdir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    if not _can_delete_task(meta):
        abort(403)
    work_id, label = _get_actor_info()
    record_audit(
        action="task_delete",
        actor={"work_id": work_id, "label": label},
        detail={"task_id": task_id, "task_name": meta.get("name", "")},
        task_id=task_id,
    )
    if os.path.isdir(tdir):
        import shutil
        shutil.rmtree(tdir)
    delete_task_record(task_id)
    return redirect(url_for("tasks_bp.tasks"))

@tasks_bp.post("/tasks/<task_id>/rename", endpoint="rename_task")
def rename_task(task_id):
    new_name = request.form.get("name", "").strip()
    if not new_name:
        return "缺少名稱", 400
    if task_name_exists(new_name, exclude_id=task_id):
        return "任務名稱已存在", 400
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    meta_path = os.path.join(tdir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    meta["name"] = new_name
    if "created" not in meta:
        meta["created"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    record_task_in_db(task_id, name=new_name)
    work_id, label = _get_actor_info()
    record_audit(
        action="task_rename",
        actor={"work_id": work_id, "label": label},
        detail={"task_id": task_id, "name": new_name},
        task_id=task_id,
    )
    return redirect(url_for("tasks_bp.tasks"))

@tasks_bp.post("/tasks/<task_id>/description", endpoint="update_task_description")
def update_task_description(task_id):
    new_desc = request.form.get("description", "").strip()
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    meta_path = os.path.join(tdir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    meta["description"] = new_desc
    if "name" not in meta:
        meta["name"] = task_id
    if "created" not in meta:
        meta["created"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    record_task_in_db(task_id, description=new_desc)
    work_id, label = _get_actor_info()
    record_audit(
        action="task_update_description",
        actor={"work_id": work_id, "label": label},
        detail={"task_id": task_id, "description": new_desc},
        task_id=task_id,
    )
    return redirect(url_for("tasks_bp.tasks"))

@tasks_bp.get("/tasks/<task_id>", endpoint="task_detail")
def task_detail(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    meta_path = os.path.join(tdir, "meta.json")
    name = task_id
    description = ""
    creator = ""
    nas_path = ""
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
            name = meta.get("name", task_id)
            description = meta.get("description", "")
            creator = meta.get("creator", "") or ""
            nas_path = meta.get("nas_path", "") or ""
    nas_diff = None
    if nas_path and os.path.isdir(nas_path):
        try:
            task_files = {p.replace("\\", "/") for p in list_files(files_dir)}
            nas_files = {p.replace("\\", "/") for p in list_files(nas_path)}
            added = sorted(nas_files - task_files)
            removed = sorted(task_files - nas_files)
            if added or removed:
                limit = 5
                nas_diff = {
                    "added": added[:limit],
                    "removed": removed[:limit],
                    "added_count": len(added),
                    "removed_count": len(removed),
                    "limit": limit,
                }
        except Exception:
            current_app.logger.exception("Failed to compare NAS files")
    tree = build_file_tree(files_dir)
    return render_template(
        "tasks/task_detail.html",
        task={"id": task_id, "name": name, "description": description, "creator": creator, "nas_path": nas_path},
        nas_diff=nas_diff,
        files_tree=tree,
    )

@tasks_bp.post("/tasks/<task_id>/templates/parse", endpoint="parse_template_doc")
def parse_template_doc(task_id):
    """Upload or parse an existing template docx and return paragraph metadata."""
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)

    upload = request.files.get("template_file")
    template_rel = ""
    existing = request.form.get("template_path", "").strip()

    if upload and upload.filename:
        if not upload.filename.lower().endswith(".docx"):
            return jsonify({"ok": False, "error": "僅支援 .docx 模板"}), 400
        safe_name = deduplicate_name(files_dir, secure_filename(upload.filename))
        save_path = os.path.join(files_dir, safe_name)
        upload.save(save_path)
        template_rel = safe_name
    elif existing:
        normalized = os.path.normpath(existing)
        if normalized.startswith("..") or os.path.isabs(normalized):
            return jsonify({"ok": False, "error": "無效的檔案路徑"}), 400
        template_rel = normalized
    else:
        return jsonify({"ok": False, "error": "請選擇或上傳模板檔案"}), 400

    template_abs = os.path.join(files_dir, template_rel)
    if not os.path.isfile(template_abs):
        return jsonify({"ok": False, "error": "找不到模板檔案"}), 404

    try:
        paragraphs = parse_template_paragraphs(template_abs)
    except Exception as e:
        current_app.logger.exception("Failed to parse template docx")
        return jsonify({"ok": False, "error": f"解析模板失敗: {e}"}), 400

    return jsonify(
        {
            "ok": True,
            "template_file": template_rel.replace("\\", "/"),
            "paragraphs": paragraphs,
        }
    )
