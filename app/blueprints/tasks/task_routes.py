from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from datetime import datetime

from flask import abort, current_app, flash, jsonify, redirect, render_template, request, url_for
from app.services.audit_service import record_audit
from app.services.flow_service import parse_template_paragraphs
from app.services.nas_service import get_configured_nas_roots, resolve_nas_path
from app.services.task_service import (
    build_task_output_path,
    can_delete_task as _can_delete_task,
    deduplicate_name,
    delete_task_record,
    enqueue_task_source_sync_job,
    ensure_windows_long_path,
    is_task_source_ready,
    list_tasks,
    load_task_context as _load_task_context,
    record_task_in_db,
    task_name_exists,
)
from app.services.user_context_service import get_actor_info as _get_actor_info
from .blueprint import tasks_bp
from .mapping_routes import _safe_uploaded_filename

TASK_TEXT_LIMIT = 50


def _parse_task_id_csv(value: str | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw_id in (value or "").split(","):
        task_id = raw_id.strip()
        if not task_id or task_id in seen:
            continue
        seen.add(task_id)
        result.append(task_id)
    return result


def _validate_limited_text(value: str, label: str, *, required: bool = False) -> str | None:
    normalized = (value or "").strip()
    if required and not normalized:
        return f"請輸入{label}"
    if len(normalized) > TASK_TEXT_LIMIT:
        return f"{label}最多 {TASK_TEXT_LIMIT} 字"
    return None


def _wants_json_response() -> bool:
    return (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or request.accept_mimetypes.best == "application/json"
    )


def _build_task_listing_context() -> dict:
    task_list_all = list_tasks()
    pin_scope_key, _ = _get_actor_info()
    pinned_task_ids = _parse_task_id_csv(request.args.get("pinned_task_ids"))
    if pinned_task_ids:
        pinned_order = {task_id: index for index, task_id in enumerate(pinned_task_ids)}
        task_list_all.sort(
            key=lambda task: (
                0 if task.get("id") in pinned_order else 1,
                pinned_order.get(task.get("id"), 0),
            )
        )

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
    return {
        "tasks": task_list,
        "pagination": pagination,
        "all_task_ids": [(t.get("id") or "").strip() for t in task_list_all if (t.get("id") or "").strip()],
        "ready_task_ids": [
            (t.get("id") or "").strip()
            for t in task_list_all
            if (t.get("id") or "").strip()
            and str(t.get("source_sync_status") or "").strip().lower() in {"", "completed"}
        ],
        "pin_scope_key": pin_scope_key or "anonymous",
        "pinned_task_ids": ",".join(
            task_id
            for task_id in pinned_task_ids
            if any((t.get("id") or "").strip() == task_id for t in task_list_all)
        ),
        "allowed_nas_roots": get_configured_nas_roots(),
        "total_tasks": total_count,
    }


@tasks_bp.get("/", endpoint="launcher")
def launcher():
    return render_template("tasks/launcher.html")


@tasks_bp.get("/tasks", endpoint="tasks")
def tasks():
    return render_template("tasks/tasks.html", **_build_task_listing_context())


@tasks_bp.get("/standards-legacy", endpoint="standards")
def standards():
    return render_template("tasks/standards.html", **_build_task_listing_context())

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
    except ValueError as exc:
        return _fail(str(exc))
    except FileNotFoundError as exc:
        return _fail(str(exc))
    task_name = request.form.get("task_name", "").strip() or "未命名任務"
    task_desc = request.form.get("task_desc", "").strip()
    name_error = _validate_limited_text(task_name, "任務名稱", required=True)
    if name_error:
        return _fail(name_error)
    desc_error = _validate_limited_text(task_desc, "任務描述")
    if desc_error:
        return _fail(desc_error)
    os.makedirs(current_app.config["TASK_FOLDER"], exist_ok=True)
    if task_name_exists(task_name):
        return _fail("任務名稱已存在")
    tid = str(uuid.uuid4())[:8]
    tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
    files_dir = os.path.join(tdir, "files")
    output_dir = build_task_output_path(tid)
    os.makedirs(files_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
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
        "output_path": output_dir,
        "source_sync_status": "queued",
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
    try:
        record_task_in_db(
            tid,
            name=task_name,
            description=task_desc,
            creator=creator or None,
            nas_path=display_nas_path or None,
            output_path=output_dir,
            created_at=created_at,
            raise_on_error=True,
        )
    except Exception:
        shutil.rmtree(tdir, ignore_errors=True)
        return _fail("建立任務資料庫紀錄失敗，請稍後再試")
    try:
        sync_job_id = enqueue_task_source_sync_job(
            tid,
            resolved_path,
            actor={"work_id": work_id, "label": creator},
        )
    except Exception:
        current_app.logger.exception("建立任務來源同步工作失敗")
        delete_task_record(tid)
        shutil.rmtree(tdir, ignore_errors=True)
        return _fail("建立任務來源同步工作失敗，請稍後再試")
    record_audit(
        action="task_create",
        actor={"work_id": work_id, "label": creator},
        detail={"task_id": tid, "task_name": task_name, "nas_path": display_nas_path, "sync_job_id": sync_job_id},
        task_id=tid,
    )
    flash("任務已建立，來源檔案正在背景同步。同步完成後即可執行流程。", "success")
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
    name_error = _validate_limited_text(new_name, "任務名稱", required=True)
    if name_error:
        return _fail(name_error)
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
    new_output_dir = build_task_output_path(new_id)
    os.makedirs(new_dir, exist_ok=False)
    try:
        for subdir in ("files", "flows"):
            src = os.path.join(tdir, subdir)
            dest = os.path.join(new_dir, subdir)
            if os.path.isdir(src):
                shutil.copytree(ensure_windows_long_path(src), ensure_windows_long_path(dest))
            elif subdir == "files":
                os.makedirs(dest, exist_ok=True)
        os.makedirs(new_output_dir, exist_ok=True)
    except Exception:
        current_app.logger.exception("複製任務資料夾失敗")
        shutil.rmtree(new_dir, ignore_errors=True)
        return _fail("複製任務資料夾失敗，請稍後再試")

    copied_description = (meta.get("description", "") or "").strip()[:TASK_TEXT_LIMIT]
    new_meta = {
        "name": new_name,
        "description": copied_description,
        "nas_path": target_nas_path,
        "output_path": new_output_dir,
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

    try:
        record_task_in_db(
            new_id,
            name=new_name,
            description=new_meta.get("description") or None,
            creator=creator or None,
            nas_path=new_meta.get("nas_path") or None,
            output_path=new_meta.get("output_path") or None,
            created_at=created_at,
            raise_on_error=True,
        )
    except Exception:
        shutil.rmtree(new_dir, ignore_errors=True)
        return _fail("建立複製任務的資料庫紀錄失敗，請稍後再試")
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
        if _wants_json_response():
            return jsonify({"ok": False, "error": "缺少名稱"}), 400
        return "缺少名稱", 400
    name_error = _validate_limited_text(new_name, "任務名稱", required=True)
    if name_error:
        if _wants_json_response():
            return jsonify({"ok": False, "error": name_error}), 400
        return name_error, 400
    if task_name_exists(new_name, exclude_id=task_id):
        if _wants_json_response():
            return jsonify({"ok": False, "error": "任務名稱已存在"}), 400
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
    if _wants_json_response():
        return jsonify({"ok": True, "task_id": task_id, "name": new_name, "message": "已更新任務名稱"})
    return redirect(url_for("tasks_bp.tasks"))

@tasks_bp.post("/tasks/<task_id>/description", endpoint="update_task_description")
def update_task_description(task_id):
    new_desc = request.form.get("description", "").strip()
    desc_error = _validate_limited_text(new_desc, "任務描述")
    if desc_error:
        if _wants_json_response():
            return jsonify({"ok": False, "error": desc_error}), 400
        return desc_error, 400
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
    if _wants_json_response():
        return jsonify({"ok": True, "task_id": task_id, "description": new_desc, "message": "已更新任務描述"})
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
    output_path = ""
    source_sync_status = ""
    source_sync_error = ""
    source_sync_file_count = None
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
            name = meta.get("name", task_id)
            description = meta.get("description", "")
            creator = meta.get("creator", "") or ""
            nas_path = meta.get("nas_path", "") or ""
            output_path = meta.get("output_path", "") or build_task_output_path(task_id)
            source_sync_status = meta.get("source_sync_status", "") or ""
            source_sync_error = meta.get("source_sync_error", "") or ""
            source_sync_file_count = meta.get("source_sync_file_count")
    task_meta = {
        "id": task_id,
        "name": name,
        "description": description,
        "creator": creator,
        "nas_path": nas_path,
        "output_path": output_path,
        "source_sync_status": source_sync_status,
        "source_sync_error": source_sync_error,
        "source_sync_file_count": source_sync_file_count,
    }
    task_meta["source_ready"] = is_task_source_ready(task_meta)
    return render_template(
        "tasks/task_detail.html",
        task=task_meta,
        files_api_url=url_for("flow_file_bp.api_flow_list_task_files", task_id=task_id),
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
        safe_name = deduplicate_name(files_dir, _safe_uploaded_filename(upload.filename))
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
