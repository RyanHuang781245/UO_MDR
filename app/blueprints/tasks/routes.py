from __future__ import annotations

import json
import os
import shutil
import uuid
import zipfile
from datetime import datetime
import re

from flask import Blueprint, abort, current_app, flash, jsonify, redirect, render_template, request, send_file, send_from_directory, url_for
from flask_login import current_user
from werkzeug.utils import secure_filename

from app.services.flow_service import (
    SKIP_DOCX_CLEANUP,
    build_version_context,
    clean_compare_html_content,
    collect_titles_to_hide,
    load_titles_to_hide_from_log,
    load_version_metadata,
    parse_template_paragraphs,
    remove_hidden_runs,
    remove_paragraphs_with_text,
    save_compare_output,
    save_version_metadata,
    sanitize_version_slug,
    translate_file,
)
from app.services.audit_service import record_audit

from app.services.task_service import (
    allowed_file,
    build_file_tree,
    deduplicate_name,
    delete_task_record,
    enforce_max_copy_size,
    ensure_windows_long_path,
    gather_available_files,
    list_dirs,
    list_files,
    list_tasks,
    record_task_in_db,
    task_name_exists,
)

from app.services.nas_service import get_configured_nas_roots, resolve_nas_path, validate_nas_path
from modules.auth_models import ROLE_ADMIN, user_has_role
from modules.file_copier import copy_files

tasks_bp = Blueprint("tasks_bp", __name__, template_folder="templates")


def _get_actor_info():
    if current_user and getattr(current_user, "is_authenticated", False):
        display_name = (getattr(current_user, "display_name", "") or "").strip()
        chinese_only = "".join(re.findall(r"[\u4e00-\u9fff\u3400-\u4dbf\uF900-\uFAFF]+", display_name))
        work_id = (getattr(current_user, "work_id", "") or "").strip()
        if chinese_only:
            label = f"{work_id} {chinese_only}" if work_id else chinese_only
        else:
            label = display_name or work_id
        return work_id, label
    return "", ""


def _apply_last_edit(meta: dict) -> None:
    work_id, label = _get_actor_info()
    meta["last_edited"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    if label:
        meta["last_editor"] = label
    if work_id:
        meta["last_editor_work_id"] = work_id


def _get_creator_work_id(meta: dict) -> str:
    creator_work_id = (meta.get("creator_work_id") or "").strip()
    if creator_work_id:
        return creator_work_id
    creator = (meta.get("creator") or "").strip()
    if creator:
        return creator.split()[0]
    return ""


def _can_delete_task(meta: dict) -> bool:
    if not current_app.config.get("AUTH_ENABLED", True):
        return True
    if not current_user or not getattr(current_user, "is_authenticated", False):
        return False
    if user_has_role(current_user.id, ROLE_ADMIN):
        return True
    creator_work_id = _get_creator_work_id(meta)
    return bool(creator_work_id) and current_user.work_id == creator_work_id

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
    return render_template("tasks/copy_files.html", dirs=dirs, message=message, task_id=task_id)

@tasks_bp.route("/tasks/<task_id>/mapping", methods=["GET", "POST"], endpoint="task_mapping")
def task_mapping(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    files_dir = os.path.join(tdir, "files")
    out_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id)
    messages = []
    outputs = []
    log_file = None
    step_runs = []

    def _format_step_label(entry: dict) -> tuple[str, str]:
        stype = entry.get("type") or ""
        params = entry.get("params") or {}

        def _base(path: str) -> str:
            return os.path.basename(path) if path else "?"

        if stype == "extract_word_chapter":
            src = _base(params.get("input_file", ""))
            chapter = params.get("target_chapter_section", "")
            title = params.get("target_title_section", "")
            sub = params.get("subheading_text", "")
            parts = [f"chapter {chapter}"] if chapter else []
            if title:
                parts.append(f"title {title}")
            if sub:
                parts.append(f"subheading {sub}")
            detail = ", ".join(parts)
            suffix = f" ({detail})" if detail else ""
            return "Extract chapter", f"{src}{suffix}".strip()
        if stype == "extract_word_all_content":
            src = _base(params.get("input_file", ""))
            return "Extract all", src
        if stype == "extract_specific_table_from_word":
            src = _base(params.get("input_file", ""))
            label = params.get("target_table_label", "")
            detail = f"{src} ({label})" if label else src
            return "Extract table", detail
        if stype == "extract_specific_figure_from_word":
            src = _base(params.get("input_file", ""))
            label = params.get("target_figure_label", "")
            detail = f"{src} ({label})" if label else src
            return "Extract figure", detail
        if stype == "insert_text":
            return "Append text", ""
        if stype == "template_merge":
            tpl = _base(entry.get("template_file", ""))
            return "Template merge", tpl
        return stype or "step", ""
    if request.method == "POST":
        f = request.files.get("mapping_file")
        if not f or not f.filename:
            messages.append("請選擇檔案")
        else:
            path = os.path.join(tdir, secure_filename(f.filename))
            f.save(path)
            try:
                from modules.mapping_processor import process_mapping_excel
                result = process_mapping_excel(path, files_dir, out_dir)
                messages = result["logs"]
                outputs = result["outputs"]
                log_file = result.get("log_file")
            except Exception as e:
                messages = [str(e)]
    if log_file:
        log_path = os.path.join(out_dir, log_file)
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    log_data = json.load(f)
                for run in log_data.get("runs", []):
                    for entry in run.get("workflow_log", []):
                        if "step" not in entry:
                            continue
                        action, detail = _format_step_label(entry)
                        step_runs.append(
                            {
                                "action": action,
                                "detail": detail,
                                "status": entry.get("status") or "ok",
                                "error": entry.get("error") or "",
                            }
                        )
                if step_runs:
                    messages = [m for m in messages if not (m or "").startswith("ERROR:")]
            except Exception as e:
                messages.append(f"ERROR: failed to read log file ({e})")
    has_error = any("ERROR" in (m or "") for m in messages) or any(
        step.get("status") == "error" for step in step_runs
    )
    rel_outputs = []
    for p in outputs:
        rel = os.path.relpath(p, out_dir)
        rel_outputs.append(rel.replace("\\", "/"))
    return render_template(
        "tasks/mapping.html",
        task_id=task_id,
        messages=messages,
        outputs=rel_outputs,
        log_file=log_file,
        has_error=has_error,
        step_runs=step_runs,
    )

@tasks_bp.get("/tasks/<task_id>/output/<path:filename>", endpoint="task_download_output")
def task_download_output(task_id, filename):
    out_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id)
    safe_name = filename.replace("\\", "/")
    file_path = os.path.join(out_dir, safe_name)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(out_dir, safe_name, as_attachment=True)

@tasks_bp.get("/", endpoint="tasks")
def tasks():
    task_list = list_tasks()
    for t in task_list:
        meta = {
            "creator_work_id": t.get("creator_work_id", ""),
            "creator": t.get("creator", ""),
        }
        t["can_delete"] = _can_delete_task(meta)
    return render_template(
        "tasks/tasks.html",
        tasks=task_list,
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

@tasks_bp.post("/tasks/<task_id>/files", endpoint="upload_task_file")
def upload_task_file(task_id):
    """Upload additional files to an existing task."""
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)

    uploads = request.files.getlist("upload_files")
    has_uploads = any(f and f.filename for f in uploads)
    uploaded_names = []
    if has_uploads:
        for upload in uploads:
            if not upload or not upload.filename:
                continue
            safe_name = secure_filename(upload.filename)
            if not safe_name:
                return "檔名不合法", 400
            if not allowed_file(safe_name):
                return "僅支援 DOCX、PDF、ZIP 或 Excel 檔案", 400
            dest_name = deduplicate_name(files_dir, safe_name)
            dest_path = ensure_windows_long_path(os.path.join(files_dir, dest_name))
            try:
                if dest_name.lower().endswith(".zip"):
                    upload.save(dest_path)
                    with zipfile.ZipFile(dest_path, "r") as zf:
                        zf.extractall(files_dir)
                        for info in zf.infolist():
                            if info.is_dir():
                                continue
                    os.remove(dest_path)
                else:
                    upload.save(dest_path)
                uploaded_names.append(dest_name)
            except Exception:
                current_app.logger.exception("本機檔案上傳失敗")
                return "上傳失敗，請稍後再試", 400
        if uploaded_names:
            work_id, label = _get_actor_info()
            record_audit(
                action="task_upload_files",
                actor={"work_id": work_id, "label": label},
                detail={"task_id": task_id, "count": len(uploaded_names), "files": uploaded_names},
                task_id=task_id,
            )
        return redirect(url_for("tasks_bp.task_detail", task_id=task_id))

    nas_input = request.form.get("nas_file_path", "").strip()
    if not nas_input:
        return "請選擇要上傳的檔案", 400
    try:
        source_path = validate_nas_path(
            nas_input,
            allowed_roots=current_app.config.get("ALLOWED_SOURCE_ROOTS", []),
        )
        enforce_max_copy_size(source_path)
    except ValueError as e:
        return str(e), 400
    except FileNotFoundError as e:
        return str(e), 404

    try:
        source_path = ensure_windows_long_path(source_path)
        if os.path.isdir(source_path):
            dest_name = deduplicate_name(files_dir, os.path.basename(source_path))
            dest_path = ensure_windows_long_path(os.path.join(files_dir, dest_name))
            shutil.copytree(source_path, dest_path)
        else:
            if not allowed_file(source_path):
                return "僅支援 DOCX、PDF、ZIP 或 Excel 檔案，或複製整個資料夾", 400
            dest_name = deduplicate_name(files_dir, os.path.basename(source_path))
            dest_path = ensure_windows_long_path(os.path.join(files_dir, dest_name))
            if dest_name.lower().endswith(".zip"):
                shutil.copy2(source_path, dest_path)
                with zipfile.ZipFile(dest_path, "r") as zf:
                    zf.extractall(files_dir)
                os.remove(dest_path)
            else:
                shutil.copy2(source_path, dest_path)
    except PermissionError:
        return "沒有足夠的權限讀取或複製指定路徑", 400
    except FileNotFoundError:
        return "找不到指定的檔案或資料夾", 404
    except shutil.Error:
        current_app.logger.exception("複製檔案時發生錯誤")
        return "複製檔案時發生錯誤，請稍後再試", 400
    except Exception:
        current_app.logger.exception("處理 NAS 檔案時發生未預期錯誤")
        return "處理檔案時發生錯誤，請稍後再試", 400

    return redirect(url_for("tasks_bp.task_detail", task_id=task_id))


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
        task_files = {p.replace("\\", "/") for p in list_files(files_dir)}
        nas_files = {p.replace("\\", "/") for p in list_files(nas_path)}
        added = sorted(nas_files - task_files)
        removed = sorted(task_files - nas_files)
        if not added and not removed:
            return jsonify({"ok": True, "diff": None, "message": "未偵測到變更"}), 200
        limit = 5
        diff = {
            "added": added[:limit],
            "removed": removed[:limit],
            "added_count": len(added),
            "removed_count": len(removed),
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
        for root, dirs, files in os.walk(src_dir):
            rel = os.path.relpath(root, src_dir)
            dest_root = dst_dir if rel == "." else os.path.join(dst_dir, rel)
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
                except FileNotFoundError:
                    pass
        _apply_last_edit(meta)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        flash(f"已更新 NAS 文件（新增 {copied}、更新 {updated}、刪除 {deleted}）。", "success")
        work_id, label = _get_actor_info()
        record_audit(
            action="nas_sync",
            actor={"work_id": work_id, "label": label},
            detail={"task_id": task_id, "nas_path": nas_path, "copied": copied, "updated": updated, "deleted": deleted},
            task_id=task_id,
        )
    except PermissionError:
        flash("沒有足夠的權限讀取或複製指定路徑。", "danger")
    except Exception:
        current_app.logger.exception("更新 NAS 文件失敗")
        flash("更新 NAS 文件失敗，請稍後再試。", "danger")

    return redirect(url_for("tasks_bp.task_detail", task_id=task_id))


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

@tasks_bp.get("/tasks/<task_id>/result/<job_id>", endpoint="task_result")
def task_result(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    if not os.path.exists(docx_path):
        return "Job not found or failed.", 404
    log_json_path = os.path.join(job_dir, "log.json")
    log_entries = []
    overall_status = "ok"
    if os.path.exists(log_json_path):
        with open(log_json_path, "r", encoding="utf-8") as f:
            log_entries = json.load(f)
        if any(e.get("status") == "error" for e in log_entries):
            overall_status = "error"
    return render_template(
        "tasks/run.html",
        job_id=job_id,
        docx_path=url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="docx"),
        log_path=url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="log"),
        translate_path=url_for("tasks_bp.task_translate", task_id=task_id, job_id=job_id),
        compare_path=url_for("tasks_bp.task_compare", task_id=task_id, job_id=job_id),
        back_link=url_for("flows_bp.flow_builder", task_id=task_id),
        log_entries=log_entries,
        overall_status=overall_status,
    )

@tasks_bp.get("/tasks/<task_id>/translate/<job_id>", endpoint="task_translate")
def task_translate(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    src = os.path.join(job_dir, "result.docx")
    if not os.path.exists(src):
        abort(404)
    out_docx = os.path.join(job_dir, "translated.docx")
    if not os.path.exists(out_docx):
        tmp_md = os.path.join(job_dir, "translated.md")
        translate_file(src, tmp_md)
        import docx
        doc = docx.Document()
        with open(tmp_md, "r", encoding="utf-8") as f:
            for line in f.read().splitlines():
                doc.add_paragraph(line)
        doc.save(out_docx)
    return send_file(
        out_docx,
        as_attachment=True,
        download_name=f"translated_{job_id}.docx",
    )

@tasks_bp.get("/tasks/<task_id>/compare/<job_id>", endpoint="task_compare")
def task_compare(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(docx_path) or not os.path.exists(log_path):
        abort(404)

    from spire.doc import Document, FileFormat

    with open(log_path, "r", encoding="utf-8") as f:
        entries = json.load(f)
    titles_to_hide = collect_titles_to_hide(entries)

    html_name = "result.html"
    html_path = os.path.join(job_dir, html_name)
    if not os.path.exists(html_path):
        doc = Document()
        doc.LoadFromFile(docx_path)
        doc.HtmlExportOptions.ImageEmbedded = True
        doc.SaveToFile(html_path, FileFormat.Html)
        doc.Close()
        if not SKIP_DOCX_CLEANUP:
            remove_hidden_runs(docx_path, preserve_texts=titles_to_hide)

    chapter_sources = {}
    source_urls = {}
    converted_docx = {}
    current = None
    for entry in entries:
        stype = entry.get("type")
        params = entry.get("params", {})
        if stype == "insert_roman_heading":
            current = params.get("text", "")
            chapter_sources.setdefault(current, [])
        elif stype == "extract_pdf_chapter_to_table":
            pdf_dir = os.path.join(job_dir, "pdfs_extracted")
            pdfs = []
            if os.path.isdir(pdf_dir):
                for fn in sorted(os.listdir(pdf_dir)):
                    if fn.lower().endswith(".pdf"):
                        pdfs.append(fn)
                        rel = os.path.join("pdfs_extracted", fn)
                        source_urls[fn] = url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=rel
                        )
            chapter_sources.setdefault(current or "未分類", []).extend(pdfs)
        elif stype == "extract_word_chapter":
            infile = params.get("input_file", "")
            base = os.path.basename(infile)
            sec = params.get("target_chapter_section", "")
            use_title = str(params.get("target_title", "")).lower() in ["1", "true", "yes", "on"]
            title = params.get("target_title_section", "") if use_title else ""
            info = base
            if sec:
                info += f" 章節 {sec}"
            if title:
                info += f" 標題 {title}"
            chapter_sources.setdefault(current or "未分類", []).append(info)
            if base not in converted_docx and infile and os.path.exists(infile):
                preview_dir = os.path.join(job_dir, "source_html")
                os.makedirs(preview_dir, exist_ok=True)
                html_name_src = f"{os.path.splitext(base)[0]}.html"
                html_rel = os.path.join("source_html", html_name_src)
                html_path_src = os.path.join(job_dir, html_rel)
                doc = Document()
                doc.LoadFromFile(infile)
                doc.HtmlExportOptions.ImageEmbedded = True
                doc.SaveToFile(html_path_src, FileFormat.Html)
                doc.Close()
                converted_docx[base] = html_rel
            if base in converted_docx:
                source_urls[info] = url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=converted_docx[base]
                )
        elif stype == "extract_word_all_content":
            infile = params.get("input_file", "")
            base = os.path.basename(infile)
            chapter_sources.setdefault(current or "未分類", []).append(base)
            if base not in converted_docx and infile and os.path.exists(infile):
                preview_dir = os.path.join(job_dir, "source_html")
                os.makedirs(preview_dir, exist_ok=True)
                html_name_src = f"{os.path.splitext(base)[0]}.html"
                html_rel = os.path.join("source_html", html_name_src)
                html_path_src = os.path.join(job_dir, html_rel)
                doc = Document()
                doc.LoadFromFile(infile)
                doc.HtmlExportOptions.ImageEmbedded = True
                doc.SaveToFile(html_path_src, FileFormat.Html)
                doc.Close()
                converted_docx[base] = html_rel
            if base in converted_docx:
                source_urls[base] = url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=converted_docx[base]
                )

    chapters = list(chapter_sources.keys())
    html_url = url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=html_name)
    versions = build_version_context(task_id, job_id, job_dir)
    return render_template(
        "tasks/compare.html",
        html_url=html_url,
        chapters=chapters,
        chapter_sources=chapter_sources,
        source_urls=source_urls,
        titles_to_hide=titles_to_hide,
        back_link=url_for("tasks_bp.task_result", task_id=task_id, job_id=job_id),
        save_url=url_for("tasks_bp.task_compare_save", task_id=task_id, job_id=job_id),
        save_as_url=url_for("tasks_bp.task_compare_save_as", task_id=task_id, job_id=job_id),
        download_url=url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="docx"),
        versions=versions,
    )

@tasks_bp.post("/tasks/<task_id>/compare/<job_id>/save", endpoint="task_compare_save")
def task_compare_save(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    titles_to_hide = load_titles_to_hide_from_log(job_dir)
    html_content = request.form.get("html")
    if not html_content:
        data = request.get_json(silent=True) or {}
        html_content = data.get("html", "")
    if not html_content:
        return "缺少內容", 400
    html_content = clean_compare_html_content(html_content)
    save_compare_output(job_dir, html_content, titles_to_hide)
    return "OK"

@tasks_bp.post("/tasks/<task_id>/compare/<job_id>/save-as", endpoint="task_compare_save_as")
def task_compare_save_as(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    titles_to_hide = load_titles_to_hide_from_log(job_dir)
    payload = request.get_json(silent=True) or {}
    html_content = payload.get("html")
    name = payload.get("name") or ""
    if not html_content:
        html_content = request.form.get("html")
        name = request.form.get("name") or name
    if not html_content:
        return jsonify({"error": "缺少內容"}), 400
    version_name = (name or "").strip()
    if not version_name:
        return jsonify({"error": "缺少版本名稱"}), 400
    html_content = clean_compare_html_content(html_content)
    versions_dir = os.path.join(job_dir, "versions")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    version_id = f"{timestamp}_{unique_suffix}"
    slug = sanitize_version_slug(version_name)
    base_name = f"{version_id}_{slug}" if slug else version_id
    save_compare_output(
        job_dir,
        html_content,
        titles_to_hide,
        base_name=base_name,
        subdir="versions",
    )
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    versions = [v for v in versions if v.get("id") != version_id]
    created_ts = datetime.now()
    versions.append(
        {
            "id": version_id,
            "name": version_name,
            "slug": slug,
            "base_name": base_name,
            "created_at": created_ts.isoformat(timespec="seconds"),
        }
    )
    versions.sort(key=lambda v: v.get("created_at", ""), reverse=True)
    metadata["versions"] = versions
    save_version_metadata(versions_dir, metadata)
    version_payload = {
        "id": version_id,
        "name": version_name,
        "created_at_display": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "html_url": url_for("tasks_bp.task_view_file",
            task_id=task_id,
            job_id=job_id,
            filename=f"versions/{base_name}.html",
        ),
        "docx_url": url_for("tasks_bp.task_download_version",
            task_id=task_id,
            job_id=job_id,
            version_id=version_id,
        ),
        "restore_url": url_for("tasks_bp.task_compare_restore_version",
            task_id=task_id,
            job_id=job_id,
            version_id=version_id,
        ),
        "delete_url": url_for("tasks_bp.task_compare_delete_version",
            task_id=task_id,
            job_id=job_id,
            version_id=version_id,
        ),
    }
    return jsonify({"status": "ok", "version": version_payload})

@tasks_bp.get("/tasks/<task_id>/view/<job_id>/<path:filename>", endpoint="task_view_file")
def task_view_file(task_id, job_id, filename):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    safe_filename = filename.replace("\\", "/")
    file_path = os.path.join(job_dir, safe_filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(job_dir, safe_filename)

@tasks_bp.post("/tasks/<task_id>/compare/<job_id>/restore/<version_id>", endpoint="task_compare_restore_version")
def task_compare_restore_version(task_id, job_id, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((v for v in versions if v.get("id") == version_id), None)
    if not version:
        return jsonify({"error": "找不到指定版本"}), 404
    base_name = version.get("base_name")
    if not base_name:
        return jsonify({"error": "版本資料不完整"}), 404
    html_src = os.path.join(versions_dir, f"{base_name}.html")
    docx_src = os.path.join(versions_dir, f"{base_name}.docx")
    if not os.path.exists(html_src) or not os.path.exists(docx_src):
        return jsonify({"error": "版本檔案不存在"}), 404
    shutil.copyfile(html_src, os.path.join(job_dir, "result.html"))
    shutil.copyfile(docx_src, os.path.join(job_dir, "result.docx"))
    return jsonify({"status": "ok"})

@tasks_bp.post("/tasks/<task_id>/compare/<job_id>/delete/<version_id>", endpoint="task_compare_delete_version")
def task_compare_delete_version(task_id, job_id, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((v for v in versions if v.get("id") == version_id), None)
    if not version:
        return jsonify({"error": "找不到指定版本"}), 404
    metadata["versions"] = [v for v in versions if v.get("id") != version_id]
    save_version_metadata(versions_dir, metadata)
    base_name = version.get("base_name")
    if base_name:
        for ext in ("html", "docx"):
            path = os.path.join(versions_dir, f"{base_name}.{ext}")
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass
    return jsonify({"status": "ok"})

@tasks_bp.get("/tasks/<task_id>/download/<job_id>/version/<version_id>", endpoint="task_download_version")
def task_download_version(task_id, job_id, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((v for v in versions if v.get("id") == version_id), None)
    if not version:
        abort(404)
    base_name = version.get("base_name")
    if not base_name:
        abort(404)
    docx_src = os.path.join(versions_dir, f"{base_name}.docx")
    if not os.path.exists(docx_src):
        abort(404)
    slug = version.get("slug") or version_id
    download_name = f"{slug}_{version_id}.docx"
    return send_file(docx_src, as_attachment=True, download_name=download_name)

@tasks_bp.get("/tasks/<task_id>/download/<job_id>/<kind>", endpoint="task_download")
def task_download(task_id, job_id, kind):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if kind == "docx":
        result_path = os.path.join(job_dir, "result.docx")
        if not os.path.exists(result_path):
            abort(404)
        titles_to_remove = []
        log_path = os.path.join(job_dir, "log.json")
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    entries = json.load(f)
                titles_to_remove = collect_titles_to_hide(entries)
            except Exception:
                titles_to_remove = []

        download_path = os.path.join(job_dir, "result_download.docx")
        shutil.copyfile(result_path, download_path)
        if titles_to_remove:
            remove_paragraphs_with_text(download_path, titles_to_remove)
        if not SKIP_DOCX_CLEANUP:
            remove_hidden_runs(download_path)
        return send_file(
            download_path,
            as_attachment=True,
            download_name=f"result_{job_id}.docx",
        )
    elif kind == "log":
        return send_file(
            os.path.join(job_dir, "log.json"),
            as_attachment=True,
            download_name=f"log_{job_id}.json",
        )
    abort(404)
