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

from app.services.task_service import (
    allowed_file,
    build_file_tree,
    deduplicate_name,
    enforce_max_copy_size,
    ensure_windows_long_path,
    gather_available_files,
    list_dirs,
    list_files,
    list_tasks,
    task_name_exists,
)

from app.services.nas_service import get_configured_nas_roots, resolve_nas_path, validate_nas_path
from modules.file_copier import copy_files

tasks_bp = Blueprint("tasks_bp", __name__, template_folder="templates")


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
            except Exception as e:
                messages = [str(e)]
    rel_outputs = [os.path.basename(p) for p in outputs]
    return render_template("tasks/mapping.html", task_id=task_id, messages=messages, outputs=rel_outputs)


@tasks_bp.get("/tasks/<task_id>/output/<filename>", endpoint="task_download_output")
def task_download_output(task_id, filename):
    out_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id)
    file_path = os.path.join(out_dir, filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(out_dir, filename, as_attachment=True)


@tasks_bp.get("/", endpoint="tasks")
def tasks():
    task_list = list_tasks()
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
    creator = ""
    if current_user and getattr(current_user, "is_authenticated", False):
        display_name = (getattr(current_user, "display_name", "") or "").strip()
        chinese_only = "".join(re.findall(r"[\u4e00-\u9fff\u3400-\u4dbf\uF900-\uFAFF]+", display_name))
        work_id = (current_user.work_id or "").strip()
        if chinese_only:
            creator = f"{work_id} {chinese_only}" if work_id else chinese_only
        else:
            creator = display_name or work_id
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

    meta_payload = {
        "name": task_name,
        "description": task_desc,
        "created": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "nas_path": display_nas_path,
    }
    if creator:
        meta_payload["creator"] = creator
    with open(os.path.join(tdir, "meta.json"), "w", encoding="utf-8") as meta:
        json.dump(
            meta_payload,
            meta,
            ensure_ascii=False,
            indent=2,
        )
    return redirect(url_for("tasks_bp.tasks"))


@tasks_bp.post("/tasks/<task_id>/delete", endpoint="delete_task")
def delete_task(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if os.path.isdir(tdir):
        import shutil
        shutil.rmtree(tdir)
    return redirect(url_for("tasks_bp.tasks"))


@tasks_bp.post("/tasks/<task_id>/files", endpoint="upload_task_file")
def upload_task_file(task_id):
    """Upload additional files to an existing task."""
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)

    nas_input = request.form.get("nas_file_path", "").strip()
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
                return "僅支援 DOCX、PDF 或 ZIP 檔案，或複製整個資料夾", 400
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
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
            name = meta.get("name", task_id)
            description = meta.get("description", "")
            creator = meta.get("creator", "") or ""
            nas_path = meta.get("nas_path", "") or ""
    tree = build_file_tree(files_dir)
    return render_template(
        "tasks/task_detail.html",
        task={"id": task_id, "name": name, "description": description, "creator": creator, "nas_path": nas_path},
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
