from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime

from flask import abort, current_app, jsonify, render_template, request, send_file, send_from_directory, url_for

from app.services.flow_service import (
    SKIP_DOCX_CLEANUP,
    clean_compare_html_content,
    collect_titles_to_hide,
    load_titles_to_hide_from_log,
    load_version_metadata,
    remove_hidden_runs,
    remove_paragraphs_with_text,
    save_compare_output,
    save_version_metadata,
    sanitize_version_slug,
    translate_file,
)
from app.services.task_service import load_task_context as _load_task_context
from app.utils import normalize_docx_output_filename

from .blueprint import tasks_bp
from .compare_helpers import (
    _build_compare_source_label,
    _build_object_trace_candidates,
    _build_page_source_map,
    _build_paragraph_trace,
    _build_provenance_source_lookup,
    _build_provenance_trace,
    _ensure_html_preview,
    _ensure_pdf_preview,
    _ensure_provenance_preview_docx,
    _trace_source_label,
)


@tasks_bp.get("/tasks/<task_id>/result/<job_id>", endpoint="task_result")
def task_result(task_id, job_id):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    if not os.path.exists(docx_path):
        return "Job not found or failed.", 404
    log_json_path = os.path.join(job_dir, "log.json")
    log_entries = []
    overall_status = "ok"
    if os.path.exists(log_json_path):
        with open(log_json_path, "r", encoding="utf-8") as file_obj:
            log_entries = json.load(file_obj)
        if any(entry.get("status") == "error" for entry in log_entries):
            overall_status = "error"
    return render_template(
        "tasks/run.html",
        task=_load_task_context(task_id),
        job_id=job_id,
        docx_path=url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="docx"),
        log_path=url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="log"),
        translate_path=url_for("tasks_bp.task_translate", task_id=task_id, job_id=job_id),
        compare_path=url_for("tasks_bp.task_compare", task_id=task_id, job_id=job_id),
        back_link=url_for("flow_builder_bp.flow_builder", task_id=task_id),
        log_entries=log_entries,
        overall_status=overall_status,
    )


@tasks_bp.get("/tasks/<task_id>/translate/<job_id>", endpoint="task_translate")
def task_translate(task_id, job_id):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    source_path = os.path.join(job_dir, "result.docx")
    if not os.path.exists(source_path):
        abort(404)
    output_docx = os.path.join(job_dir, "translated.docx")
    if not os.path.exists(output_docx):
        markdown_path = os.path.join(job_dir, "translated.md")
        translate_file(source_path, markdown_path)
        import docx

        document = docx.Document()
        with open(markdown_path, "r", encoding="utf-8") as file_obj:
            for line in file_obj.read().splitlines():
                document.add_paragraph(line)
        document.save(output_docx)
    return send_file(
        output_docx,
        as_attachment=True,
        download_name=f"translated_{job_id}.docx",
    )


@tasks_bp.get("/tasks/<task_id>/compare/<job_id>", endpoint="task_compare")
def task_compare(task_id, job_id):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(docx_path) or not os.path.exists(log_path):
        abort(404)

    with open(log_path, "r", encoding="utf-8") as file_obj:
        entries = json.load(file_obj)
    titles_to_hide = collect_titles_to_hide(entries)
    preview_messages = []
    source_lookup = _build_provenance_source_lookup(entries)
    preview_docx_path = docx_path
    preview_docx_rel, preview_docx_error = _ensure_provenance_preview_docx(
        docx_path,
        log_path,
        job_dir,
        source_lookup,
    )
    if preview_docx_error:
        preview_messages.append(f"來源標記預覽建立失敗: {preview_docx_error}")
    elif preview_docx_rel:
        preview_docx_path = os.path.join(job_dir, preview_docx_rel)

    result_pdf_rel, result_pdf_error = _ensure_pdf_preview(preview_docx_path, job_dir, "preview_pdf")
    if result_pdf_error:
        preview_messages.append(f"結果文件預覽失敗: {result_pdf_error}")
    result_html_rel, result_html_error = _ensure_html_preview(
        preview_docx_path,
        job_dir,
        "preview_html",
        "provenance_preview",
    )
    if result_html_error:
        preview_messages.append(f"HTML 預覽建立失敗: {result_html_error}")

    chapter_sources = {}
    source_urls = {}
    converted_docx = {}
    current = None
    for entry in entries:
        step_type = entry.get("type")
        params = entry.get("params", {})
        if step_type == "insert_roman_heading":
            current = params.get("text", "")
            chapter_sources.setdefault(current, [])
        elif step_type == "extract_pdf_chapter_to_table":
            pdf_dir = os.path.join(job_dir, "pdfs_extracted")
            pdfs = []
            if os.path.isdir(pdf_dir):
                for filename in sorted(os.listdir(pdf_dir)):
                    if filename.lower().endswith(".pdf"):
                        pdfs.append(filename)
                        rel = os.path.join("pdfs_extracted", filename)
                        source_urls[filename] = url_for(
                            "tasks_bp.task_view_file",
                            task_id=task_id,
                            job_id=job_id,
                            filename=rel,
                        )
            chapter_sources.setdefault(current or "未分類", []).extend(pdfs)
        elif step_type == "extract_word_chapter":
            input_file = params.get("input_file", "")
            basename = os.path.basename(input_file)
            source_label = _trace_source_label(entry)
            section_start = params.get("target_chapter_section", "")
            section_end = params.get("explicit_end_number", "")
            section = f"{section_start}-{section_end}" if section_start and section_end else section_start
            title = params.get("target_chapter_title") or params.get("target_title_section", "")
            info = source_label
            if section:
                info += f" 章節 {section}"
            if title:
                info += f" 標題 {title}"
            chapter_sources.setdefault(current or "未分類", []).append(info)
            source_key = os.path.abspath(input_file) if input_file else ""
            if source_key and source_key not in converted_docx and os.path.exists(input_file):
                pdf_rel, pdf_error = _ensure_pdf_preview(input_file, job_dir, "source_pdf")
                if pdf_rel:
                    converted_docx[source_key] = pdf_rel
                elif pdf_error:
                    preview_messages.append(f"{basename} 預覽失敗: {pdf_error}")
            if source_key in converted_docx:
                source_urls[info] = url_for(
                    "tasks_bp.task_view_file",
                    task_id=task_id,
                    job_id=job_id,
                    filename=converted_docx[source_key],
                )
                source_urls.setdefault(
                    source_label,
                    url_for(
                        "tasks_bp.task_view_file",
                        task_id=task_id,
                        job_id=job_id,
                        filename=converted_docx[source_key],
                    ),
                )
        elif step_type == "extract_word_all_content":
            input_file = params.get("input_file", "")
            basename = os.path.basename(input_file)
            source_label = _trace_source_label(entry)
            chapter_sources.setdefault(current or "未分類", []).append(source_label)
            source_key = os.path.abspath(input_file) if input_file else ""
            if source_key and source_key not in converted_docx and os.path.exists(input_file):
                pdf_rel, pdf_error = _ensure_pdf_preview(input_file, job_dir, "source_pdf")
                if pdf_rel:
                    converted_docx[source_key] = pdf_rel
                elif pdf_error:
                    preview_messages.append(f"{basename} 預覽失敗: {pdf_error}")
            if source_key in converted_docx:
                source_urls[source_label] = url_for(
                    "tasks_bp.task_view_file",
                    task_id=task_id,
                    job_id=job_id,
                    filename=converted_docx[source_key],
                )
        elif step_type == "extract_pdf_pages_as_images":
            input_file = params.get("input_file", "")
            basename = os.path.basename(input_file)
            source_label = _trace_source_label(entry)
            chapter_sources.setdefault(current or "未分類", []).append(source_label)
            pdf_rel, pdf_error = _ensure_pdf_preview(input_file, job_dir, "source_pdf")
            if pdf_rel:
                source_urls.setdefault(
                    source_label,
                    url_for(
                        "tasks_bp.task_view_file",
                        task_id=task_id,
                        job_id=job_id,
                        filename=pdf_rel,
                    ),
                )
            elif pdf_error:
                preview_messages.append(f"{basename} 預覽失敗: {pdf_error}")
        elif step_type in {"extract_specific_figure_from_word", "extract_specific_table_from_word"}:
            input_file = params.get("input_file", "")
            basename = os.path.basename(input_file)
            source_label = _trace_source_label(entry)
            info = _build_compare_source_label(entry)
            chapter_sources.setdefault(current or "未分類", []).append(info)
            source_key = os.path.abspath(input_file) if input_file else ""
            if source_key and source_key not in converted_docx and os.path.exists(input_file):
                pdf_rel, pdf_error = _ensure_pdf_preview(input_file, job_dir, "source_pdf")
                if pdf_rel:
                    converted_docx[source_key] = pdf_rel
                elif pdf_error:
                    preview_messages.append(f"{basename} 預覽失敗: {pdf_error}")
            if source_key in converted_docx:
                source_url = url_for(
                    "tasks_bp.task_view_file",
                    task_id=task_id,
                    job_id=job_id,
                    filename=converted_docx[source_key],
                )
                source_urls[info] = source_url
                source_urls.setdefault(source_label, source_url)

    chapters = list(chapter_sources.keys())
    provenance_trace = _build_provenance_trace(job_dir, docx_path, log_path, entries, titles_to_hide)
    if provenance_trace:
        paragraph_trace, object_trace_candidates = provenance_trace
    else:
        paragraph_trace = _build_paragraph_trace(job_dir, docx_path, log_path, entries, titles_to_hide)
        object_trace_candidates = _build_object_trace_candidates(entries, titles_to_hide)
    result_pdf_abs = os.path.join(job_dir, result_pdf_rel) if result_pdf_rel else ""
    paragraph_trace, page_source_map = _build_page_source_map(
        job_dir,
        result_pdf_abs,
        paragraph_trace,
        object_trace_candidates,
        source_lookup,
    )
    return render_template(
        "tasks/compare.html",
        task=_load_task_context(task_id),
        preview_url=url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=result_pdf_rel)
        if result_pdf_rel
        else "",
        html_preview_url=url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=result_html_rel)
        if result_html_rel
        else "",
        chapters=chapters,
        chapter_sources=chapter_sources,
        source_urls=source_urls,
        titles_to_hide=titles_to_hide,
        paragraph_trace=paragraph_trace,
        page_source_map=page_source_map,
        preview_messages=list(dict.fromkeys(preview_messages)),
        back_link=url_for("tasks_bp.task_result", task_id=task_id, job_id=job_id),
        download_url=url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="docx"),
    )


@tasks_bp.post("/tasks/<task_id>/compare/<job_id>/save", endpoint="task_compare_save")
def task_compare_save(task_id, job_id):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
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
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
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
    versions = [version for version in versions if version.get("id") != version_id]
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
    versions.sort(key=lambda version: version.get("created_at", ""), reverse=True)
    metadata["versions"] = versions
    save_version_metadata(versions_dir, metadata)
    version_payload = {
        "id": version_id,
        "name": version_name,
        "created_at_display": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "html_url": url_for("tasks_bp.task_view_file", task_id=task_id, job_id=job_id, filename=f"versions/{base_name}.html"),
        "docx_url": url_for("tasks_bp.task_download_version", task_id=task_id, job_id=job_id, version_id=version_id),
        "restore_url": url_for("tasks_bp.task_compare_restore_version", task_id=task_id, job_id=job_id, version_id=version_id),
        "delete_url": url_for("tasks_bp.task_compare_delete_version", task_id=task_id, job_id=job_id, version_id=version_id),
    }
    return jsonify({"status": "ok", "version": version_payload})


@tasks_bp.get("/tasks/<task_id>/view/<job_id>/<path:filename>", endpoint="task_view_file")
def task_view_file(task_id, job_id, filename):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    safe_filename = filename.replace("\\", "/")
    file_path = os.path.join(job_dir, safe_filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(job_dir, safe_filename)


@tasks_bp.post("/tasks/<task_id>/compare/<job_id>/restore/<version_id>", endpoint="task_compare_restore_version")
def task_compare_restore_version(task_id, job_id, version_id):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((item for item in versions if item.get("id") == version_id), None)
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
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((item for item in versions if item.get("id") == version_id), None)
    if not version:
        return jsonify({"error": "找不到指定版本"}), 404
    metadata["versions"] = [item for item in versions if item.get("id") != version_id]
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
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((item for item in versions if item.get("id") == version_id), None)
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
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    if kind == "docx":
        result_path = os.path.join(job_dir, "result.docx")
        if not os.path.exists(result_path):
            abort(404)
        titles_to_remove = []
        log_path = os.path.join(job_dir, "log.json")
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as file_obj:
                    entries = json.load(file_obj)
                titles_to_remove = collect_titles_to_hide(entries)
            except Exception:
                titles_to_remove = []

        download_path = os.path.join(job_dir, "result_download.docx")
        shutil.copyfile(result_path, download_path)
        if titles_to_remove:
            remove_paragraphs_with_text(download_path, titles_to_remove)
        if not SKIP_DOCX_CLEANUP:
            remove_hidden_runs(download_path)
        download_name = f"result_{job_id}.docx"
        meta_path = os.path.join(job_dir, "meta.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as file_obj:
                    meta = json.load(file_obj)
                if isinstance(meta, dict):
                    candidate_name, candidate_error = normalize_docx_output_filename(
                        meta.get("output_filename"),
                        default="",
                    )
                    if not candidate_error and candidate_name:
                        download_name = candidate_name
            except Exception:
                pass
        return send_file(
            download_path,
            as_attachment=True,
            download_name=download_name,
        )
    if kind == "log":
        return send_file(
            os.path.join(job_dir, "log.json"),
            as_attachment=True,
            download_name=f"log_{job_id}.json",
        )
    abort(404)
