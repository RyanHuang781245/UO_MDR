from __future__ import annotations

import json
import os
import re
import shutil
import threading
import time
import uuid
from datetime import datetime

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, send_file, url_for
from flask_login import current_user
from werkzeug.utils import secure_filename

from app.services.flow_service import (
    DEFAULT_APPLY_FORMATTING,
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DEFAULT_LINE_SPACING,
    DOCUMENT_FORMAT_PRESETS,
    LINE_SPACING_CHOICES,
    SKIP_DOCX_CLEANUP,
    SUPPORTED_STEPS,
    apply_basic_style,
    collect_titles_to_hide,
    coerce_line_spacing,
    hide_paragraphs_with_text,
    normalize_document_format,
    parse_template_paragraphs,
    remove_hidden_runs,
    run_workflow,
)
from app.services.notification_service import send_batch_notification
from app.services.audit_service import record_audit
from app.services.task_service import build_file_tree, gather_available_files
from app.utils import parse_bool
from app.blueprints.tasks.routes import _load_task_context

flows_bp = Blueprint("flows_bp", __name__, template_folder="templates")


_INVALID_FLOW_NAME_CHARS = r'\\/:*?"<>|'
_WINDOWS_RESERVED_FLOW_NAMES = {
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


def _validate_flow_name(name: str) -> str | None:
    text = (name or "").strip()
    if not text:
        return "缺少流程名稱"
    if len(text) > 50:
        return "流程名稱最多 50 字"
    if text in {".", ".."}:
        return "流程名稱不合法"
    if any(ord(ch) < 32 for ch in text):
        return "流程名稱含有不可見控制字元"
    if any(ch in _INVALID_FLOW_NAME_CHARS for ch in text):
        return '流程名稱不可包含 \\ / : * ? " < > |'
    if text[-1] in {" ", "."}:
        return "流程名稱結尾不可為空白或句點"
    if text.upper() in _WINDOWS_RESERVED_FLOW_NAMES:
        return "流程名稱為系統保留字，請更換名稱"
    return None


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


def _touch_task_last_edit(task_id: str, work_id: str | None = None, label: str | None = None) -> None:
    meta_path = os.path.join(current_app.config["TASK_FOLDER"], task_id, "meta.json")
    if not os.path.exists(meta_path):
        return
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        meta = {}
    if work_id is None or label is None:
        work_id, label = _get_actor_info()
    meta["last_edited"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    if label:
        meta["last_editor"] = label
    if work_id:
        meta["last_editor_work_id"] = work_id
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _batch_status_path(task_id: str, batch_id: str) -> str:
    return os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", "batch", f"{batch_id}.json")


def _write_batch_status(task_id: str, batch_id: str, payload: dict) -> None:
    status_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", "batch")
    os.makedirs(status_dir, exist_ok=True)
    path = _batch_status_path(task_id, batch_id)
    _write_json_with_replace_retry(path, payload)


def _load_batch_status(task_id: str, batch_id: str) -> dict | None:
    path = _batch_status_path(task_id, batch_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_job_meta(job_dir: str, payload: dict) -> None:
    try:
        meta_path = os.path.join(job_dir, "meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        current_app.logger.exception("Failed to write job meta")


def _read_job_meta(job_dir: str) -> dict:
    meta_path = os.path.join(job_dir, "meta.json")
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}

def _update_job_meta(job_dir: str, **fields) -> None:
    meta = _read_job_meta(job_dir)
    meta.update(fields)
    _write_job_meta(job_dir, meta)


def _job_has_error(job_dir: str) -> bool:
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(log_path):
        return False
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            entries = json.load(f)
        if not isinstance(entries, list):
            return False
        return any(isinstance(e, dict) and e.get("status") == "error" for e in entries)
    except Exception:
        return False


def _normalize_task_file_rel_path(raw_path: str) -> str:
    cleaned = (raw_path or "").strip().replace("\\", "/")
    if cleaned in {"", ".", "/"}:
        return ""
    if cleaned.startswith("/") or os.path.isabs(cleaned):
        raise ValueError("Invalid task file path")
    normalized = os.path.normpath(cleaned).replace("\\", "/")
    if normalized in {"", "."}:
        return ""
    if normalized == ".." or normalized.startswith("../"):
        raise ValueError("Invalid task file path")
    return normalized


def _resolve_task_file_path(files_dir: str, rel_path: str, expect_dir: bool | None = None) -> str:
    rel = _normalize_task_file_rel_path(rel_path)
    base_abs = os.path.abspath(files_dir)
    candidate = os.path.abspath(os.path.join(base_abs, rel))
    try:
        if os.path.commonpath([base_abs, candidate]) != base_abs:
            raise ValueError("Invalid task file path")
    except ValueError as exc:
        raise ValueError("Invalid task file path") from exc

    if expect_dir is True and not os.path.isdir(candidate):
        raise FileNotFoundError("Task directory not found")
    if expect_dir is False and not os.path.isfile(candidate):
        raise FileNotFoundError("Task file not found")
    return candidate


def _normalize_step_file_value(raw_value: str, accept: str) -> str:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return ""
    rel = _normalize_task_file_rel_path(cleaned)
    if accept.endswith(":dir") and rel == "":
        return "."
    return rel


def _resolve_runtime_step_params(files_dir: str, schema: dict, raw_params: dict) -> dict:
    params = {}
    for key, value in raw_params.items():
        accept = schema.get("accepts", {}).get(key, "text")
        if isinstance(accept, str) and accept.startswith("file") and value:
            params[key] = _resolve_task_file_path(files_dir, str(value), expect_dir=accept.endswith(":dir"))
        else:
            params[key] = value
    return params


def _execute_saved_flow(
    task_id: str,
    flow_name: str,
    source: str = "manual",
    global_batch_id: str = "",
    task_batch_id: str = "",
) -> str:
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        raise FileNotFoundError("Task files not found")
    flow_path = os.path.join(tdir, "flows", f"{flow_name}.json")
    if not os.path.exists(flow_path):
        raise FileNotFoundError("Flow not found")
    with open(flow_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = DEFAULT_LINE_SPACING
    template_file = None
    apply_formatting = DEFAULT_APPLY_FORMATTING
    template_cfg = None
    if isinstance(data, dict):
        workflow = data.get("steps", [])
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing_raw = str(data.get("line_spacing", f"{DEFAULT_LINE_SPACING:g}"))
        line_spacing_none = line_spacing_raw.strip().lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_raw)
        apply_formatting = parse_bool(data.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
        if document_format == "none" or line_spacing_none:
            apply_formatting = False
        template_file = data.get("template_file")
    else:
        workflow = data
    if template_file:
        try:
            tpl_abs = _resolve_task_file_path(files_dir, str(template_file), expect_dir=False)
        except (ValueError, FileNotFoundError):
            tpl_abs = ""
        if tpl_abs and os.path.isfile(tpl_abs):
            template_paragraphs = parse_template_paragraphs(tpl_abs)
            template_cfg = {"path": tpl_abs, "paragraphs": template_paragraphs}
    runtime_steps = []
    for step in workflow:
        stype = step.get("type")
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = _resolve_runtime_step_params(files_dir, schema, step.get("params", {}) or {})
        runtime_steps.append({"type": stype, "params": params})
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    _write_job_meta(
        job_dir,
        {
            "flow_name": flow_name,
            "mode": "batch",
            "source": source,
            "global_batch_id": global_batch_id,
            "task_batch_id": task_batch_id,
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    log_entries = workflow_result.get("log_json", []) or []
    has_step_error = any(e.get("status") == "error" for e in log_entries)
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    if apply_formatting and document_format != "none":
        preset = DOCUMENT_FORMAT_PRESETS.get(document_format) or DOCUMENT_FORMAT_PRESETS[DEFAULT_DOCUMENT_FORMAT_KEY]
        apply_basic_style(
            result_path,
            western_font=preset.get("western_font") or "",
            east_asian_font=preset.get("east_asian_font") or "",
            font_size=int(preset.get("font_size") or 12),
            line_spacing=line_spacing,
            space_before=int(preset.get("space_before") or 6),
            space_after=int(preset.get("space_after") or 6),
        )
    if not SKIP_DOCX_CLEANUP:
        remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
        hide_paragraphs_with_text(result_path, titles_to_hide)
    if has_step_error:
        _update_job_meta(
            job_dir,
            status="failed",
            error="Workflow step failed",
            completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        _update_job_meta(
            job_dir,
            status="completed",
            completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
    return job_id


def _run_single_job(
    app,
    task_id: str,
    runtime_steps: list[dict],
    template_cfg: dict | None,
    document_format: str,
    line_spacing: float,
    apply_formatting: bool,
    job_id: str,
    actor: dict,
    flow_name: str | None = None,
) -> None:
    with app.app_context():
        tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
        job_dir = os.path.join(tdir, "jobs", job_id)
        try:
            _update_job_meta(job_dir, status="running", started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
            result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
            log_entries = workflow_result.get("log_json", []) or []
            has_step_error = any(e.get("status") == "error" for e in log_entries)
            titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
            if apply_formatting and document_format != "none":
                preset = DOCUMENT_FORMAT_PRESETS.get(document_format) or DOCUMENT_FORMAT_PRESETS[DEFAULT_DOCUMENT_FORMAT_KEY]
                apply_basic_style(
                    result_path,
                    western_font=preset.get("western_font") or "",
                    east_asian_font=preset.get("east_asian_font") or "",
                    font_size=int(preset.get("font_size") or 12),
                    line_spacing=line_spacing,
                    space_before=int(preset.get("space_before") or 6),
                    space_after=int(preset.get("space_after") or 6),
                )
            if not SKIP_DOCX_CLEANUP:
                remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
                hide_paragraphs_with_text(result_path, titles_to_hide)
            _touch_task_last_edit(task_id, work_id=actor.get("work_id"), label=actor.get("label"))
            if has_step_error:
                _update_job_meta(
                    job_dir,
                    status="failed",
                    error="Workflow step failed",
                    completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                record_audit(
                    action="flow_run_single_failed",
                    actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
                    detail={"task_id": task_id, "flow": flow_name, "job_id": job_id, "status": "failed"},
                    task_id=task_id,
                )
            else:
                _update_job_meta(job_dir, status="completed", completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                record_audit(
                    action="flow_run_single_completed",
                    actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
                    detail={"task_id": task_id, "flow": flow_name, "job_id": job_id, "status": "completed"},
                    task_id=task_id,
                )
        except Exception as exc:
            current_app.logger.exception("Single flow execution failed")
            _update_job_meta(
                job_dir,
                status="failed",
                error=str(exc),
                completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            record_audit(
                action="flow_run_single_failed",
                actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
                detail={"task_id": task_id, "flow": flow_name, "job_id": job_id, "status": "failed", "error": str(exc)},
                task_id=task_id,
            )


def _run_flow_batch(
    app,
    task_id: str,
    flow_sequence: list[str],
    batch_id: str,
    actor: dict,
    source: str = "batch",
    global_batch_id: str = "",
) -> None:
    with app.app_context():
        status = _load_batch_status(task_id, batch_id) or {}
        status.update({"status": "running", "current_index": 0})
        _write_batch_status(task_id, batch_id, status)
        results = []
        failed_count = 0
        for idx, flow_name in enumerate(flow_sequence, start=1):
            status.update({"current_index": idx, "current_flow": flow_name})
            _write_batch_status(task_id, batch_id, status)
            job_id = ""
            try:
                job_id = _execute_saved_flow(
                    task_id,
                    flow_name,
                    source=source,
                    global_batch_id=global_batch_id,
                    task_batch_id=batch_id,
                )
                job_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", job_id)
                job_meta = _read_job_meta(job_dir)
                job_status = (job_meta.get("status") or "").lower()
                job_error = (job_meta.get("error") or "").strip()
                if job_status == "failed" or _job_has_error(job_dir):
                    raise RuntimeError(job_error or "Workflow step failed")
                results.append({"flow": flow_name, "job_id": job_id, "status": "completed"})
                _touch_task_last_edit(task_id, work_id=actor.get("work_id"), label=actor.get("label"))
            except Exception as exc:
                failed_item = {"flow": flow_name, "status": "failed", "error": str(exc)}
                if job_id:
                    failed_item["job_id"] = job_id
                results.append(failed_item)
                failed_count += 1
                status.update(
                    {
                        "results": results,
                        "last_error": str(exc),
                    }
                )
                _write_batch_status(task_id, batch_id, status)
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        has_failed = failed_count > 0
        final_status = "completed_with_errors" if has_failed else "completed"
        status.update({"status": final_status, "results": results, "completed_at": completed_at})
        if has_failed:
            status["error"] = f"{failed_count} flow(s) failed"
        else:
            status.pop("error", None)
        _write_batch_status(task_id, batch_id, status)
        record_audit(
            action="flow_batch_completed_with_errors" if has_failed else "flow_batch_completed",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={
                "task_id": task_id,
                "batch_id": batch_id,
                "status": final_status,
                "count": len(results),
                "failed_count": failed_count,
                "results": results,
            },
            task_id=task_id,
        )
        send_batch_notification(
            task_id=task_id,
            batch_id=batch_id,
            status="failed" if has_failed else "completed",
            results=results,
            actor_work_id=actor.get("work_id", ""),
            actor_label=actor.get("label", ""),
            completed_at=completed_at,
            error=status.get("error") if has_failed else None,
        )


def _load_saved_flows(flow_dir: str) -> list[dict]:
    flows = []
    for fn in os.listdir(flow_dir):
        if fn.endswith(".json") and fn != "order.json":
            path = os.path.join(flow_dir, fn)
            created = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
            has_copy = False
            steps_data = []
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    steps_data = data.get("steps", [])
                    created = data.get("created", created)
                elif isinstance(data, list):
                    steps_data = data
                has_copy = any(
                    isinstance(s, dict) and s.get("type") == "copy_files"
                    for s in steps_data
                )
            except Exception:
                pass
            flows.append(
                {
                    "name": os.path.splitext(fn)[0],
                    "created": created,
                    "has_copy": has_copy,
                }
            )
    flows.sort(key=lambda f: f["name"])
    return flows


def _list_flow_runs(task_id: str) -> list[dict]:
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    if not os.path.isdir(jobs_dir):
        return []
    results = []
    for name in os.listdir(jobs_dir):
        job_dir = os.path.join(jobs_dir, name)
        if name == "batch" or not os.path.isdir(job_dir):
            continue
        meta = _read_job_meta(job_dir)
        if meta.get("mode") == "batch":
            continue
        source = (meta.get("source") or "manual").strip().lower()
        if source not in {"manual", "global_batch"}:
            source = "manual"
        flow_name = (meta.get("flow_name") or "").strip() or "未命名流程"
        started_at = meta.get("started_at")
        if not started_at:
            started_at = datetime.fromtimestamp(os.path.getmtime(job_dir)).strftime("%Y-%m-%d %H:%M:%S")
        result_path = os.path.join(job_dir, "result.docx")
        log_path = os.path.join(job_dir, "log.json")
        completed = os.path.exists(result_path)
        status = meta.get("status")
        log_error = _job_has_error(job_dir)
        if log_error:
            status = "failed"
            if meta.get("status") != "failed":
                _update_job_meta(
                    job_dir,
                    status="failed",
                    error=meta.get("error") or "Workflow step failed",
                    completed_at=meta.get("completed_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
        if not status:
            status = "completed" if completed else "pending"
        results.append(
            {
                "job_id": name,
                "flow_name": flow_name,
                "started_at": started_at,
                "status": status,
                "source": source,
                "has_result": completed,
                "has_log": os.path.exists(log_path),
                "error": meta.get("error") or "",
            }
        )
    results.sort(key=lambda r: r["started_at"], reverse=True)
    return results


@flows_bp.post("/tasks/<task_id>/flows/runs/<job_id>/delete", endpoint="delete_flow_run")
def delete_flow_run(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if not os.path.isdir(job_dir):
        abort(404)
    try:
        shutil.rmtree(job_dir)
        flash("已刪除執行紀錄。", "success")
    except Exception:
        current_app.logger.exception("Failed to delete flow run")
        flash("刪除失敗，請稍後再試。", "danger")
    return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="single"))


@flows_bp.post("/tasks/<task_id>/flows/runs/delete", endpoint="delete_flow_runs_bulk")
def delete_flow_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    raw = request.form.get("job_ids", "")
    job_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not job_ids:
        flash("請先選取要刪除的執行紀錄。", "warning")
        return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="single"))
    deleted = 0
    for job_id in job_ids:
        job_dir = os.path.join(jobs_dir, job_id)
        if not os.path.isdir(job_dir):
            continue
        try:
            shutil.rmtree(job_dir)
            deleted += 1
        except Exception:
            current_app.logger.exception("Failed to delete flow run")
    if deleted:
        flash(f"已刪除 {deleted} 筆執行紀錄。", "success")
    else:
        flash("沒有可刪除的執行紀錄。", "warning")
    return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="single"))


@flows_bp.post("/tasks/<task_id>/flows/runs/download", endpoint="download_flow_runs_bulk")
def download_flow_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    kind = request.form.get("kind", "docx")
    raw = request.form.get("job_ids", "")
    job_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not job_ids:
        flash("請先選取要下載的執行紀錄。", "warning")
        return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="single"))
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"flow_runs_{kind}_{stamp}.zip"
    zip_path = os.path.join(jobs_dir, zip_name)
    import zipfile
    added = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for job_id in job_ids:
            job_dir = os.path.join(jobs_dir, job_id)
            if not os.path.isdir(job_dir):
                continue
            filename = "result.docx" if kind == "docx" else "log.json"
            src = os.path.join(job_dir, filename)
            if not os.path.exists(src):
                continue
            arcname = os.path.join(job_id, filename)
            zf.write(src, arcname=arcname)
            added += 1
    if added == 0:
        flash("沒有可下載的檔案。", "warning")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="single"))
    return send_file(zip_path, as_attachment=True, download_name=zip_name)


def _list_batch_statuses(task_id: str) -> list[dict]:
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    status_dir = os.path.join(tdir, "jobs", "batch")
    if not os.path.isdir(status_dir):
        return []
    items = []
    for fn in os.listdir(status_dir):
        if not fn.endswith(".json"):
            continue
        batch_id = os.path.splitext(fn)[0]
        status = _load_batch_status(task_id, batch_id) or {}
        created_at = status.get("created_at")
        if not created_at:
            created_at = datetime.fromtimestamp(os.path.getmtime(os.path.join(status_dir, fn))).strftime("%Y-%m-%d %H:%M:%S")
        items.append(
            {
                "id": batch_id,
                "status": status.get("status") or "unknown",
                "created_at": created_at,
                "current_flow": status.get("current_flow") or "",
                "current_index": status.get("current_index") or 0,
                "total": len(status.get("flows") or []),
            }
        )
    items.sort(key=lambda r: r["created_at"], reverse=True)
    return items


@flows_bp.get("/tasks/<task_id>/flows", endpoint="flow_builder")
def flow_builder(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    
    task_context = _load_task_context(task_id)
    if not task_context:
        abort(404)
    task_context["id"] = task_id

    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    flows_all = _load_saved_flows(flow_dir)
    
    # Pagination for saved flows
    flow_page = request.args.get("fpage", 1, type=int)
    per_page = 10
    total_count = len(flows_all)
    total_pages = (total_count + per_page - 1) // per_page
    start = (flow_page - 1) * per_page
    flows = flows_all[start : start + per_page]
    flow_pagination = {
        "page": flow_page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": flow_page > 1,
        "has_next": flow_page < total_pages
    }
    
    preset = None
    template_file = None
    template_paragraphs = []
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = f"{DEFAULT_LINE_SPACING:g}"
    apply_formatting = DEFAULT_APPLY_FORMATTING
    loaded_name = request.args.get("flow")
    job_id = request.args.get("job")
    if loaded_name:
        p = os.path.join(flow_dir, f"{loaded_name}.json")
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                steps_data = data.get("steps", [])
                template_file = data.get("template_file")
                document_format = normalize_document_format(data.get("document_format"))
                line_spacing = str(data.get("line_spacing", f"{DEFAULT_LINE_SPACING:g}"))
                apply_formatting = parse_bool(data.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
            else:
                steps_data = data
            preset = [
                s for s in steps_data
                if isinstance(s, dict) and s.get("type") in SUPPORTED_STEPS
            ]
    if template_file:
        try:
            template_file = _normalize_task_file_rel_path(str(template_file))
            tpl_abs = _resolve_task_file_path(files_dir, template_file, expect_dir=False)
        except (ValueError, FileNotFoundError):
            template_file = None
            tpl_abs = ""
        if tpl_abs:
            try:
                template_paragraphs = parse_template_paragraphs(tpl_abs)
            except Exception:
                template_paragraphs = []
        else:
            template_file = None
    avail = gather_available_files(files_dir)
    tree = build_file_tree(files_dir)
    return render_template(
        "flows/flow.html",
        task=task_context,
        steps=SUPPORTED_STEPS,
        files=avail,
        flows=flows,
        preset=preset,
        loaded_name=loaded_name,
        job_id=job_id,
        files_tree=tree,
        template_file=template_file,
        template_paragraphs=template_paragraphs,
        document_format=document_format,
        line_spacing=line_spacing,
        apply_formatting=apply_formatting,
        document_format_presets=DOCUMENT_FORMAT_PRESETS,
        line_spacing_choices=LINE_SPACING_CHOICES,
        flow_pagination=flow_pagination,
    )


# Global Batch Helpers
def _global_batch_status_path(batch_id: str) -> str:
    path = os.path.join(current_app.config["TASK_FOLDER"], "global_batches")
    os.makedirs(path, exist_ok=True)
    return os.path.join(path, f"{batch_id}.json")


def _normalize_global_task_ids(raw_ids: str) -> list[str]:
    task_ids: list[str] = []
    seen: set[str] = set()
    for part in (raw_ids or "").split(","):
        tid = part.strip()
        if not tid or tid in seen:
            continue
        seen.add(tid)
        task_ids.append(tid)
    return task_ids


def _write_json_with_replace_retry(path: str, payload: dict, retries: int = 8, delay_sec: float = 0.03) -> None:
    last_exc = None
    for attempt in range(retries):
        tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
            return
        except PermissionError as exc:
            last_exc = exc
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            if attempt == retries - 1:
                raise
            time.sleep(delay_sec * (attempt + 1))
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise
    if last_exc:
        raise last_exc


def _write_global_batch_status(batch_id: str, payload: dict) -> None:
    path = _global_batch_status_path(batch_id)
    _write_json_with_replace_retry(path, payload)


def _load_global_batch_status(batch_id: str) -> dict | None:
    path = _global_batch_status_path(batch_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            status = json.load(f)
        if isinstance(status, dict):
            return _enrich_global_batch_status(status)
        return None
    except Exception:
            return None


def _build_job_relpaths(task_id: str, job_id: str) -> dict:
    base = os.path.join(task_id, "jobs", job_id).replace("\\", "/")
    return {
        "job_relpath": base,
        "result_relpath": f"{base}/result.docx",
        "log_relpath": f"{base}/log.json",
    }


def _enrich_global_batch_status(status: dict) -> dict:
    results = status.get("results")
    if not isinstance(results, list):
        return status

    for task_result in results:
        if not isinstance(task_result, dict):
            continue
        task_id = (task_result.get("task_id") or "").strip()
        flows = task_result.get("flows")
        if not isinstance(flows, list):
            continue
        for flow in flows:
            if not isinstance(flow, dict):
                continue
            job_id = (flow.get("job_id") or "").strip()
            if not task_id or not job_id:
                continue
            paths = _build_job_relpaths(task_id, job_id)
            flow.setdefault("job_relpath", paths["job_relpath"])
            flow.setdefault("result_relpath", paths["result_relpath"])
            flow.setdefault("log_relpath", paths["log_relpath"])
    return status


def _list_global_batch_statuses(limit: int = 100) -> list[dict]:
    status_dir = os.path.join(current_app.config["TASK_FOLDER"], "global_batches")
    if not os.path.isdir(status_dir):
        return []
    items: list[dict] = []
    for fn in os.listdir(status_dir):
        if not fn.endswith(".json"):
            continue
        batch_id = os.path.splitext(fn)[0]
        status = _load_global_batch_status(batch_id) or {}
        created_at = status.get("created_at")
        if not created_at:
            created_at = datetime.fromtimestamp(
                os.path.getmtime(os.path.join(status_dir, fn))
            ).strftime("%Y-%m-%d %H:%M:%S")
        tasks = status.get("tasks") or []
        results = status.get("results") or []
        ok_count = sum(1 for item in results if item.get("ok"))
        fail_count = sum(1 for item in results if not item.get("ok"))
        items.append(
            {
                "id": batch_id,
                "status": (status.get("status") or "unknown").lower(),
                "created_at": created_at,
                "completed_at": status.get("completed_at") or "",
                "current_task_name": status.get("current_task_name") or "",
                "task_count": len(tasks),
                "ok_count": ok_count,
                "fail_count": fail_count,
            }
        )
    items.sort(key=lambda r: r["created_at"], reverse=True)
    return items[: max(limit, 0)]


def _run_tasks_batch(app, task_ids: list[str], batch_id: str, actor: dict) -> None:
    with app.app_context():
        from app.blueprints.tasks.routes import _load_task_context

        status = _load_global_batch_status(batch_id) or {}
        status.update(
            {
                "status": "running",
                "current_index": 0,
                "current_task_id": "",
                "current_task_name": "",
            }
        )
        _write_global_batch_status(batch_id, status)

        results = []
        any_failed = False
        terminal_error = ""
        try:
            for i, tid in enumerate(task_ids, start=1):
                task_meta = _load_task_context(tid) or {}
                task_name = (task_meta.get("name") or tid).strip() or tid
                status["current_task_id"] = tid
                status["current_index"] = i
                status["current_task_name"] = task_name
                _write_global_batch_status(batch_id, status)

                tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
                flow_dir = os.path.join(tdir, "flows")
                task_ok = True
                task_errors = []
                flow_results: list[dict] = []
                task_batch_id = ""

                if not task_meta:
                    task_ok = False
                    task_errors.append("Task not found")
                elif not os.path.isdir(flow_dir):
                    task_ok = False
                    task_errors.append("Flow directory not found")
                else:
                    flows_to_run = _load_saved_flows(flow_dir)
                    if not flows_to_run:
                        task_ok = False
                        task_errors.append("No saved flow found")
                    else:
                        flow_sequence = [
                            (f.get("name") or "").strip()
                            for f in flows_to_run
                            if (f.get("name") or "").strip()
                        ]
                        if not flow_sequence:
                            task_ok = False
                            task_errors.append("No runnable flow found")
                        else:
                            task_batch_id = str(uuid.uuid4())[:8]
                            _write_batch_status(
                                tid,
                                task_batch_id,
                                {
                                    "id": task_batch_id,
                                    "status": "queued",
                                    "flows": flow_sequence,
                                    "current_index": 0,
                                    "current_flow": "",
                                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "actor": actor.get("label") or actor.get("work_id", ""),
                                },
                            )
                            _run_flow_batch(
                                app,
                                tid,
                                flow_sequence,
                                task_batch_id,
                                actor,
                                source="global_batch",
                                global_batch_id=batch_id,
                            )
                            task_batch_status = _load_batch_status(tid, task_batch_id) or {}
                            task_ok = (task_batch_status.get("status") or "").lower() == "completed"
                            batch_results = task_batch_status.get("results") or []
                            for item in batch_results:
                                flow_name = (item.get("flow") or "").strip()
                                flow_ok = (item.get("status") or "").lower() == "completed"
                                flow_error = (item.get("error") or "").strip()
                                flow_job_id = (item.get("job_id") or "").strip()
                                path_info = _build_job_relpaths(tid, flow_job_id) if flow_job_id else {}
                                flow_results.append(
                                    {
                                        "flow": flow_name,
                                        "ok": flow_ok,
                                        "job_id": flow_job_id,
                                        "error": flow_error,
                                        "job_relpath": path_info.get("job_relpath", ""),
                                        "result_relpath": path_info.get("result_relpath", ""),
                                        "log_relpath": path_info.get("log_relpath", ""),
                                    }
                                )
                                if not flow_ok:
                                    task_errors.append(
                                        f"{flow_name}: {flow_error or 'Workflow step failed'}"
                                    )

                results.append({
                    "task_id": tid,
                    "name": task_name,
                    "ok": task_ok,
                    "errors": task_errors,
                    "flows": flow_results,
                    "task_batch_id": task_batch_id,
                })
                any_failed = any_failed or (not task_ok)
                status["results"] = results
                _write_global_batch_status(batch_id, status)

        except Exception as exc:
            current_app.logger.exception("Global batch failed")
            any_failed = True
            terminal_error = str(exc)
            status["error"] = terminal_error
        finally:
            completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status["status"] = "failed" if any_failed else "completed"
            status["completed_at"] = completed_at
            status["results"] = results
            _write_global_batch_status(batch_id, status)

            actor_work_id = actor.get("work_id", "")
            actor_label = actor.get("label", "")

            record_audit(
                action="global_task_batch_completed" if status["status"] == "completed" else "global_task_batch_failed",
                actor={"work_id": actor_work_id, "label": actor_label},
                detail={
                    "batch_id": batch_id,
                    "status": status["status"],
                    "tasks": task_ids,
                    "count": len(results),
                    "failed_count": sum(1 for item in results if not item.get("ok")),
                    "error": terminal_error or status.get("error") or "",
                },
                task_id=None,
            )

@flows_bp.get("/batch/global", endpoint="global_batch_page")
def global_batch_page():
    raw_ids = request.args.get("task_ids", "")
    task_ids = _normalize_global_task_ids(raw_ids)
    batch_id = request.args.get("batch")
    batch_status = _load_global_batch_status(batch_id) if batch_id else None

    if not task_ids:
        if batch_id:
            status = _load_global_batch_status(batch_id)
            if status:
                task_ids = status.get("tasks", [])
            else:
                flash("找不到指定的任務排程批次。", "warning")
                batch_id = None

    tasks = []
    from app.blueprints.tasks.routes import _load_task_context
    for tid in task_ids:
        task_meta = _load_task_context(tid) or {}
        tasks.append(
            {
                "id": tid,
                "name": (task_meta.get("name") or tid).strip() or tid,
                "description": task_meta.get("description", ""),
                "missing": not bool(task_meta),
            }
        )

    # Global Batch History Pagination
    all_history = _list_global_batch_statuses(limit=500)
    page = request.args.get("page", 1, type=int)
    per_page = 10
    total_count = len(all_history)
    total_pages = (total_count + per_page - 1) // per_page
    start = (page - 1) * per_page
    history_slice = all_history[start : start + per_page]

    pagination = {
        "page": page,
        "total_pages": total_pages,
        "total_count": total_count,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }

    return render_template(
        "flows/global_batch.html",
        tasks=tasks,
        batch_id=batch_id,
        batch_status=batch_status,
        global_batches=history_slice,
        pagination=pagination,
    )


@flows_bp.post("/batch/global/run", endpoint="run_global_batch")
def run_global_batch():
    raw_ids = request.form.get("task_ids", "")
    task_ids = _normalize_global_task_ids(raw_ids)
    if not task_ids:
        flash("無效的任務清單。", "danger")
        return redirect(url_for("tasks_bp.tasks"))

    from app.blueprints.tasks.routes import _load_task_context
    valid_task_ids = []
    invalid_task_ids = []
    for tid in task_ids:
        if _load_task_context(tid):
            valid_task_ids.append(tid)
        else:
            invalid_task_ids.append(tid)

    if not valid_task_ids:
        flash("找不到可執行的任務。", "danger")
        return redirect(url_for("tasks_bp.tasks"))
    if invalid_task_ids:
        flash(f"以下任務不存在，已略過：{', '.join(invalid_task_ids)}", "warning")

    batch_id = str(uuid.uuid4())[:8]
    work_id, label = _get_actor_info()

    status = {
        "id": batch_id,
        "status": "queued",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tasks": valid_task_ids,
        "invalid_tasks": invalid_task_ids,
        "current_index": 0,
        "current_task_id": "",
        "current_task_name": "",
        "actor": {"work_id": work_id, "label": label},
        "results": [],
    }
    _write_global_batch_status(batch_id, status)

    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_tasks_batch,
        args=(app, valid_task_ids, batch_id, {"work_id": work_id, "label": label}),
        daemon=True,
    )
    thread.start()

    record_audit(
        action="global_task_batch_queued",
        actor={"work_id": work_id, "label": label},
        detail={"batch_id": batch_id, "tasks": valid_task_ids, "invalid_tasks": invalid_task_ids},
        task_id=None,
    )

    return redirect(url_for("flows_bp.global_batch_page", batch=batch_id))

@flows_bp.get("/batch/global/<batch_id>/status", endpoint="global_batch_status")
def global_batch_status(batch_id):
    status = _load_global_batch_status(batch_id)
    if not status:
        return {"ok": False, "error": "Batch not found"}, 404
    return {"ok": True, "status": status}


@flows_bp.post("/batch/global/<batch_id>/download", endpoint="download_global_batch")
def download_global_batch(batch_id):
    status = _load_global_batch_status(batch_id)
    if not status:
        flash("Batch not found", "warning")
        return redirect(url_for("flows_bp.global_batch_page"))

    kind = (request.form.get("kind") or "docx").strip().lower()
    if kind not in {"docx", "log"}:
        kind = "docx"
    filename = "result.docx" if kind == "docx" else "log.json"

    status_dir = os.path.dirname(_global_batch_status_path(batch_id))
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"global_batch_{batch_id}_{kind}_{stamp}.zip"
    zip_path = os.path.join(status_dir, zip_name)

    import zipfile

    added = 0
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for task_result in status.get("results") or []:
                task_id = (task_result.get("task_id") or "").strip()
                if not task_id:
                    continue
                task_name = (task_result.get("name") or task_id).strip() or task_id
                task_slug = re.sub(r"[^\w\-]+", "_", task_name).strip("_") or task_id
                for flow in task_result.get("flows") or []:
                    if not flow.get("ok"):
                        continue
                    job_id = (flow.get("job_id") or "").strip()
                    flow_name = (flow.get("flow") or "flow").strip() or "flow"
                    flow_slug = re.sub(r"[^\w\-]+", "_", flow_name).strip("_") or "flow"
                    if not job_id:
                        continue
                    job_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", job_id)
                    src = os.path.join(job_dir, filename)
                    if not os.path.exists(src):
                        continue
                    arcname = os.path.join(task_slug, flow_slug, job_id, filename)
                    zf.write(src, arcname=arcname)
                    added += 1
        if added == 0:
            flash("No downloadable files found in this batch", "warning")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return redirect(url_for("flows_bp.global_batch_page", batch=batch_id))
        return send_file(zip_path, as_attachment=True, download_name=zip_name)
    except Exception:
        current_app.logger.exception("Failed to build global batch download zip")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        flash("Failed to prepare batch download", "danger")
        return redirect(url_for("flows_bp.global_batch_page", batch=batch_id))

@flows_bp.get("/tasks/<task_id>/flows/results", endpoint="flow_results")
def flow_results(task_id):
    task_context = _load_task_context(task_id)
    if not task_context:
        abort(404)
    task_context["id"] = task_id
    
    view = (request.args.get("view") or "single").lower()
    if view == "batch":
        return redirect(url_for("flows_bp.global_batch_page"))

    page = max(request.args.get("page", 1, type=int), 1)
    per_page = 10

    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()

    def _date_prefix(text: str) -> str:
        text = (text or "").strip()
        return text[:10] if len(text) >= 10 else ""

    def _match_date(value: str) -> bool:
        d = _date_prefix(value)
        if start_date and (not d or d < start_date):
            return False
        if end_date and (not d or d > end_date):
            return False
        return True

    runs_all = _list_flow_runs(task_id)
    if q:
        q_lower = q.lower()
        runs_all = [
            r
            for r in runs_all
            if q_lower in (r.get("flow_name") or "").lower()
            or q_lower in (r.get("started_at") or "").lower()
            or q_lower in (r.get("job_id") or "").lower()
        ]
    if status:
        runs_all = [r for r in runs_all if (r.get("status") or "").lower() == status]
    if start_date or end_date:
        runs_all = [r for r in runs_all if _match_date(r.get("started_at") or "")]

    total_count = len(runs_all)
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    runs = runs_all[start : start + per_page]
    pagination = {
        "page": page,
        "per_page": per_page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }
    running = [r for r in runs_all if r["status"] in ("running", "queued")]

    return render_template(
        "flows/results.html",
        task=task_context,
        view="single",
        runs=runs,
        batches=[],
        running=running,
        pagination=pagination,
        filters={
            "q": q,
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


@flows_bp.get("/tasks/<task_id>/flows/runs/<job_id>/status", endpoint="flow_run_status")
def flow_run_status(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if not os.path.isdir(job_dir):
        return {"ok": False, "error": "Run not found"}, 404
    meta = _read_job_meta(job_dir)
    status = meta.get("status") or "unknown"
    if _job_has_error(job_dir):
        status = "failed"
    flow_name = meta.get("flow_name") or ""
    return {"ok": True, "status": status, "flow_name": flow_name}


@flows_bp.get("/tasks/<task_id>/flows/runs/active", endpoint="flow_run_active")
def flow_run_active(task_id):
    runs = _list_flow_runs(task_id)
    active = [r for r in runs if r["status"] in ("queued", "running")]
    return {
        "ok": True,
        "runs": [
            {"job_id": r["job_id"], "status": r["status"], "flow_name": r["flow_name"]}
            for r in active
        ],
    }


@flows_bp.get("/api/tasks/<task_id>/flow-files", endpoint="api_flow_list_task_files")
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


@flows_bp.post("/tasks/<task_id>/flows/run", endpoint="run_flow")
def run_flow(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    action = request.form.get("action", "run")
    flow_name = request.form.get("flow_name", "").strip()
    if flow_name:
        name_error = _validate_flow_name(flow_name)
        if name_error:
            return name_error, 400
    ordered_ids = request.form.get("ordered_ids", "").split(",")
    template_file_raw = request.form.get("template_file", "").strip()
    template_file = ""
    if template_file_raw:
        try:
            template_file = _normalize_task_file_rel_path(template_file_raw)
        except ValueError:
            return "模板路徑不合法", 400
        if not template_file:
            return "模板路徑不合法", 400
    document_format = normalize_document_format(request.form.get("document_format"))
    line_spacing_raw = request.form.get("line_spacing")
    line_spacing_value = (line_spacing_raw or f"{DEFAULT_LINE_SPACING:g}").strip()
    line_spacing_none = line_spacing_value.lower() == "none"
    line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_value)
    apply_formatting_param = request.form.get("apply_formatting")
    apply_formatting = parse_bool(apply_formatting_param, DEFAULT_APPLY_FORMATTING)
    if document_format == "none" or line_spacing_none:
        apply_formatting = False
    workflow = []
    for sid in ordered_ids:
        sid = sid.strip()
        if not sid:
            continue
        stype = request.form.get(f"step_{sid}_type", "")
        if not stype or stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k in schema.get("inputs", []):
            field = f"step_{sid}_{k}"
            val = request.form.get(field, "")
            accept = schema.get("accepts", {}).get(k, "text")
            if isinstance(accept, str) and accept.startswith("file"):
                try:
                    params[k] = _normalize_step_file_value(val, accept)
                except ValueError:
                    return f"步驟檔案路徑不合法：{k}", 400
            else:
                params[k] = val
        workflow.append({"type": stype, "params": params})
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    if action == "save" and not flow_name:
        return "缺少流程名稱", 400
    should_save_flow = action == "save" or (action == "run" and bool(flow_name))
    if should_save_flow:
        path = os.path.join(flow_dir, f"{flow_name}.json")
        created = datetime.now().strftime("%Y-%m-%d %H:%M")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and "created" in data:
                    created = data["created"]
            except Exception:
                pass
        data = {
            "created": created,
            "steps": workflow,
            "template_file": template_file,
            "document_format": document_format,
            "line_spacing": line_spacing_value,
            "apply_formatting": apply_formatting,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _touch_task_last_edit(task_id)
        if action == "save":
            fpage = request.form.get("fpage")
            return redirect(url_for("flows_bp.flow_builder", task_id=task_id, fpage=fpage))

    runtime_steps = []
    for step in workflow:
        stype = step["type"]
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        try:
            params = _resolve_runtime_step_params(files_dir, schema, step["params"])
        except (ValueError, FileNotFoundError) as exc:
            return str(exc), 400
        runtime_steps.append({"type": stype, "params": params})

    template_cfg = None
    if template_file:
        try:
            tpl_abs = _resolve_task_file_path(files_dir, template_file, expect_dir=False)
        except (ValueError, FileNotFoundError):
            return "找不到模板檔案，請重新載入", 400
        try:
            template_paragraphs = parse_template_paragraphs(tpl_abs)
        except Exception as e:
            current_app.logger.exception("Failed to parse template for run")
            return f"解析模板失敗: {e}", 400
        template_cfg = {"path": tpl_abs, "paragraphs": template_paragraphs}

    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    _write_job_meta(
        job_dir,
        {
            "flow_name": flow_name or "未命名流程",
            "mode": "single",
            "status": "queued",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    work_id, label = _get_actor_info()
    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_single_job,
        args=(
            app,
            task_id,
            runtime_steps,
            template_cfg,
            document_format,
            line_spacing,
            apply_formatting,
            job_id,
            {"work_id": work_id, "label": label},
            flow_name,
        ),
        daemon=True,
    )
    thread.start()
    record_audit(
        action="flow_run_single",
        actor={"work_id": work_id, "label": label},
        detail={"task_id": task_id, "flow": flow_name, "job_id": job_id},
        task_id=task_id,
    )
    fpage = request.form.get("fpage")
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, job=job_id, fpage=fpage))


@flows_bp.post("/tasks/<task_id>/flows/execute/<flow_name>", endpoint="execute_flow")
def execute_flow(task_id, flow_name):
    """Execute a previously saved flow."""
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    flow_path = os.path.join(tdir, "flows", f"{flow_name}.json")
    if not os.path.exists(flow_path):
        abort(404)
    with open(flow_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = DEFAULT_LINE_SPACING
    apply_formatting = DEFAULT_APPLY_FORMATTING
    template_file = None
    template_cfg = None
    if isinstance(data, dict):
        workflow = data.get("steps", [])
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing_raw = str(data.get("line_spacing", f"{DEFAULT_LINE_SPACING:g}"))
        line_spacing_none = line_spacing_raw.strip().lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_raw)
        template_file = data.get("template_file")
        apply_formatting = parse_bool(data.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
        if document_format == "none" or line_spacing_none:
            apply_formatting = False
    else:
        workflow = data
    override_document_format = request.form.get("document_format")
    override_line_spacing = request.form.get("line_spacing")
    apply_formatting_param = request.form.get("apply_formatting")
    if override_document_format is not None:
        document_format = normalize_document_format(override_document_format)
    line_spacing_none = False
    if override_line_spacing is not None:
        line_spacing_value = (override_line_spacing or f"{DEFAULT_LINE_SPACING:g}").strip()
        line_spacing_none = line_spacing_value.lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_value)
    if apply_formatting_param is not None:
        apply_formatting = parse_bool(apply_formatting_param, DEFAULT_APPLY_FORMATTING)
    if document_format == "none" or line_spacing_none:
        apply_formatting = False
    if template_file:
        try:
            template_file = _normalize_task_file_rel_path(str(template_file))
            tpl_abs = _resolve_task_file_path(files_dir, template_file, expect_dir=False)
            template_paragraphs = parse_template_paragraphs(tpl_abs)
            template_cfg = {"path": tpl_abs, "paragraphs": template_paragraphs}
        except (ValueError, FileNotFoundError):
            template_file = None
        except Exception:
            current_app.logger.exception("Failed to parse template for saved flow")
            template_file = None
    runtime_steps = []
    for step in workflow:
        stype = step.get("type")
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        try:
            params = _resolve_runtime_step_params(files_dir, schema, step.get("params", {}) or {})
        except (ValueError, FileNotFoundError) as exc:
            return str(exc), 400
        runtime_steps.append({"type": stype, "params": params})
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    _write_job_meta(
        job_dir,
        {
            "flow_name": flow_name,
            "mode": "single",
            "source": "manual",
            "status": "queued",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    work_id, label = _get_actor_info()
    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_single_job,
        args=(
            app,
            task_id,
            runtime_steps,
            template_cfg,
            document_format,
            line_spacing,
            apply_formatting,
            job_id,
            {"work_id": work_id, "label": label},
            flow_name,
        ),
        daemon=True,
    )
    thread.start()
    record_audit(
        action="flow_run_single",
        actor={"work_id": work_id, "label": label},
        detail={"task_id": task_id, "flow": flow_name, "job_id": job_id},
        task_id=task_id,
    )
    fpage = request.form.get("fpage")
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, job=job_id, fpage=fpage))


@flows_bp.post("/tasks/<task_id>/flows/update-format/<flow_name>", endpoint="update_flow_format")
def update_flow_format(task_id, flow_name):
    """Update the document formatting metadata for a saved flow."""
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        abort(404)

    document_format = normalize_document_format(request.form.get("document_format"))
    line_spacing_raw = request.form.get("line_spacing")
    line_spacing_value = (line_spacing_raw or f"{DEFAULT_LINE_SPACING:g}").strip()
    line_spacing_none = line_spacing_value.lower() == "none"
    line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_value)
    apply_formatting_param = request.form.get("apply_formatting")
    apply_formatting = parse_bool(apply_formatting_param, DEFAULT_APPLY_FORMATTING)
    if document_format == "none" or line_spacing_none:
        apply_formatting = False

    try:
        with open(flow_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return "流程檔案格式錯誤", 400
    except Exception:
        data = {}

    if isinstance(data, dict):
        payload = data
    elif isinstance(data, list):
        payload = {"steps": data}
    else:
        payload = {"steps": []}

    current_apply = parse_bool(payload.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
    new_apply = apply_formatting if apply_formatting_param is not None else current_apply

    payload["document_format"] = document_format
    payload["line_spacing"] = line_spacing_value
    payload["apply_formatting"] = new_apply
    payload.pop("center_titles", None)

    if "created" not in payload:
        created = datetime.fromtimestamp(os.path.getmtime(flow_path)).strftime("%Y-%m-%d %H:%M")
        payload["created"] = created

    with open(flow_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    _touch_task_last_edit(task_id)
    fpage = request.form.get("fpage")
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, fpage=fpage))


@flows_bp.post("/tasks/<task_id>/flows/delete/<flow_name>", endpoint="delete_flow")
def delete_flow(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    path = os.path.join(flow_dir, f"{flow_name}.json")
    if os.path.exists(path):
        os.remove(path)
        _touch_task_last_edit(task_id)
    fpage = request.form.get("fpage")
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, fpage=fpage))


@flows_bp.post("/tasks/<task_id>/flows/rename/<flow_name>", endpoint="rename_flow")
def rename_flow(task_id, flow_name):
    new_name = request.form.get("name", "").strip()
    name_error = _validate_flow_name(new_name)
    if name_error:
        return name_error, 400
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    old_path = os.path.join(flow_dir, f"{flow_name}.json")
    new_path = os.path.join(flow_dir, f"{new_name}.json")
    if not os.path.exists(old_path):
        abort(404)
    if os.path.exists(new_path):
        return "流程名稱已存在", 400
    os.rename(old_path, new_path)
    _touch_task_last_edit(task_id)
    fpage = request.form.get("fpage")
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, fpage=fpage))


@flows_bp.get("/tasks/<task_id>/flows/export/<flow_name>", endpoint="export_flow")
def export_flow(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    path = os.path.join(tdir, "flows", f"{flow_name}.json")
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=f"{flow_name}.json")


@flows_bp.post("/tasks/<task_id>/flows/import", endpoint="import_flow")
def import_flow(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    f = request.files.get("flow_file")
    if not f or not f.filename.endswith(".json"):
        return "請上傳 JSON 檔", 400
    name = os.path.splitext(secure_filename(f.filename))[0]
    path = os.path.join(flow_dir, f"{name}.json")
    f.save(path)
    _touch_task_last_edit(task_id)
    fpage = request.form.get("fpage")
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, fpage=fpage))
