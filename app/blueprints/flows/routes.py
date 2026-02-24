from __future__ import annotations

import json
import os
import re
import shutil
import threading
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
    center_table_figure_paragraphs,
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

flows_bp = Blueprint("flows_bp", __name__, template_folder="templates")


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
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


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


def _execute_saved_flow(task_id: str, flow_name: str) -> str:
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
        center_titles = data.get("center_titles", True) or any(
            isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in workflow
        )
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
        center_titles = True
    if template_file:
        tpl_abs = os.path.join(files_dir, template_file)
        if os.path.isfile(tpl_abs):
            template_paragraphs = parse_template_paragraphs(tpl_abs)
            template_cfg = {"path": tpl_abs, "paragraphs": template_paragraphs}
    runtime_steps = []
    for step in workflow:
        stype = step.get("type")
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k, v in step.get("params", {}).items():
            accept = schema.get("accepts", {}).get(k, "text")
            if accept.startswith("file") and v:
                params[k] = os.path.join(files_dir, v)
            else:
                params[k] = v
        runtime_steps.append({"type": stype, "params": params})
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    _write_job_meta(
        job_dir,
        {
            "flow_name": flow_name,
            "mode": "batch",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    log_entries = workflow_result.get("log_json", []) or []
    has_step_error = any(e.get("status") == "error" for e in log_entries)
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    if center_titles:
        center_table_figure_paragraphs(result_path)
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
    center_titles: bool,
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
            if center_titles:
                center_table_figure_paragraphs(result_path)
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


def _run_flow_batch(app, task_id: str, flow_sequence: list[str], batch_id: str, actor: dict) -> None:
    with app.app_context():
        status = _load_batch_status(task_id, batch_id) or {}
        status.update({"status": "running", "current_index": 0})
        _write_batch_status(task_id, batch_id, status)
        results = []
        for idx, flow_name in enumerate(flow_sequence, start=1):
            status.update({"current_index": idx, "current_flow": flow_name})
            _write_batch_status(task_id, batch_id, status)
            try:
                job_id = _execute_saved_flow(task_id, flow_name)
                results.append({"flow": flow_name, "job_id": job_id, "status": "completed"})
                _touch_task_last_edit(task_id, work_id=actor.get("work_id"), label=actor.get("label"))
            except Exception as exc:
                results.append({"flow": flow_name, "status": "failed", "error": str(exc)})
                completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                status.update(
                    {
                        "status": "failed",
                        "error": str(exc),
                        "results": results,
                        "completed_at": completed_at,
                    }
                )
                _write_batch_status(task_id, batch_id, status)
                record_audit(
                    action="flow_batch_failed",
                    actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
                    detail={
                        "task_id": task_id,
                        "batch_id": batch_id,
                        "status": "failed",
                        "error": str(exc),
                        "results": results,
                    },
                    task_id=task_id,
                )
                send_batch_notification(
                    task_id=task_id,
                    batch_id=batch_id,
                    status="failed",
                    results=results,
                    actor_work_id=actor.get("work_id", ""),
                    actor_label=actor.get("label", ""),
                    completed_at=completed_at,
                    error=str(exc),
                )
                return
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status.update({"status": "completed", "results": results, "completed_at": completed_at})
        _write_batch_status(task_id, batch_id, status)
        record_audit(
            action="flow_batch_completed",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={
                "task_id": task_id,
                "batch_id": batch_id,
                "status": "completed",
                "count": len(results),
                "results": results,
            },
            task_id=task_id,
        )
        send_batch_notification(
            task_id=task_id,
            batch_id=batch_id,
            status="completed",
            results=results,
            actor_work_id=actor.get("work_id", ""),
            actor_label=actor.get("label", ""),
            completed_at=completed_at,
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


@flows_bp.post("/tasks/<task_id>/flows/batches/delete", endpoint="delete_flow_batches_bulk")
def delete_flow_batches_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    status_dir = os.path.join(tdir, "jobs", "batch")
    raw = request.form.get("batch_ids", "")
    batch_ids = [b.strip() for b in raw.split(",") if b.strip()]
    if not batch_ids:
        flash("請先選取要刪除的批次紀錄。", "warning")
        return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="batch"))
    deleted = 0
    for batch_id in batch_ids:
        path = _batch_status_path(task_id, batch_id)
        if not os.path.exists(path):
            continue
        try:
            status = _load_batch_status(task_id, batch_id) or {}
            results = status.get("results") or []
            for r in results:
                job_id = r.get("job_id")
                if not job_id:
                    continue
                job_dir = os.path.join(jobs_dir, job_id)
                if os.path.isdir(job_dir):
                    try:
                        import shutil
                        shutil.rmtree(job_dir)
                    except Exception:
                        current_app.logger.exception("Failed to delete batch job directory")
            os.remove(path)
            deleted += 1
        except Exception:
            current_app.logger.exception("Failed to delete batch status")
    if deleted:
        flash(f"已刪除 {deleted} 筆批次紀錄。", "success")
    else:
        flash("沒有可刪除的批次紀錄。", "warning")
    return redirect(url_for("flows_bp.flow_results", task_id=task_id, view="batch"))


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
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    flows = _load_saved_flows(flow_dir)
    preset = None
    center_titles = True
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
                center_titles = data.get("center_titles", True) or any(
                    isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in steps_data
                )
                template_file = data.get("template_file")
                document_format = normalize_document_format(data.get("document_format"))
                line_spacing = str(data.get("line_spacing", f"{DEFAULT_LINE_SPACING:g}"))
                apply_formatting = parse_bool(data.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
            else:
                steps_data = data
                center_titles = True
            preset = [
                s for s in steps_data
                if isinstance(s, dict) and s.get("type") in SUPPORTED_STEPS
            ]
    if template_file:
        tpl_abs = os.path.join(files_dir, template_file)
        if os.path.exists(tpl_abs):
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
        task={"id": task_id},
        steps=SUPPORTED_STEPS,
        files=avail,
        flows=flows,
        preset=preset,
        loaded_name=loaded_name,
        job_id=job_id,
        center_titles=center_titles,
        files_tree=tree,
        template_file=template_file,
        template_paragraphs=template_paragraphs,
        document_format=document_format,
        line_spacing=line_spacing,
        apply_formatting=apply_formatting,
        document_format_presets=DOCUMENT_FORMAT_PRESETS,
        line_spacing_choices=LINE_SPACING_CHOICES,
    )


@flows_bp.get("/tasks/<task_id>/flows/batch", endpoint="flow_batch_page")
def flow_batch_page(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    if not os.path.isdir(flow_dir):
        abort(404)
    flows = _load_saved_flows(flow_dir)
    batch_id = request.args.get("batch")
    return render_template(
        "flows/batch.html",
        task={"id": task_id},
        flows=flows,
        batch_id=batch_id,
    )


@flows_bp.get("/tasks/<task_id>/flows/runs", endpoint="flow_runs")
def flow_runs(task_id):
    runs = _list_flow_runs(task_id)
    return render_template(
        "flows/runs.html",
        task={"id": task_id},
        runs=runs,
    )


@flows_bp.get("/tasks/<task_id>/flows/batches", endpoint="flow_batch_list")
def flow_batch_list(task_id):
    batches = _list_batch_statuses(task_id)
    running = [b for b in batches if b["status"] in ("running", "queued")]
    return render_template(
        "flows/batch_list.html",
        task={"id": task_id},
        batches=batches,
        running=running,
    )


@flows_bp.get("/tasks/<task_id>/flows/results", endpoint="flow_results")
def flow_results(task_id):
    view = (request.args.get("view") or "single").lower()
    if view not in ("single", "batch"):
        view = "single"
    runs = _list_flow_runs(task_id)
    batches = _list_batch_statuses(task_id)
    running = [b for b in batches if b["status"] in ("running", "queued")]
    return render_template(
        "flows/results.html",
        task={"id": task_id},
        view=view,
        runs=runs,
        batches=batches,
        running=running,
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
    active = [r for r in runs if r["status"] in ("queued", "running", "failed")]
    return {
        "ok": True,
        "runs": [
            {"job_id": r["job_id"], "status": r["status"], "flow_name": r["flow_name"]}
            for r in active
        ],
    }


@flows_bp.post("/tasks/<task_id>/flows/run", endpoint="run_flow")
def run_flow(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    action = request.form.get("action", "run")
    flow_name = request.form.get("flow_name", "").strip()
    ordered_ids = request.form.get("ordered_ids", "").split(",")
    center_titles = request.form.get("center_titles") == "on"
    template_file = request.form.get("template_file", "").strip()
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
            params[k] = val
        workflow.append({"type": stype, "params": params})
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    if action == "save":
        if not flow_name:
            return "缺少流程名稱", 400
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
            "center_titles": center_titles,
            "template_file": template_file,
            "document_format": document_format,
            "line_spacing": line_spacing_value,
            "apply_formatting": apply_formatting,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _touch_task_last_edit(task_id)
        return redirect(url_for("flows_bp.flow_builder", task_id=task_id))

    runtime_steps = []
    for step in workflow:
        stype = step["type"]
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k, v in step["params"].items():
            accept = schema.get("accepts", {}).get(k, "text")
            if accept.startswith("file") and v:
                params[k] = os.path.join(files_dir, v)
            else:
                params[k] = v
        runtime_steps.append({"type": stype, "params": params})

    template_cfg = None
    if template_file:
        tpl_abs = os.path.join(files_dir, template_file)
        if not os.path.isfile(tpl_abs):
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
            center_titles,
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
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, job=job_id))


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
        center_titles = data.get("center_titles", True) or any(
            isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in workflow
        )
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
        center_titles = True
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
        tpl_abs = os.path.join(files_dir, template_file)
        if os.path.isfile(tpl_abs):
            try:
                template_paragraphs = parse_template_paragraphs(tpl_abs)
                template_cfg = {"path": tpl_abs, "paragraphs": template_paragraphs}
            except Exception as e:
                current_app.logger.exception("Failed to parse template for saved flow")
        else:
            template_file = None
    runtime_steps = []
    for step in workflow:
        stype = step.get("type")
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k, v in step.get("params", {}).items():
            accept = schema.get("accepts", {}).get(k, "text")
            if accept.startswith("file") and v:
                params[k] = os.path.join(files_dir, v)
            else:
                params[k] = v
        runtime_steps.append({"type": stype, "params": params})
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    _write_job_meta(
        job_dir,
        {
            "flow_name": flow_name,
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
            center_titles,
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
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, job=job_id))


@flows_bp.post("/tasks/<task_id>/flows/run-batch", endpoint="run_flow_batch")
def run_flow_batch(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    if not os.path.isdir(flow_dir):
        abort(404)
    sequence_raw = request.form.get("flow_sequence", "").strip()
    if not sequence_raw:
        return "缺少流程順序", 400
    flow_sequence = [s.strip() for s in sequence_raw.split(",") if s.strip()]
    if not flow_sequence:
        return "缺少流程順序", 400
    for name in flow_sequence:
        if not os.path.exists(os.path.join(flow_dir, f"{name}.json")):
            return f"找不到流程：{name}", 404
    batch_id = str(uuid.uuid4())[:8]
    work_id, label = _get_actor_info()
    status = {
        "id": batch_id,
        "status": "queued",
        "flows": flow_sequence,
        "current_index": 0,
        "current_flow": "",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "actor": label or work_id,
    }
    _write_batch_status(task_id, batch_id, status)
    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_flow_batch,
        args=(app, task_id, flow_sequence, batch_id, {"work_id": work_id, "label": label}),
        daemon=True,
    )
    thread.start()
    record_audit(
        action="flow_run_batch",
        actor={"work_id": work_id, "label": label},
        detail={"task_id": task_id, "batch_id": batch_id, "flows": flow_sequence},
        task_id=task_id,
    )
    return redirect(url_for("flows_bp.flow_batch_page", task_id=task_id, batch=batch_id))


@flows_bp.get("/tasks/<task_id>/flows/batch/<batch_id>/status", endpoint="flow_batch_status")
def flow_batch_status(task_id, batch_id):
    status = _load_batch_status(task_id, batch_id)
    if not status:
        return {"ok": False, "error": "Batch not found"}, 404
    return {"ok": True, "status": status}


@flows_bp.get("/tasks/<task_id>/flows/batch/<batch_id>", endpoint="flow_batch_result")
def flow_batch_result(task_id, batch_id):
    status = _load_batch_status(task_id, batch_id)
    if not status:
        abort(404)
    results = status.get("results") or []
    return render_template(
        "flows/batch_result.html",
        task={"id": task_id},
        batch_id=batch_id,
        status=status,
        results=results,
    )


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

    if "created" not in payload:
        created = datetime.fromtimestamp(os.path.getmtime(flow_path)).strftime("%Y-%m-%d %H:%M")
        payload["created"] = created
    payload.setdefault("center_titles", True)

    with open(flow_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    _touch_task_last_edit(task_id)
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id))


@flows_bp.post("/tasks/<task_id>/flows/delete/<flow_name>", endpoint="delete_flow")
def delete_flow(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    path = os.path.join(flow_dir, f"{flow_name}.json")
    if os.path.exists(path):
        os.remove(path)
        _touch_task_last_edit(task_id)
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id))


@flows_bp.post("/tasks/<task_id>/flows/rename/<flow_name>", endpoint="rename_flow")
def rename_flow(task_id, flow_name):
    new_name = request.form.get("name", "").strip()
    if not new_name:
        return "缺少流程名稱", 400
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
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id))


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
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id))
