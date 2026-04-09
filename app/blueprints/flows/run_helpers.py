from __future__ import annotations

import json
import os
import uuid
from datetime import datetime

from flask import current_app

from app.jobs.store import (
    job_has_error as _job_has_error,
    load_batch_status as _load_batch_status,
    read_job_meta as _read_job_meta,
    update_job_meta as _update_job_meta,
    write_batch_status as _write_batch_status,
    write_job_meta as _write_job_meta,
)
from app.services.audit_service import record_audit
from app.services.flow_service import (
    DEFAULT_APPLY_FORMATTING,
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DEFAULT_LINE_SPACING,
    DOCUMENT_FORMAT_PRESETS,
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
from app.services.flow_version_service import flow_version_count as _flow_version_count
from app.services.notification_service import send_batch_notification
from app.utils import normalize_docx_output_filename, parse_bool

from .flow_file_helpers import _resolve_task_file_path
from .flow_route_helpers import _touch_task_last_edit


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
    output_filename = ""
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
        output_filename, output_filename_error = normalize_docx_output_filename(
            data.get("output_filename"),
            default="",
        )
        if output_filename_error:
            output_filename = ""
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
        params = {}
        for key, value in (step.get("params", {}) or {}).items():
            accept = schema.get("accepts", {}).get(key, "text")
            if isinstance(accept, str) and accept.startswith("file") and value:
                expect_dir = (
                    True if accept.endswith(":dir")
                    else False if accept.endswith(":docx") or accept.endswith(":pdf") or accept.endswith(":zip")
                    else None
                )
                params[key] = _resolve_task_file_path(files_dir, str(value), expect_dir=expect_dir)
            else:
                params[key] = value
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
            "output_filename": output_filename,
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
                status.update({"results": results, "last_error": str(exc)})
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
            flow_name = os.path.splitext(fn)[0]
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
                    isinstance(s, dict) and s.get("type") in {"copy_files", "copy_directory"}
                    for s in steps_data
                )
            except Exception:
                pass
            version_count = _flow_version_count(flow_dir, flow_name)
            flows.append(
                {
                    "name": flow_name,
                    "created": created,
                    "has_copy": has_copy,
                    "version_count": version_count,
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


def _read_mapping_run_meta(run_dir: str) -> dict:
    meta_path = os.path.join(run_dir, "meta.json")
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _list_mapping_runs(task_id: str) -> list[dict]:
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_dir = os.path.join(tdir, "mapping_job")
    if not os.path.isdir(mapping_dir):
        return []

    results = []
    for name in os.listdir(mapping_dir):
        run_dir = os.path.join(mapping_dir, name)
        if not os.path.isdir(run_dir):
            continue
        meta = _read_mapping_run_meta(run_dir)
        if meta.get("record_type") != "mapping_run":
            continue
        started_at = meta.get("started_at")
        if not started_at:
            started_at = datetime.fromtimestamp(os.path.getmtime(run_dir)).strftime("%Y-%m-%d %H:%M:%S")
        zip_name = (meta.get("zip_file") or "").strip()
        log_name = (meta.get("log_file") or "").strip()
        zip_rel = f"{name}/{zip_name}" if zip_name else ""
        log_rel = f"{name}/{log_name}" if log_name else ""
        results.append(
            {
                "run_id": name,
                "mapping_file": (meta.get("mapping_display_name") or meta.get("mapping_file") or "").strip() or "未命名 Mapping",
                "scheme_name": (meta.get("scheme_name") or "").strip(),
                "started_at": started_at,
                "status": (meta.get("status") or "unknown").strip().lower(),
                "output_count": int(meta.get("output_count") or len(meta.get("outputs") or [])),
                "has_zip": bool(zip_name and os.path.isfile(os.path.join(mapping_dir, zip_rel))),
                "has_log": bool(log_name and os.path.isfile(os.path.join(mapping_dir, log_rel))),
                "zip_file": zip_rel,
                "log_file": log_rel,
                "reference_ok": bool(meta.get("reference_ok")),
                "extract_ok": bool(meta.get("extract_ok")),
                "source": (meta.get("source") or "").strip() or "manual",
                "error": (meta.get("error") or "").strip(),
            }
        )
    results.sort(key=lambda r: r["started_at"], reverse=True)
    return results


def list_run_results(
    task_id: str,
    active_tab: str,
    page: int = 1,
    per_page: int = 10,
    q: str = "",
    status: str = "",
    start_date: str = "",
    end_date: str = "",
) -> dict:
    active_tab = "mapping" if active_tab == "mapping" else "flows"
    q = (q or "").strip()
    status = (status or "").strip().lower()
    start_date = (start_date or "").strip()
    end_date = (end_date or "").strip()

    def _date_prefix(text: str) -> str:
        text = (text or "").strip()
        return text[:10] if len(text) >= 10 else ""

    def _match_date(value: str) -> bool:
        date_text = _date_prefix(value)
        if start_date and (not date_text or date_text < start_date):
            return False
        if end_date and (not date_text or date_text > end_date):
            return False
        return True

    flow_runs_all = _list_flow_runs(task_id)
    mapping_runs_all = _list_mapping_runs(task_id)
    runs_all = flow_runs_all if active_tab == "flows" else mapping_runs_all

    if q:
        q_lower = q.lower()
        if active_tab == "flows":
            runs_all = [
                run
                for run in runs_all
                if q_lower in (run.get("flow_name") or "").lower()
                or q_lower in (run.get("started_at") or "").lower()
                or q_lower in (run.get("job_id") or "").lower()
            ]
        else:
            runs_all = [
                run
                for run in runs_all
                if q_lower in (run.get("mapping_file") or "").lower()
                or q_lower in (run.get("started_at") or "").lower()
                or q_lower in (run.get("run_id") or "").lower()
            ]

    if status:
        runs_all = [run for run in runs_all if (run.get("status") or "").lower() == status]

    if start_date or end_date:
        runs_all = [run for run in runs_all if _match_date(run.get("started_at") or "")]

    total_count = len(runs_all)
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * per_page
    runs = runs_all[start : start + per_page]

    return {
        "runs": runs,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        },
        "filters": {
            "q": q,
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
        },
        "tab_counts": {
            "flows": len(flow_runs_all),
            "mapping": len(mapping_runs_all),
        },
        "running": [run for run in flow_runs_all if run["status"] in ("running", "queued")],
    }
