from __future__ import annotations

import os
import uuid
from datetime import datetime

from flask import current_app

from app.blueprints.flows.flow_route_helpers import _touch_task_last_edit
from app.jobs.store import update_job_meta, write_job_meta
from app.jobs.thread_queue import start_daemon_job
from app.services.audit_service import record_audit
from app.services.flow_service import (
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DOCUMENT_FORMAT_PRESETS,
    SKIP_DOCX_CLEANUP,
    apply_basic_style,
    collect_titles_to_hide,
    hide_paragraphs_with_text,
    remove_hidden_runs,
    run_workflow,
)


def run_single_flow_job(
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
        task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
        job_dir = os.path.join(task_dir, "jobs", job_id)
        try:
            update_job_meta(job_dir, status="running", started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
            result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
            log_entries = workflow_result.get("log_json", []) or []
            has_step_error = any(entry.get("status") == "error" for entry in log_entries)
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
                update_job_meta(
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
                update_job_meta(job_dir, status="completed", completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                record_audit(
                    action="flow_run_single_completed",
                    actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
                    detail={"task_id": task_id, "flow": flow_name, "job_id": job_id, "status": "completed"},
                    task_id=task_id,
                )
        except Exception as exc:
            current_app.logger.exception("Single flow execution failed")
            update_job_meta(
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


def enqueue_single_flow_job(
    task_id: str,
    runtime_steps: list[dict],
    template_cfg: dict | None,
    document_format: str,
    line_spacing: float,
    apply_formatting: bool,
    actor: dict,
    flow_name: str,
    output_filename: str = "",
    source: str = "manual",
) -> str:
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(task_dir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    write_job_meta(
        job_dir,
        {
            "flow_name": flow_name or "未命名流程",
            "mode": "single",
            "source": source,
            "status": "queued",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output_filename": output_filename,
        },
    )
    app = current_app._get_current_object()
    start_daemon_job(
        run_single_flow_job,
        app,
        task_id,
        runtime_steps,
        template_cfg,
        document_format,
        line_spacing,
        apply_formatting,
        job_id,
        actor,
        flow_name,
    )
    record_audit(
        action="flow_run_single",
        actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
        detail={"task_id": task_id, "flow": flow_name, "job_id": job_id},
        task_id=task_id,
    )
    return job_id
