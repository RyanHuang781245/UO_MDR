from __future__ import annotations

import inspect
import os
import uuid
from datetime import datetime

from flask import current_app

from app.blueprints.flows.flow_route_helpers import _touch_task_last_edit
from app.jobs.store import update_job_meta, write_job_meta
from app.services.execution_service import FLOW_SINGLE_JOB, JobCanceledError, enqueue_job, ensure_job_not_canceled
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
from app.services.task_service import build_task_output_path, load_task_context as _load_task_context


def _normalize_output_rel_path(raw_path: str) -> str:
    text = (raw_path or "").strip()
    if text in {"", ".", "/"}:
        return ""
    if os.path.isabs(text) or text.startswith(("/", "\\")):
        raise ValueError("輸出資料夾必須為相對路徑")
    normalized = os.path.normpath(text.replace("\\", "/")).replace("\\", "/")
    if normalized in {"", "."}:
        return ""
    if normalized == ".." or normalized.startswith("../"):
        raise ValueError("輸出資料夾不合法")
    return normalized


def _resolve_flow_output_root(task_id: str) -> str:
    task_meta = _load_task_context(task_id) or {}
    raw_output_root = str(task_meta.get("output_path") or build_task_output_path(task_id)).strip()
    if not raw_output_root:
        return ""
    return os.path.abspath(raw_output_root)


def run_single_flow_job(job_id: str, payload: dict) -> dict:
    task_id = str(payload.get("task_id") or "").strip()
    runtime_steps = list(payload.get("runtime_steps") or [])
    template_cfg = payload.get("template_cfg")
    document_format = str(payload.get("document_format") or DEFAULT_DOCUMENT_FORMAT_KEY)
    line_spacing = float(payload.get("line_spacing") or 1.5)
    apply_formatting = bool(payload.get("apply_formatting"))
    actor = payload.get("actor") or {}
    flow_name = str(payload.get("flow_name") or "").strip() or None
    if not task_id:
        raise RuntimeError("Missing task_id for flow job")

    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(task_dir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    output_root = _resolve_flow_output_root(task_id)
    runtime_steps = []
    copy_step_present = False
    for step in list(payload.get("runtime_steps") or []):
        step_type = str(step.get("type") or "").strip()
        params = dict(step.get("params") or {})
        if step_type in {"copy_files", "copy_directory"}:
            copy_step_present = True
            if not output_root:
                raise RuntimeError("任務尚未設定輸出路徑，無法執行複製步驟")
            rel_dest = _normalize_output_rel_path(str(params.get("dest_dir") or ""))
            dest_dir = os.path.join(output_root, rel_dest.replace("/", os.sep)) if rel_dest else output_root
            os.makedirs(dest_dir, exist_ok=True)
            params["dest_dir"] = dest_dir
        runtime_steps.append({"type": step_type, "params": params})

    def _check_canceled() -> None:
        ensure_job_not_canceled(job_id)

    def _run_workflow_with_cancel() -> dict:
        kwargs = {"workdir": job_dir, "template": template_cfg}
        try:
            if "cancel_check" in inspect.signature(run_workflow).parameters:
                kwargs["cancel_check"] = _check_canceled
        except (TypeError, ValueError):
            pass
        return run_workflow(runtime_steps, **kwargs)

    try:
        update_job_meta(job_dir, status="running", started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        _check_canceled()
        workflow_result = _run_workflow_with_cancel()
        result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
        log_entries = workflow_result.get("log_json", []) or []
        has_step_error = any(entry.get("status") == "error" for entry in log_entries)
        titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
        if apply_formatting and document_format != "none":
            _check_canceled()
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
            _check_canceled()
            remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
            hide_paragraphs_with_text(result_path, titles_to_hide)
        _check_canceled()
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
            raise RuntimeError("Workflow step failed")

        update_job_meta(job_dir, status="completed", completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        record_audit(
            action="flow_run_single_completed",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={"task_id": task_id, "flow": flow_name, "job_id": job_id, "status": "completed"},
            task_id=task_id,
        )
        artifacts = []
        for artifact_type, filename in (("meta_json", "meta.json"), ("log_json", "log.json"), ("result_docx", "result.docx")):
            path = os.path.join(job_dir, filename)
            if os.path.isfile(path):
                artifacts.append(
                    {
                        "artifact_type": artifact_type,
                        "rel_path": os.path.join(task_id, "jobs", job_id, filename).replace("\\", "/"),
                        "size_bytes": os.path.getsize(path),
                    }
                )
        return {
            "artifact_root": os.path.join(task_id, "jobs", job_id).replace("\\", "/"),
            "artifacts": artifacts,
        }
    except JobCanceledError as exc:
        update_job_meta(
            job_dir,
            status="canceled",
            error=str(exc),
            completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        record_audit(
            action="flow_run_single_canceled",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={"task_id": task_id, "flow": flow_name, "job_id": job_id, "status": "canceled", "error": str(exc)},
            task_id=task_id,
        )
        raise
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
        raise


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
    output_root = _resolve_flow_output_root(task_id)
    has_copy_output = any(
        str(step.get("type") or "").strip() in {"copy_files", "copy_directory"}
        for step in (runtime_steps or [])
    )
    write_job_meta(
        job_dir,
        {
            "flow_name": flow_name or "未命名流程",
            "mode": "single",
            "source": source,
            "status": "queued",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output_filename": output_filename,
            "output_root": output_root,
            "has_copy_output": has_copy_output,
        },
    )
    payload = {
        "task_id": task_id,
        "runtime_steps": runtime_steps,
        "template_cfg": template_cfg,
        "document_format": document_format,
        "line_spacing": line_spacing,
        "apply_formatting": apply_formatting,
        "actor": actor,
        "flow_name": flow_name,
        "output_filename": output_filename,
        "source": source,
    }
    job_id = enqueue_job(
        FLOW_SINGLE_JOB,
        payload,
        task_id=task_id,
        target_name=flow_name or "未命名流程",
        actor=actor,
        queue_name="heavy",
        job_id=job_id,
        artifact_root=os.path.join(task_id, "jobs").replace("\\", "/"),
    )
    record_audit(
        action="flow_run_single",
        actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
        detail={"task_id": task_id, "flow": flow_name, "job_id": job_id},
        task_id=task_id,
    )
    return job_id
