from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime

from flask import Blueprint, abort, current_app, redirect, render_template, request, send_file, url_for
from flask_login import current_user
from werkzeug.utils import secure_filename

from app.services.flow_service import (
    DEFAULT_APPLY_FORMATTING,
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DEFAULT_LINE_SPACING,
    SKIP_DOCX_CLEANUP,
    SUPPORTED_STEPS,
    center_table_figure_paragraphs,
    collect_titles_to_hide,
    coerce_line_spacing,
    hide_paragraphs_with_text,
    normalize_document_format,
    parse_template_paragraphs,
    remove_hidden_runs,
    run_workflow,
)
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
    template_cfg = None
    if isinstance(data, dict):
        workflow = data.get("steps", [])
        center_titles = data.get("center_titles", True) or any(
            isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in workflow
        )
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing = coerce_line_spacing(data.get("line_spacing", DEFAULT_LINE_SPACING))
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
    workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    if center_titles:
        center_table_figure_paragraphs(result_path)
    if not SKIP_DOCX_CLEANUP:
        remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
        hide_paragraphs_with_text(result_path, titles_to_hide)
    return job_id


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
                results.append({"flow": flow_name, "job_id": job_id, "ok": True})
                _touch_task_last_edit(task_id, work_id=actor.get("work_id"), label=actor.get("label"))
            except Exception as exc:
                results.append({"flow": flow_name, "ok": False, "error": str(exc)})
                status.update({"status": "failed", "error": str(exc), "results": results})
                _write_batch_status(task_id, batch_id, status)
                return
        status.update({"status": "completed", "results": results})
        _write_batch_status(task_id, batch_id, status)


@flows_bp.get("/tasks/<task_id>/flows", endpoint="flow_builder")
def flow_builder(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    flows = []
    for fn in os.listdir(flow_dir):
        if fn.endswith(".json"):
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
    preset = None
    center_titles = True
    template_file = None
    template_paragraphs = []
    loaded_name = request.args.get("flow")
    batch_id = request.args.get("batch")
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
        batch_id=batch_id,
        center_titles=center_titles,
        files_tree=tree,
        template_file=template_file,
        template_paragraphs=template_paragraphs,
    )


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
    workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    # Skip renumber_figures_tables_file to avoid Spire watermark; formatting handled by python-docx helpers.
    # renumber_figures_tables_file(result_path)
    if center_titles:
        center_table_figure_paragraphs(result_path)
    if not SKIP_DOCX_CLEANUP:
        remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
        hide_paragraphs_with_text(result_path, titles_to_hide)
    _touch_task_last_edit(task_id)
    return redirect(url_for("tasks_bp.task_result", task_id=task_id, job_id=job_id))


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
    apply_formatting = False
    template_file = None
    template_cfg = None
    if isinstance(data, dict):
        workflow = data.get("steps", [])
        center_titles = data.get("center_titles", True) or any(
            isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in workflow
        )
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing = coerce_line_spacing(data.get("line_spacing", DEFAULT_LINE_SPACING))
        template_file = data.get("template_file")
    else:
        workflow = data
        center_titles = True
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
    workflow_result = run_workflow(runtime_steps, workdir=job_dir, template=template_cfg)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    # Skip renumber_figures_tables_file to avoid Spire watermark; formatting handled by python-docx helpers.
    # renumber_figures_tables_file(result_path)
    if center_titles:
        center_table_figure_paragraphs(result_path)
    if not SKIP_DOCX_CLEANUP:
        remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
        hide_paragraphs_with_text(result_path, titles_to_hide)
    _touch_task_last_edit(task_id)
    return redirect(url_for("tasks_bp.task_result", task_id=task_id, job_id=job_id))


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
    return redirect(url_for("flows_bp.flow_builder", task_id=task_id, batch=batch_id))


@flows_bp.get("/tasks/<task_id>/flows/batch/<batch_id>/status", endpoint="flow_batch_status")
def flow_batch_status(task_id, batch_id):
    status = _load_batch_status(task_id, batch_id)
    if not status:
        return {"ok": False, "error": "Batch not found"}, 404
    return {"ok": True, "status": status}


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
