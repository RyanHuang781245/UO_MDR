from __future__ import annotations

import os

from flask import abort, current_app, redirect, request, url_for

from app.services.flow_definition_service import (
    apply_execution_overrides,
    build_flow_payload,
    build_workflow_from_form,
    load_saved_flow_execution_context,
    save_flow_payload,
    update_flow_format_payload,
)
from app.services.flow_service import (
    DEFAULT_APPLY_FORMATTING,
    DEFAULT_ENABLE_FIGURE_REFERENCE,
    DEFAULT_LINE_SPACING,
    SUPPORTED_STEPS,
    coerce_line_spacing,
    normalize_document_format,
    parse_template_paragraphs,
)
from app.services.flow_version_service import has_duplicate_manual_version_name as _has_duplicate_manual_version_name
from app.services.flow_version_service import snapshot_flow_version as _snapshot_flow_version
from app.services.user_context_service import get_actor_info as _get_actor_info
from app.utils import normalize_docx_output_path, parse_bool

from .execution_helpers import _queue_single_flow_job, _resolve_runtime_step_params
from .flow_execution_blueprint import flow_execution_bp
from .flow_file_helpers import _normalize_step_file_value, _normalize_task_file_rel_path, _resolve_task_file_path
from .flow_route_helpers import _touch_task_last_edit
from .flow_validation_helpers import _validate_flow_name


@flow_execution_bp.post("/run", endpoint="run_flow")
def run_flow(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    action = request.form.get("action", "run")
    flow_name = request.form.get("flow_name", "").strip()
    save_as_name = request.form.get("save_as_name", "").strip()
    version_name = request.form.get("version_name", "").strip()
    target_flow_name = save_as_name if action == "save_as" else flow_name
    if flow_name:
        name_error = _validate_flow_name(flow_name)
        if name_error:
            return name_error, 400
    if action == "save_as":
        if not target_flow_name:
            return "缺少另存流程名稱", 400
        name_error = _validate_flow_name(target_flow_name)
        if name_error:
            return name_error, 400
    if action == "save_version":
        if not flow_name:
            return "缺少流程名稱", 400
        if not version_name:
            return "缺少版本名稱", 400
        if len(version_name) > 50:
            return "版本名稱最多 50 字", 400
    output_filename, output_filename_error = normalize_docx_output_path(
        request.form.get("output_filename", ""),
        default="",
    )
    if output_filename_error:
        return output_filename_error, 400
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
    enable_figure_reference = parse_bool(
        request.form.get("enable_figure_reference"),
        DEFAULT_ENABLE_FIGURE_REFERENCE,
    )
    if document_format == "none" or line_spacing_none:
        apply_formatting = False

    try:
        workflow = build_workflow_from_form(request.form, SUPPORTED_STEPS, _normalize_step_file_value)
    except ValueError as exc:
        return f"步驟檔案路徑不合法：{exc}", 400

    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    if action == "save" and not flow_name:
        return "缺少流程名稱", 400
    should_save_flow = action in {"save", "save_as", "save_version"} or (action == "run" and bool(flow_name))
    if should_save_flow:
        path = os.path.join(flow_dir, f"{target_flow_name}.json")
        if action == "save_as" and os.path.exists(path):
            return "流程名稱已存在", 400
        if action == "save_version" and _has_duplicate_manual_version_name(flow_dir, target_flow_name, version_name):
            return "版本名稱已存在", 400
        data = build_flow_payload(
            path,
            workflow,
            template_file=template_file,
            document_format=document_format,
            line_spacing_value=line_spacing_value,
            apply_formatting=apply_formatting,
            enable_figure_reference=enable_figure_reference,
            output_filename=output_filename,
        )
        _work_id, actor_label = _get_actor_info()
        save_flow_payload(path, data)
        if action == "save_version":
            _snapshot_flow_version(
                flow_dir,
                target_flow_name,
                data,
                source="manual_snapshot",
                actor_label=actor_label,
                version_name=version_name,
                force=True,
            )
        _touch_task_last_edit(task_id)
        if action == "save":
            return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, fpage=request.form.get("fpage")))
        if action in {"save_as", "save_version"}:
            return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, flow=target_flow_name))

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
        except Exception as exc:
            current_app.logger.exception("Failed to parse template for run")
            return f"解析模板失敗: {exc}", 400
        template_cfg = {"path": tpl_abs, "paragraphs": template_paragraphs}

    work_id, label = _get_actor_info()
    job_id = _queue_single_flow_job(
        task_id=task_id,
        runtime_steps=runtime_steps,
        template_cfg=template_cfg,
        document_format=document_format,
        line_spacing=line_spacing,
        apply_formatting=apply_formatting,
        enable_figure_reference=enable_figure_reference,
        actor={"work_id": work_id, "label": label},
        flow_name=flow_name,
        output_filename=output_filename,
    )
    return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, job=job_id, fpage=request.form.get("fpage")))


@flow_execution_bp.post("/execute/<flow_name>", endpoint="execute_flow")
def execute_flow(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    flow_path = os.path.join(tdir, "flows", f"{flow_name}.json")
    if not os.path.exists(flow_path):
        abort(404)
    context = load_saved_flow_execution_context(flow_path)
    context = apply_execution_overrides(
        context,
        document_format_raw=request.form.get("document_format"),
        line_spacing_raw=request.form.get("line_spacing"),
        apply_formatting_raw=request.form.get("apply_formatting"),
    )
    workflow = context["workflow"]
    document_format = context["document_format"]
    line_spacing = context["line_spacing"]
    apply_formatting = context["apply_formatting"]
    enable_figure_reference = context["enable_figure_reference"]
    output_filename = context["output_filename"]
    template_file = context["template_file"]
    template_cfg = None

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

    work_id, label = _get_actor_info()
    job_id = _queue_single_flow_job(
        task_id=task_id,
        runtime_steps=runtime_steps,
        template_cfg=template_cfg,
        document_format=document_format,
        line_spacing=line_spacing,
        apply_formatting=apply_formatting,
        enable_figure_reference=enable_figure_reference,
        actor={"work_id": work_id, "label": label},
        flow_name=flow_name,
        output_filename=output_filename,
        source="manual",
    )
    return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, job=job_id, fpage=request.form.get("fpage")))


@flow_execution_bp.post("/update-format/<flow_name>", endpoint="update_flow_format")
def update_flow_format(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        abort(404)

    document_format = normalize_document_format(request.form.get("document_format"))
    line_spacing_raw = request.form.get("line_spacing")
    line_spacing_value = (line_spacing_raw or f"{DEFAULT_LINE_SPACING:g}").strip()
    line_spacing_none = line_spacing_value.lower() == "none"
    _line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_value)
    apply_formatting_param = request.form.get("apply_formatting")
    apply_formatting = parse_bool(apply_formatting_param, DEFAULT_APPLY_FORMATTING)
    if document_format == "none" or line_spacing_none:
        apply_formatting = False

    try:
        update_flow_format_payload(
            flow_path,
            document_format=document_format,
            line_spacing_value=line_spacing_value,
            apply_formatting_param=apply_formatting_param,
        )
    except ValueError as exc:
        return str(exc), 400

    _touch_task_last_edit(task_id)
    return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, fpage=request.form.get("fpage")))
