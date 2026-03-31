from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Callable

from app.services.flow_service import DEFAULT_APPLY_FORMATTING, DEFAULT_DOCUMENT_FORMAT_KEY, DEFAULT_LINE_SPACING, coerce_line_spacing, normalize_document_format
from app.utils import normalize_docx_output_filename, parse_bool


def build_workflow_from_form(form, supported_steps: dict, normalize_step_file_value: Callable[[str, str], str]) -> list[dict]:
    ordered_ids = form.get("ordered_ids", "").split(",")
    workflow = []
    for step_id in ordered_ids:
        step_id = step_id.strip()
        if not step_id:
            continue
        step_type = form.get(f"step_{step_id}_type", "")
        if not step_type or step_type not in supported_steps:
            continue
        schema = supported_steps.get(step_type, {})
        params = {}
        for key in schema.get("inputs", []):
            field = f"step_{step_id}_{key}"
            value = form.get(field, "")
            accept = schema.get("accepts", {}).get(key, "text")
            if isinstance(accept, str) and accept.startswith("file"):
                params[key] = normalize_step_file_value(value, accept)
            else:
                params[key] = value
        workflow.append({"type": step_type, "params": params})
    return workflow


def load_flow_file(flow_path: str):
    with open(flow_path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def build_flow_payload(
    flow_path: str,
    workflow: list[dict],
    *,
    template_file: str,
    document_format: str,
    line_spacing_value: str,
    apply_formatting: bool,
    output_filename: str,
) -> dict:
    created = datetime.now().strftime("%Y-%m-%d %H:%M")
    if os.path.exists(flow_path):
        try:
            existing_payload = load_flow_file(flow_path)
            if isinstance(existing_payload, dict) and "created" in existing_payload:
                created = existing_payload["created"]
        except Exception:
            pass
    return {
        "created": created,
        "steps": workflow,
        "template_file": template_file,
        "document_format": document_format,
        "line_spacing": line_spacing_value,
        "apply_formatting": apply_formatting,
        "output_filename": output_filename,
    }


def save_flow_payload(flow_path: str, payload: dict) -> None:
    with open(flow_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def load_saved_flow_execution_context(flow_path: str) -> dict:
    data = load_flow_file(flow_path)
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = DEFAULT_LINE_SPACING
    apply_formatting = DEFAULT_APPLY_FORMATTING
    output_filename = ""
    template_file = None

    if isinstance(data, dict):
        workflow = data.get("steps", [])
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing_raw = str(data.get("line_spacing", f"{DEFAULT_LINE_SPACING:g}"))
        line_spacing_none = line_spacing_raw.strip().lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_raw)
        template_file = data.get("template_file")
        apply_formatting = parse_bool(data.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
        output_filename, output_filename_error = normalize_docx_output_filename(data.get("output_filename"), default="")
        if output_filename_error:
            output_filename = ""
        if document_format == "none" or line_spacing_none:
            apply_formatting = False
    else:
        workflow = data

    return {
        "raw_data": data,
        "workflow": workflow,
        "document_format": document_format,
        "line_spacing": line_spacing,
        "apply_formatting": apply_formatting,
        "output_filename": output_filename,
        "template_file": template_file,
    }


def apply_execution_overrides(
    context: dict,
    *,
    document_format_raw: str | None,
    line_spacing_raw: str | None,
    apply_formatting_raw,
) -> dict:
    document_format = context["document_format"]
    line_spacing = context["line_spacing"]
    apply_formatting = context["apply_formatting"]
    line_spacing_none = False

    if document_format_raw is not None:
        document_format = normalize_document_format(document_format_raw)
    if line_spacing_raw is not None:
        line_spacing_value = (line_spacing_raw or f"{DEFAULT_LINE_SPACING:g}").strip()
        line_spacing_none = line_spacing_value.lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_value)
    if apply_formatting_raw is not None:
        apply_formatting = parse_bool(apply_formatting_raw, DEFAULT_APPLY_FORMATTING)
    if document_format == "none" or line_spacing_none:
        apply_formatting = False

    return {
        **context,
        "document_format": document_format,
        "line_spacing": line_spacing,
        "apply_formatting": apply_formatting,
    }


def update_flow_format_payload(flow_path: str, *, document_format: str, line_spacing_value: str, apply_formatting_param) -> dict:
    try:
        data = load_flow_file(flow_path)
    except json.JSONDecodeError:
        raise ValueError("流程檔案格式錯誤")
    except Exception:
        data = {}

    if isinstance(data, dict):
        payload = data
    elif isinstance(data, list):
        payload = {"steps": data}
    else:
        payload = {"steps": []}

    current_apply = parse_bool(payload.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
    new_apply = parse_bool(apply_formatting_param, DEFAULT_APPLY_FORMATTING) if apply_formatting_param is not None else current_apply

    payload["document_format"] = document_format
    payload["line_spacing"] = line_spacing_value
    payload["apply_formatting"] = new_apply
    payload.pop("center_titles", None)

    if "created" not in payload:
        payload["created"] = datetime.fromtimestamp(os.path.getmtime(flow_path)).strftime("%Y-%m-%d %H:%M")

    save_flow_payload(flow_path, payload)
    return payload
