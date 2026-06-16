from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Callable

from app.services.flow_service import (
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DEFAULT_ENABLE_FIGURE_REFERENCE,
    DEFAULT_LINE_SPACING_KEY,
    DEFAULT_LINE_SPACING,
    DOCUMENT_FORMAT_PRESETS,
    coerce_line_spacing,
    normalize_document_format,
)
from app.utils import normalize_docx_output_path, parse_bool


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
    enable_figure_reference: bool,
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
        "enable_figure_reference": enable_figure_reference,
        "output_filename": output_filename,
    }


def save_flow_payload(flow_path: str, payload: dict) -> None:
    with open(flow_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def should_apply_formatting(document_format: str, line_spacing_raw: str) -> bool:
    return document_format != "none" or str(line_spacing_raw or "").strip().lower() != "none"


def build_basic_style_kwargs(document_format: str, line_spacing_raw: str, line_spacing: float) -> dict:
    preset = DOCUMENT_FORMAT_PRESETS.get(document_format) or {}
    fallback = DOCUMENT_FORMAT_PRESETS.get(DEFAULT_DOCUMENT_FORMAT_KEY) or {}
    apply_font = document_format != "none"
    apply_spacing = str(line_spacing_raw or "").strip().lower() != "none"
    return {
        "western_font": (preset.get("western_font") or "") if apply_font else None,
        "east_asian_font": (preset.get("east_asian_font") or "") if apply_font else None,
        "font_size": int(preset.get("font_size") or 12) if apply_font else None,
        "line_spacing": line_spacing if apply_spacing else None,
        "space_before": int((preset or fallback).get("space_before") or 6) if apply_spacing else None,
        "space_after": int((preset or fallback).get("space_after") or 6) if apply_spacing else None,
    }


def load_saved_flow_execution_context(flow_path: str) -> dict:
    data = load_flow_file(flow_path)
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing_value = DEFAULT_LINE_SPACING_KEY
    line_spacing = DEFAULT_LINE_SPACING
    apply_formatting = False
    enable_figure_reference = DEFAULT_ENABLE_FIGURE_REFERENCE
    output_filename = ""
    template_file = None

    if isinstance(data, dict):
        workflow = data.get("steps", [])
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing_raw = str(data.get("line_spacing", DEFAULT_LINE_SPACING_KEY))
        line_spacing_value = line_spacing_raw
        line_spacing_none = line_spacing_raw.strip().lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_raw)
        template_file = data.get("template_file")
        apply_formatting = should_apply_formatting(document_format, line_spacing_raw)
        enable_figure_reference = parse_bool(
            data.get("enable_figure_reference"),
            DEFAULT_ENABLE_FIGURE_REFERENCE,
        )
        output_filename, output_filename_error = normalize_docx_output_path(data.get("output_filename"), default="")
        if output_filename_error:
            output_filename = ""
    else:
        workflow = data

    return {
        "raw_data": data,
        "workflow": workflow,
        "document_format": document_format,
        "line_spacing_value": line_spacing_value,
        "line_spacing": line_spacing,
        "apply_formatting": apply_formatting,
        "enable_figure_reference": enable_figure_reference,
        "output_filename": output_filename,
        "template_file": template_file,
    }


def apply_execution_overrides(
    context: dict,
    *,
    document_format_raw: str | None,
    line_spacing_raw: str | None,
) -> dict:
    document_format = context["document_format"]
    line_spacing = context["line_spacing"]
    line_spacing_value = str(context.get("line_spacing_value") or DEFAULT_LINE_SPACING_KEY)

    if document_format_raw is not None:
        document_format = normalize_document_format(document_format_raw)
    if line_spacing_raw is not None:
        line_spacing_value = (line_spacing_raw or DEFAULT_LINE_SPACING_KEY).strip()
        line_spacing_none = line_spacing_value.lower() == "none"
        line_spacing = DEFAULT_LINE_SPACING if line_spacing_none else coerce_line_spacing(line_spacing_value)
    else:
        line_spacing_none = line_spacing_value.lower() == "none"
    apply_formatting = should_apply_formatting(document_format, line_spacing_value)

    return {
        **context,
        "document_format": document_format,
        "line_spacing_value": line_spacing_value,
        "line_spacing": line_spacing,
        "apply_formatting": apply_formatting,
    }


def update_flow_format_payload(flow_path: str, *, document_format: str, line_spacing_value: str) -> dict:
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

    payload["document_format"] = document_format
    payload["line_spacing"] = line_spacing_value
    payload["apply_formatting"] = should_apply_formatting(document_format, line_spacing_value)
    payload.pop("center_titles", None)

    if "created" not in payload:
        payload["created"] = datetime.fromtimestamp(os.path.getmtime(flow_path)).strftime("%Y-%m-%d %H:%M")

    save_flow_payload(flow_path, payload)
    return payload
