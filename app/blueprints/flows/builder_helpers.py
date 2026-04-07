from __future__ import annotations

import json
import os
from datetime import datetime

from flask import current_app, url_for

from app.services.flow_service import (
    DEFAULT_APPLY_FORMATTING,
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DEFAULT_LINE_SPACING,
    DOCUMENT_FORMAT_PRESETS,
    LINE_SPACING_CHOICES,
    SUPPORTED_STEPS,
    normalize_document_format,
    parse_template_paragraphs,
)
from app.services.flow_version_service import (
    FLOW_VERSION_LIMIT,
    flow_version_display_name as _flow_version_display_name,
    flow_version_source_label as _flow_version_source_label,
    latest_restore_backup_context as _latest_restore_backup_context,
    load_flow_version_entry as _load_flow_version_entry,
)
from app.services.task_service import (
    gather_available_files,
    load_task_context as _load_task_context,
)
from app.utils import normalize_docx_output_filename, parse_bool

from .flow_file_helpers import _normalize_task_file_rel_path, _resolve_task_file_path
from .flow_route_helpers import _serialize_restore_backup
from .run_helpers import _load_saved_flows


def _paginate_saved_flows(flows_all: list[dict], page: int, per_page: int = 10) -> tuple[list[dict], dict]:
    total_count = len(flows_all)
    total_pages = (total_count + per_page - 1) // per_page
    start = (page - 1) * per_page
    flows = flows_all[start : start + per_page]
    return flows, {
        "page": page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def _build_preview_version(task_id: str, flow_name: str, version_id: str, version_meta: dict) -> dict:
    created_at = version_meta.get("created_at", "")
    created_display = created_at
    if created_at:
        try:
            created_display = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            created_display = created_at
    resolved_version_id = version_meta.get("id") or version_id
    return {
        "id": resolved_version_id,
        "name": _flow_version_display_name(version_meta.get("name") or "", version_meta.get("source") or ""),
        "source": _flow_version_source_label(version_meta.get("source") or ""),
        "created_at_display": created_display,
        "back_to_flow_url": url_for("flow_builder_bp.flow_builder", task_id=task_id, flow=flow_name),
        "restore_url": url_for(
            "flow_version_bp.restore_flow_version",
            task_id=task_id,
            flow_name=flow_name,
            version_id=resolved_version_id,
        ),
    }


def build_flow_builder_context(task_id: str, args) -> dict:
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(task_dir, "files")
    if not os.path.isdir(files_dir):
        raise FileNotFoundError("Task files not found")

    task_context = _load_task_context(task_id)
    if not task_context:
        raise FileNotFoundError("Task not found")
    task_context["id"] = task_id

    flow_dir = os.path.join(task_dir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    flows_all = _load_saved_flows(flow_dir)

    flow_page = args.get("fpage", 1, type=int)
    flows, flow_pagination = _paginate_saved_flows(flows_all, flow_page)

    preset = None
    template_file = None
    template_paragraphs: list[dict] = []
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = f"{DEFAULT_LINE_SPACING:g}"
    apply_formatting = DEFAULT_APPLY_FORMATTING
    output_filename = ""
    loaded_name = args.get("flow")
    version_id = (args.get("version_id") or "").strip()
    is_version_preview = False
    preview_version = None
    latest_restore_backup = None
    job_id = args.get("job")

    if loaded_name:
        flow_path = os.path.join(flow_dir, f"{loaded_name}.json")
        if version_id:
            loaded_version = _load_flow_version_entry(flow_dir, loaded_name, version_id)
            if not loaded_version:
                raise FileNotFoundError("Flow version not found")
            flow_path, version_meta = loaded_version
            is_version_preview = True
            preview_version = _build_preview_version(task_id, loaded_name, version_id, version_meta)
        elif (args.get("show_restore_notice") or "").strip() == "1":
            latest_restore_backup = _serialize_restore_backup(
                task_id,
                loaded_name,
                _latest_restore_backup_context(flow_dir, loaded_name),
            )

        if os.path.exists(flow_path):
            with open(flow_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                steps_data = data.get("steps", [])
                template_file = data.get("template_file")
                document_format = normalize_document_format(data.get("document_format"))
                line_spacing = str(data.get("line_spacing", f"{DEFAULT_LINE_SPACING:g}"))
                apply_formatting = parse_bool(data.get("apply_formatting"), DEFAULT_APPLY_FORMATTING)
                output_filename, output_filename_error = normalize_docx_output_filename(
                    data.get("output_filename"),
                    default="",
                )
                if output_filename_error:
                    output_filename = ""
            else:
                steps_data = data
            preset = [
                step
                for step in steps_data
                if isinstance(step, dict) and step.get("type") in SUPPORTED_STEPS
            ]

    if template_file:
        try:
            template_file = _normalize_task_file_rel_path(str(template_file))
            template_abs = _resolve_task_file_path(files_dir, template_file, expect_dir=False)
        except (ValueError, FileNotFoundError):
            template_file = None
            template_abs = ""
        if template_abs:
            try:
                template_paragraphs = parse_template_paragraphs(template_abs)
            except Exception:
                template_paragraphs = []
        else:
            template_file = None

    return {
        "task": task_context,
        "steps": SUPPORTED_STEPS,
        "files": gather_available_files(files_dir),
        "flows": flows,
        "preset": preset,
        "loaded_name": loaded_name,
        "job_id": job_id,
        "template_file": template_file,
        "template_paragraphs": template_paragraphs,
        "document_format": document_format,
        "line_spacing": line_spacing,
        "apply_formatting": apply_formatting,
        "output_filename": output_filename,
        "is_version_preview": is_version_preview,
        "preview_version": preview_version,
        "latest_restore_backup": latest_restore_backup,
        "flow_version_limit": FLOW_VERSION_LIMIT,
        "document_format_presets": DOCUMENT_FORMAT_PRESETS,
        "line_spacing_choices": LINE_SPACING_CHOICES,
        "flow_pagination": flow_pagination,
    }
