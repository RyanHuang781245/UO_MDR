from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from typing import Optional

from flask import url_for

from app.utils import parse_bool

SKIP_DOCX_CLEANUP = os.getenv("SKIP_DOCX_CLEANUP", "").strip().lower() in ("1", "true", "yes", "y")

def _optional_dependency_stub(feature: str):
    def _stub(*_args, **_kwargs):
        raise RuntimeError(
            f"{feature} requires optional document-processing dependencies "
            "(e.g. spire.doc / python-docx / PyMuPDF)."
        )

    return _stub


try:
    from modules.workflow import SUPPORTED_STEPS, run_workflow
except Exception:  # optional dependency (spire.doc) may be missing
    SUPPORTED_STEPS = {}
    run_workflow = _optional_dependency_stub("Workflow execution")

try:
    from modules.template_manager import parse_template_paragraphs
except Exception:
    parse_template_paragraphs = _optional_dependency_stub("Template parsing")

try:
    from modules.Extract_AllFile_to_FinalWord import (
        center_table_figure_paragraphs,
        apply_basic_style,
        remove_hidden_runs,
        hide_paragraphs_with_text,
        remove_paragraphs_with_text,
    )
except Exception:  # optional dependencies may be missing
    center_table_figure_paragraphs = _optional_dependency_stub("center_table_figure_paragraphs")
    apply_basic_style = _optional_dependency_stub("apply_basic_style")
    remove_hidden_runs = _optional_dependency_stub("remove_hidden_runs")
    hide_paragraphs_with_text = _optional_dependency_stub("hide_paragraphs_with_text")
    remove_paragraphs_with_text = _optional_dependency_stub("remove_paragraphs_with_text")

try:
    from modules.Edit_Word import renumber_figures_tables_file
except Exception:
    renumber_figures_tables_file = _optional_dependency_stub("renumber_figures_tables_file")

try:
    from modules.translate_with_bedrock import translate_file
except Exception:
    translate_file = _optional_dependency_stub("translate_file")

DOCUMENT_FORMAT_PRESETS = {
    "none": {
        "label": "無（保留原文件格式）",
        "western_font": "",
        "east_asian_font": "",
        "font_size": 0,
    },
    "default": {
        "label": "Times New Roman / 新細明體（12 pt）",
        "western_font": "Times New Roman",
        "east_asian_font": "新細明體",
        "font_size": 12,
        "space_before": 6,
        "space_after": 6,
    },
    "modern": {
        "label": "Calibri / 微軟正黑體（12 pt）",
        "western_font": "Calibri",
        "east_asian_font": "微軟正黑體",
        "font_size": 12,
        "space_before": 6,
        "space_after": 6,
    },
}

DEFAULT_DOCUMENT_FORMAT_KEY = "default"
DEFAULT_LINE_SPACING = 1.5
LINE_SPACING_CHOICES = [
    ("none", "無（保留原行距）"),
    ("1", "單行（1.0）"),
    ("1.15", "1.15 倍行距"),
    ("1.5", "1.5 倍行距"),
    ("2", "雙行（2.0）"),
]
DEFAULT_APPLY_FORMATTING = False

def normalize_document_format(key: str) -> str:
    if not key or key not in DOCUMENT_FORMAT_PRESETS:
        return DEFAULT_DOCUMENT_FORMAT_KEY
    return key

def coerce_line_spacing(value) -> float:
    if isinstance(value, str) and value.strip().lower() == "none":
        return DEFAULT_LINE_SPACING
    try:
        spacing = float(value)
        if spacing <= 0:
            raise ValueError
        return spacing
    except (TypeError, ValueError):
        return DEFAULT_LINE_SPACING

def collect_titles_to_hide(entries):
    titles = []
    seen = set()
    if not isinstance(entries, list):
        return titles
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        captured = entry.get("captured_titles")
        if not captured:
            result_meta = entry.get("result")
            if isinstance(result_meta, dict):
                captured = result_meta.get("captured_titles")
        if not captured:
            continue
        for title in captured:
            if not isinstance(title, str):
                continue
            trimmed = title.strip()
            normalized = " ".join(trimmed.split())
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            titles.append(trimmed)
    return titles

def load_titles_to_hide_from_log(job_dir):
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(log_path):
        return []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            entries = json.load(f)
        return collect_titles_to_hide(entries)
    except Exception:
        return []

def clean_compare_html_content(html_content):
    html_content = re.sub(
        r'<(\w+)[^>]*style="[^"]*display\s*:\s*none[^"]*"[^>]*>.*?</\1>',
        "",
        html_content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    html_content = re.sub(
        r"<p[^>]*>(?:\s|&nbsp;|&#160;)*</p>",
        "",
        html_content,
        flags=re.IGNORECASE,
    )
    return html_content

def save_compare_output(
    job_dir,
    html_content,
    titles_to_hide,
    base_name="result",
    subdir=None,
):
    target_dir = job_dir if not subdir else os.path.join(job_dir, subdir)
    os.makedirs(target_dir, exist_ok=True)
    html_path = os.path.join(target_dir, f"{base_name}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    from spire.doc import Document, FileFormat

    doc = Document()
    doc.LoadFromFile(html_path, FileFormat.Html)
    doc.SaveToFile(os.path.join(target_dir, f"{base_name}.docx"), FileFormat.Docx)
    doc.Close()
    result_docx = os.path.join(target_dir, f"{base_name}.docx")
    if not SKIP_DOCX_CLEANUP:
        remove_hidden_runs(result_docx, preserve_texts=titles_to_hide)
    apply_basic_style(result_docx)
    if not SKIP_DOCX_CLEANUP:
        hide_paragraphs_with_text(result_docx, titles_to_hide)
    return html_path, result_docx

def load_version_metadata(versions_dir):
    metadata = {"versions": []}
    if not os.path.isdir(versions_dir):
        return metadata
    meta_path = os.path.join(versions_dir, "metadata.json")
    if not os.path.exists(meta_path):
        return metadata
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("versions"), list):
            metadata = data
    except Exception:
        metadata = {"versions": []}
    return metadata

def save_version_metadata(versions_dir, metadata):
    os.makedirs(versions_dir, exist_ok=True)
    meta_path = os.path.join(versions_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

def sanitize_version_slug(name):
    if not name:
        return "version"
    slug = re.sub(r"[^\w\-]+", "_", name.strip(), flags=re.UNICODE)
    slug = slug.strip("_")
    if not slug:
        slug = "version"
    return slug[:60]

def build_version_context(task_id, job_id, job_dir):
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    context = []
    versions = metadata.get("versions", [])
    for item in sorted(versions, key=lambda v: v.get("created_at", ""), reverse=True):
        version_id = item.get("id")
        base_name = item.get("base_name")
        if not version_id or not base_name:
            continue
        html_rel = f"versions/{base_name}.html"
        docx_rel = os.path.join(versions_dir, f"{base_name}.docx")
        html_abs = os.path.join(versions_dir, f"{base_name}.html")
        if not os.path.exists(docx_rel) or not os.path.exists(html_abs):
            continue
        created_display = item.get("created_at", "")
        created_at = item.get("created_at")
        if created_at:
            try:
                created_display = datetime.fromisoformat(created_at).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                created_display = created_at
        context.append(
            {
                "id": version_id,
                "name": item.get("name") or version_id,
                "created_at_display": created_display,
                "html_url": url_for("tasks_bp.task_view_file",
                    task_id=task_id,
                    job_id=job_id,
                    filename=html_rel,
                ),
                "docx_url": url_for("tasks_bp.task_download_version",
                    task_id=task_id,
                    job_id=job_id,
                    version_id=version_id,
                ),
                "restore_url": url_for("tasks_bp.task_compare_restore_version",
                    task_id=task_id,
                    job_id=job_id,
                    version_id=version_id,
                ),
                "delete_url": url_for("tasks_bp.task_compare_delete_version",
                    task_id=task_id,
                    job_id=job_id,
                    version_id=version_id,
                ),
            }
        )
    return context
