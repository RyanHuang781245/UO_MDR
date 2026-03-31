from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path

from flask import abort, current_app, render_template, request, send_file, send_from_directory
from werkzeug.utils import secure_filename

from app.services.task_service import load_task_context as _load_task_context
from app.services.user_context_service import get_actor_info as _get_actor_info
from .blueprint import tasks_bp

_INVALID_UPLOAD_FILENAME_CHARS = '\\/:*?"<>|'
_WINDOWS_RESERVED_FILE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def _safe_uploaded_filename(filename: str, default_stem: str = "upload") -> str:
    raw_name = os.path.basename((filename or "").replace("\\", "/")).strip()
    secured = secure_filename(raw_name)
    raw_stem, raw_ext = os.path.splitext(raw_name)
    secured_raw_stem = secure_filename(raw_stem) if raw_stem else ""
    if secured and (not raw_stem or secured_raw_stem):
        return secured

    cleaned = "".join(
        "_" if (ord(ch) < 32 or ch in _INVALID_UPLOAD_FILENAME_CHARS) else ch
        for ch in raw_name
    ).strip().strip(".")
    if cleaned in {"", ".", ".."}:
        cleaned = default_stem

    stem, ext = os.path.splitext(cleaned)
    if not stem:
        stem = default_stem
    if stem.upper() in _WINDOWS_RESERVED_FILE_NAMES:
        stem = f"_{stem}"
    return f"{stem}{ext}" if ext else stem

@tasks_bp.route("/tasks/<task_id>/mapping", methods=["GET", "POST"], endpoint="task_mapping")
def task_mapping(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    files_dir = os.path.join(tdir, "files")
    out_dir = os.path.join(tdir, "mapping_job")
    messages = []
    outputs = []
    log_file = None
    zip_file = None
    log_file_name = None
    zip_file_name = None
    step_runs = []
    last_mapping_marker = os.path.join(tdir, "mapping_last.txt")
    validation_state_path = os.path.join(tdir, "mapping_validation_state.json")
    last_mapping_file = None
    validation_state = {
        "mapping_file": "",
        "mapping_display_name": "",
        "reference_ok": False,
        "extract_ok": False,
        "run_id": "",
    }
    current_run_id = None
    current_mapping_display_name = ""

    # 如果是頁面跳轉/重新整理 (GET)，則清掉之前的暫存紀錄與檔案
    if request.method == "GET":
        if os.path.isfile(last_mapping_marker):
            try:
                cached_name = Path(last_mapping_marker).read_text(encoding="utf-8").strip()
                cached_path = os.path.join(tdir, cached_name)
                if cached_name and os.path.isfile(cached_path):
                    os.remove(cached_path)
                os.remove(last_mapping_marker)
            except Exception:
                pass
        try:
            if os.path.isfile(validation_state_path):
                os.remove(validation_state_path)
        except Exception:
            pass
    else:
        # POST 請求時才讀取暫存紀錄
        if os.path.isfile(last_mapping_marker):
            try:
                cached_name = Path(last_mapping_marker).read_text(encoding="utf-8").strip()
                cached_path = os.path.join(tdir, cached_name)
                if cached_name and os.path.isfile(cached_path):
                    last_mapping_file = cached_name
            except Exception:
                last_mapping_file = None
        if os.path.isfile(validation_state_path):
            try:
                loaded_state = json.loads(Path(validation_state_path).read_text(encoding="utf-8"))
                if isinstance(loaded_state, dict):
                    validation_state.update(
                        {
                            "mapping_file": str(loaded_state.get("mapping_file") or ""),
                            "mapping_display_name": str(loaded_state.get("mapping_display_name") or ""),
                            "reference_ok": bool(loaded_state.get("reference_ok")),
                            "extract_ok": bool(loaded_state.get("extract_ok")),
                            "run_id": str(loaded_state.get("run_id") or ""),
                        }
                    )
                    current_mapping_display_name = validation_state.get("mapping_display_name") or ""
            except Exception:
                pass

    def _format_step_label(entry: dict) -> tuple[str, str]:
        stype = entry.get("type") or ""
        params = entry.get("params") or {}

        def _boolish(value, default: bool = False) -> bool:
            if value in (None, ""):
                return default
            return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

        def _base(path: str) -> str:
            if not path: return "?"
            name = os.path.basename(path)
            # 移除 "Section 1_", "Section 2_" 等前綴
            import re
            name = re.sub(r"^Section\s+\d+_", "", name)
            return name

        row_no = params.get("mapping_row")
        row_prefix = f"(Row {row_no}) " if row_no not in (None, "", "None") else ""
        preset_action = (params.get("mapping_action_label") or "").strip()
        preset_detail = (params.get("mapping_detail_label") or "").strip()
        if preset_action:
            return f"{row_prefix}{preset_action}", preset_detail
        if stype == "extract_word_chapter":
            src = _base(params.get("input_file", ""))
            chapter_start = (params.get("target_chapter_section") or "").strip()
            chapter_end = (params.get("explicit_end_number") or "").strip()
            chapter = f"{chapter_start}-{chapter_end}" if chapter_start and chapter_end else chapter_start
            title = (params.get("target_chapter_title") or params.get("target_title_section") or "").strip()
            sub = (params.get("target_subtitle") or params.get("subheading_text") or "").strip()
            
            # 組合章節與標題: "1.1.1 General description"
            main_header = f"{chapter} {title}".strip()
            
            parts = [src]
            if main_header:
                parts.append(main_header)
            if sub:
                parts.append(sub)
            if _boolish(params.get("hide_chapter_title"), default=False):
                parts.append("不含標題")
            
            return f"{row_prefix}擷取章節", " | ".join(parts)
        if stype == "extract_word_all_content":
            src = _base(params.get("input_file", ""))
            return f"{row_prefix}擷取全文", src.strip()
        if stype == "extract_pdf_pages_as_images":
            src = _base(params.get("input_file", ""))
            pages = params.get("pages")
            parts = [src.strip()]
            if pages:
                parts.append(f"pages={pages}")
            return f"{row_prefix}擷取 PDF 圖片", " | ".join(p for p in parts if p)
        if stype == "extract_specific_table_from_word":
            src = _base(params.get("input_file", ""))
            section = (params.get("target_chapter_section") or "").strip()
            title = (params.get("target_chapter_title") or params.get("target_title_section") or "").strip()
            main_header = f"{section} {title}".strip()
            label = (
                params.get("target_caption_label")
                or params.get("target_table_label", "")
                or params.get("target_figure_label", "")
            ).strip()
            table_title = (params.get("target_table_title") or "").strip()
            table_index = str(params.get("target_table_index") or "").strip()
            parts = [src]
            if main_header:
                parts.append(main_header)
            if label:
                parts.append(label)
            if table_title:
                parts.append(f"title={table_title}")
            if table_index:
                parts.append(f"index={table_index}")
            if not _boolish(params.get("include_caption"), default=True):
                parts.append("不含標題")
            return f"{row_prefix}擷取表格", " | ".join(parts)
        if stype == "extract_specific_figure_from_word":
            src = _base(params.get("input_file", ""))
            section = (params.get("target_chapter_section") or "").strip()
            title = (params.get("target_chapter_title") or params.get("target_title_section") or "").strip()
            main_header = f"{section} {title}".strip()
            label = (
                params.get("target_caption_label")
                or params.get("target_figure_label", "")
                or params.get("target_table_label", "")
            ).strip()
            figure_title = (params.get("target_figure_title") or "").strip()
            figure_index = str(params.get("target_figure_index") or "").strip()
            parts = [src]
            if main_header:
                parts.append(main_header)
            if label:
                parts.append(label)
            if figure_title:
                parts.append(f"title={figure_title}")
            if figure_index:
                parts.append(f"index={figure_index}")
            if not _boolish(params.get("include_caption"), default=True):
                parts.append("不含標題")
            return f"{row_prefix}擷取圖片", " | ".join(parts)
        if stype == "insert_text":
            text_val = (params.get("text") or "").strip()
            return f"{row_prefix}插入文字", text_val
        if stype == "copy_file":
            src = _base(params.get("source", ""))
            dest = (params.get("destination") or "").strip().replace("\\", "/")
            target_name = (params.get("target_name") or "").strip()
            parts = [src]
            if target_name:
                parts.append(f"目標名稱={target_name}")
            if dest:
                parts.append(dest)
            return f"{row_prefix}複製檔案", " | ".join(p for p in parts if p)
        if stype == "copy_folder":
            src = _base(params.get("source", ""))
            dest = (params.get("destination") or "").strip().replace("\\", "/")
            target_name = (params.get("target_name") or "").strip()
            parts = [src]
            if target_name:
                parts.append(f"目標名稱={target_name}")
            if dest:
                parts.append(dest)
            return f"{row_prefix}複製資料夾", " | ".join(p for p in parts if p)
        if stype == "template_merge":
            tpl = _base(entry.get("template_file", ""))
            return f"{row_prefix}模版合併", tpl.strip()
        return f"{row_prefix}{stype or '步驟'}", ""

    def _truncate_detail(text: str, limit: int = 160) -> tuple[str, bool]:
        if len(text) <= limit:
            return text, False
        trimmed = text[: max(0, limit - 1)].rstrip()
        return f"{trimmed}…", True
    if request.method == "POST":
        action = request.form.get("action") or "run"
        mapping_path = None
        uploaded_new_mapping = False
        if action == "run_cached":
            if not last_mapping_file:
                messages.append("找不到上次檢查的檔案，請重新上傳。")
            else:
                mapping_path = os.path.join(tdir, last_mapping_file)
        else:
            f = request.files.get("mapping_file")
            if f and f.filename:
                display_name = os.path.basename((f.filename or "").replace("\\", "/")).strip()
                filename = _safe_uploaded_filename(
                    f.filename,
                    default_stem=f"mapping_{uuid.uuid4().hex[:8]}",
                )
                mapping_path = os.path.join(tdir, filename)
                f.save(mapping_path)
                uploaded_new_mapping = True
                current_mapping_display_name = display_name or filename
                try:
                    Path(last_mapping_marker).write_text(filename, encoding="utf-8")
                    last_mapping_file = filename
                    validation_state = {
                        "mapping_file": filename,
                        "mapping_display_name": current_mapping_display_name,
                        "reference_ok": False,
                        "extract_ok": False,
                        "run_id": "",
                    }
                except Exception:
                    pass
            elif last_mapping_file:
                mapping_path = os.path.join(tdir, last_mapping_file)
                current_mapping_display_name = current_mapping_display_name or last_mapping_file
            else:
                messages.append("請選擇檔案")

        if action == "check_extract" and not validation_state.get("reference_ok"):
            messages.append("請先通過檢查引用文件。")
            mapping_path = None
        if action == "run_cached" and not validation_state.get("extract_ok"):
            messages.append("請先通過檢查擷取參數。")
            mapping_path = None
        if mapping_path:
            try:
                from modules.mapping_processor import process_mapping_excel
                existing_run_id = str(validation_state.get("run_id") or "").strip()
                current_run_id = existing_run_id if (existing_run_id and not uploaded_new_mapping) else uuid.uuid4().hex[:8]
                run_out_dir = os.path.join(out_dir, current_run_id)
                result = process_mapping_excel(
                    mapping_path,
                    files_dir,
                    run_out_dir,
                    log_dir=run_out_dir,
                    validate_only=(action == "check"),
                    validate_extract_only=(action == "check_extract"),
                )
                messages = result["logs"]
                outputs = result["outputs"]
                log_file_raw = result.get("log_file")
                zip_file_raw = result.get("zip_file")
                log_file_name = log_file_raw
                zip_file_name = zip_file_raw
                log_file = f"{current_run_id}/{log_file_raw}" if log_file_raw else None
                zip_file = f"{current_run_id}/{zip_file_raw}" if zip_file_raw else None
                current_has_error = any("ERROR" in (m or "") for m in messages)
                current_mapping_name = os.path.basename(mapping_path)
                current_mapping_display_name = current_mapping_display_name or validation_state.get("mapping_display_name") or current_mapping_name
                if action == "check":
                    validation_state = {
                        "mapping_file": current_mapping_name,
                        "mapping_display_name": current_mapping_display_name,
                        "reference_ok": not current_has_error,
                        "extract_ok": False,
                        "run_id": current_run_id,
                    }
                elif action == "check_extract":
                    validation_state = {
                        "mapping_file": current_mapping_name,
                        "mapping_display_name": current_mapping_display_name,
                        "reference_ok": bool(validation_state.get("reference_ok")),
                        "extract_ok": not current_has_error,
                        "run_id": current_run_id,
                    }
                elif action == "run_cached":
                    validation_state = {
                        "mapping_file": current_mapping_name,
                        "mapping_display_name": current_mapping_display_name,
                        "reference_ok": bool(validation_state.get("reference_ok")),
                        "extract_ok": bool(validation_state.get("extract_ok")),
                        "run_id": current_run_id,
                    }
                if action in {"check", "check_extract"}:
                    try:
                        Path(validation_state_path).write_text(
                            json.dumps(validation_state, ensure_ascii=False),
                            encoding="utf-8",
                        )
                    except Exception:
                        pass
                elif action == "run_cached":
                    try:
                        Path(validation_state_path).write_text(
                            json.dumps(validation_state, ensure_ascii=False),
                            encoding="utf-8",
                        )
                    except Exception:
                        pass
            except Exception as e:
                messages = [str(e)]
    if log_file:
        log_candidates = [
            os.path.join(out_dir, log_file),
        ]
        log_path = next((p for p in log_candidates if os.path.isfile(p)), None)
        if log_path:
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    log_data = json.load(f)
                for run in log_data.get("runs", []):
                    for entry in run.get("workflow_log", []):
                        if "step" not in entry:
                            continue
                        action, detail = _format_step_label(entry)
                        row_no = (entry.get("params") or {}).get("mapping_row")
                        detail_short, detail_long = _truncate_detail(detail) if detail else ("", False)
                        step_runs.append(
                            {
                                "action": action,
                                "detail": detail,
                                "detail_short": detail_short,
                                "detail_long": detail_long,
                                "row_no": row_no,
                                "status": entry.get("status") or "ok",
                                "error": entry.get("error") or "",
                            }
                        )
                if step_runs:
                    messages = [m for m in messages if not (m or "").startswith("WF_ERROR:")]
            except Exception as e:
                messages.append(f"ERROR: failed to read log file ({e})")
    has_error = any("ERROR" in (m or "") for m in messages) or any(
        step.get("status") == "error" for step in step_runs
    )
    warning_messages = [m for m in messages if (m or "").startswith("WARN:") or (m or "").startswith("WARNING:")]
    has_warning = bool(warning_messages)
    warning_confirm = None
    if has_warning:
        trimmed = []
        for m in warning_messages[:3]:
            trimmed.append(m.replace("WARN:", "").replace("WARNING:", "").strip())
        warning_confirm = "Warnings found. Run anyway?\n" + "\n".join(trimmed)

    error_messages = [m for m in messages if (m or "").startswith("ERROR:")]
    if error_messages:
        def _norm_error_text(text: str) -> str:
            return re.sub(r"\s+", " ", (text or "").strip())

        existing_row_errors: dict[int | None, set[str]] = {}
        for step in step_runs:
            if step.get("status") != "error":
                continue
            row_no = step.get("row_no")
            bucket = existing_row_errors.setdefault(row_no, set())
            for candidate in (step.get("error"), step.get("detail")):
                normalized = _norm_error_text(str(candidate or ""))
                if normalized:
                    bucket.add(normalized)

        error_steps = []
        for msg in error_messages:
            raw = (msg or "").replace("ERROR:", "", 1).strip()
            raw = re.sub(r"^Row\s+\d+\s*:\s*", "", raw, flags=re.IGNORECASE)
            action = raw
            detail = ""
            error_text = raw
            row_match = re.search(r"Row\s+(\d+)", msg or "", re.IGNORECASE)
            row_prefix = f"(Row {row_match.group(1)}) " if row_match else ""
            if "::" in raw:
                parts = [p.strip() for p in raw.split("::", 2)]
                if len(parts) >= 2:
                    base_action = parts[0] or action
                    if base_action.startswith("(Row "):
                        action = base_action
                    else:
                        action = f"{row_prefix}{base_action}".strip()
                    detail = parts[1]
                if len(parts) == 3:
                    error_text = parts[2]
            elif ":" in raw:
                head, tail = raw.split(":", 1)
                base_action = head.strip() or raw
                if base_action.startswith("(Row "):
                    action = base_action
                else:
                    action = f"{row_prefix}{base_action}".strip()
                detail = tail.strip()
            display_detail = detail or error_text
            parsed_row_no = int(row_match.group(1)) if row_match else None
            norm_error_text = _norm_error_text(error_text)
            norm_display_detail = _norm_error_text(display_detail)
            existing_bucket = existing_row_errors.get(parsed_row_no, set())
            if norm_error_text in existing_bucket or norm_display_detail in existing_bucket:
                continue

            detail_short, detail_long = _truncate_detail(display_detail)
            error_steps.append(
                {
                    "action": action,
                    "detail": display_detail,
                    "detail_short": detail_short,
                    "detail_long": detail_long,
                    "row_no": parsed_row_no,
                    "status": "error",
                    "error": error_text,
                }
            )
        if error_steps:
            step_runs = error_steps + step_runs
        error_messages = []
    if step_runs:
        step_runs = sorted(
            step_runs,
            key=lambda s: (s.get("row_no") is None, s.get("row_no") or 10**9),
        )
        row_has_error = {}
        for step in step_runs:
            row_no = step.get("row_no")
            if row_no is None:
                continue
            if step.get("status") == "error":
                row_has_error[row_no] = True
            else:
                row_has_error.setdefault(row_no, False)
        if row_has_error:
            step_runs = [
                step
                for step in step_runs
                if step.get("row_no") is None
                or not row_has_error.get(step.get("row_no"))
                or step.get("status") == "error"
            ]
    step_ok_count = sum(1 for step in step_runs if step.get("status") != "error")
    step_error_count = sum(1 for step in step_runs if step.get("status") == "error")
    rel_outputs = []
    for p in outputs:
        rel = os.path.relpath(p, out_dir)
        rel_outputs.append(rel.replace("\\", "/"))
    if request.method == "POST" and (request.form.get("action") == "run_cached") and current_run_id:
        run_dir = os.path.join(out_dir, current_run_id)
        run_outputs = []
        run_prefix = f"{current_run_id}/"
        for rel in rel_outputs:
            run_outputs.append(rel[len(run_prefix):] if rel.startswith(run_prefix) else rel)
        first_error = ""
        for step in step_runs:
            if step.get("status") == "error":
                first_error = str(step.get("error") or step.get("detail") or "").strip()
                if first_error:
                    break
        if not first_error:
            for msg in messages:
                if "ERROR" in (msg or ""):
                    first_error = str(msg).strip()
                    break
        status_text = "failed" if has_error else "completed"
        work_id, actor_label = _get_actor_info()
        _write_mapping_run_meta(
            run_dir,
            {
                "record_type": "mapping_run",
                "run_id": current_run_id,
                "mapping_file": validation_state.get("mapping_file") or last_mapping_file or "",
                "mapping_display_name": current_mapping_display_name or validation_state.get("mapping_display_name") or validation_state.get("mapping_file") or last_mapping_file or "",
                "status": status_text,
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reference_ok": bool(validation_state.get("reference_ok")),
                "extract_ok": bool(validation_state.get("extract_ok")),
                "outputs": run_outputs,
                "output_count": len(run_outputs),
                "zip_file": zip_file_name or "",
                "log_file": log_file_name or "",
                "error": first_error,
                "actor_work_id": work_id,
                "actor_label": actor_label,
            },
        )
    return render_template(
        "tasks/mapping.html",
        task_id=task_id,
        task=_load_task_context(task_id),
        messages=messages,
        outputs=rel_outputs,
        log_file=log_file,
        zip_file=zip_file,
        has_error=has_error,
        has_warning=has_warning,
        warning_confirm=warning_confirm,
        step_runs=step_runs,
        step_ok_count=step_ok_count,
        step_error_count=step_error_count,
        error_messages=error_messages,
        last_mapping_file=last_mapping_file,
        current_mapping_display_name=current_mapping_display_name or last_mapping_file,
        allow_check_extract=bool(
            last_mapping_file
            and validation_state.get("mapping_file") == last_mapping_file
            and validation_state.get("reference_ok")
        ),
        allow_direct_run=bool(
            last_mapping_file
            and validation_state.get("mapping_file") == last_mapping_file
            and validation_state.get("extract_ok")
            and request.method == "POST"
            and (request.form.get("action") == "check_extract")
            and not has_error
        ),
    )


@tasks_bp.get("/tasks/<task_id>/mapping/example", endpoint="task_download_mapping_example")
def task_download_mapping_example(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)

    static_dir = current_app.static_folder or ""
    rel_sample = "samples/mapping_example.xlsx"
    sample_path = os.path.join(static_dir, rel_sample)
    checked_paths: list[str] = [sample_path]

    if os.path.isfile(sample_path):
        return send_from_directory(
            static_dir,
            rel_sample,
            as_attachment=True,
            download_name="mapping_example.xlsx",
        )

    project_root = Path(current_app.root_path).parent
    fallback_candidates = [
        project_root / "Mapping.xlsx",
        project_root / "mapping.xlsx",
        project_root / "MAPPING.xlsx",
    ]
    for candidate in fallback_candidates:
        checked_paths.append(str(candidate))
        if candidate.is_file():
            return send_file(
                str(candidate),
                as_attachment=True,
                download_name="mapping_example.xlsx",
            )

    current_app.logger.error(
        "Mapping example file not found. checked_paths=%s",
        checked_paths,
    )
    return (
        "找不到 Mapping 範例檔案。請確認 static/samples/mapping_example.xlsx 或專案根目錄 Mapping.xlsx 是否存在。",
        404,
    )

def _send_mapping_output_file(task_id: str, filename: str):
    safe_name = (filename or "").replace("\\", "/").strip("/")
    if not safe_name:
        abort(404)

    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_job_dir = os.path.join(tdir, "mapping_job")
    legacy_out_dir = os.path.join(current_app.config["OUTPUT_FOLDER"], task_id)

    for base_dir in (mapping_job_dir, legacy_out_dir):
        file_path = os.path.join(base_dir, safe_name)
        if os.path.isfile(file_path):
            return send_from_directory(base_dir, safe_name, as_attachment=True)
    abort(404)


def _write_mapping_run_meta(run_dir: str, payload: dict) -> None:
    try:
        os.makedirs(run_dir, exist_ok=True)
        meta_path = os.path.join(run_dir, "meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        current_app.logger.exception("Failed to write mapping run meta")


@tasks_bp.get("/tasks/<task_id>/output/download", endpoint="task_download_output_query")
def task_download_output_query(task_id):
    return _send_mapping_output_file(task_id, request.args.get("filename", ""))


@tasks_bp.get("/tasks/<task_id>/output/<path:filename>", endpoint="task_download_output")
def task_download_output(task_id, filename):
    return _send_mapping_output_file(task_id, filename)
