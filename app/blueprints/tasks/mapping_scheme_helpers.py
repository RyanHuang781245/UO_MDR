from __future__ import annotations

import inspect
import json
import os
import shutil
import uuid
from datetime import datetime

from flask import current_app

from app.services.execution_service import (
    JobCanceledError,
    MAPPING_SCHEME_RUN_JOB,
    enqueue_job,
    ensure_job_not_canceled,
    find_active_job,
)
from app.services.mapping_metadata_service import sync_run_payload, sync_scheme_payload, delete_mapping_scheme_record


def _task_dir(task_id: str) -> str:
    return os.path.join(current_app.config["TASK_FOLDER"], task_id)


def mapping_schemes_dir(task_id: str) -> str:
    path = os.path.join(_task_dir(task_id), "mappings")
    os.makedirs(path, exist_ok=True)
    return path


def mapping_scheme_dir(task_id: str, scheme_id: str) -> str:
    return os.path.join(mapping_schemes_dir(task_id), scheme_id)


def mapping_scheme_meta_path(task_id: str, scheme_id: str) -> str:
    return os.path.join(mapping_scheme_dir(task_id, scheme_id), "meta.json")


def mapping_schedule_path(task_id: str) -> str:
    return os.path.join(_task_dir(task_id), "mapping_schedule.json")


def write_mapping_run_meta(run_dir: str, payload: dict) -> None:
    try:
        os.makedirs(run_dir, exist_ok=True)
        meta_path = os.path.join(run_dir, "meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        task_id = os.path.basename(os.path.dirname(os.path.dirname(run_dir)))
        sync_run_payload(task_id, payload)
    except Exception:
        current_app.logger.exception("Failed to write mapping run meta")


def task_files_last_updated(task_id: str) -> float:
    files_dir = os.path.join(_task_dir(task_id), "files")
    if not os.path.isdir(files_dir):
        return 0.0

    latest = os.path.getmtime(files_dir)
    for root, dirs, files in os.walk(files_dir):
        for name in dirs:
            try:
                latest = max(latest, os.path.getmtime(os.path.join(root, name)))
            except OSError:
                continue
        for name in files:
            try:
                latest = max(latest, os.path.getmtime(os.path.join(root, name)))
            except OSError:
                continue
    return latest


def _enrich_scheme(task_id: str, payload: dict, current_files_updated_at: float | None = None) -> dict:
    scheme = dict(payload or {})
    scheme_id = str(scheme.get("id") or "").strip()
    scheme_dir = mapping_scheme_dir(task_id, scheme_id) if scheme_id else ""
    source_file = str(scheme.get("source_file") or "source.xlsx").strip() or "source.xlsx"
    if current_files_updated_at is None:
        current_files_updated_at = task_files_last_updated(task_id)
    saved_files_updated_at = float(scheme.get("task_files_updated_at") or 0.0)
    needs_review = saved_files_updated_at > 0 and current_files_updated_at > (saved_files_updated_at + 1e-6)
    reference_ok = bool(scheme.get("reference_ok"))
    extract_ok = bool(scheme.get("extract_ok"))

    if not reference_ok or not extract_ok:
        status_key = "error"
        status_label = "有錯誤"
    elif needs_review:
        status_key = "needs_review"
        status_label = "需重檢查"
    else:
        status_key = "ready"
        status_label = "可執行"

    scheme["task_id"] = task_id
    scheme["source_path"] = os.path.join(scheme_dir, source_file) if scheme_dir else ""
    scheme["source_exists"] = bool(scheme.get("source_path")) and os.path.isfile(scheme["source_path"])
    scheme["current_task_files_updated_at"] = current_files_updated_at
    scheme["needs_review"] = needs_review
    scheme["status_key"] = status_key
    scheme["status_label"] = status_label
    scheme["is_runnable"] = reference_ok and extract_ok and not needs_review and scheme["source_exists"]
    scheme["enable_figure_reference"] = bool(scheme.get("enable_figure_reference", True))
    scheme["display_name"] = (
        str(scheme.get("name") or "").strip()
        or str(scheme.get("mapping_display_name") or "").strip()
        or str(scheme.get("mapping_file") or "").strip()
        or scheme_id
    )
    return scheme


def load_mapping_scheme(task_id: str, scheme_id: str, current_files_updated_at: float | None = None) -> dict | None:
    meta_path = mapping_scheme_meta_path(task_id, scheme_id)
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return None
        return _enrich_scheme(task_id, payload, current_files_updated_at=current_files_updated_at)
    except Exception:
        current_app.logger.exception("Failed to load mapping scheme")
        return None


def list_mapping_schemes(task_id: str) -> list[dict]:
    base_dir = mapping_schemes_dir(task_id)
    current_files_updated_at = task_files_last_updated(task_id)
    results: list[dict] = []
    for name in os.listdir(base_dir):
        scheme_dir = os.path.join(base_dir, name)
        if not os.path.isdir(scheme_dir):
            continue
        scheme = load_mapping_scheme(task_id, name, current_files_updated_at=current_files_updated_at)
        if scheme:
            results.append(scheme)
    results.sort(
        key=lambda item: (
            item.get("updated_at") or item.get("saved_at") or "",
            item.get("display_name") or "",
        ),
        reverse=True,
    )
    return results


def save_mapping_scheme(
    task_id: str,
    source_path: str,
    scheme_name: str,
    validation_state: dict,
    actor: dict | None = None,
) -> dict:
    actor = actor or {}
    scheme_id = uuid.uuid4().hex[:8]
    scheme_dir = mapping_scheme_dir(task_id, scheme_id)
    os.makedirs(scheme_dir, exist_ok=True)

    original_name = os.path.basename(source_path)
    _, ext = os.path.splitext(original_name)
    source_file = f"source{ext or '.xlsx'}"
    target_path = os.path.join(scheme_dir, source_file)
    shutil.copy2(source_path, target_path)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "id": scheme_id,
        "name": (scheme_name or "").strip() or os.path.splitext(original_name)[0] or original_name,
        "mapping_file": str(validation_state.get("mapping_file") or original_name),
        "mapping_display_name": str(validation_state.get("mapping_display_name") or original_name),
        "source_file": source_file,
        "reference_ok": bool(validation_state.get("reference_ok")),
        "extract_ok": bool(validation_state.get("extract_ok")),
        "task_files_updated_at": task_files_last_updated(task_id),
        "saved_at": now,
        "updated_at": now,
        "actor_work_id": actor.get("work_id", ""),
        "actor_label": actor.get("label", ""),
        "enable_figure_reference": bool(validation_state.get("enable_figure_reference", True)),
    }

    with open(mapping_scheme_meta_path(task_id, scheme_id), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    enriched = _enrich_scheme(task_id, payload)
    sync_scheme_payload(task_id, enriched)
    return enriched


def set_scheduled_mapping_scheme(task_id: str, scheme_id: str) -> None:
    payload = {
        "scheme_id": (scheme_id or "").strip(),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(mapping_schedule_path(task_id), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_scheduled_mapping_scheme(task_id: str) -> dict | None:
    path = mapping_schedule_path(task_id)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return None
    except Exception:
        current_app.logger.exception("Failed to load scheduled mapping scheme")
        return None

    scheme_id = str(payload.get("scheme_id") or "").strip()
    if not scheme_id:
        return None
    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme:
        return {"scheme_id": scheme_id, "missing": True}
    scheme["scheduled_updated_at"] = str(payload.get("updated_at") or "")
    scheme["is_scheduled"] = True
    return scheme


def delete_mapping_scheme(task_id: str, scheme_id: str) -> bool:
    scheme_dir = mapping_scheme_dir(task_id, scheme_id)
    if not os.path.isdir(scheme_dir):
        return False

    schedule_path = mapping_schedule_path(task_id)
    if os.path.isfile(schedule_path):
        try:
            with open(schedule_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            scheduled_scheme_id = str((payload or {}).get("scheme_id") or "").strip()
            if scheduled_scheme_id == (scheme_id or "").strip():
                os.remove(schedule_path)
        except Exception:
            current_app.logger.exception("Failed to clear mapping schedule while deleting scheme")

    shutil.rmtree(scheme_dir, ignore_errors=True)
    delete_mapping_scheme_record(scheme_id)
    return not os.path.exists(scheme_dir)


def rename_mapping_scheme(task_id: str, scheme_id: str, new_name: str) -> dict:
    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme:
        raise FileNotFoundError("Mapping scheme not found")

    cleaned_name = str(new_name or "").strip()
    if not cleaned_name:
        raise ValueError("方案名稱不可空白")

    meta_path = mapping_scheme_meta_path(task_id, scheme_id)
    with open(meta_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("Mapping scheme metadata is invalid")

    payload["name"] = cleaned_name
    payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    updated_scheme = load_mapping_scheme(task_id, scheme_id)
    if not updated_scheme:
        raise RuntimeError("Failed to reload mapping scheme after rename")
    sync_scheme_payload(task_id, updated_scheme)
    return updated_scheme


def set_mapping_scheme_figure_reference(task_id: str, scheme_id: str, enabled: bool) -> dict:
    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme:
        raise FileNotFoundError("Mapping scheme not found")

    meta_path = mapping_scheme_meta_path(task_id, scheme_id)
    with open(meta_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("Mapping scheme metadata is invalid")

    payload["enable_figure_reference"] = bool(enabled)
    payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    updated_scheme = load_mapping_scheme(task_id, scheme_id)
    if not updated_scheme:
        raise RuntimeError("Failed to reload mapping scheme after update")
    sync_scheme_payload(task_id, updated_scheme)
    return updated_scheme


def execute_saved_mapping_scheme(
    task_id: str,
    scheme_id: str,
    actor: dict | None = None,
    source: str = "manual",
    global_batch_id: str = "",
    run_id: str = "",
    enable_figure_reference: bool = True,
) -> dict:
    actor = actor or {}
    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme:
        raise FileNotFoundError("Mapping scheme not found")
    if not scheme.get("source_exists"):
        raise FileNotFoundError("Mapping scheme source file not found")
    if scheme.get("needs_review"):
        raise RuntimeError("Mapping scheme requires revalidation")
    if not scheme.get("reference_ok") or not scheme.get("extract_ok"):
        raise RuntimeError("Mapping scheme is not validated")

    from modules.mapping_processor import process_mapping_excel

    task_dir = _task_dir(task_id)
    files_dir = os.path.join(task_dir, "files")
    out_dir = os.path.join(task_dir, "mapping_job")
    run_id = (run_id or "").strip() or uuid.uuid4().hex[:8]
    run_dir = os.path.join(out_dir, run_id)

    def _check_canceled() -> None:
        if run_id:
            ensure_job_not_canceled(run_id)

    process_kwargs = {
        "log_dir": run_dir,
        "validate_only": False,
        "validate_extract_only": False,
        "enable_figure_reference": bool(enable_figure_reference),
    }
    try:
        if "cancel_check" in inspect.signature(process_mapping_excel).parameters:
            process_kwargs["cancel_check"] = _check_canceled
    except (TypeError, ValueError):
        pass

    try:
        result = process_mapping_excel(
            scheme["source_path"],
            files_dir,
            run_dir,
            **process_kwargs,
        )
    except JobCanceledError:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        write_mapping_run_meta(
            run_dir,
            {
                "record_type": "mapping_run",
                "run_id": run_id,
                "mapping_file": scheme.get("mapping_file") or "",
                "mapping_display_name": scheme.get("mapping_display_name") or scheme.get("name") or "",
                "scheme_id": scheme.get("id") or "",
                "scheme_name": scheme.get("name") or "",
                "status": "canceled",
                "started_at": now,
                "completed_at": now,
                "reference_ok": bool(scheme.get("reference_ok")),
                "extract_ok": bool(scheme.get("extract_ok")),
                "outputs": [],
                "output_count": 0,
                "zip_file": "",
                "log_file": "",
                "error": "Canceled during execution",
                "actor_work_id": actor.get("work_id", ""),
                "actor_label": actor.get("label", ""),
                "source": source,
                "global_batch_id": global_batch_id,
                "enable_figure_reference": bool(enable_figure_reference),
            },
        )
        raise

    messages = result.get("logs") or []
    outputs = result.get("outputs") or []
    has_error = any("ERROR" in str(message or "") for message in messages)
    log_file_name = result.get("log_file") or ""
    zip_file_name = result.get("zip_file") or ""
    run_outputs: list[str] = []
    run_prefix = f"{run_id}/"
    for output_path in outputs:
        rel = os.path.relpath(output_path, out_dir).replace("\\", "/")
        run_outputs.append(rel[len(run_prefix):] if rel.startswith(run_prefix) else rel)

    first_error = ""
    for message in messages:
        if "ERROR" in str(message or ""):
            first_error = str(message).strip()
            break

    status = "failed" if has_error else "completed"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_mapping_run_meta(
        run_dir,
        {
            "record_type": "mapping_run",
            "run_id": run_id,
            "mapping_file": scheme.get("mapping_file") or "",
            "mapping_display_name": scheme.get("mapping_display_name") or scheme.get("name") or "",
            "scheme_id": scheme.get("id") or "",
            "scheme_name": scheme.get("name") or "",
            "status": status,
            "started_at": now,
            "completed_at": now,
            "reference_ok": bool(scheme.get("reference_ok")),
            "extract_ok": bool(scheme.get("extract_ok")),
            "outputs": run_outputs,
            "output_count": len(run_outputs),
            "zip_file": zip_file_name,
            "log_file": log_file_name,
            "error": first_error,
            "actor_work_id": actor.get("work_id", ""),
            "actor_label": actor.get("label", ""),
            "source": source,
            "global_batch_id": global_batch_id,
            "enable_figure_reference": bool(enable_figure_reference),
        },
    )

    return {
        "run_id": run_id,
        "status": status,
        "ok": status == "completed",
        "error": first_error,
        "output_count": len(run_outputs),
        "outputs": run_outputs,
        "zip_file": zip_file_name,
        "log_file": log_file_name,
        "zip_relpath": f"{run_id}/{zip_file_name}" if zip_file_name else "",
        "log_relpath": f"{run_id}/{log_file_name}" if log_file_name else "",
        "messages": messages,
    }


def enqueue_saved_mapping_scheme_run(
    task_id: str,
    scheme_id: str,
    actor: dict | None = None,
    source: str = "manual",
    global_batch_id: str = "",
    parent_job_id: str = "",
    job_id: str | None = None,
    enable_figure_reference: bool | None = None,
) -> str:
    actor = actor or {}
    scheme = load_mapping_scheme(task_id, scheme_id)
    if not scheme:
        raise FileNotFoundError("Mapping scheme not found")
    if not scheme.get("source_exists"):
        raise FileNotFoundError("Mapping scheme source file not found")
    if scheme.get("needs_review"):
        raise RuntimeError("Mapping scheme requires revalidation")
    if not scheme.get("reference_ok") or not scheme.get("extract_ok"):
        raise RuntimeError("Mapping scheme is not validated")

    effective_enable_figure_reference = bool(
        scheme.get("enable_figure_reference", True)
        if enable_figure_reference is None
        else enable_figure_reference
    )

    existing = find_active_job(
        MAPPING_SCHEME_RUN_JOB,
        task_id=task_id,
        target_name=scheme.get("display_name") or scheme.get("mapping_display_name") or scheme_id,
        payload_matcher=lambda data: (
            str(data.get("scheme_id") or "").strip() == str(scheme_id or "").strip()
            and bool(data.get("enable_figure_reference", True)) == bool(effective_enable_figure_reference)
        ),
    )
    if existing:
        current_app.logger.info(
            "Deduplicated mapping scheme enqueue request: task_id=%s scheme_id=%s existing_job_id=%s",
            task_id,
            scheme_id,
            existing.job_id,
        )
        return str(existing.job_id)

    return enqueue_job(
        MAPPING_SCHEME_RUN_JOB,
        {
            "task_id": task_id,
            "scheme_id": scheme_id,
            "source": source,
            "global_batch_id": global_batch_id,
            "actor": actor,
            "mapping_display_name": scheme.get("display_name") or scheme.get("mapping_display_name") or "",
            "scheme_name": scheme.get("name") or "",
            "reference_ok": bool(scheme.get("reference_ok")),
            "extract_ok": bool(scheme.get("extract_ok")),
            "enable_figure_reference": effective_enable_figure_reference,
        },
        task_id=task_id,
        target_name=scheme.get("display_name") or scheme.get("mapping_display_name") or scheme_id,
        actor=actor,
        queue_name="heavy",
        parent_job_id=parent_job_id,
        job_id=job_id,
        artifact_root=os.path.join(task_id, "mapping_job").replace("\\", "/"),
    )


def run_saved_mapping_scheme_job(job_id: str, payload: dict) -> dict:
    task_id = str(payload.get("task_id") or "").strip()
    scheme_id = str(payload.get("scheme_id") or "").strip()
    source = str(payload.get("source") or "manual").strip() or "manual"
    global_batch_id = str(payload.get("global_batch_id") or "").strip()
    actor = dict(payload.get("actor") or {})
    enable_figure_reference = bool(payload.get("enable_figure_reference", True))
    if not task_id or not scheme_id:
        raise RuntimeError("Invalid mapping scheme job payload")

    run_result = execute_saved_mapping_scheme(
        task_id,
        scheme_id,
        actor=actor,
        source=source,
        global_batch_id=global_batch_id,
        run_id=job_id,
        enable_figure_reference=enable_figure_reference,
    )

    artifacts = []
    for artifact_type, filename in (("log_json", run_result.get("log_file") or ""), ("result_zip", run_result.get("zip_file") or "")):
        filename = str(filename).strip()
        if not filename:
            continue
        path = os.path.join(_task_dir(task_id), "mapping_job", job_id, filename)
        if os.path.isfile(path):
            artifacts.append(
                {
                    "artifact_type": artifact_type,
                    "rel_path": os.path.join(task_id, "mapping_job", job_id, filename).replace("\\", "/"),
                    "size_bytes": os.path.getsize(path),
                }
            )

    return {
        "artifact_root": os.path.join(task_id, "mapping_job", job_id).replace("\\", "/"),
        "artifacts": artifacts,
        "result_payload": {
            "run_id": run_result.get("run_id") or job_id,
            "mapping_file": payload.get("mapping_display_name") or payload.get("scheme_name") or "",
            "scheme_name": payload.get("scheme_name") or "",
            "status": run_result.get("status") or ("completed" if run_result.get("ok") else "failed"),
            "output_count": int(run_result.get("output_count") or 0),
            "zip_file": run_result.get("zip_file") or "",
            "log_file": run_result.get("log_file") or "",
            "zip_relpath": run_result.get("zip_relpath") or "",
            "log_relpath": run_result.get("log_relpath") or "",
            "reference_ok": bool(payload.get("reference_ok")),
            "extract_ok": bool(payload.get("extract_ok")),
            "source": source,
            "error": run_result.get("error") or "",
            "messages": list(run_result.get("messages") or []),
            "enable_figure_reference": enable_figure_reference,
        },
    }
