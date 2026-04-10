from __future__ import annotations

import json
import os
from datetime import datetime
from datetime import timedelta
from typing import Any

from flask import current_app
from sqlalchemy import or_

from app.extensions import db
from app.models.mapping_metadata import (
    MappingJobRecord,
    MappingRunRecord,
    MappingSchemeRecord,
    TaskFileState,
    ensure_schema,
)


def init_mapping_metadata(app) -> None:
    with app.app_context():
        try:
            ensure_schema()
        except Exception:
            db.session.rollback()
            app.logger.exception("Mapping metadata initialization failed")


def _coerce_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _commit() -> None:
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise


def _format_dt(value: datetime | None) -> str:
    if not value:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_date_start(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def _parse_date_end(value: str) -> datetime | None:
    start = _parse_date_start(value)
    if not start:
        return None
    return start + timedelta(days=1)


def get_task_files_revision(task_id: str, create: bool = False) -> int:
    record = db.session.get(TaskFileState, task_id)
    if record:
        return int(record.files_revision or 0)
    if not create:
        return 0
    record = TaskFileState(task_id=task_id, files_revision=0)
    db.session.add(record)
    _commit()
    return 0


def bump_task_files_revision(task_id: str) -> int:
    record = db.session.get(TaskFileState, task_id)
    if not record:
        record = TaskFileState(task_id=task_id, files_revision=0)
        db.session.add(record)
    record.files_revision = int(record.files_revision or 0) + 1
    record.updated_at = datetime.now()
    _commit()
    return int(record.files_revision or 0)


def record_mapping_scheme(task_id: str, payload: dict) -> None:
    scheme_id = str(payload.get("id") or payload.get("scheme_id") or "").strip()
    if not scheme_id:
        return
    record = db.session.get(MappingSchemeRecord, scheme_id) or MappingSchemeRecord(scheme_id=scheme_id)
    record.task_id = task_id
    record.name = str(payload.get("name") or payload.get("display_name") or scheme_id).strip() or scheme_id
    record.mapping_file = str(payload.get("mapping_file") or "").strip() or None
    record.mapping_display_name = str(payload.get("mapping_display_name") or "").strip() or None
    record.source_path = str(payload.get("source_path") or "").strip() or None
    record.reference_ok = bool(payload.get("reference_ok"))
    record.extract_ok = bool(payload.get("extract_ok"))
    record.validated_against_revision = int(
        payload.get("validated_against_revision")
        or payload.get("task_files_revision")
        or payload.get("task_files_updated_at")
        or get_task_files_revision(task_id, create=True)
    )
    record.status_key = str(payload.get("status_key") or "").strip() or _derive_scheme_status(record)
    record.saved_at = _coerce_dt(payload.get("saved_at")) or record.saved_at or datetime.now()
    record.updated_at = _coerce_dt(payload.get("updated_at")) or datetime.now()
    record.actor_work_id = str(payload.get("actor_work_id") or "").strip() or None
    record.actor_label = str(payload.get("actor_label") or "").strip() or None
    db.session.add(record)
    _commit()


def delete_mapping_scheme_record(scheme_id: str) -> None:
    record = db.session.get(MappingSchemeRecord, scheme_id)
    if not record:
        return
    db.session.delete(record)
    _commit()


def record_mapping_run(task_id: str, payload: dict) -> None:
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        return
    record = db.session.get(MappingRunRecord, run_id) or MappingRunRecord(run_id=run_id)
    record.task_id = task_id
    record.scheme_id = str(payload.get("scheme_id") or "").strip() or None
    record.mapping_display_name = str(payload.get("mapping_display_name") or payload.get("mapping_file") or "").strip() or None
    record.status = str(payload.get("status") or "unknown").strip().lower()
    record.output_count = int(payload.get("output_count") or len(payload.get("outputs") or []))
    record.zip_file = str(payload.get("zip_file") or "").strip() or None
    record.log_file = str(payload.get("log_file") or "").strip() or None
    record.error = str(payload.get("error") or "").strip() or None
    record.reference_ok = bool(payload.get("reference_ok"))
    record.extract_ok = bool(payload.get("extract_ok"))
    record.source = str(payload.get("source") or "manual").strip() or "manual"
    record.started_at = _coerce_dt(payload.get("started_at")) or record.started_at or datetime.now()
    record.completed_at = _coerce_dt(payload.get("completed_at"))
    db.session.add(record)
    _commit()


def record_mapping_job(task_id: str, job_id: str, payload: dict) -> None:
    job_id = str(job_id or "").strip()
    if not job_id:
        return
    record = db.session.get(MappingJobRecord, job_id) or MappingJobRecord(job_id=job_id)
    record.task_id = task_id
    record.action = str(payload.get("action") or payload.get("current_action") or "").strip() or None
    record.status = str(payload.get("status") or "queued").strip().lower() or "queued"
    record.mapping_display_name = str(payload.get("mapping_display_name") or "").strip() or None
    record.resume_url = str(payload.get("resume_url") or "").strip() or None
    record.error = str(payload.get("error") or "").strip() or None
    record.payload_json = json.dumps(payload, ensure_ascii=False)
    record.created_at = _coerce_dt(payload.get("created_at")) or record.created_at or datetime.now()
    record.started_at = _coerce_dt(payload.get("started_at"))
    record.completed_at = _coerce_dt(payload.get("completed_at"))
    db.session.add(record)
    _commit()


def _derive_scheme_status(record: MappingSchemeRecord) -> str:
    current_revision = get_task_files_revision(record.task_id, create=True)
    if not record.reference_ok or not record.extract_ok:
        return "error"
    if int(record.validated_against_revision or 0) < int(current_revision or 0):
        return "needs_review"
    return "ready"


def list_mapping_scheme_rows(
    task_id: str,
    page: int = 1,
    per_page: int = 10,
    q: str = "",
    status_key: str = "",
) -> dict:
    query = MappingSchemeRecord.query.filter_by(task_id=task_id)
    q = (q or "").strip()
    status_key = (status_key or "").strip().lower()
    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                MappingSchemeRecord.name.ilike(like),
                MappingSchemeRecord.mapping_display_name.ilike(like),
                MappingSchemeRecord.mapping_file.ilike(like),
            )
        )
    if status_key:
        query = query.filter(MappingSchemeRecord.status_key == status_key)
    query = query.order_by(MappingSchemeRecord.updated_at.desc(), MappingSchemeRecord.scheme_id.desc())
    page = max(int(page or 1), 1)
    per_page = max(int(per_page or 10), 1)
    total_count = query.count()
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = min(page, total_pages)
    rows = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "rows": rows,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        },
    }


def list_mapping_run_rows(
    task_id: str,
    page: int = 1,
    per_page: int = 10,
    q: str = "",
    status: str = "",
    start_date: str = "",
    end_date: str = "",
) -> dict:
    query = MappingRunRecord.query.filter_by(task_id=task_id)
    q = (q or "").strip()
    status = (status or "").strip().lower()
    start_at = _parse_date_start(start_date)
    end_before = _parse_date_end(end_date)
    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                MappingRunRecord.mapping_display_name.ilike(like),
                MappingRunRecord.run_id.ilike(like),
            )
        )
    if status:
        query = query.filter(MappingRunRecord.status == status)
    if start_at:
        query = query.filter(MappingRunRecord.started_at >= start_at)
    if end_before:
        query = query.filter(MappingRunRecord.started_at < end_before)
    query = query.order_by(MappingRunRecord.started_at.desc(), MappingRunRecord.run_id.desc())
    page = max(int(page or 1), 1)
    per_page = max(int(per_page or 10), 1)
    total_count = query.count()
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = min(page, total_pages)
    rows = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "rows": rows,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        },
    }


def list_mapping_scheme_payloads(
    task_id: str,
    page: int = 1,
    per_page: int = 10,
    q: str = "",
    status_key: str = "",
    scheduled_scheme_id: str = "",
    current_revision: int | None = None,
) -> dict:
    result = list_mapping_scheme_rows(
        task_id,
        page=page,
        per_page=per_page,
        q=q,
        status_key=status_key,
    )
    rows = result.get("rows") or []
    items: list[dict] = []
    for row in rows:
        reference_ok = bool(row.reference_ok)
        extract_ok = bool(row.extract_ok)
        computed_status = str(row.status_key or "").strip().lower() or "error"
        if (
            current_revision is not None
            and reference_ok
            and extract_ok
            and int(row.validated_against_revision or 0) < int(current_revision)
        ):
            computed_status = "needs_review"
        if computed_status == "ready":
            status_label = "可執行"
        elif computed_status == "needs_review":
            status_label = "需重檢查"
        else:
            status_label = "有錯誤"

        source_path = str(row.source_path or "").strip()
        source_exists = bool(source_path and os.path.isfile(source_path))
        items.append(
            {
                "id": row.scheme_id,
                "display_name": str(row.name or row.mapping_display_name or row.mapping_file or row.scheme_id),
                "name": str(row.name or ""),
                "mapping_file": str(row.mapping_file or ""),
                "mapping_display_name": str(row.mapping_display_name or row.mapping_file or ""),
                "source_path": source_path,
                "source_exists": source_exists,
                "reference_ok": reference_ok,
                "extract_ok": extract_ok,
                "status_key": computed_status,
                "status_label": status_label,
                "saved_at": _format_dt(row.saved_at),
                "updated_at": _format_dt(row.updated_at),
                "actor_work_id": str(row.actor_work_id or ""),
                "actor_label": str(row.actor_label or ""),
                "is_scheduled": bool(scheduled_scheme_id and row.scheme_id == scheduled_scheme_id),
                "is_runnable": computed_status == "ready" and reference_ok and extract_ok and source_exists,
                "needs_review": computed_status == "needs_review",
            }
        )
    return {
        "items": items,
        "pagination": result.get("pagination") or {
            "page": 1,
            "per_page": per_page,
            "total_count": 0,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
        },
    }


def list_mapping_run_payloads(
    task_id: str,
    page: int = 1,
    per_page: int = 10,
    q: str = "",
    status: str = "",
    start_date: str = "",
    end_date: str = "",
) -> dict:
    result = list_mapping_run_rows(
        task_id,
        page=page,
        per_page=per_page,
        q=q,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )
    rows = result.get("rows") or []
    task_mapping_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "mapping_job")
    items: list[dict] = []
    for row in rows:
        scheme_name = ""
        if row.scheme_id:
            scheme = db.session.get(MappingSchemeRecord, row.scheme_id)
            scheme_name = str((scheme.name if scheme else "") or "").strip()
        zip_name = str(row.zip_file or "").strip()
        log_name = str(row.log_file or "").strip()
        zip_rel = f"{row.run_id}/{zip_name}" if zip_name else ""
        log_rel = f"{row.run_id}/{log_name}" if log_name else ""
        items.append(
            {
                "run_id": row.run_id,
                "mapping_file": str(row.mapping_display_name or "").strip() or "未命名 Mapping",
                "scheme_name": scheme_name,
                "started_at": _format_dt(row.started_at),
                "status": str(row.status or "unknown").strip().lower(),
                "output_count": int(row.output_count or 0),
                "has_zip": bool(zip_name and os.path.isfile(os.path.join(task_mapping_dir, zip_rel))),
                "has_log": bool(log_name and os.path.isfile(os.path.join(task_mapping_dir, log_rel))),
                "zip_file": zip_rel,
                "log_file": log_rel,
                "reference_ok": bool(row.reference_ok),
                "extract_ok": bool(row.extract_ok),
                "source": str(row.source or "").strip() or "manual",
                "error": str(row.error or "").strip(),
            }
        )
    return {
        "runs": items,
        "pagination": result.get("pagination") or {
            "page": 1,
            "per_page": per_page,
            "total_count": 0,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
        },
        "filters": {
            "q": (q or "").strip(),
            "status": (status or "").strip().lower(),
            "start_date": (start_date or "").strip(),
            "end_date": (end_date or "").strip(),
        },
    }


def sync_scheme_payload(task_id: str, payload: dict) -> None:
    try:
        record_mapping_scheme(task_id, payload)
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to sync mapping scheme metadata")


def sync_run_payload(task_id: str, payload: dict) -> None:
    try:
        record_mapping_run(task_id, payload)
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to sync mapping run metadata")


def sync_job_payload(task_id: str, job_id: str, payload: dict) -> None:
    try:
        record_mapping_job(task_id, job_id, payload)
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to sync mapping job metadata")
