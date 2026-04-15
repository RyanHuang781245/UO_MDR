from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

import click
from flask import current_app
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.execution import JobArtifactRecord, JobEventRecord, JobRecord, TaskExecutionLock, ensure_schema

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_CLAIMED = "claimed"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELED = "canceled"
JOB_STATUS_TIMEOUT = "timeout"

FLOW_SINGLE_JOB = "flow_single"
MAPPING_OPERATION_JOB = "mapping_operation"
MAPPING_SCHEME_RUN_JOB = "mapping_scheme_run"
GLOBAL_BATCH_JOB = "global_batch"
GLOBAL_BATCH_ITEM_JOB = "global_batch_item"

WRITE_LOCK_JOB_TYPES = {
    FLOW_SINGLE_JOB,
    MAPPING_OPERATION_JOB,
    MAPPING_SCHEME_RUN_JOB,
}

ACTIVE_JOB_STATUSES = {
    JOB_STATUS_QUEUED,
    JOB_STATUS_CLAIMED,
    JOB_STATUS_RUNNING,
}

ALLOWED_STATUS_TRANSITIONS: dict[str, set[str]] = {
    JOB_STATUS_QUEUED: {JOB_STATUS_CLAIMED, JOB_STATUS_CANCELED},
    JOB_STATUS_CLAIMED: {JOB_STATUS_RUNNING, JOB_STATUS_CANCELED, JOB_STATUS_QUEUED},
    JOB_STATUS_RUNNING: {JOB_STATUS_COMPLETED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED, JOB_STATUS_TIMEOUT, JOB_STATUS_QUEUED},
    JOB_STATUS_COMPLETED: set(),
    JOB_STATUS_FAILED: set(),
    JOB_STATUS_CANCELED: set(),
    JOB_STATUS_TIMEOUT: set(),
}


class JobCanceledError(RuntimeError):
    pass


def init_execution_metadata(app) -> None:
    with app.app_context():
        try:
            ensure_schema()
        except Exception:
            db.session.rollback()
            app.logger.exception("Execution metadata initialization failed")


def register_execution_cli(app) -> None:
    @app.cli.command("jobs-worker")
    @click.option("--once", is_flag=True, help="Process at most one job and exit.")
    @click.option("--max-jobs", default=0, type=int, help="Stop after processing this many jobs. 0 means unlimited.")
    @click.option("--poll-interval", default=None, type=float, help="Override polling interval in seconds.")
    def jobs_worker_command(once: bool, max_jobs: int, poll_interval: float | None) -> None:
        app_obj = current_app._get_current_object()
        processed = run_worker_loop(
            app_obj,
            once=once,
            max_jobs=max_jobs if max_jobs > 0 else None,
            poll_interval=poll_interval,
        )
        click.echo(f"processed_jobs={processed}")

    @app.cli.command("jobs-requeue-stale")
    def jobs_requeue_stale_command() -> None:
        app_obj = current_app._get_current_object()
        count = requeue_stale_jobs(app_obj)
        click.echo(f"requeued_jobs={count}")


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload if payload is not None else {}, ensure_ascii=False)


def _json_loads(payload: str | None) -> dict:
    try:
        data = json.loads(str(payload or "{}"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _utcnow() -> datetime:
    return datetime.now()


def build_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def default_poll_interval() -> float:
    return float(current_app.config.get("JOB_POLL_INTERVAL_SECONDS") or 2.0)


def default_heartbeat_interval() -> float:
    return float(current_app.config.get("JOB_HEARTBEAT_INTERVAL_SECONDS") or 10.0)


def default_lock_ttl_seconds() -> int:
    return int(current_app.config.get("JOB_LOCK_TTL_SECONDS") or 14400)


def stale_after_seconds() -> int:
    return int(current_app.config.get("JOB_STALE_AFTER_SECONDS") or 21600)


def job_executor_mode() -> str:
    return str(current_app.config.get("JOB_EXECUTOR_MODE") or "worker").strip().lower()


def log_job_event(job_id: str, event_type: str, message: str = "", payload: dict | None = None) -> None:
    record = JobEventRecord(
        job_id=job_id,
        event_type=event_type,
        message=message or None,
        payload_json=_json_dumps(payload or {}),
    )
    db.session.add(record)


def _is_status_transition_allowed(from_status: str, to_status: str) -> bool:
    src = str(from_status or "").strip().lower()
    dst = str(to_status or "").strip().lower()
    if not src or src == dst:
        return True
    return dst in ALLOWED_STATUS_TRANSITIONS.get(src, set())


def find_active_job(
    job_type: str,
    *,
    task_id: str = "",
    target_name: str = "",
    payload_matcher: callable | None = None,
) -> JobRecord | None:
    query = JobRecord.query.filter(
        JobRecord.job_type == str(job_type or "").strip(),
        JobRecord.status.in_(list(ACTIVE_JOB_STATUSES)),
    )
    if task_id:
        query = query.filter(JobRecord.task_id == str(task_id).strip())
    if target_name:
        query = query.filter(JobRecord.target_name == str(target_name).strip())
    for row in query.order_by(JobRecord.created_at.desc(), JobRecord.job_id.desc()).all():
        if payload_matcher is None:
            return row
        try:
            if payload_matcher(get_job_payload(row)):
                return row
        except Exception:
            continue
    return None


def enqueue_job(
    job_type: str,
    payload: dict,
    *,
    task_id: str | None = None,
    target_name: str = "",
    actor: dict | None = None,
    priority: int = 100,
    queue_name: str = "default",
    parent_job_id: str = "",
    max_attempts: int = 1,
    job_id: str | None = None,
    artifact_root: str = "",
) -> str:
    actor = actor or {}
    job_id = (job_id or uuid.uuid4().hex[:8]).strip()
    record = JobRecord(
        job_id=job_id,
        parent_job_id=(parent_job_id or "").strip() or None,
        job_type=job_type,
        queue_name=(queue_name or "default").strip() or "default",
        task_id=(task_id or "").strip() or None,
        target_name=(target_name or "").strip() or None,
        status=JOB_STATUS_QUEUED,
        priority=int(priority),
        payload_json=_json_dumps(payload),
        artifact_root=(artifact_root or "").strip() or None,
        max_attempts=max(int(max_attempts or 1), 1),
        created_by_work_id=(actor.get("work_id") or "").strip() or None,
        created_by_label=(actor.get("label") or "").strip() or None,
    )
    db.session.add(record)
    log_job_event(job_id, "queued", f"{job_type} queued", {"queue_name": queue_name, "task_id": task_id or ""})
    db.session.commit()

    if job_executor_mode() == "inline":
        run_job_by_id(current_app._get_current_object(), job_id, worker_id="inline")
    return job_id


def get_job(job_id: str) -> JobRecord | None:
    return db.session.get(JobRecord, str(job_id or "").strip())


def get_job_payload(job: JobRecord | str) -> dict:
    record = get_job(job) if isinstance(job, str) else job
    if not record:
        return {}
    return _json_loads(record.payload_json)


def get_job_result_payload(job: JobRecord | str) -> dict:
    record = get_job(job) if isinstance(job, str) else job
    if not record:
        return {}
    return _json_loads(record.result_json)


def is_job_cancel_requested(job_id: str) -> bool:
    record = (
        JobRecord.query.with_entities(JobRecord.cancel_requested_at, JobRecord.status)
        .filter_by(job_id=str(job_id or "").strip())
        .first()
    )
    if not record:
        return False
    cancel_requested_at, status = record
    return bool(cancel_requested_at) or status == JOB_STATUS_CANCELED


def ensure_job_not_canceled(job_id: str, message: str = "Job canceled during execution") -> None:
    if is_job_cancel_requested(job_id):
        raise JobCanceledError(message)


def set_job_artifact_root(job_id: str, artifact_root: str) -> None:
    record = get_job(job_id)
    if not record:
        return
    record.artifact_root = (artifact_root or "").strip() or None
    db.session.add(record)
    db.session.commit()


def set_job_result_payload(job_id: str, payload: dict | None) -> None:
    record = get_job(job_id)
    if not record:
        return
    record.result_json = _json_dumps(payload or {})
    db.session.add(record)
    db.session.commit()


def update_job_status(
    job_id: str,
    status: str,
    *,
    worker_id: str = "",
    error_summary: str = "",
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    heartbeat_at: datetime | None = None,
) -> None:
    record = get_job(job_id)
    if not record:
        return
    if not _is_status_transition_allowed(str(record.status or ""), status):
        current_app.logger.warning(
            "Ignored invalid job status transition: job_id=%s %s->%s",
            job_id,
            record.status,
            status,
        )
        return
    record.status = status
    if worker_id:
        record.worker_id = worker_id
    if error_summary or status in {JOB_STATUS_FAILED, JOB_STATUS_TIMEOUT, JOB_STATUS_CANCELED}:
        record.error_summary = (error_summary or "").strip() or None
    if started_at is not None:
        record.started_at = started_at
    if completed_at is not None:
        record.completed_at = completed_at
    if heartbeat_at is not None:
        record.heartbeat_at = heartbeat_at
    db.session.add(record)
    db.session.commit()


def touch_job_heartbeat(job_id: str) -> None:
    record = get_job(job_id)
    if not record:
        return
    if record.status not in {JOB_STATUS_CLAIMED, JOB_STATUS_RUNNING}:
        return
    record.heartbeat_at = _utcnow()
    db.session.add(record)
    db.session.commit()


def record_job_artifact(job_id: str, artifact_type: str, rel_path: str, size_bytes: int | None = None) -> None:
    rel_path = str(rel_path or "").replace("\\", "/").strip("/")
    if not rel_path:
        return
    existing = JobArtifactRecord.query.filter_by(job_id=job_id, artifact_type=artifact_type, rel_path=rel_path).first()
    if existing:
        existing.size_bytes = size_bytes
        db.session.add(existing)
    else:
        db.session.add(
            JobArtifactRecord(
                job_id=job_id,
                artifact_type=artifact_type,
                rel_path=rel_path,
                size_bytes=size_bytes,
            )
        )
    db.session.commit()


def delete_job_record(job_id: str) -> None:
    record = get_job(job_id)
    if not record:
        return
    JobArtifactRecord.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    JobEventRecord.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    db.session.delete(record)
    db.session.commit()


def acquire_task_lock(task_id: str, job_id: str, ttl_seconds: int | None = None) -> bool:
    task_id = str(task_id or "").strip()
    job_id = str(job_id or "").strip()
    if not task_id or not job_id:
        return True
    ttl_seconds = int(ttl_seconds or default_lock_ttl_seconds())
    now = _utcnow()
    expires_at = now + timedelta(seconds=ttl_seconds)

    record = db.session.get(TaskExecutionLock, task_id)
    if record:
        if record.job_id == job_id:
            record.expires_at = expires_at
            db.session.add(record)
            db.session.commit()
            return True
        if record.expires_at and record.expires_at > now:
            return False
        db.session.delete(record)
        db.session.commit()

    try:
        db.session.add(
            TaskExecutionLock(
                task_id=task_id,
                job_id=job_id,
                lock_type="write",
                acquired_at=now,
                expires_at=expires_at,
            )
        )
        db.session.commit()
        return True
    except IntegrityError:
        db.session.rollback()
        return False


def release_task_lock(task_id: str, job_id: str) -> None:
    task_id = str(task_id or "").strip()
    if not task_id:
        return
    record = db.session.get(TaskExecutionLock, task_id)
    if record and record.job_id == str(job_id or "").strip():
        db.session.delete(record)
        db.session.commit()


def _claim_job_record(job_id: str, worker_id: str) -> JobRecord | None:
    now = _utcnow()
    updated = (
        JobRecord.query.filter_by(job_id=job_id, status=JOB_STATUS_QUEUED)
        .update(
            {
                "status": JOB_STATUS_CLAIMED,
                "worker_id": worker_id,
                "claimed_at": now,
                "heartbeat_at": now,
                "attempt_count": JobRecord.attempt_count + 1,
            },
            synchronize_session=False,
        )
    )
    if not updated:
        db.session.rollback()
        return None
    db.session.commit()
    record = get_job(job_id)
    if record:
        log_job_event(job_id, "claimed", f"claimed by {worker_id}", {"worker_id": worker_id})
        db.session.commit()
    return record


def claim_next_job(worker_id: str, queue_names: list[str] | None = None) -> JobRecord | None:
    queue_names = [q.strip() for q in (queue_names or []) if q and q.strip()]
    query = JobRecord.query.filter(JobRecord.status == JOB_STATUS_QUEUED)
    if queue_names:
        query = query.filter(JobRecord.queue_name.in_(queue_names))
    candidate_ids = [
        item.job_id
        for item in query.order_by(JobRecord.priority.desc(), JobRecord.created_at.asc()).limit(20).all()
    ]
    for job_id in candidate_ids:
        record = _claim_job_record(job_id, worker_id)
        if record:
            return record
    return None


def defer_job(job_id: str, message: str) -> None:
    record = get_job(job_id)
    if not record:
        return
    if not _is_status_transition_allowed(str(record.status or ""), JOB_STATUS_QUEUED):
        current_app.logger.warning(
            "Ignored invalid defer status transition: job_id=%s %s->%s",
            job_id,
            record.status,
            JOB_STATUS_QUEUED,
        )
        return
    record.status = JOB_STATUS_QUEUED
    record.worker_id = None
    record.claimed_at = None
    record.heartbeat_at = None
    record.cancel_requested_at = None
    record.cancel_reason = None
    db.session.add(record)
    log_job_event(job_id, "deferred", message)
    db.session.commit()


def mark_job_running(job_id: str, worker_id: str) -> None:
    now = _utcnow()
    record = get_job(job_id)
    if not record:
        return
    if not _is_status_transition_allowed(str(record.status or ""), JOB_STATUS_RUNNING):
        current_app.logger.warning(
            "Ignored invalid running status transition: job_id=%s %s->%s",
            job_id,
            record.status,
            JOB_STATUS_RUNNING,
        )
        return
    record.status = JOB_STATUS_RUNNING
    record.worker_id = worker_id
    record.started_at = record.started_at or now
    record.heartbeat_at = now
    db.session.add(record)
    log_job_event(job_id, "running", f"running on {worker_id}", {"worker_id": worker_id})
    db.session.commit()


def mark_job_completed(job_id: str) -> None:
    now = _utcnow()
    record = get_job(job_id)
    if not record:
        return
    if not _is_status_transition_allowed(str(record.status or ""), JOB_STATUS_COMPLETED):
        current_app.logger.warning(
            "Ignored invalid completed status transition: job_id=%s %s->%s",
            job_id,
            record.status,
            JOB_STATUS_COMPLETED,
        )
        return
    record.status = JOB_STATUS_COMPLETED
    record.completed_at = now
    record.heartbeat_at = now
    record.cancel_requested_at = None
    record.cancel_reason = None
    db.session.add(record)
    log_job_event(job_id, "completed", "job completed")
    db.session.commit()


def mark_job_failed(job_id: str, error: str) -> None:
    now = _utcnow()
    record = get_job(job_id)
    if not record:
        return
    if not _is_status_transition_allowed(str(record.status or ""), JOB_STATUS_FAILED):
        current_app.logger.warning(
            "Ignored invalid failed status transition: job_id=%s %s->%s",
            job_id,
            record.status,
            JOB_STATUS_FAILED,
        )
        return
    record.status = JOB_STATUS_FAILED
    record.error_summary = (error or "").strip() or None
    record.completed_at = now
    record.heartbeat_at = now
    record.cancel_requested_at = None
    record.cancel_reason = None
    db.session.add(record)
    log_job_event(job_id, "failed", error or "job failed")
    db.session.commit()


def mark_job_canceled(job_id: str, message: str = "Job canceled") -> None:
    now = _utcnow()
    record = get_job(job_id)
    if not record:
        return
    if not _is_status_transition_allowed(str(record.status or ""), JOB_STATUS_CANCELED):
        current_app.logger.warning(
            "Ignored invalid canceled status transition: job_id=%s %s->%s",
            job_id,
            record.status,
            JOB_STATUS_CANCELED,
        )
        return
    record.status = JOB_STATUS_CANCELED
    record.error_summary = (message or "Job canceled").strip()
    record.completed_at = now
    record.heartbeat_at = now
    if not record.cancel_requested_at:
        record.cancel_requested_at = now
    if not record.cancel_reason:
        record.cancel_reason = record.error_summary
    db.session.add(record)
    log_job_event(job_id, "canceled", record.error_summary or "job canceled")
    db.session.commit()


def cancel_job(job_id: str) -> tuple[bool, str]:
    record = get_job(job_id)
    if not record:
        return False, "Job not found"
    if record.status == JOB_STATUS_QUEUED:
        mark_job_canceled(job_id, "Canceled before execution")
        return True, "Job canceled"
    if record.status in {JOB_STATUS_CLAIMED, JOB_STATUS_RUNNING}:
        if record.cancel_requested_at:
            return True, "Cancellation already requested"
        now = _utcnow()
        record.cancel_requested_at = now
        record.cancel_reason = "Cancellation requested by user"
        db.session.add(record)
        log_job_event(job_id, "cancel_requested", "job cancellation requested")
        db.session.commit()
        return True, "Cancellation requested"
    if record.status == JOB_STATUS_CANCELED:
        return True, "Job already canceled"
    return False, f"Job cannot be canceled from status '{record.status}'"


def retry_job(job_id: str, actor: dict | None = None) -> tuple[bool, str, str]:
    record = get_job(job_id)
    if not record:
        return False, "Job not found", ""
    if record.status not in {JOB_STATUS_FAILED, JOB_STATUS_CANCELED, JOB_STATUS_TIMEOUT}:
        return False, f"Job cannot be retried from status '{record.status}'", ""
    actor = actor or {
        "work_id": record.created_by_work_id or "",
        "label": record.created_by_label or "",
    }
    new_job_id = enqueue_job(
        record.job_type,
        get_job_payload(record),
        task_id=record.task_id,
        target_name=record.target_name or "",
        actor=actor,
        priority=int(record.priority or 100),
        queue_name=record.queue_name or "default",
        parent_job_id=record.parent_job_id or "",
        max_attempts=max(int(record.max_attempts or 1), 1),
        artifact_root=(record.artifact_root or "").strip(),
    )
    log_job_event(job_id, "retried", f"retried as {new_job_id}", {"new_job_id": new_job_id})
    db.session.commit()
    return True, "Job retried", new_job_id


def _heartbeat_loop(app, job_id: str, stop_event: threading.Event, interval: float) -> None:
    with app.app_context():
        while not stop_event.wait(max(interval, 0.05)):
            try:
                touch_job_heartbeat(job_id)
            except Exception:
                db.session.rollback()
                current_app.logger.exception("Failed to update job heartbeat: job_id=%s", job_id)


def requeue_stale_jobs(app) -> int:
    with app.app_context():
        threshold = _utcnow() - timedelta(seconds=stale_after_seconds())
        stale_jobs = JobRecord.query.filter(
            JobRecord.status.in_([JOB_STATUS_CLAIMED, JOB_STATUS_RUNNING]),
            JobRecord.heartbeat_at.isnot(None),
            JobRecord.heartbeat_at < threshold,
        ).all()
        count = 0
        for job in stale_jobs:
            release_task_lock(job.task_id or "", job.job_id)
            job.status = JOB_STATUS_QUEUED
            job.worker_id = None
            job.claimed_at = None
            job.started_at = None
            job.heartbeat_at = None
            job.cancel_requested_at = None
            job.cancel_reason = None
            db.session.add(job)
            log_job_event(job.job_id, "requeued", "stale job requeued")
            count += 1
        if count:
            db.session.commit()
        return count


def _dispatch_job(job: JobRecord) -> dict | None:
    payload = get_job_payload(job)
    if job.job_type == FLOW_SINGLE_JOB:
        from app.jobs.executor import run_single_flow_job

        return run_single_flow_job(job.job_id, payload)
    if job.job_type == MAPPING_OPERATION_JOB:
        from app.blueprints.tasks.mapping_routes import _run_mapping_operation_job

        return _run_mapping_operation_job(job.job_id, payload)
    if job.job_type == MAPPING_SCHEME_RUN_JOB:
        from app.blueprints.tasks.mapping_scheme_helpers import run_saved_mapping_scheme_job

        return run_saved_mapping_scheme_job(job.job_id, payload)
    if job.job_type == GLOBAL_BATCH_JOB:
        from app.blueprints.flows.global_batch_routes import _run_tasks_batch

        return _run_tasks_batch(job.job_id, payload)
    if job.job_type == GLOBAL_BATCH_ITEM_JOB:
        from app.blueprints.flows.global_batch_routes import _run_global_batch_item_job

        return _run_global_batch_item_job(job.job_id, payload)
    raise RuntimeError(f"Unsupported job type: {job.job_type}")


def run_job_by_id(app, job_id: str, worker_id: str | None = None) -> bool:
    with app.app_context():
        record = _claim_job_record(job_id, worker_id or build_worker_id())
        if not record:
            return False
        return _run_claimed_job(record, worker_id or record.worker_id or build_worker_id())


def _run_claimed_job(job: JobRecord, worker_id: str) -> bool:
    ensure_job_not_canceled(job.job_id, "Job canceled before execution started")
    lock_acquired = False
    if job.job_type in WRITE_LOCK_JOB_TYPES and job.task_id:
        lock_acquired = acquire_task_lock(job.task_id, job.job_id)
        if not lock_acquired:
            defer_job(job.job_id, f"Task {job.task_id} is busy")
            return False

    mark_job_running(job.job_id, worker_id)
    heartbeat_stop = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(current_app._get_current_object(), job.job_id, heartbeat_stop, default_heartbeat_interval()),
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        ensure_job_not_canceled(job.job_id)
        result = _dispatch_job(job) or {}
        ensure_job_not_canceled(job.job_id)
        artifact_root = (result.get("artifact_root") or "").strip()
        if artifact_root:
            set_job_artifact_root(job.job_id, artifact_root)
        result_payload = result.get("result_payload")
        if result_payload is not None:
            set_job_result_payload(job.job_id, result_payload if isinstance(result_payload, dict) else {"value": result_payload})
        for artifact in result.get("artifacts") or []:
            if not isinstance(artifact, dict):
                continue
            record_job_artifact(
                job.job_id,
                str(artifact.get("artifact_type") or "file"),
                str(artifact.get("rel_path") or ""),
                artifact.get("size_bytes"),
            )
        mark_job_completed(job.job_id)
        return True
    except JobCanceledError as exc:
        mark_job_canceled(job.job_id, str(exc) or "Job canceled")
        return True
    except Exception as exc:
        current_app.logger.exception("Job execution failed: job_id=%s type=%s", job.job_id, job.job_type)
        mark_job_failed(job.job_id, str(exc))
        return False
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1.0)
        if job.task_id and lock_acquired:
            release_task_lock(job.task_id, job.job_id)


def process_next_job(app, worker_id: str | None = None) -> bool:
    with app.app_context():
        worker_id = worker_id or build_worker_id()
        record = claim_next_job(worker_id)
        if not record:
            return False
        return _run_claimed_job(record, worker_id)


def run_worker_loop(
    app,
    *,
    once: bool = False,
    max_jobs: int | None = None,
    poll_interval: float | None = None,
) -> int:
    processed = 0
    worker_id = build_worker_id()
    interval = float(default_poll_interval() if poll_interval is None else poll_interval)

    while True:
        did_work = process_next_job(app, worker_id=worker_id)
        if did_work:
            processed += 1
            if once:
                return processed
            if max_jobs is not None and processed >= max_jobs:
                return processed
            continue
        if once:
            return processed
        time.sleep(max(interval, 0.1))
