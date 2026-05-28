import json
import os
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import click
from flask import current_app

from app.models.auth import AuditLog, SystemErrorLog, db

_SYSTEM_ERROR_LEVEL_ORDER = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}


def record_audit(
    action: str,
    actor: Dict[str, str] | None = None,
    detail: Optional[Dict[str, Any]] = None,
    task_id: Optional[str] = None,
) -> None:
    work_id = None
    actor_label = ""
    if actor:
        work_id = actor.get("work_id") or actor.get("username")
        actor_label = (actor.get("label") or "").strip()

    detail_payload = dict(detail or {})
    if actor_label and "_actor_label" not in detail_payload:
        detail_payload["_actor_label"] = actor_label

    detail_json = json.dumps(detail_payload, ensure_ascii=False)
    
    # 1. Primary: Record to Database
    db_success = False
    try:
        log = AuditLog(
            action=action,
            work_id=work_id,
            detail=detail_json,
            task_id=task_id,
        )
        db.session.add(log)
        db.session.commit()
        db_success = True
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Database audit failed, falling back to JSONL")

    # 2. Secondary/Fallback: Record to JSONL ONLY IF database failed
    if not db_success:
        try:
            task_root = current_app.config.get("TASK_FOLDER", "")
            if not task_root:
                current_app.logger.error("TASK_FOLDER not configured, cannot save fallback log")
                return

            if task_id:
                log_dir = os.path.join(task_root, str(task_id))
                log_path = os.path.join(log_dir, "task_log.jsonl")
            else:
                log_dir = task_root
                log_path = os.path.join(log_dir, "fallback_audit.jsonl")
            
            current_app.logger.info(f"Writing fallback log to: {log_path} (task_id: {task_id})")
            
            payload = {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "action": action,
                "work_id": work_id,
                "actor_label": actor_label,
                "detail": detail_payload,
                "task_id": task_id,
                "fallback": True
            }
            _append_jsonl(log_path, payload)
        except Exception as e:
            current_app.logger.critical(f"CRITICAL: Both DB and JSONL audit failed! Error: {str(e)}")


def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))
        f.write("\n")


def _ensure_system_error_table() -> None:
    SystemErrorLog.__table__.create(bind=db.engine, checkfirst=True)


def _normalize_system_error_level(level: str) -> str:
    normalized = str(level or "ERROR").strip().upper() or "ERROR"
    if normalized == "WARN":
        return "WARNING"
    return normalized


def _system_error_level_value(level: str) -> int:
    normalized = _normalize_system_error_level(level)
    return _SYSTEM_ERROR_LEVEL_ORDER.get(normalized, _SYSTEM_ERROR_LEVEL_ORDER["ERROR"])


def system_error_db_min_level() -> str:
    return _normalize_system_error_level(current_app.config.get("SYSTEM_ERROR_DB_MIN_LEVEL") or "ERROR")


def should_persist_system_error(level: str) -> bool:
    return _system_error_level_value(level) >= _system_error_level_value(system_error_db_min_level())


def _system_error_fallback_path() -> str:
    log_dir = str(current_app.config.get("APP_LOG_DIR") or "").strip()
    if log_dir:
        return os.path.join(log_dir, "system-error-fallback.jsonl")
    task_root = str(current_app.config.get("TASK_FOLDER") or "").strip()
    if task_root:
        return os.path.join(task_root, "system-error-fallback.jsonl")
    return os.path.join(os.getcwd(), "system-error-fallback.jsonl")


def record_system_error(
    component: str,
    message: str,
    *,
    detail: Optional[Dict[str, Any]] = None,
    exc: Exception | None = None,
    task_id: str | None = None,
    level: str = "ERROR",
) -> bool:
    payload = dict(detail or {})
    error_type = ""
    normalized_level = _normalize_system_error_level(level)
    if not should_persist_system_error(normalized_level):
        current_app.logger.debug(
            "Skipping system error log below DB min level: level=%s min_level=%s component=%s",
            normalized_level,
            system_error_db_min_level(),
            component,
        )
        return False
    if exc is not None:
        error_type = exc.__class__.__name__
        payload.setdefault("exception_message", str(exc))
        payload.setdefault(
            "traceback",
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).strip(),
        )
    detail_json = json.dumps(payload, ensure_ascii=False)

    try:
        _ensure_system_error_table()
        log = SystemErrorLog(
            level=normalized_level,
            component=str(component or "").strip() or "unknown",
            message=str(message or "").strip() or "System error",
            error_type=error_type or None,
            detail=detail_json,
            task_id=(str(task_id or "").strip() or None),
        )
        db.session.add(log)
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        current_app.logger.warning(
            "Failed to persist system error log: component=%s message=%s",
            component,
            message,
            exc_info=True,
        )
        try:
            fallback_payload = {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "level": normalized_level,
                "component": str(component or "").strip() or "unknown",
                "message": str(message or "").strip() or "System error",
                "error_type": error_type,
                "detail": payload,
                "task_id": (str(task_id or "").strip() or None),
                "fallback": True,
            }
            _append_jsonl(_system_error_fallback_path(), fallback_payload)
            return True
        except Exception:
            current_app.logger.critical(
                "CRITICAL: Both DB and JSONL system error logging failed: component=%s message=%s",
                component,
                message,
                exc_info=True,
            )
            return False


def cleanup_audit_logs(*, retention_days: int, dry_run: bool = False) -> dict[str, Any]:
    days = int(retention_days)
    if days <= 0:
        raise ValueError("retention_days must be greater than 0")

    cutoff = datetime.now() - timedelta(days=days)
    query = AuditLog.query.filter(AuditLog.created_at < cutoff)
    matched_count = query.count()

    if not dry_run and matched_count:
        query.delete(synchronize_session=False)
        db.session.commit()

    return {
        "retention_days": days,
        "cutoff": cutoff,
        "matched_count": matched_count,
        "deleted_count": 0 if dry_run else matched_count,
        "dry_run": dry_run,
    }


def register_audit_cli(app) -> None:
    @app.cli.command("audit-cleanup")
    @click.option("--days", default=None, type=int, help="Retention period in days. Defaults to AUDIT_LOG_RETENTION_DAYS.")
    @click.option("--dry-run", is_flag=True, help="Show how many rows would be deleted without deleting them.")
    def audit_cleanup_command(days: int | None, dry_run: bool) -> None:
        retention_days = int(days or current_app.config.get("AUDIT_LOG_RETENTION_DAYS") or 180)
        result = cleanup_audit_logs(retention_days=retention_days, dry_run=dry_run)
        click.echo(
            "audit_cleanup "
            f"retention_days={result['retention_days']} "
            f"cutoff={result['cutoff'].strftime('%Y-%m-%d %H:%M:%S')} "
            f"matched={result['matched_count']} "
            f"deleted={result['deleted_count']} "
            f"dry_run={'1' if result['dry_run'] else '0'}"
        )

    @app.cli.command("system-error-test")
    @click.option(
        "--level",
        default="ERROR",
        type=click.Choice(["DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
        help="System error level to record.",
    )
    @click.option("--component", default="manual.test", help="Component name for the test record.")
    @click.option("--task-id", default="test-task", help="Optional task id for the test record.")
    def system_error_test_command(level: str, component: str, task_id: str) -> None:
        normalized_level = _normalize_system_error_level(level)
        should_record = should_persist_system_error(normalized_level)
        ok = record_system_error(
            component,
            f"Manual {normalized_level} system error test",
            level=normalized_level,
            task_id=task_id or None,
            detail={"source": "cli-test", "requested_level": str(level).upper()},
            exc=RuntimeError(f"{normalized_level} test error"),
        )
        click.echo(
            "system_error_test "
            f"recorded={'1' if ok else '0'} "
            f"skipped_by_level={'1' if not should_record else '0'} "
            f"level={normalized_level} "
            f"min_level={system_error_db_min_level()} "
            f"component={component} "
            f"task_id={task_id or '-'}"
        )
