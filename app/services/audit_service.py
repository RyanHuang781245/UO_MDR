import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

from flask import current_app

from app.models.auth import AuditLog, db


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
