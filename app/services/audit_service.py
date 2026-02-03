import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

from flask import current_app


def record_audit(
    action: str,
    actor: Dict[str, str] | None = None,
    detail: Optional[Dict[str, Any]] = None,
    task_id: Optional[str] = None,
) -> None:
    payload = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "actor": actor or {},
        "detail": detail or {},
    }
    try:
        task_root = current_app.config.get("TASK_FOLDER", "")
        if task_id:
            task_dir = os.path.join(task_root, task_id)
            if os.path.isdir(task_dir):
                _append_jsonl(os.path.join(task_dir, "audit.jsonl"), payload)
    except Exception:
        current_app.logger.exception("Failed to record audit log")


def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))
        f.write("\n")
