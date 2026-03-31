from __future__ import annotations

from datetime import datetime

from app.services.user_context_service import get_actor_info as _get_actor_info


def _apply_last_edit(meta: dict) -> None:
    work_id, label = _get_actor_info()
    meta["last_edited"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    if label:
        meta["last_editor"] = label
    if work_id:
        meta["last_editor_work_id"] = work_id
