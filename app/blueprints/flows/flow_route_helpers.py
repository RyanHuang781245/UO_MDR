from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime

from flask import current_app, url_for

from app.services.user_context_service import get_actor_info as _get_actor_info


def _touch_task_last_edit(task_id: str, work_id: str | None = None, label: str | None = None) -> None:
    meta_path = os.path.join(current_app.config["TASK_FOLDER"], task_id, "meta.json")
    if not os.path.exists(meta_path):
        return
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        meta = {}
    if work_id is None or label is None:
        work_id, label = _get_actor_info()
    meta["last_edited"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    if label:
        meta["last_editor"] = label
    if work_id:
        meta["last_editor_work_id"] = work_id
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _serialize_flow_versions(task_id: str, flow_name: str, versions: list[dict]) -> list[dict]:
    serialized: list[dict] = []
    for item in versions:
        version_id = (item.get("id") or "").strip()
        if not version_id:
            continue
        serialized.append(
            {
                **item,
                "view_url": url_for("flow_builder_bp.flow_builder", task_id=task_id, flow=flow_name, version_id=version_id),
                "rename_url": url_for("flow_version_api_bp.rename_flow_version", task_id=task_id, flow_name=flow_name, version_id=version_id),
                "delete_url": url_for("flow_version_api_bp.delete_flow_version", task_id=task_id, flow_name=flow_name, version_id=version_id),
                "download_url": url_for("flow_version_bp.download_flow_version", task_id=task_id, flow_name=flow_name, version_id=version_id),
                "restore_url": url_for("flow_version_bp.restore_flow_version", task_id=task_id, flow_name=flow_name, version_id=version_id),
            }
        )
    return serialized


def _serialize_restore_backup(task_id: str, flow_name: str, backup: dict | None) -> dict | None:
    if not backup:
        return None
    version_id = (backup.get("id") or "").strip()
    if not version_id:
        return None
    return {
        **backup,
        "restore_url": url_for("flow_version_bp.restore_flow_version", task_id=task_id, flow_name=flow_name, version_id=version_id),
    }


def _write_json_with_replace_retry(path: str, payload: dict, retries: int = 8, delay_sec: float = 0.03) -> None:
    last_exc = None
    for attempt in range(retries):
        tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
            return
        except PermissionError as exc:
            last_exc = exc
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            if attempt == retries - 1:
                raise
            time.sleep(delay_sec * (attempt + 1))
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise
    if last_exc:
        raise last_exc
