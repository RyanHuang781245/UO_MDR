from __future__ import annotations

import json
import os

from flask import abort, current_app, flash, request, send_file

from app.services.flow_version_service import (
    build_flow_version_context as _build_flow_version_context,
    delete_flow_version_entry as _delete_flow_version_entry,
    flow_version_count as _flow_version_count,
    has_duplicate_manual_version_name as _has_duplicate_manual_version_name,
    load_flow_version_entry as _load_flow_version_entry,
    normalize_flow_payload as _normalize_flow_payload,
    rename_flow_version_entry as _rename_flow_version_entry,
    snapshot_flow_version as _snapshot_flow_version,
)
from app.services.user_context_service import get_actor_info as _get_actor_info
from .flow_version_api_blueprint import flow_version_api_bp
from .flow_version_blueprint import flow_version_bp
from .flow_route_helpers import _serialize_flow_versions, _touch_task_last_edit


@flow_version_api_bp.get("", endpoint="list_flow_versions")
def list_flow_versions(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        return {"ok": False, "error": "Flow not found"}, 404
    versions = _serialize_flow_versions(
        task_id,
        flow_name,
        _build_flow_version_context(flow_dir, flow_name),
    )
    return {"ok": True, "versions": versions}


@flow_version_api_bp.post("", endpoint="create_flow_version")
def create_flow_version(task_id, flow_name):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        return {"ok": False, "error": "Flow not found"}, 404

    if request.is_json:
        payload_data = request.get_json(silent=True) or {}
        version_name = (payload_data.get("version_name") or "").strip()
    else:
        version_name = (request.form.get("version_name") or "").strip()
    if not version_name:
        return {"ok": False, "error": "缺少版本名稱"}, 400
    if len(version_name) > 80:
        return {"ok": False, "error": "版本名稱長度不可超過 80 字"}, 400
    if _has_duplicate_manual_version_name(flow_dir, flow_name, version_name):
        return {"ok": False, "error": "版本名稱已存在"}, 400

    try:
        with open(flow_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {"ok": False, "error": "Flow file is invalid"}, 400

    _work_id, actor_label = _get_actor_info()
    saved = _snapshot_flow_version(
        flow_dir,
        flow_name,
        _normalize_flow_payload(payload),
        source="manual_snapshot",
        actor_label=actor_label,
        version_name=version_name,
        force=True,
    )
    if not saved:
        return {"ok": False, "error": "建立版本失敗"}, 400
    _touch_task_last_edit(task_id)
    return {
        "ok": True,
        "version": {
            "id": saved.get("id"),
            "name": saved.get("name") or version_name,
        },
        "version_count": _flow_version_count(flow_dir, flow_name),
        "versions": _serialize_flow_versions(
            task_id,
            flow_name,
            _build_flow_version_context(flow_dir, flow_name),
        ),
    }


@flow_version_bp.get("/<version_id>/download", endpoint="download_flow_version")
def download_flow_version(task_id, flow_name, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    loaded = _load_flow_version_entry(flow_dir, flow_name, version_id)
    if not loaded:
        abort(404)
    version_path, version = loaded
    slug = version.get("slug") or version_id
    return send_file(version_path, as_attachment=True, download_name=f"{flow_name}_{slug}_{version_id}.json")


@flow_version_api_bp.post("/<version_id>/delete", endpoint="delete_flow_version")
def delete_flow_version(task_id, flow_name, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        return {"ok": False, "error": "Flow not found"}, 404

    deleted = _delete_flow_version_entry(flow_dir, flow_name, version_id, allow_sources={"manual_snapshot"})
    if not deleted:
        return {"ok": False, "error": "Version not found"}, 404
    if deleted.get("error"):
        return {"ok": False, "error": "手動版本以外的版本不可刪除"}, 400

    _touch_task_last_edit(task_id)
    return {
        "ok": True,
        "deleted_version": {
            "id": version_id,
            "name": deleted["version"].get("name") or version_id,
        },
        "version_count": _flow_version_count(flow_dir, flow_name),
        "versions": _serialize_flow_versions(
            task_id,
            flow_name,
            _build_flow_version_context(flow_dir, flow_name),
        ),
    }


@flow_version_api_bp.post("/<version_id>/rename", endpoint="rename_flow_version")
def rename_flow_version(task_id, flow_name, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        return {"ok": False, "error": "Flow not found"}, 404

    if request.is_json:
        payload_data = request.get_json(silent=True) or {}
        version_name = (payload_data.get("version_name") or "").strip()
    else:
        version_name = (request.form.get("version_name") or "").strip()
    if not version_name:
        return {"ok": False, "error": "缺少版本名稱"}, 400
    if len(version_name) > 50:
        return {"ok": False, "error": "版本名稱長度不可超過 50 字"}, 400

    renamed = _rename_flow_version_entry(flow_dir, flow_name, version_id, version_name, allow_sources={"manual_snapshot"})
    if not renamed:
        return {"ok": False, "error": "Version not found"}, 404
    if renamed.get("error") == "Version name already exists":
        return {"ok": False, "error": "版本名稱已存在"}, 400
    if renamed.get("error"):
        return {"ok": False, "error": "手動版本以外的版本不可重新命名"}, 400

    _touch_task_last_edit(task_id)
    return {
        "ok": True,
        "renamed_version": {
            "id": version_id,
            "name": renamed["version"].get("name") or version_name,
        },
        "version_count": _flow_version_count(flow_dir, flow_name),
        "versions": _serialize_flow_versions(
            task_id,
            flow_name,
            _build_flow_version_context(flow_dir, flow_name),
        ),
    }


@flow_version_bp.post("/<version_id>/restore", endpoint="restore_flow_version")
def restore_flow_version(task_id, flow_name, version_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        return {"ok": False, "error": "Flow not found"}, 404
    loaded = _load_flow_version_entry(flow_dir, flow_name, version_id)
    if not loaded:
        return {"ok": False, "error": "Version not found"}, 404
    version_path, version = loaded
    try:
        with open(flow_path, "r", encoding="utf-8") as f:
            current_payload = json.load(f)
        with open(version_path, "r", encoding="utf-8") as f:
            restore_payload = json.load(f)
    except Exception:
        return {"ok": False, "error": "Version file is invalid"}, 400

    _work_id, actor_label = _get_actor_info()
    _snapshot_flow_version(
        flow_dir,
        flow_name,
        _normalize_flow_payload(current_payload),
        source="before_restore",
        actor_label=actor_label,
        version_name=f"回復前備份（目標：{version.get('name') or version_id}）",
        force=True,
        extra_metadata={
            "restored_to_version_id": version.get("id") or version_id,
            "restored_to_version_name": version.get("name") or version_id,
        },
    )
    with open(flow_path, "w", encoding="utf-8") as f:
        json.dump(_normalize_flow_payload(restore_payload), f, ensure_ascii=False, indent=2)
    _touch_task_last_edit(task_id)
    if (version.get("source") or "").strip() == "before_restore":
        flash("已成功撤銷上次回復。", "success")
    else:
        flash(f"已成功回復版本「{version.get('name') or version_id}」。", "success")
    return {
        "ok": True,
        "restored_version": {
            "id": version.get("id"),
            "name": version.get("name") or version_id,
        },
    }
