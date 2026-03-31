from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime

from flask import current_app

from app.services.flow_service import (
    load_version_metadata,
    sanitize_version_slug,
    save_version_metadata,
)

FLOW_VERSION_LIMIT = 20


def flow_versions_dir(flow_dir: str, flow_name: str) -> str:
    return os.path.join(flow_dir, "_versions", flow_name)


def normalize_flow_payload(data):
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {"steps": data}
    return {"steps": []}


def flow_content_hash(payload: dict) -> str:
    normalized = normalize_flow_payload(payload)
    text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _prune_flow_versions(versions_dir: str, versions: list[dict]) -> list[dict]:
    kept = versions[:FLOW_VERSION_LIMIT]
    removed = versions[FLOW_VERSION_LIMIT:]
    for item in removed:
        base_name = item.get("base_name")
        if not base_name:
            continue
        path = os.path.join(versions_dir, f"{base_name}.json")
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            current_app.logger.exception("Failed to remove old flow version")
    return kept


def _delete_flow_version_files(versions_dir: str, versions: list[dict]) -> None:
    for item in versions:
        base_name = item.get("base_name")
        if not base_name:
            continue
        path = os.path.join(versions_dir, f"{base_name}.json")
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            current_app.logger.exception("Failed to remove flow version file")


def flow_version_source_label(source: str) -> str:
    mapping = {
        "auto_save": "自動保存",
        "before_restore": "回復前備份",
        "manual_snapshot": "手動版本",
    }
    return mapping.get((source or "").strip(), (source or "").strip() or "未知")


def flow_version_display_name(name: str, source: str) -> str:
    raw_name = (name or "").strip()
    source_label = flow_version_source_label(source)
    if not raw_name:
        return source_label
    auto_prefixes = ("自動保存 ", "回復前備份 ")
    if raw_name.startswith(auto_prefixes):
        return source_label
    return raw_name


def has_duplicate_manual_version_name(
    flow_dir: str,
    flow_name: str,
    version_name: str,
    *,
    exclude_version_id: str | None = None,
) -> bool:
    target = (version_name or "").strip().casefold()
    if not target:
        return False
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    for item in metadata.get("versions", []):
        if (item.get("source") or "").strip() != "manual_snapshot":
            continue
        if exclude_version_id and item.get("id") == exclude_version_id:
            continue
        if ((item.get("name") or "").strip().casefold()) == target:
            return True
    return False


def snapshot_flow_version(
    flow_dir: str,
    flow_name: str,
    payload: dict,
    *,
    source: str,
    actor_label: str = "",
    version_name: str | None = None,
    force: bool = False,
    extra_metadata: dict | None = None,
) -> dict | None:
    normalized = normalize_flow_payload(payload)
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    content_hash = flow_content_hash(normalized)
    latest = versions[0] if versions else None
    if not force and latest and latest.get("content_hash") == content_hash:
        return None

    created_ts = datetime.now()
    timestamp = created_ts.strftime("%Y%m%d%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    version_id = f"{timestamp}_{unique_suffix}"
    display_name = (version_name or "").strip()
    if not display_name:
        display_name = flow_version_source_label(source)
    slug = sanitize_version_slug(display_name)
    base_name = f"{version_id}_{slug}" if slug else version_id

    os.makedirs(versions_dir, exist_ok=True)
    version_path = os.path.join(versions_dir, f"{base_name}.json")
    with open(version_path, "w", encoding="utf-8") as file_obj:
        json.dump(normalized, file_obj, ensure_ascii=False, indent=2)

    versions = [version for version in versions if version.get("id") != version_id]
    if source == "before_restore":
        restore_backups = [version for version in versions if (version.get("source") or "").strip() == "before_restore"]
        if restore_backups:
            _delete_flow_version_files(versions_dir, restore_backups)
            backup_ids = {version.get("id") for version in restore_backups}
            versions = [version for version in versions if version.get("id") not in backup_ids]
    versions.append(
        {
            "id": version_id,
            "name": display_name,
            "slug": slug,
            "base_name": base_name,
            "created_at": created_ts.isoformat(timespec="seconds"),
            "created_by": actor_label,
            "flow_name": flow_name,
            "source": source,
            "content_hash": content_hash,
            **(extra_metadata or {}),
        }
    )
    versions.sort(key=lambda version: version.get("created_at", ""), reverse=True)
    metadata["versions"] = _prune_flow_versions(versions_dir, versions)
    save_version_metadata(versions_dir, metadata)
    return metadata["versions"][0]


def load_flow_version_entry(flow_dir: str, flow_name: str, version_id: str) -> tuple[str, dict] | None:
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((item for item in versions if item.get("id") == version_id), None)
    if not version:
        return None
    base_name = version.get("base_name")
    if not base_name:
        return None
    version_path = os.path.join(versions_dir, f"{base_name}.json")
    if not os.path.exists(version_path):
        return None
    return version_path, version


def delete_flow_version_entry(
    flow_dir: str,
    flow_name: str,
    version_id: str,
    *,
    allow_sources: set[str] | None = None,
) -> dict | None:
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((item for item in versions if item.get("id") == version_id), None)
    if not version:
        return None
    source = (version.get("source") or "").strip()
    if allow_sources is not None and source not in allow_sources:
        return {"error": "Version source is not deletable"}

    base_name = version.get("base_name")
    if base_name:
        path = os.path.join(versions_dir, f"{base_name}.json")
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            current_app.logger.exception("Failed to remove flow version file")
            return {"error": "Failed to remove version file"}

    metadata["versions"] = [item for item in versions if item.get("id") != version_id]
    save_version_metadata(versions_dir, metadata)
    return {"version": version}


def rename_flow_version_entry(
    flow_dir: str,
    flow_name: str,
    version_id: str,
    version_name: str,
    *,
    allow_sources: set[str] | None = None,
) -> dict | None:
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((item for item in versions if item.get("id") == version_id), None)
    if not version:
        return None
    source = (version.get("source") or "").strip()
    if allow_sources is not None and source not in allow_sources:
        return {"error": "Version source is not renameable"}

    new_name = (version_name or "").strip()
    if has_duplicate_manual_version_name(flow_dir, flow_name, new_name, exclude_version_id=version_id):
        return {"error": "Version name already exists"}

    version["name"] = new_name
    version["slug"] = sanitize_version_slug(new_name)
    save_version_metadata(versions_dir, metadata)
    return {"version": version}


def build_flow_version_context(flow_dir: str, flow_name: str) -> list[dict]:
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    context = []
    for item in sorted(metadata.get("versions", []), key=lambda version: version.get("created_at", ""), reverse=True):
        if (item.get("source") or "").strip() != "manual_snapshot":
            continue
        version_id = item.get("id")
        base_name = item.get("base_name")
        if not version_id or not base_name:
            continue
        version_path = os.path.join(versions_dir, f"{base_name}.json")
        if not os.path.exists(version_path):
            continue
        created_at = item.get("created_at", "")
        created_display = created_at
        if created_at:
            try:
                created_display = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                created_display = created_at
        context.append(
            {
                "id": version_id,
                "name": flow_version_display_name(item.get("name") or "", item.get("source") or ""),
                "created_at_display": created_display,
                "created_by": item.get("created_by") or "",
                "source": flow_version_source_label(item.get("source") or ""),
            }
        )
    return context


def flow_version_count(flow_dir: str, flow_name: str) -> int:
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    return sum(1 for item in metadata.get("versions", []) if (item.get("source") or "").strip() == "manual_snapshot")


def latest_restore_backup_context(flow_dir: str, flow_name: str) -> dict | None:
    versions_dir = flow_versions_dir(flow_dir, flow_name)
    metadata = load_version_metadata(versions_dir)
    backups = [
        item
        for item in sorted(metadata.get("versions", []), key=lambda version: version.get("created_at", ""), reverse=True)
        if (item.get("source") or "").strip() == "before_restore"
    ]
    for item in backups:
        base_name = item.get("base_name")
        version_id = item.get("id")
        if not base_name or not version_id:
            continue
        version_path = os.path.join(versions_dir, f"{base_name}.json")
        if not os.path.exists(version_path):
            continue
        created_at = item.get("created_at", "")
        created_display = created_at
        if created_at:
            try:
                created_display = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                created_display = created_at
        return {
            "id": version_id,
            "name": flow_version_display_name(item.get("name") or "", item.get("source") or ""),
            "created_at_display": created_display,
        }
    return None
