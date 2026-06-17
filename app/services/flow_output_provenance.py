from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any


FLOW_OUTPUT_PROVENANCE_FILENAME = ".uo_flow_output_provenance.json"


def _registry_path(output_root: str) -> str:
    return os.path.join(output_root, FLOW_OUTPUT_PROVENANCE_FILENAME)


def normalize_output_provenance_path(raw_path: str) -> str:
    cleaned = (raw_path or "").strip().replace("\\", "/")
    if cleaned in {"", ".", "/"}:
        return ""
    normalized = os.path.normpath(cleaned).replace("\\", "/")
    if normalized in {"", "."}:
        return ""
    return normalized


def load_flow_output_provenance(output_root: str) -> dict[str, dict[str, Any]]:
    path = _registry_path(output_root)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        normalize_output_provenance_path(str(key)): value
        for key, value in data.items()
        if normalize_output_provenance_path(str(key)) and isinstance(value, dict)
    }


def save_flow_output_provenance(output_root: str, registry: dict[str, dict[str, Any]]) -> None:
    os.makedirs(output_root, exist_ok=True)
    path = _registry_path(output_root)
    cleaned = {
        normalize_output_provenance_path(str(key)): value
        for key, value in registry.items()
        if normalize_output_provenance_path(str(key)) and isinstance(value, dict)
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cleaned, fh, ensure_ascii=False, indent=2, sort_keys=True)


def record_flow_output_provenance(
    output_root: str,
    rel_path: str,
    *,
    flow_name: str,
    job_id: str,
    started_at: str = "",
    completed_at: str = "",
    overwrote_existing: bool = False,
) -> dict[str, Any]:
    normalized = normalize_output_provenance_path(rel_path)
    if not output_root or not normalized:
        return {}
    registry = load_flow_output_provenance(output_root)
    record = {
        "flow_name": str(flow_name or "").strip() or "未命名流程",
        "job_id": str(job_id or "").strip(),
        "started_at": str(started_at or "").strip(),
        "completed_at": str(completed_at or "").strip(),
        "published_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "output_path": normalized,
        "overwrote_existing": bool(overwrote_existing),
    }
    registry[normalized] = record
    save_flow_output_provenance(output_root, registry)
    return record


def remove_flow_output_provenance(output_root: str, rel_path: str) -> None:
    normalized = normalize_output_provenance_path(rel_path)
    if not output_root or not normalized:
        return
    registry = load_flow_output_provenance(output_root)
    prefix = f"{normalized}/"
    changed = False
    for key in list(registry):
        if key == normalized or key.startswith(prefix):
            registry.pop(key, None)
            changed = True
    if changed:
        save_flow_output_provenance(output_root, registry)


def rename_flow_output_provenance(output_root: str, old_rel_path: str, new_rel_path: str) -> None:
    old_path = normalize_output_provenance_path(old_rel_path)
    new_path = normalize_output_provenance_path(new_rel_path)
    if not output_root or not old_path or not new_path or old_path == new_path:
        return
    registry = load_flow_output_provenance(output_root)
    old_prefix = f"{old_path}/"
    updates: dict[str, dict[str, Any]] = {}
    changed = False
    for key in list(registry):
        if key == old_path:
            next_key = new_path
        elif key.startswith(old_prefix):
            next_key = f"{new_path}/{key[len(old_prefix):]}"
        else:
            continue
        record = dict(registry.pop(key))
        record["output_path"] = next_key
        updates[next_key] = record
        changed = True
    if changed:
        registry.update(updates)
        save_flow_output_provenance(output_root, registry)
