from __future__ import annotations

import base64
import json


def encode_batch_item(item: dict) -> str:
    raw = json.dumps(item or {}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_batch_item(token: str) -> dict | None:
    token = (token or "").strip()
    if not token:
        return None
    padding = "=" * (-len(token) % 4)
    try:
        payload = base64.urlsafe_b64decode(f"{token}{padding}".encode("ascii")).decode("utf-8")
        data = json.loads(payload)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def batch_item_key(item: dict) -> tuple[str, str, str]:
    kind = str(item.get("kind") or "task").strip().lower() or "task"
    task_id = str(item.get("task_id") or "").strip()
    scheme_id = str(item.get("scheme_id") or "").strip()
    return kind, task_id, scheme_id


def normalize_batch_items(raw_items: str = "", raw_task_ids: str = "") -> list[dict]:
    results: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    for part in (raw_items or "").split(","):
        token = part.strip()
        if not token:
            continue
        item = decode_batch_item(token)
        if not item:
            continue
        normalized = {
            "kind": str(item.get("kind") or "").strip().lower() or "task",
            "task_id": str(item.get("task_id") or "").strip(),
            "scheme_id": str(item.get("scheme_id") or "").strip(),
        }
        key = batch_item_key(normalized)
        if not normalized["task_id"] or key in seen:
            continue
        if normalized["kind"] not in {"task", "mapping_scheme"}:
            continue
        seen.add(key)
        results.append(normalized)

    for part in (raw_task_ids or "").split(","):
        task_id = part.strip()
        if not task_id:
            continue
        item = {"kind": "task", "task_id": task_id, "scheme_id": ""}
        key = batch_item_key(item)
        if key in seen:
            continue
        seen.add(key)
        results.append(item)

    return results
