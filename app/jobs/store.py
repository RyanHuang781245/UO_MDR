from __future__ import annotations

import json
import os

from flask import current_app

from app.blueprints.flows.flow_route_helpers import _write_json_with_replace_retry


def batch_status_path(task_id: str, batch_id: str) -> str:
    return os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", "batch", f"{batch_id}.json")


def write_batch_status(task_id: str, batch_id: str, payload: dict) -> None:
    status_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", "batch")
    os.makedirs(status_dir, exist_ok=True)
    _write_json_with_replace_retry(batch_status_path(task_id, batch_id), payload)


def load_batch_status(task_id: str, batch_id: str) -> dict | None:
    path = batch_status_path(task_id, batch_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except Exception:
        return None


def write_job_meta(job_dir: str, payload: dict) -> None:
    try:
        meta_path = os.path.join(job_dir, "meta.json")
        with open(meta_path, "w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2)
    except Exception:
        current_app.logger.exception("Failed to write job meta")


def read_job_meta(job_dir: str) -> dict:
    meta_path = os.path.join(job_dir, "meta.json")
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}


def update_job_meta(job_dir: str, **fields) -> None:
    meta = read_job_meta(job_dir)
    meta.update(fields)
    write_job_meta(job_dir, meta)


def job_has_error(job_dir: str) -> bool:
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(log_path):
        return False
    try:
        with open(log_path, "r", encoding="utf-8") as file_obj:
            entries = json.load(file_obj)
        if not isinstance(entries, list):
            return False
        return any(isinstance(entry, dict) and entry.get("status") == "error" for entry in entries)
    except Exception:
        return False
