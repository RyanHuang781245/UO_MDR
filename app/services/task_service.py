from __future__ import annotations

import os
import shutil
import json
import zipfile
from datetime import datetime

from flask import current_app

from modules.auth_models import commit_session, db
from modules.task_models import TaskRecord, ensure_schema as ensure_task_schema

ALLOWED_DOCX = {".docx"}
ALLOWED_PDF = {".pdf"}
ALLOWED_ZIP = {".zip"}
ALLOWED_EXCEL = {".xlsx", ".xls"}

def allowed_file(filename, kinds=("docx", "pdf", "zip", "excel")):
    ext = os.path.splitext(filename)[1].lower()
    if "docx" in kinds and ext in ALLOWED_DOCX:
        return True
    if "pdf" in kinds and ext in ALLOWED_PDF:
        return True
    if "zip" in kinds and ext in ALLOWED_ZIP:
        return True
    if "excel" in kinds and ext in ALLOWED_EXCEL:
        return True
    return False

def list_files(base_dir):
    files = []
    for root, _, fns in os.walk(base_dir):
        for fn in fns:
            rel = os.path.relpath(os.path.join(root, fn), base_dir)
            files.append(rel)
    return sorted(files)

def build_file_tree(base_dir):
    tree = {"dirs": {}, "files": []}
    for root, dirs, files in os.walk(base_dir):
        rel = os.path.relpath(root, base_dir)
        node = tree
        if rel != ".":
            for part in rel.split(os.sep):
                node = node["dirs"].setdefault(part, {"dirs": {}, "files": []})
        node["files"].extend(sorted(files))
    return tree

def list_dirs(base_dir):
    dirs = []
    for root, dirnames, _ in os.walk(base_dir):
        rel_root = os.path.relpath(root, base_dir)
        for d in dirnames:
            path = os.path.normpath(os.path.join(rel_root, d))
            dirs.append(path)
    return sorted(dirs)

def deduplicate_name(base_dir: str, name: str) -> str:
    candidate = name
    stem, ext = os.path.splitext(name)
    counter = 1
    while os.path.exists(os.path.join(base_dir, candidate)):
        candidate = f"{stem} ({counter}){ext}"
        counter += 1
    return candidate

def ensure_windows_long_path(path: str) -> str:
    """Add the Windows long-path prefix to avoid MAX_PATH issues."""
    if os.name != "nt" or not path:
        return path
    normalized = os.path.abspath(path)
    if normalized.startswith("\\\\?\\"):
        return normalized
    if normalized.startswith("\\\\"):
        return "\\\\?\\UNC\\" + normalized[2:]
    return "\\\\?\\" + normalized

def enforce_max_copy_size(path: str):
    max_bytes = current_app.config.get("NAS_MAX_COPY_FILE_SIZE")
    if not max_bytes:
        return
    checked_path = ensure_windows_long_path(path)

    def _check(target: str):
        try:
            return os.path.getsize(target)
        except OSError:
            return 0

    if os.path.isfile(checked_path):
        if _check(checked_path) > max_bytes:
            raise ValueError("檔案超過允許的大小限制，請分批處理或聯絡系統管理員")
        return

    for root, _, files in os.walk(checked_path):
        for fn in files:
            fpath = os.path.join(root, fn)
            if _check(fpath) > max_bytes:
                current_app.logger.warning("檔案大小超過限制：%s", fpath)
                raise ValueError("檔案超過允許的大小限制，請分批處理或聯絡系統管理員")

def task_name_exists(name, exclude_id=None):
    for tid in os.listdir(current_app.config["TASK_FOLDER"]):
        if exclude_id and tid == exclude_id:
            continue
        tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
        if not os.path.isdir(tdir):
            continue
        meta_path = os.path.join(tdir, "meta.json")
        tname = tid
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                tname = json.load(f).get("name", tid)
        if tname == name:
            return True
    return False
def gather_available_files(files_dir):
    mapping = {"docx": [], "pdf": [], "zip": [], "dir": []}
    for rel in list_files(files_dir):
        ext = os.path.splitext(rel)[1].lower()
        if ext == ".docx":
            mapping["docx"].append(rel)
        elif ext == ".pdf":
            mapping["pdf"].append(rel)
        elif ext == ".zip":
            mapping["zip"].append(rel)
    dirs = list_dirs(files_dir)
    dirs.insert(0, ".")
    mapping["dir"] = dirs
    return mapping


def list_tasks():
    task_list = []
    for tid in os.listdir(current_app.config["TASK_FOLDER"]):
        tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
        if os.path.isdir(tdir):
            meta_path = os.path.join(tdir, "meta.json")
            name = tid
            description = ""
            created = None
            creator = ""
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    name = meta.get("name", tid)
                    description = meta.get("description", "")
                    created = meta.get("created")
                    creator = meta.get("creator", "") or ""
            if not created:
                created = datetime.fromtimestamp(os.path.getmtime(tdir)).strftime("%Y-%m-%d %H:%M")
            task_list.append(
                {
                    "id": tid,
                    "name": name,
                    "description": description,
                    "created": created,
                    "creator": creator,
                }
            )
    task_list.sort(key=lambda x: x["created"], reverse=True)
    return task_list


def init_task_store(app) -> None:
    with app.app_context():
        try:
            ensure_task_schema()
        except Exception:
            db.session.rollback()
            app.logger.exception("Task schema initialization failed")


def record_task_in_db(
    task_id: str,
    name: str | None = None,
    description: str | None = None,
    creator: str | None = None,
    nas_path: str | None = None,
    created_at: datetime | None = None,
) -> None:
    try:
        task = db.session.get(TaskRecord, task_id)
        if not task:
            task = TaskRecord(id=task_id, name=name or task_id)
            db.session.add(task)
        if name is not None:
            task.name = name
        if description is not None:
            task.description = description
        if creator is not None:
            task.creator = creator
        if nas_path is not None:
            task.nas_path = nas_path
        if created_at and not task.created_at:
            task.created_at = created_at
        commit_session()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to record task in DB")


def delete_task_record(task_id: str) -> None:
    try:
        task = db.session.get(TaskRecord, task_id)
        if task:
            db.session.delete(task)
            commit_session()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to delete task record")

