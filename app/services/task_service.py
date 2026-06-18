from __future__ import annotations

import os
import shutil
import json
import uuid
import zipfile
from datetime import datetime

from flask import current_app
from flask_login import current_user

from app.extensions import db
from app.models.auth import ROLE_ADMIN, commit_session, user_has_role
from app.models.settings import SystemSetting
from app.models.task import TaskRecord, ensure_schema as ensure_task_schema
from app.services.audit_service import record_system_error
from app.services.audit_service import record_audit
from app.services.schema_control import auto_schema_management_enabled

ALLOWED_DOCX = {".docx"}
ALLOWED_PDF = {".pdf"}
ALLOWED_ZIP = {".zip"}
ALLOWED_EXCEL = {".xlsx", ".xls"}
ALLOWED_IMAGE = {".png", ".jpg", ".jpeg", ".bmp", ".gif"}
TASK_SOURCE_SYNC_ACTIVE_STATUSES = {"queued", "running"}
TASK_SOURCE_SYNC_READY_STATUSES = {"", "completed"}


def _normalize_rel_path(rel_path: str) -> str:
    return (rel_path or "").replace("\\", "/")


def build_task_output_path(task_id: str) -> str:
    return os.path.join(current_app.config["TASK_FOLDER"], task_id, "output")


def _task_meta_path(task_id: str) -> str:
    return os.path.join(current_app.config["TASK_FOLDER"], task_id, "meta.json")


def _load_task_meta(task_id: str) -> dict:
    meta_path = _task_meta_path(task_id)
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_task_meta(task_id: str, payload: dict) -> None:
    meta_path = _task_meta_path(task_id)
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def update_task_source_sync_status(
    task_id: str,
    status: str,
    *,
    job_id: str = "",
    error: str = "",
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    file_count: int | None = None,
) -> None:
    meta = _load_task_meta(task_id)
    meta["source_sync_status"] = (status or "").strip().lower()
    if job_id:
        meta["source_sync_job_id"] = job_id
    if error:
        meta["source_sync_error"] = error
    else:
        meta.pop("source_sync_error", None)
    if started_at is not None:
        meta["source_sync_started_at"] = started_at.strftime("%Y-%m-%d %H:%M:%S")
    if completed_at is not None:
        meta["source_sync_completed_at"] = completed_at.strftime("%Y-%m-%d %H:%M:%S")
    if file_count is not None:
        meta["source_sync_file_count"] = int(file_count)
    _write_task_meta(task_id, meta)


def is_task_source_ready(meta: dict) -> bool:
    return str((meta or {}).get("source_sync_status") or "").strip().lower() in TASK_SOURCE_SYNC_READY_STATUSES


def _parse_task_created_at(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None

def allowed_file(filename, kinds=("docx", "pdf", "zip", "excel", "image")):
    ext = os.path.splitext(filename)[1].lower()
    if "docx" in kinds and ext in ALLOWED_DOCX:
        return True
    if "pdf" in kinds and ext in ALLOWED_PDF:
        return True
    if "zip" in kinds and ext in ALLOWED_ZIP:
        return True
    if "excel" in kinds and ext in ALLOWED_EXCEL:
        return True
    if "image" in kinds and ext in ALLOWED_IMAGE:
        return True
    return False


def is_ignored_source_file(filename: str) -> bool:
    return os.path.basename(filename).startswith("~$")


def list_files(base_dir):
    files = []
    for root, _, fns in os.walk(base_dir):
        for fn in fns:
            rel = _normalize_rel_path(os.path.relpath(os.path.join(root, fn), base_dir))
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
            path = _normalize_rel_path(os.path.normpath(os.path.join(rel_root, d)))
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
    try:
        settings = SystemSetting.query.order_by(SystemSetting.id).first()
        if settings and settings.nas_max_copy_file_size_mb is not None:
            mb = int(settings.nas_max_copy_file_size_mb)
            if mb <= 0:
                max_bytes = None
            else:
                max_bytes = mb * 1024 * 1024
    except Exception as exc:
        record_system_error(
            "task.settings_load",
            "Failed to load NAS size limit from system settings",
            exc=exc,
        )
        current_app.logger.exception("Failed to load NAS size limit from system settings")
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

    total_size = 0
    for root, _, files in os.walk(checked_path):
        for fn in files:
            fpath = os.path.join(root, fn)
            total_size += _check(fpath)
            if total_size > max_bytes:
                current_app.logger.warning("資料夾總大小超過限制：%s", checked_path)
                raise ValueError("資料夾總大小超過允許的大小限制，請分批處理或聯絡系統管理員")


def normalize_task_copy_permissions(path: str) -> None:
    """Keep local task copies writable after importing files from NAS."""
    if not path or not os.path.exists(path):
        return

    dir_mode = 0o775
    file_mode = 0o664

    def _chmod(target: str, mode: int) -> None:
        try:
            os.chmod(target, mode)
        except OSError:
            current_app.logger.warning("Failed to normalize task copy permission: %s", target, exc_info=True)

    if os.path.isfile(path):
        _chmod(path, file_mode)
        return

    for root, dirs, files in os.walk(path):
        _chmod(root, dir_mode)
        for dirname in dirs:
            _chmod(os.path.join(root, dirname), dir_mode)
        for filename in files:
            _chmod(os.path.join(root, filename), file_mode)


def _copytree_with_count(src_dir: str, dest_dir: str) -> int:
    copied = 0
    os.makedirs(dest_dir, exist_ok=True)
    normalize_task_copy_permissions(dest_dir)
    for root, dirs, files in os.walk(src_dir):
        rel_root = os.path.relpath(root, src_dir)
        dest_root = dest_dir if rel_root == "." else os.path.join(dest_dir, rel_root)
        os.makedirs(dest_root, exist_ok=True)
        normalize_task_copy_permissions(dest_root)
        for dirname in dirs:
            dest_subdir = os.path.join(dest_root, dirname)
            os.makedirs(dest_subdir, exist_ok=True)
            normalize_task_copy_permissions(dest_subdir)
        for filename in files:
            src_file = os.path.join(root, filename)
            dest_file = os.path.join(dest_root, filename)
            shutil.copy2(src_file, dest_file)
            normalize_task_copy_permissions(dest_file)
            copied += 1
    return copied


def run_task_source_sync_job(job_id: str, payload: dict) -> dict:
    task_id = str(payload.get("task_id") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    actor = payload.get("actor") or {}
    if not task_id:
        raise RuntimeError("Missing task_id for task source sync job")
    if not source_path:
        raise RuntimeError("Missing source path for task source sync job")

    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(task_dir, "files")
    if not os.path.isdir(task_dir):
        raise RuntimeError("找不到任務資料夾")

    started_at = datetime.now()
    update_task_source_sync_status(task_id, "running", job_id=job_id, started_at=started_at)
    try:
        source_dir = ensure_windows_long_path(source_path)
        dest_dir = ensure_windows_long_path(files_dir)
        if not os.path.isdir(source_dir):
            raise RuntimeError("指定的 NAS 路徑不是資料夾")
        enforce_max_copy_size(source_dir)
        if os.path.isdir(dest_dir):
            shutil.rmtree(dest_dir, ignore_errors=True)
        copied_count = _copytree_with_count(source_dir, dest_dir)
        completed_at = datetime.now()
        update_task_source_sync_status(
            task_id,
            "completed",
            job_id=job_id,
            completed_at=completed_at,
            file_count=copied_count,
        )
        record_audit(
            action="task_source_sync_completed",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={
                "task_id": task_id,
                "job_id": job_id,
                "source_path": source_path,
                "file_count": copied_count,
            },
            task_id=task_id,
        )
        return {
            "result_payload": {
                "task_id": task_id,
                "status": "completed",
                "file_count": copied_count,
            }
        }
    except PermissionError as exc:
        error = "沒有足夠的權限讀取或複製指定路徑"
        update_task_source_sync_status(task_id, "failed", job_id=job_id, error=error, completed_at=datetime.now())
        record_audit(
            action="task_source_sync_failed",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={"task_id": task_id, "job_id": job_id, "source_path": source_path, "error": error},
            task_id=task_id,
        )
        raise RuntimeError(error) from exc
    except Exception as exc:
        error = str(exc) or "複製 NAS 目錄時發生錯誤"
        current_app.logger.exception("Task source sync failed: task_id=%s job_id=%s", task_id, job_id)
        update_task_source_sync_status(task_id, "failed", job_id=job_id, error=error, completed_at=datetime.now())
        record_audit(
            action="task_source_sync_failed",
            actor={"work_id": actor.get("work_id", ""), "label": actor.get("label", "")},
            detail={"task_id": task_id, "job_id": job_id, "source_path": source_path, "error": error},
            task_id=task_id,
        )
        raise


def enqueue_task_source_sync_job(task_id: str, source_path: str, actor: dict | None = None) -> str:
    from app.services.execution_service import TASK_SOURCE_SYNC_JOB, enqueue_job

    actor = actor or {}
    payload = {
        "task_id": task_id,
        "source_path": source_path,
        "actor": actor,
    }
    job_id = str(uuid.uuid4())[:8]
    update_task_source_sync_status(task_id, "queued", job_id=job_id)
    return enqueue_job(
        TASK_SOURCE_SYNC_JOB,
        payload,
        task_id=task_id,
        target_name="source_files",
        actor=actor,
        queue_name="default",
        job_id=job_id,
        artifact_root=os.path.join(task_id, "files").replace("\\", "/"),
    )


def _iter_task_dirs():
    task_root = current_app.config["TASK_FOLDER"]
    for tid in os.listdir(task_root):
        tdir = os.path.join(task_root, tid)
        if not os.path.isdir(tdir):
            continue
        meta_path = os.path.join(tdir, "meta.json")
        # Keep system folders (e.g. global_batches) out of task listing/name checks.
        if not os.path.isfile(meta_path):
            continue
        yield tid, tdir, meta_path


def task_name_exists(name, exclude_id=None):
    for tid, _tdir, meta_path in _iter_task_dirs():
        if exclude_id and tid == exclude_id:
            continue
        tname = tid
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                tname = json.load(f).get("name", tid)
        except Exception:
            tname = tid
        if tname == name:
            return True
    return False


def load_task_context(task_id: str) -> dict:
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    meta_path = os.path.join(task_dir, "meta.json")
    task = {"id": task_id}
    default_output_path = build_task_output_path(task_id)
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as file_obj:
                meta = json.load(file_obj)
            task.update(
                {
                    "name": meta.get("name", task_id),
                    "description": meta.get("description", ""),
                    "creator": meta.get("creator", "") or "",
                    "nas_path": meta.get("nas_path", "") or "",
                    "output_path": meta.get("output_path", "") or default_output_path,
                }
            )
        except Exception:
            pass
    return task


def get_creator_work_id(meta: dict) -> str:
    creator_work_id = (meta.get("creator_work_id") or "").strip()
    if creator_work_id:
        return creator_work_id
    creator = (meta.get("creator") or "").strip()
    if creator:
        return creator.split()[0]
    return ""


def can_delete_task(meta: dict) -> bool:
    if not current_app.config.get("AUTH_ENABLED", True):
        return True
    if not current_user or not getattr(current_user, "is_authenticated", False):
        return False
    if user_has_role(current_user.id, ROLE_ADMIN):
        return True
    creator_work_id = get_creator_work_id(meta)
    return bool(creator_work_id) and current_user.work_id == creator_work_id


def gather_available_files(files_dir):
    mapping = {"docx": [], "pdf": [], "zip": [], "dir": [], "path": [], "image": []}
    for rel in list_files(files_dir):
        if is_ignored_source_file(rel):
            continue
        ext = os.path.splitext(rel)[1].lower()
        if ext == ".docx":
            mapping["docx"].append(rel)
        elif ext == ".pdf":
            mapping["pdf"].append(rel)
        elif ext == ".zip":
            mapping["zip"].append(rel)
        elif ext in ALLOWED_IMAGE:
            mapping["image"].append(rel)
    dirs = list_dirs(files_dir)
    dirs.insert(0, ".")
    mapping["dir"] = dirs
    mapping["path"] = sorted(set(mapping["docx"] + mapping["pdf"] + mapping["zip"] + mapping["image"] + dirs), key=str.lower)
    return mapping


def list_tasks():
    task_list = []
    existing_task_ids: set[str] = set()
    try:
        existing_task_ids = {
            str(task_id).strip()
            for (task_id,) in db.session.query(TaskRecord.id).all()
            if str(task_id or "").strip()
        }
    except Exception as exc:
        db.session.rollback()
        record_system_error(
            "task.list_existing_ids",
            "Failed to load existing task ids from DB",
            exc=exc,
        )
        current_app.logger.exception("Failed to load existing task ids from DB")
    for tid, tdir, meta_path in _iter_task_dirs():
        name = tid
        description = ""
        created = None
        creator = ""
        creator_work_id = ""
        last_editor = ""
        last_edited = ""
        nas_path = ""
        output_path = ""
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
                name = meta.get("name", tid)
                description = meta.get("description", "")
                created = meta.get("created")
                creator = meta.get("creator", "") or ""
                creator_work_id = meta.get("creator_work_id", "") or ""
                last_editor = meta.get("last_editor", "") or ""
                last_edited = meta.get("last_edited", "") or ""
                nas_path = meta.get("nas_path", "") or ""
                output_path = meta.get("output_path", "") or build_task_output_path(tid)
        except Exception:
            pass
        if not created:
            created = datetime.fromtimestamp(os.path.getmtime(tdir)).strftime("%Y-%m-%d %H:%M")
        if not last_edited:
            last_edited = created
        if not last_editor:
            last_editor = creator
        if tid not in existing_task_ids:
            record_task_in_db(
                tid,
                name=name,
                description=description or None,
                creator=creator or None,
                nas_path=nas_path or None,
                output_path=output_path or build_task_output_path(tid),
                created_at=_parse_task_created_at(created),
            )
            existing_task_ids.add(tid)
        task_list.append(
            {
                "id": tid,
                "name": name,
                "description": description,
                "created": created,
                "creator": creator,
                "creator_work_id": creator_work_id,
                "last_editor": last_editor,
                "last_edited": last_edited,
                "nas_path": nas_path,
                "output_path": output_path,
                "source_sync_status": meta.get("source_sync_status", "") or "",
                "source_sync_error": meta.get("source_sync_error", "") or "",
                "source_sync_job_id": meta.get("source_sync_job_id", "") or "",
                "source_sync_file_count": meta.get("source_sync_file_count"),
            }
        )
    task_list.sort(key=lambda x: x["created"], reverse=True)
    return task_list


def init_task_store(app) -> None:
    if not auto_schema_management_enabled(app):
        app.logger.info("Skipping task schema bootstrap because AUTO_SCHEMA_MANAGEMENT is disabled.")
        return
    with app.app_context():
        try:
            ensure_task_schema()
        except Exception as exc:
            db.session.rollback()
            record_system_error(
                "task.init_schema",
                "Task schema initialization failed",
                exc=exc,
            )
            app.logger.exception("Task schema initialization failed")


def record_task_in_db(
    task_id: str,
    name: str | None = None,
    description: str | None = None,
    creator: str | None = None,
    nas_path: str | None = None,
    output_path: str | None = None,
    created_at: datetime | None = None,
    raise_on_error: bool = False,
) -> bool:
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
        if output_path is not None:
            task.output_path = output_path
        if created_at and not task.created_at:
            task.created_at = created_at
        commit_session()
        return True
    except Exception as exc:
        db.session.rollback()
        record_system_error(
            "task.record_db",
            "Failed to record task in DB",
            exc=exc,
            task_id=task_id,
            detail={"task_name": name or task_id},
        )
        current_app.logger.exception("Failed to record task in DB")
        if raise_on_error:
            raise
        return False


def delete_task_record(task_id: str) -> None:
    try:
        task = db.session.get(TaskRecord, task_id)
        if task:
            db.session.delete(task)
            commit_session()
    except Exception as exc:
        db.session.rollback()
        record_system_error(
            "task.delete_db",
            "Failed to delete task record",
            exc=exc,
            task_id=task_id,
        )
        current_app.logger.exception("Failed to delete task record")
