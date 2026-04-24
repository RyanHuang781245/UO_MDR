from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from flask import current_app

from app.extensions import db
from app.models.auth import commit_session
from app.models.standard_update import (
    HarmonisedReleaseRecord,
    StandardUpdateRecord,
    ensure_schema as ensure_standard_update_schema,
)
from app.services.task_service import deduplicate_name, list_files
from app.services.user_context_service import get_actor_info

ALLOWED_WORD_EXTENSIONS = {".docx"}
ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"}
STATUS_DRAFT = "draft"
STATUS_READY = "ready"
STATUS_PREVIEWED = "previewed"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
HARMONISED_SOURCE_SYSTEM = "system"
HARMONISED_SOURCE_CUSTOM = "custom"
_INVALID_UPLOAD_FILENAME_CHARS = '\\/:*?"<>|'
_WINDOWS_RESERVED_FILE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def _safe_uploaded_filename(filename: str, default_stem: str = "upload") -> str:
    raw_name = os.path.basename((filename or "").replace("\\", "/")).strip()
    cleaned = "".join(
        "_" if (ord(ch) < 32 or ch in _INVALID_UPLOAD_FILENAME_CHARS) else ch
        for ch in raw_name
    ).strip().strip(".")
    if cleaned in {"", ".", ".."}:
        cleaned = default_stem

    stem, ext = os.path.splitext(cleaned)
    if not stem:
        stem = default_stem
    if stem.upper() in _WINDOWS_RESERVED_FILE_NAMES:
        stem = f"_{stem}"
    return f"{stem}{ext}" if ext else stem


def init_standard_update_store(app) -> None:
    with app.app_context():
        try:
            ensure_standard_update_schema()
        except Exception:
            db.session.rollback()
            app.logger.exception("Standard update schema initialization failed")


def standard_update_root() -> str:
    return current_app.config["STANDARD_UPDATE_FOLDER"]


def _ensure_storage_available(path: str | os.PathLike) -> tuple[bool, str]:
    target = Path(path)
    try:
        target.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target, prefix=".write-test-", delete=True):
            pass
        return True, "主要存取路徑可讀寫"
    except Exception as exc:
        return False, str(exc)


def resolve_harmonised_reference_storage(base_dir: str | os.PathLike, configured_path: str) -> dict:
    fallback_root = str(Path(base_dir) / "harmonised_store")
    configured_root = (configured_path or "").strip()

    if configured_root:
        ok, detail = _ensure_storage_available(configured_root)
        if ok:
            return {
                "configured_root": configured_root,
                "effective_root": configured_root,
                "fallback_root": fallback_root,
                "storage_mode": "primary",
                "primary_storage_ok": True,
                "status_message": "主要存取路徑可讀寫",
            }

        fallback_ok, fallback_detail = _ensure_storage_available(fallback_root)
        if fallback_ok:
            return {
                "configured_root": configured_root,
                "effective_root": fallback_root,
                "fallback_root": fallback_root,
                "storage_mode": "fallback",
                "primary_storage_ok": False,
                "status_message": f"主要存取路徑不可用：{detail}",
            }
        raise RuntimeError(
            f"主要與備援儲存路徑都不可用: {configured_root}, {fallback_root} ({fallback_detail})"
        )

    ok, detail = _ensure_storage_available(fallback_root)
    if ok:
        return {
            "configured_root": "",
            "effective_root": fallback_root,
            "fallback_root": fallback_root,
            "storage_mode": "default",
            "primary_storage_ok": False,
            "status_message": "未設定主要存取路徑",
        }
    raise RuntimeError(f"本機備援目錄不可用: {fallback_root} ({detail})")


def harmonised_reference_root() -> str:
    return current_app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"]


def harmonised_reference_configured_root() -> str:
    return (current_app.config.get("REGULATION_EU_2017_745_REFERENCE_FOLDER_CONFIGURED") or "").strip()


def harmonised_reference_fallback_root() -> str:
    return current_app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER_FALLBACK"]


def harmonised_reference_storage_mode() -> str:
    return (current_app.config.get("REGULATION_EU_2017_745_REFERENCE_STORAGE_MODE") or "").strip()


def harmonised_reference_status_message() -> str:
    return (current_app.config.get("REGULATION_EU_2017_745_REFERENCE_STATUS_MESSAGE") or "").strip()


def test_harmonised_reference_storage(path: str) -> tuple[bool, str]:
    target = (path or "").strip()
    if not target:
        return False, "未設定主要存取路徑"
    ok, detail = _ensure_storage_available(target)
    if ok:
        return True, "主要存取路徑可讀寫"
    return False, f"主要存取路徑不可用：{detail}"


def standard_update_dir(task_id: str) -> str:
    return os.path.join(standard_update_root(), task_id)


def standard_update_input_dir(task_id: str) -> str:
    return os.path.join(standard_update_dir(task_id), "input")


def standard_update_input_kind_dir(task_id: str, kind: str) -> str:
    folder = {
        "word": "word",
        "standard_excel": "standard_excel",
        "regulation": "regulation",
        "harmonised": "harmonised",
    }.get(kind, kind)
    return os.path.join(standard_update_input_dir(task_id), folder)


def standard_update_output_dir(task_id: str) -> str:
    return os.path.join(standard_update_dir(task_id), "output")


def standard_update_meta_path(task_id: str) -> str:
    return os.path.join(standard_update_dir(task_id), "meta.json")


def standard_update_reference_dir(task_id: str) -> str:
    return os.path.join(standard_update_dir(task_id), "reference")


def normalize_harmonised_source_mode(value: str | None) -> str:
    return HARMONISED_SOURCE_CUSTOM if (value or "").strip().lower() == HARMONISED_SOURCE_CUSTOM else HARMONISED_SOURCE_SYSTEM


def _snapshot_harmonised_release_for_task(task_id: str, release: dict) -> dict:
    source_path = os.path.abspath((release or {}).get("path", "") or "")
    if not source_path or not os.path.isfile(source_path):
        return {}
    ext = Path(source_path).suffix.lower()
    if ext not in ALLOWED_EXCEL_EXTENSIONS:
        return {}

    reference_dir = standard_update_reference_dir(task_id)
    os.makedirs(reference_dir, exist_ok=True)
    target_name = os.path.basename(source_path)
    target_path = os.path.join(reference_dir, target_name)

    shutil.copy2(source_path, target_path)
    for entry in os.listdir(reference_dir):
        candidate = os.path.join(reference_dir, entry)
        if candidate == target_path or not os.path.isfile(candidate):
            continue
        if Path(candidate).suffix.lower() not in ALLOWED_EXCEL_EXTENSIONS:
            continue
        try:
            os.remove(candidate)
        except OSError:
            current_app.logger.warning("Failed to remove stale harmonised snapshot: %s", candidate)

    stat = os.stat(target_path)
    return {
        "file_name": os.path.basename(target_path),
        "path": target_path,
        "version_label": (release or {}).get("version_label", "") or datetime.fromtimestamp(stat.st_mtime).strftime("%Y%m%d-%H%M"),
        "downloaded_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
        "source_url": (release or {}).get("source_url", "") or "",
    }


def create_standard_update(name: str, description: str = "", *, harmonised_source_mode: str = HARMONISED_SOURCE_SYSTEM) -> str:
    task_id = str(uuid.uuid4())[:8]
    task_dir = standard_update_dir(task_id)
    input_dir = standard_update_input_dir(task_id)
    output_dir = standard_update_output_dir(task_id)
    os.makedirs(input_dir, exist_ok=False)
    os.makedirs(standard_update_input_kind_dir(task_id, "word"), exist_ok=True)
    os.makedirs(standard_update_input_kind_dir(task_id, "standard_excel"), exist_ok=True)
    os.makedirs(standard_update_input_kind_dir(task_id, "regulation"), exist_ok=True)
    os.makedirs(standard_update_input_kind_dir(task_id, "harmonised"), exist_ok=True)
    os.makedirs(standard_update_reference_dir(task_id), exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    work_id, creator_name = get_actor_info()
    now = datetime.now()
    resolved_harmonised_source_mode = normalize_harmonised_source_mode(harmonised_source_mode)
    harmonised_release = (
        _snapshot_harmonised_release_for_task(task_id, sync_harmonised_release_snapshot())
        if resolved_harmonised_source_mode == HARMONISED_SOURCE_SYSTEM
        else {}
    )
    meta = {
        "id": task_id,
        "name": name,
        "description": description,
        "creator_name": creator_name,
        "creator_work_id": work_id,
        "created": now.strftime("%Y-%m-%d %H:%M"),
        "updated": now.strftime("%Y-%m-%d %H:%M"),
        "status": STATUS_DRAFT,
        "harmonised_source_mode": resolved_harmonised_source_mode,
        "word_file_path": "",
        "standard_excel_path": "",
        "regulation_excel_path": "",
        "harmonised_snapshot_path": harmonised_release.get("path", ""),
        "harmonised_snapshot_version": harmonised_release.get("version_label", ""),
        "custom_harmonised_path": "",
        "custom_harmonised_version": "",
        "last_output_path": "",
        "last_run_at": "",
        "last_run_status": "",
    }
    with open(standard_update_meta_path(task_id), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    try:
        record = StandardUpdateRecord(
            id=task_id,
            name=name,
            description=description or None,
            creator_name=creator_name or None,
            creator_work_id=work_id or None,
            status=STATUS_DRAFT,
            harmonised_source_mode=resolved_harmonised_source_mode,
            harmonised_snapshot_path=harmonised_release.get("path") or None,
            harmonised_snapshot_version=harmonised_release.get("version_label") or None,
            custom_harmonised_path=None,
            custom_harmonised_version=None,
            created_at=now,
            updated_at=now,
        )
        db.session.add(record)
        commit_session()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to record standard update task in DB")
    return task_id


def list_standard_updates() -> list[dict]:
    items: list[dict] = []
    root = standard_update_root()
    if not os.path.isdir(root):
        return items
    for task_id in os.listdir(root):
        task_dir = standard_update_dir(task_id)
        if not os.path.isdir(task_dir):
            continue
        meta = load_standard_update(task_id)
        if not meta:
            continue
        items.append(meta)
    items.sort(key=lambda item: item.get("created", ""), reverse=True)
    return items


def load_standard_update(task_id: str) -> dict:
    meta_path = standard_update_meta_path(task_id)
    if not os.path.isfile(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh)
    except Exception:
        current_app.logger.exception("Failed to load standard update metadata")
        return {}
    meta.setdefault("id", task_id)
    meta.setdefault("name", task_id)
    meta.setdefault("description", "")
    meta.setdefault("creator_name", "")
    meta.setdefault("creator_work_id", "")
    meta.setdefault("created", "")
    meta.setdefault("updated", meta.get("created", ""))
    meta.setdefault("status", STATUS_DRAFT)
    meta["harmonised_source_mode"] = normalize_harmonised_source_mode(meta.get("harmonised_source_mode"))
    meta.setdefault("word_file_path", "")
    meta.setdefault("standard_excel_path", "")
    meta.setdefault("regulation_excel_path", "")
    meta.setdefault("harmonised_snapshot_path", "")
    meta.setdefault("harmonised_snapshot_version", "")
    meta.setdefault("custom_harmonised_path", "")
    meta.setdefault("custom_harmonised_version", "")
    meta.setdefault("last_output_path", "")
    meta.setdefault("last_run_at", "")
    meta.setdefault("last_run_status", "")
    meta["input_dir"] = standard_update_input_dir(task_id)
    meta["output_dir"] = standard_update_output_dir(task_id)
    meta["has_locked_harmonised"] = bool(meta.get("harmonised_snapshot_path") and os.path.isfile(meta["harmonised_snapshot_path"]))
    return meta


def save_standard_update(task_id: str, meta: dict) -> None:
    meta["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(standard_update_meta_path(task_id), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    try:
        record = db.session.get(StandardUpdateRecord, task_id)
        if not record:
            record = StandardUpdateRecord(id=task_id, name=meta.get("name") or task_id)
            db.session.add(record)
        record.name = meta.get("name") or task_id
        record.description = meta.get("description") or None
        record.creator_name = meta.get("creator_name") or None
        record.creator_work_id = meta.get("creator_work_id") or None
        record.status = meta.get("status") or STATUS_DRAFT
        record.harmonised_source_mode = normalize_harmonised_source_mode(meta.get("harmonised_source_mode"))
        record.word_file_path = meta.get("word_file_path") or None
        record.standard_excel_path = meta.get("standard_excel_path") or None
        record.harmonised_snapshot_path = meta.get("harmonised_snapshot_path") or None
        record.harmonised_snapshot_version = meta.get("harmonised_snapshot_version") or None
        record.custom_harmonised_path = meta.get("custom_harmonised_path") or None
        record.custom_harmonised_version = meta.get("custom_harmonised_version") or None
        record.last_output_path = meta.get("last_output_path") or None
        record.last_run_status = meta.get("last_run_status") or None
        record.updated_at = datetime.now()
        if meta.get("last_run_at"):
            try:
                record.last_run_at = datetime.strptime(meta["last_run_at"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        commit_session()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to persist standard update metadata")


def delete_standard_update(task_id: str) -> None:
    shutil.rmtree(standard_update_dir(task_id), ignore_errors=True)
    try:
        record = db.session.get(StandardUpdateRecord, task_id)
        if record:
            db.session.delete(record)
            commit_session()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to delete standard update DB record")


def standard_update_name_exists(name: str, exclude_id: str | None = None) -> bool:
    lowered = (name or "").strip()
    if not lowered:
        return False
    for item in list_standard_updates():
        if exclude_id and item.get("id") == exclude_id:
            continue
        if (item.get("name") or "").strip() == lowered:
            return True
    return False


def save_uploaded_input(task_id: str, upload, *, kind: str) -> str:
    if not upload or not getattr(upload, "filename", ""):
        raise ValueError("缺少上傳檔案")
    ext = Path(upload.filename).suffix.lower()
    allowed_exts = ALLOWED_WORD_EXTENSIONS if kind == "word" else ALLOWED_EXCEL_EXTENSIONS
    if ext not in allowed_exts:
        raise ValueError("檔案類型不支援")
    normalized_kind = (
        "word"
        if kind == "word"
        else ("regulation" if kind == "regulation" else ("harmonised" if kind == "harmonised" else "standard_excel"))
    )
    input_dir = standard_update_input_kind_dir(task_id, normalized_kind)
    os.makedirs(input_dir, exist_ok=True)
    safe_name = _safe_uploaded_filename(upload.filename, default_stem="upload") or ("upload" + ext)
    final_name = deduplicate_name(input_dir, safe_name)
    output_path = os.path.join(input_dir, final_name)
    upload.save(output_path)
    return final_name


def available_input_files(task_id: str) -> tuple[list[str], list[str]]:
    word_dir = standard_update_input_kind_dir(task_id, "word")
    excel_dir = standard_update_input_kind_dir(task_id, "standard_excel")
    word_options = list_files(word_dir) if os.path.isdir(word_dir) else []
    excel_options = list_files(excel_dir) if os.path.isdir(excel_dir) else []
    word_options = [rel for rel in word_options if Path(rel).suffix.lower() in ALLOWED_WORD_EXTENSIONS]
    excel_options = [rel for rel in excel_options if Path(rel).suffix.lower() in ALLOWED_EXCEL_EXTENSIONS]
    return word_options, excel_options


def input_file_history(task_id: str, *, kind: str, current_file: str = "") -> list[dict]:
    normalized_kind = (
        "word"
        if kind == "word"
        else ("regulation" if kind == "regulation" else ("harmonised" if kind == "harmonised" else "standard_excel"))
    )
    input_dir = standard_update_input_kind_dir(task_id, normalized_kind)
    allowed_exts = ALLOWED_WORD_EXTENSIONS if normalized_kind == "word" else ALLOWED_EXCEL_EXTENSIONS
    items: list[dict] = []
    if os.path.isdir(input_dir):
        for rel_path in list_files(input_dir):
            abs_path = os.path.join(input_dir, rel_path.replace("/", os.sep))
            if Path(rel_path).suffix.lower() not in allowed_exts or not os.path.isfile(abs_path):
                continue
            stat = os.stat(abs_path)
            items.append(
                {
                    "name": rel_path,
                    "uploaded_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "is_current": rel_path == (current_file or ""),
                }
            )
    elif current_file:
        legacy_path = os.path.join(standard_update_input_dir(task_id), current_file.replace("/", os.sep))
        if os.path.isfile(legacy_path):
            stat = os.stat(legacy_path)
            items.append(
                {
                    "name": current_file,
                    "uploaded_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "is_current": True,
                }
            )
    items.sort(key=lambda item: (not item["is_current"], item["uploaded_at"]), reverse=False)
    items.sort(key=lambda item: item["uploaded_at"], reverse=True)
    return items


def delete_input_file(task_id: str, *, kind: str, rel_path: str) -> dict:
    meta = load_standard_update(task_id)
    if not meta:
        raise FileNotFoundError("找不到標準更新任務")

    normalized_kind = "word" if kind == "word" else ("regulation" if kind == "regulation" else "standard_excel")
    allowed_exts = ALLOWED_WORD_EXTENSIONS if normalized_kind == "word" else ALLOWED_EXCEL_EXTENSIONS
    target_path = safe_standard_update_file(task_id, rel_path, allowed_exts, kind=normalized_kind)
    os.remove(target_path)

    remaining = input_file_history(
        task_id,
        kind=normalized_kind,
        current_file="",
    )
    replacement = remaining[0]["name"] if remaining else ""
    if normalized_kind == "word":
        if meta.get("word_file_path") == rel_path:
            meta["word_file_path"] = replacement
    elif normalized_kind == "standard_excel":
        if meta.get("standard_excel_path") == rel_path:
            meta["standard_excel_path"] = replacement
    elif normalized_kind == "harmonised":
        if meta.get("custom_harmonised_path") == rel_path:
            meta["custom_harmonised_path"] = replacement
            if replacement:
                replacement_abs = safe_standard_update_file(task_id, replacement, ALLOWED_EXCEL_EXTENSIONS, kind="harmonised")
                replacement_stat = os.stat(replacement_abs)
                meta["custom_harmonised_version"] = datetime.fromtimestamp(replacement_stat.st_mtime).strftime("%Y%m%d-%H%M")
            else:
                meta["custom_harmonised_version"] = ""
    else:
        if meta.get("regulation_excel_path") == rel_path:
            meta["regulation_excel_path"] = replacement

    if meta.get("word_file_path") and meta.get("standard_excel_path"):
        meta["status"] = STATUS_READY
    elif meta.get("status") != STATUS_FAILED:
        meta["status"] = STATUS_DRAFT
    save_standard_update(task_id, meta)
    return meta


def safe_standard_update_file(task_id: str, rel_path: str, allowed_exts: set[str], *, kind: str | None = None) -> str:
    normalized = os.path.normpath((rel_path or "").replace("/", os.sep))
    if not normalized or normalized.startswith("..") or os.path.isabs(normalized):
        raise ValueError("檔案路徑不合法")
    candidate_dirs = []
    if kind:
        candidate_dirs.append(os.path.abspath(standard_update_input_kind_dir(task_id, kind)))
    candidate_dirs.append(os.path.abspath(standard_update_input_dir(task_id)))
    for base_dir in candidate_dirs:
        abs_path = os.path.abspath(os.path.join(base_dir, normalized))
        try:
            if os.path.commonpath([base_dir, abs_path]) != base_dir:
                continue
        except ValueError:
            continue
        ext = Path(abs_path).suffix.lower()
        if ext not in allowed_exts:
            continue
        if os.path.isfile(abs_path):
            return abs_path
    raise FileNotFoundError("找不到指定檔案")


def get_active_harmonised_release() -> dict:
    folder = harmonised_reference_root()
    os.makedirs(folder, exist_ok=True)

    record = (
        HarmonisedReleaseRecord.query.filter_by(is_active=True)
        .order_by(HarmonisedReleaseRecord.downloaded_at.desc(), HarmonisedReleaseRecord.id.desc())
        .first()
    )
    if record and record.nas_path and os.path.isfile(record.nas_path):
        return {
            "id": record.id,
            "file_name": record.file_name or os.path.basename(record.nas_path),
            "path": record.nas_path,
            "version_label": record.version_label or "",
            "downloaded_at": record.downloaded_at.strftime("%Y-%m-%d %H:%M") if record.downloaded_at else "",
            "source_url": record.source_url or "",
        }

    return get_latest_harmonised_release_in_dir(folder)


def get_latest_harmonised_release_in_dir(folder: str) -> dict:
    target_folder = (folder or "").strip()
    if not target_folder:
        return {}

    os.makedirs(target_folder, exist_ok=True)

    candidates = []
    for entry in os.listdir(target_folder):
        abs_path = os.path.join(target_folder, entry)
        if os.path.isfile(abs_path) and Path(entry).suffix.lower() in ALLOWED_EXCEL_EXTENSIONS:
            candidates.append(abs_path)
    if not candidates:
        return {}

    latest = max(candidates, key=os.path.getmtime)
    stat = os.stat(latest)
    version = datetime.fromtimestamp(stat.st_mtime).strftime("%Y%m%d-%H%M")
    return {
        "id": None,
        "file_name": os.path.basename(latest),
        "path": latest,
        "version_label": version,
        "downloaded_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
        "source_url": "",
    }


def activate_harmonised_release(
    file_path: str,
    *,
    source_url: str = "",
    downloaded_at: datetime | None = None,
    version_label: str = "",
    reuse_existing: bool = True,
) -> dict:
    abs_path = os.path.abspath(file_path or "")
    if not abs_path or not os.path.isfile(abs_path):
        return {}
    if Path(abs_path).suffix.lower() not in ALLOWED_EXCEL_EXTENSIONS:
        return {}

    try:
        stat = os.stat(abs_path)
    except OSError:
        return {}

    resolved_downloaded_at = downloaded_at or datetime.fromtimestamp(stat.st_mtime)
    resolved_version = version_label or resolved_downloaded_at.strftime("%Y%m%d-%H%M")
    try:
        checksum = _sha1_file(abs_path)
    except OSError:
        checksum = ""

    try:
        HarmonisedReleaseRecord.query.update({"is_active": False})
        record = None
        if reuse_existing:
            record = HarmonisedReleaseRecord.query.filter_by(nas_path=abs_path).first()
        if record:
            current_app.logger.info(
                "Reusing harmonised release record id=%s path=%s reuse_existing=%s",
                record.id,
                abs_path,
                reuse_existing,
            )
            record.source_url = source_url or record.source_url
            record.file_name = os.path.basename(abs_path)
            record.nas_path = abs_path
            record.version_label = resolved_version
            record.checksum = checksum or record.checksum
            record.is_active = True
            record.download_status = "available"
            record.error_message = None
            record.downloaded_at = resolved_downloaded_at
        else:
            current_app.logger.info(
                "Creating harmonised release record path=%s version=%s checksum=%s reuse_existing=%s",
                abs_path,
                resolved_version,
                checksum or "",
                reuse_existing,
            )
            record = HarmonisedReleaseRecord(
                source_url=source_url or None,
                file_name=os.path.basename(abs_path),
                nas_path=abs_path,
                version_label=resolved_version,
                checksum=checksum or None,
                is_active=True,
                download_status="available",
                downloaded_at=resolved_downloaded_at,
            )
            db.session.add(record)
        commit_session()
        current_app.logger.info(
            "Activated harmonised release record id=%s active_path=%s version=%s downloaded_at=%s",
            record.id,
            record.nas_path,
            record.version_label,
            record.downloaded_at,
        )
        return {
            "id": record.id,
            "file_name": record.file_name or os.path.basename(abs_path),
            "path": record.nas_path,
            "version_label": record.version_label or resolved_version,
            "downloaded_at": record.downloaded_at.strftime("%Y-%m-%d %H:%M") if record.downloaded_at else resolved_downloaded_at.strftime("%Y-%m-%d %H:%M"),
            "source_url": record.source_url or "",
        }
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Failed to activate harmonised release")
        return {}


def register_downloaded_harmonised_release(
    file_path: str,
    *,
    source_url: str = "",
    downloaded_at: datetime | None = None,
    version_label: str = "",
) -> dict:
    return activate_harmonised_release(
        file_path,
        source_url=source_url,
        downloaded_at=downloaded_at,
        version_label=version_label,
        reuse_existing=False,
    )


def sync_latest_harmonised_release_from_store() -> dict:
    folder = harmonised_reference_root()
    latest = get_latest_harmonised_release_in_dir(folder).get("path", "")
    if not latest:
        return {}
    return activate_harmonised_release(latest)


def get_locked_harmonised_release(meta: dict) -> dict:
    path = (meta or {}).get("harmonised_snapshot_path", "") or ""
    if not path or not os.path.isfile(path):
        return {}
    stat = os.stat(path)
    return {
        "file_name": os.path.basename(path),
        "path": path,
        "version_label": (meta or {}).get("harmonised_snapshot_version", "") or datetime.fromtimestamp(stat.st_mtime).strftime("%Y%m%d-%H%M"),
        "downloaded_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
        "source_mode": "system_locked",
    }


def get_task_harmonised_release(task_id: str, meta: dict | None = None) -> dict:
    payload = meta or load_standard_update(task_id)
    if not payload:
        return {}

    source_mode = normalize_harmonised_source_mode(payload.get("harmonised_source_mode"))

    custom_rel_path = (payload.get("custom_harmonised_path") or "").strip()
    if source_mode == HARMONISED_SOURCE_CUSTOM and custom_rel_path:
        try:
            custom_path = safe_standard_update_file(task_id, custom_rel_path, ALLOWED_EXCEL_EXTENSIONS, kind="harmonised")
            stat = os.stat(custom_path)
            return {
                "file_name": os.path.basename(custom_path),
                "path": custom_path,
                "version_label": (payload.get("custom_harmonised_version") or "").strip()
                or datetime.fromtimestamp(stat.st_mtime).strftime("%Y%m%d-%H%M"),
                "downloaded_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "source_mode": "task_custom",
            }
        except (FileNotFoundError, OSError, ValueError):
            payload["custom_harmonised_path"] = ""
            payload["custom_harmonised_version"] = ""
            save_standard_update(task_id, payload)

    if source_mode == HARMONISED_SOURCE_CUSTOM:
        return {}

    release = get_locked_harmonised_release(payload)
    if release:
        release["source_mode"] = "system_locked"
    return release


def sync_harmonised_release_snapshot() -> dict:
    return get_active_harmonised_release()


def _sha1_file(path: str) -> str:
    digest = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def lock_standard_update_to_latest_harmonised(task_id: str) -> dict:
    meta = load_standard_update(task_id)
    if not meta:
        return {}
    meta["harmonised_source_mode"] = HARMONISED_SOURCE_SYSTEM
    latest = _snapshot_harmonised_release_for_task(
        task_id,
        sync_harmonised_release_snapshot(),
    )
    if not latest.get("path"):
        return {}
    meta["harmonised_snapshot_path"] = latest.get("path", "")
    meta["harmonised_snapshot_version"] = latest.get("version_label", "")
    meta["custom_harmonised_path"] = ""
    meta["custom_harmonised_version"] = ""
    save_standard_update(task_id, meta)
    return meta
