from __future__ import annotations

import os
import re
from typing import Optional

from flask import current_app
from sqlalchemy import or_

from app.utils import parse_bool
from modules.auth_models import db
from modules.nas_models import NasRoot, ensure_schema as ensure_nas_schema

def load_allowed_roots_from_env():
    roots = []
    raw = os.environ.get("TASK_ALLOWED_ROOTS", "")
    for entry in raw.split(os.path.pathsep):
        candidate = entry.strip()
        if not candidate:
            continue
        abs_path = os.path.abspath(candidate)
        if os.path.isdir(abs_path):
            roots.append(abs_path)
    return roots

def ensure_allowed_roots_loaded():
    if current_app.config.get("ALLOWED_SOURCE_ROOTS"):
        return
    roots = get_configured_nas_roots()
    if roots:
        current_app.config["ALLOWED_SOURCE_ROOTS"] = list(roots)

def normalize_relative_path(raw_path: str, allow_recursive: bool) -> str:
    if not raw_path or not raw_path.strip():
        raise ValueError("請提供要匯入的檔案或資料夾路徑")
    cleaned = raw_path.strip().replace("\\", "/")
    # Make POSIX-style absolute paths invalid on Windows too (e.g. "/abs/path").
    if cleaned.startswith("/"):
        raise ValueError("路徑不可為絕對路徑，請填寫相對於允許根目錄的路徑")
    if os.path.isabs(cleaned):
        raise ValueError("路徑不可為絕對路徑，請填寫相對於允許根目錄的路徑")
    norm_rel = os.path.normpath(cleaned)
    if norm_rel.startswith(".."):
        raise ValueError("路徑不可包含 .. 或跳脫允許的根目錄")
    if not allow_recursive and os.sep in norm_rel:
        raise ValueError("目前僅允許存取根層級的項目")
    return norm_rel

def validate_nas_path(raw_path: str, allowed_roots=None, allow_recursive=None):
    allow_recursive = (
        current_app.config.get("NAS_ALLOW_RECURSIVE", True)
        if allow_recursive is None
        else allow_recursive
    )
    norm_rel = normalize_relative_path(raw_path, allow_recursive)
    allowed_roots = allowed_roots or get_configured_nas_roots()
    if not allowed_roots:
        raise ValueError("尚未設定允許的根目錄，請聯絡系統管理員")
    for root in allowed_roots:
        root_abs = os.path.abspath(root)
        candidate = os.path.abspath(os.path.join(root_abs, norm_rel))
        try:
            if os.path.commonpath([root_abs, candidate]) != root_abs:
                continue
        except ValueError:
            continue
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("找不到指定的路徑，或不在允許的根目錄內，請重新檢查輸入的路徑是否符合格式")

def _get_env_platform() -> tuple[str, str]:
    env = current_app.config.get("APP_ENV") or "development"
    platform = "windows" if os.name == "nt" else "linux"
    return env, platform


def _guess_platform(path: str) -> Optional[str]:
    if not path:
        return None
    if path.startswith("\\") or re.match(r"^[A-Za-z]:\\", path):
        return "windows"
    if path.startswith("/"):
        return "linux"
    return None


def get_configured_nas_roots() -> list[str]:
    try:
        env, platform = _get_env_platform()
        roots = (
            db.session.query(NasRoot.path)
            .filter(
                NasRoot.env == env,
                NasRoot.platform == platform,
                or_(NasRoot.active == True, NasRoot.active.is_(None)),  # noqa: E712
            )
            .order_by(NasRoot.id.asc())
            .all()
        )
        db_roots = [path for (path,) in roots]
        if db_roots:
            return db_roots
        roots = current_app.config.get("NAS_ALLOWED_ROOTS") or current_app.config.get("ALLOWED_SOURCE_ROOTS", [])
        return list(roots) if roots else []
    except Exception:
        roots = current_app.config.get("NAS_ALLOWED_ROOTS") or current_app.config.get("ALLOWED_SOURCE_ROOTS", [])
        return list(roots) if roots else []

def resolve_nas_path_in_root(raw_path: str, root_index: int, allow_recursive=None) -> str:
    allow_recursive = (
        current_app.config.get("NAS_ALLOW_RECURSIVE", True)
        if allow_recursive is None
        else allow_recursive
    )
    roots = get_configured_nas_roots()
    if not roots:
        raise ValueError("NAS roots are not configured")
    if root_index < 0 or root_index >= len(roots):
        raise ValueError("Invalid NAS root index")

    root_abs = os.path.abspath(roots[root_index])
    norm_rel = normalize_relative_path(raw_path, allow_recursive)
    candidate = os.path.abspath(os.path.join(root_abs, norm_rel))
    try:
        if os.path.commonpath([root_abs, candidate]) != root_abs:
            raise ValueError("Path escapes the allowed NAS root")
    except ValueError:
        raise ValueError("Invalid path")

    if os.path.exists(candidate):
        return candidate
    raise FileNotFoundError("Path does not exist in the selected NAS root")

def add_nas_root(raw_path: str):
    """Add a NAS root to the database."""
    if not raw_path or not raw_path.strip():
        raise ValueError("請輸入 NAS 根目錄")
    abs_path = os.path.abspath(raw_path.strip())
    if not os.path.isdir(abs_path):
        raise FileNotFoundError("NAS 根目錄不存在或不是資料夾")
    env, platform = _get_env_platform()
    existing = NasRoot.query.filter_by(path=abs_path, env=env, platform=platform).first()
    if existing:
        return False
    try:
        db.session.add(NasRoot(path=abs_path, env=env, platform=platform, active=True))
        db.session.commit()
        for key in ("ALLOWED_NAS_ROOTS", "NAS_ALLOWED_ROOTS"):
            current_app.config.setdefault(key, [])
            if abs_path not in current_app.config[key]:
                current_app.config[key].append(abs_path)
        return True
    except Exception:
        db.session.rollback()
        raise

def remove_nas_root(abs_path: str):
    if not abs_path:
        raise ValueError("???????")
    env, platform = _get_env_platform()
    existing = NasRoot.query.filter_by(path=abs_path, env=env, platform=platform).first()
    if not existing:
        return False
    try:
        db.session.delete(existing)
        db.session.commit()
        for key in ("ALLOWED_NAS_ROOTS", "NAS_ALLOWED_ROOTS"):
            roots = current_app.config.get(key, [])
            if abs_path in roots:
                current_app.config[key] = [r for r in roots if r != abs_path]
        return True
    except Exception:
        db.session.rollback()
        raise

def resolve_nas_path(raw_path: str, allowed_roots=None, allow_recursive=None, root_index=None) -> str:
    if root_index is None or str(root_index).strip() == "":
        return validate_nas_path(raw_path, allowed_roots=allowed_roots, allow_recursive=allow_recursive)
    try:
        root_index_int = int(root_index)
    except (TypeError, ValueError):
        raise ValueError("Invalid NAS root index")
    return resolve_nas_path_in_root(raw_path, root_index_int, allow_recursive=allow_recursive)


def init_nas_config(app) -> None:
    platform = "windows" if os.name == "nt" else "linux"
    platform_key = f"ALLOWED_NAS_ROOTS_{platform.upper()}"
    nas_roots_env = os.environ.get(platform_key) or os.environ.get("ALLOWED_NAS_ROOTS", "")
    nas_allowed_roots = [
        os.path.abspath(p)
        for p in nas_roots_env.split(os.pathsep)
        if p.strip()
    ]
    app.config.setdefault("ALLOWED_NAS_ROOTS", nas_allowed_roots)
    app.config.setdefault("NAS_ALLOWED_ROOTS", list(nas_allowed_roots))
    app.config.setdefault(
        "NAS_ALLOW_RECURSIVE",
        parse_bool(os.environ.get("NAS_ALLOW_RECURSIVE"), True),
    )
    max_copy_size_mb = os.environ.get("NAS_MAX_COPY_FILE_SIZE_MB")
    try:
        app.config["NAS_MAX_COPY_FILE_SIZE"] = (
            int(max_copy_size_mb) * 1024 * 1024 if max_copy_size_mb else 500 * 1024 * 1024
        )
    except ValueError:
        app.config["NAS_MAX_COPY_FILE_SIZE"] = 500 * 1024 * 1024

    with app.app_context():
        try:
            ensure_nas_schema()
            env, platform = _get_env_platform()
            changed = False

            for root in NasRoot.query.all():
                guess = _guess_platform(root.path)
                if guess and root.platform != guess:
                    root.platform = guess
                    changed = True

            for root in NasRoot.query.filter(or_(NasRoot.env.is_(None), NasRoot.env == "")).all():
                root.env = env
                changed = True

            if changed:
                db.session.commit()

            existing = NasRoot.query.filter_by(env=env, platform=platform).first()
            if nas_allowed_roots and not existing:
                for root in nas_allowed_roots:
                    if os.path.isdir(root):
                        db.session.add(NasRoot(path=root, env=env, platform=platform, active=True))
                db.session.commit()
        except Exception:
            db.session.rollback()
            app.logger.exception("Failed to initialize NAS roots")


def list_nas_dirs(root_index: Optional[int], rel_path_raw: str) -> tuple[dict, int]:
    """Return NAS directory listing payload and HTTP status."""
    if root_index is None:
        return {"error": "root_index is required"}, 400

    allow_recursive = current_app.config.get("NAS_ALLOW_RECURSIVE", True)

    try:
        roots = get_configured_nas_roots()
        if not roots:
            return {"error": "NAS roots are not configured"}, 400
        if root_index < 0 or root_index >= len(roots):
            return {"error": "Invalid NAS root index"}, 400

        root_abs = os.path.abspath(roots[root_index])
        if rel_path_raw in {"", ".", "/"}:
            abs_dir = root_abs
            rel_path = ""
        else:
            rel_path = normalize_relative_path(rel_path_raw, allow_recursive=allow_recursive).replace("\\", "/")
            abs_dir = resolve_nas_path_in_root(rel_path, root_index, allow_recursive=allow_recursive)

        if not os.path.isdir(abs_dir):
            return {"error": "Path is not a directory"}, 400

        dirs = []
        for name in sorted(os.listdir(abs_dir), key=str.lower):
            full = os.path.join(abs_dir, name)
            if os.path.isdir(full):
                child_rel = f"{rel_path}/{name}" if rel_path else name
                dirs.append({"name": name, "path": child_rel.replace("\\", "/")})

        parent = None
        if rel_path:
            parent_parts = rel_path.split("/")
            parent = "/".join(parent_parts[:-1]) if len(parent_parts) > 1 else ""

        return (
            {
                "root_index": root_index,
                "path": rel_path,
                "parent": parent,
                "dirs": dirs,
                "allow_recursive": bool(allow_recursive),
            },
            200,
        )
    except (ValueError, FileNotFoundError) as exc:
        return {"error": str(exc)}, 400
    except PermissionError:
        return {"error": "Permission denied"}, 403
