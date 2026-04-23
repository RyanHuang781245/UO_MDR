from __future__ import annotations

import os
import tempfile
from pathlib import Path

from flask import Flask, render_template

from app.blueprints import register_blueprints
from app.config import CONFIG_MAP, BaseConfig
from app.extensions import db, ldap_manager, login_manager
from app.services import (
    auth_service,
    execution_service,
    mapping_metadata_service,
    nas_service,
    standard_update_service,
    system_service,
    task_service,
)
from modules.env_loader import load_dotenv_if_present


def _ensure_storage_available(path: str | os.PathLike) -> bool:
    target = Path(path)
    try:
        target.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target, prefix=".write-test-", delete=True):
            pass
        return True
    except Exception:
        return False


def _resolve_regulation_reference_root(base_dir: Path, configured_path: str) -> str:
    fallback_dir = base_dir / "harmonised_store"
    candidate = (configured_path or "").strip()
    if candidate and _ensure_storage_available(candidate):
        return candidate
    if candidate:
        fallback = str(fallback_dir)
        if _ensure_storage_available(fallback):
            return fallback
    return str(fallback_dir)


def create_app(config_name: str | None = None) -> Flask:
    base_dir = Path(__file__).resolve().parents[1]
    load_dotenv_if_present(str(base_dir))

    app = Flask(
        __name__,
        instance_relative_config=True,
        template_folder=str(base_dir / "app" / "templates"),
        static_folder=str(base_dir / "static"),
    )

    config_key = (config_name or "default").lower()
    config_cls = CONFIG_MAP.get(config_key, BaseConfig)
    app.config.from_object(config_cls)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = _resolve_regulation_reference_root(
        base_dir,
        app.config.get("REGULATION_EU_2017_745_REFERENCE_FOLDER", ""),
    )

    if not app.config.get("SQLALCHEMY_DATABASE_URI") and not app.config.get("TESTING"):
        raise RuntimeError("DATABASE_URL is required for MSSQL configuration.")

    app.config.setdefault("ALLOWED_SOURCE_ROOTS", [])

    db.init_app(app)
    login_manager.init_app(app)
    if app.config.get("AUTH_ENABLED", True):
        ldap_manager.init_app(app)

    nas_service.init_nas_config(app)
    task_service.init_task_store(app)
    standard_update_service.init_standard_update_store(app)
    system_service.init_system_settings(app)
    mapping_metadata_service.init_mapping_metadata(app)
    execution_service.init_execution_metadata(app)

    os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)
    os.makedirs(app.config["TASK_FOLDER"], exist_ok=True)
    os.makedirs(app.config["STANDARD_UPDATE_FOLDER"], exist_ok=True)
    os.makedirs(app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"], exist_ok=True)

    auth_service.init_auth(app)
    execution_service.register_execution_cli(app)
    register_blueprints(app)

    @app.errorhandler(403)
    def forbidden(_exc):
        return render_template("403.html"), 403

    return app
