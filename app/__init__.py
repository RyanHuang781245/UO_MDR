from __future__ import annotations

import os
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


def _build_flask_app(base_dir: Path) -> Flask:
    return Flask(
        __name__,
        instance_relative_config=True,
        template_folder=str(base_dir / "app" / "templates"),
        static_folder=str(base_dir / "static"),
    )


def _configure_app(app: Flask, base_dir: Path, config_name: str | None = None) -> None:
    config_key = (config_name or "default").lower()
    config_cls = CONFIG_MAP.get(config_key, BaseConfig)
    app.config.from_object(config_cls)
    reference_storage = standard_update_service.resolve_harmonised_reference_storage(
        base_dir,
        app.config.get("REGULATION_EU_2017_745_REFERENCE_FOLDER_CONFIGURED", ""),
    )
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = reference_storage["effective_root"]
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER_FALLBACK"] = reference_storage["fallback_root"]
    app.config["REGULATION_EU_2017_745_REFERENCE_STORAGE_MODE"] = reference_storage["storage_mode"]
    app.config["REGULATION_EU_2017_745_REFERENCE_STATUS_MESSAGE"] = reference_storage["status_message"]
    if reference_storage["storage_mode"] == "fallback":
        app.logger.warning(
            "Primary harmonised reference storage unavailable; using fallback. primary=%s fallback=%s reason=%s",
            reference_storage["configured_root"],
            reference_storage["effective_root"],
            reference_storage["status_message"],
        )
    elif reference_storage["storage_mode"] == "default":
        app.logger.info(
            "Primary harmonised reference storage is not configured; using default local storage. path=%s",
            reference_storage["effective_root"],
        )

    if not app.config.get("SQLALCHEMY_DATABASE_URI") and not app.config.get("TESTING"):
        raise RuntimeError("DATABASE_URL is required for MSSQL configuration.")

    app.config.setdefault("ALLOWED_SOURCE_ROOTS", [])


def _prepare_common_dirs(app: Flask) -> None:
    os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)
    os.makedirs(app.config["TASK_FOLDER"], exist_ok=True)
    os.makedirs(app.config["STANDARD_UPDATE_FOLDER"], exist_ok=True)
    os.makedirs(app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"], exist_ok=True)


def create_job_app(config_name: str | None = None) -> Flask:
    base_dir = Path(__file__).resolve().parents[1]
    load_dotenv_if_present(str(base_dir))

    app = _build_flask_app(base_dir)
    _configure_app(app, base_dir, config_name)
    db.init_app(app)
    standard_update_service.init_standard_update_store(app)
    system_service.init_system_settings(app)
    _prepare_common_dirs(app)
    return app


def create_app(config_name: str | None = None, *, init_auth: bool = True) -> Flask:
    base_dir = Path(__file__).resolve().parents[1]
    load_dotenv_if_present(str(base_dir))

    app = _build_flask_app(base_dir)
    _configure_app(app, base_dir, config_name)

    db.init_app(app)
    if init_auth:
        login_manager.init_app(app)
        if app.config.get("AUTH_ENABLED", True):
            ldap_manager.init_app(app)

    nas_service.init_nas_config(app)
    task_service.init_task_store(app)
    standard_update_service.init_standard_update_store(app)
    system_service.init_system_settings(app)
    mapping_metadata_service.init_mapping_metadata(app)
    execution_service.init_execution_metadata(app)

    _prepare_common_dirs(app)

    if init_auth:
        auth_service.init_auth(app)
    execution_service.register_execution_cli(app)
    register_blueprints(app)

    @app.errorhandler(403)
    def forbidden(_exc):
        return render_template("403.html"), 403

    return app
