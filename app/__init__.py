from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, render_template

from app.blueprints import register_blueprints
from app.config import CONFIG_MAP, BaseConfig
from app.extensions import db, ldap_manager, login_manager
from app.services import auth_service, nas_service, task_service
from modules.env_loader import load_dotenv_if_present


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

    if not app.config.get("SQLALCHEMY_DATABASE_URI") and not app.config.get("TESTING"):
        raise RuntimeError("DATABASE_URL is required for MSSQL configuration.")

    app.config.setdefault("ALLOWED_SOURCE_ROOTS", [])

    db.init_app(app)
    login_manager.init_app(app)
    ldap_manager.init_app(app)

    nas_service.init_nas_config(app)
    task_service.init_task_store(app)

    os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)
    os.makedirs(app.config["TASK_FOLDER"], exist_ok=True)

    auth_service.init_auth(app)
    register_blueprints(app)

    @app.errorhandler(403)
    def forbidden(_exc):
        return render_template("403.html"), 403

    return app
