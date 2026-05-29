from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from flask import Flask
from sqlalchemy import engine_from_config, pool

from app.config import BaseConfig, CONFIG_MAP
from app.extensions import db
import app.models  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = db.metadata


def _resolve_config_class():
    config_name = (
        os.environ.get("ALEMBIC_CONFIG_NAME")
        or os.environ.get("APP_ENV")
        or os.environ.get("FLASK_ENV")
        or "default"
    )
    return CONFIG_MAP.get(str(config_name).lower(), BaseConfig)


def _build_alembic_app() -> Flask:
    app = Flask("alembic")
    app.config.from_object(_resolve_config_class())
    database_url = os.environ.get("ALEMBIC_DATABASE_URL") or app.config.get("SQLALCHEMY_DATABASE_URI")
    if not database_url:
        raise RuntimeError(
            "Database URL is required for Alembic. Set ALEMBIC_DATABASE_URL or DATABASE_URL."
        )
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    app.config.setdefault("SQLALCHEMY_TRACK_MODIFICATIONS", False)
    app.config.setdefault("SQLALCHEMY_ENGINE_OPTIONS", {"pool_pre_ping": True})
    db.init_app(app)
    return app


app = _build_alembic_app()


def _configure_url() -> str:
    url = app.config["SQLALCHEMY_DATABASE_URI"]
    config.set_main_option("sqlalchemy.url", str(url))
    return str(url)


def run_migrations_offline() -> None:
    url = _configure_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    _configure_url()
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with app.app_context():
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
            )

            with context.begin_transaction():
                context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
