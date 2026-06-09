from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Mapping

from flask import Flask
from flask.logging import default_handler

from app.utils import parse_bool

_CONFIGURED_ATTR = "_uo_mdr_logging_configured"
_PROCESS_ROLE_ATTR = "_uo_mdr_process_role"
_DEFAULT_LEVEL = "INFO"
_DEFAULT_MAX_MB = 10
_DEFAULT_MAX_BYTES = _DEFAULT_MAX_MB * 1024 * 1024
_DEFAULT_BACKUP_COUNT = 10


class _ProcessRoleFilter(logging.Filter):
    def __init__(self, role: str) -> None:
        super().__init__()
        self.role = role

    def filter(self, record: logging.LogRecord) -> bool:
        record.process_role = self.role
        return True


def build_log_file_path(log_dir: Path, role: str) -> Path:
    normalized_role = "worker" if str(role).strip().lower() == "worker" else "web"
    filename = "app-worker.log" if normalized_role == "worker" else "app-web.log"
    return log_dir / filename


def configure_app_logging(app: Flask, *, role: str) -> Path | None:
    log_path = configure_process_logging(
        Path(app.config.get("BASE_DIR") or Path(app.root_path).resolve().parent),
        role=role,
        config=app.config,
    )
    _configure_flask_loggers(app)
    return log_path


def configure_process_logging(
    base_dir: Path,
    *,
    role: str,
    config: Mapping[str, Any] | None = None,
) -> Path | None:
    settings = _resolve_settings(base_dir, role=role, config=config)
    root_logger = logging.getLogger()
    configured_role = getattr(root_logger, _PROCESS_ROLE_ATTR, None)
    effective_role = configured_role or settings["role"]
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(process_role)s pid=%(process)d] %(name)s: %(message)s"
    )

    root_logger.setLevel(settings["level"])
    if settings["log_to_file"]:
        _ensure_file_handler(
            root_logger,
            build_log_file_path(settings["log_dir"], effective_role),
            level=settings["level"],
            formatter=formatter,
            role=effective_role,
            max_bytes=settings["max_bytes"],
            backup_count=settings["backup_count"],
        )
    if settings["stdout"]:
        _ensure_stdout_handler(
            root_logger,
            level=settings["level"],
            formatter=formatter,
            role=effective_role,
        )

    setattr(root_logger, _CONFIGURED_ATTR, True)
    setattr(root_logger, _PROCESS_ROLE_ATTR, effective_role)
    return build_log_file_path(settings["log_dir"], effective_role) if settings["log_to_file"] else None


def _configure_flask_loggers(app: Flask) -> None:
    if default_handler in app.logger.handlers:
        app.logger.removeHandler(default_handler)
    app.logger.propagate = True

    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.propagate = True


def _resolve_settings(base_dir: Path, *, role: str, config: Mapping[str, Any] | None) -> dict[str, Any]:
    testing = _as_bool(_get_setting(config, "TESTING", "TESTING", False), False)
    log_dir_value = _get_setting(config, "APP_LOG_DIR", "APP_LOG_DIR", str(base_dir / "logs"))
    level_name = str(_get_setting(config, "APP_LOG_LEVEL", "APP_LOG_LEVEL", _DEFAULT_LEVEL)).strip().upper()
    log_to_file = _as_bool(_get_setting(config, "APP_LOG_TO_FILE", "APP_LOG_TO_FILE", not testing), not testing)
    stdout_enabled = _as_bool(_get_setting(config, "APP_LOG_STDOUT", "APP_LOG_STDOUT", not testing), not testing)
    max_bytes = _resolve_max_bytes(config)
    backup_count = _as_int(
        _get_setting(config, "APP_LOG_BACKUP_COUNT", "APP_LOG_BACKUP_COUNT", _DEFAULT_BACKUP_COUNT),
        _DEFAULT_BACKUP_COUNT,
    )
    return {
        "role": "worker" if str(role).strip().lower() == "worker" else "web",
        "log_dir": Path(str(log_dir_value)).expanduser(),
        "level": getattr(logging, level_name, logging.INFO),
        "log_to_file": log_to_file,
        "stdout": stdout_enabled,
        "max_bytes": max(1, max_bytes),
        "backup_count": max(1, backup_count),
    }


def _ensure_file_handler(
    root_logger: logging.Logger,
    log_path: Path,
    *,
    level: int,
    formatter: logging.Formatter,
    role: str,
    max_bytes: int,
    backup_count: int,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path = str(log_path.resolve())
    for handler in root_logger.handlers:
        if isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", None) == resolved_path:
            handler.setLevel(level)
            handler.setFormatter(formatter)
            _ensure_role_filter(handler, role)
            return

    handler = RotatingFileHandler(
        resolved_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(formatter)
    handler.set_name(f"uo-mdr-file-{role}")
    _ensure_role_filter(handler, role)
    root_logger.addHandler(handler)


def _ensure_stdout_handler(
    root_logger: logging.Logger,
    *,
    level: int,
    formatter: logging.Formatter,
    role: str,
) -> None:
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and getattr(handler, "stream", None) in {sys.stdout, sys.stderr}:
            handler.setLevel(level)
            handler.setFormatter(formatter)
            _ensure_role_filter(handler, role)
            return

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    handler.set_name("uo-mdr-stdout")
    _ensure_role_filter(handler, role)
    root_logger.addHandler(handler)


def _ensure_role_filter(handler: logging.Handler, role: str) -> None:
    for existing_filter in handler.filters:
        if isinstance(existing_filter, _ProcessRoleFilter):
            existing_filter.role = role
            return
    handler.addFilter(_ProcessRoleFilter(role))


def _get_setting(config: Mapping[str, Any] | None, key: str, env_key: str, default: Any) -> Any:
    if config is not None and key in config and config[key] is not None:
        return config[key]
    return os.environ.get(env_key, default)


def _resolve_max_bytes(config: Mapping[str, Any] | None) -> int:
    max_mb = _get_setting(config, "APP_LOG_MAX_MB", "APP_LOG_MAX_MB", _DEFAULT_MAX_MB)
    return _as_int(max_mb, _DEFAULT_MAX_MB) * 1024 * 1024


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    return parse_bool(value, default)


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
