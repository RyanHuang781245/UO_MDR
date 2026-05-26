from __future__ import annotations

import logging
from pathlib import Path

import pytest

from app.logging_config import configure_process_logging


@pytest.fixture
def isolated_root_logger():
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    original_role = getattr(root_logger, "_uo_mdr_process_role", None)
    original_configured = getattr(root_logger, "_uo_mdr_logging_configured", None)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    if hasattr(root_logger, "_uo_mdr_process_role"):
        delattr(root_logger, "_uo_mdr_process_role")
    if hasattr(root_logger, "_uo_mdr_logging_configured"):
        delattr(root_logger, "_uo_mdr_logging_configured")

    try:
        yield root_logger
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
        if original_role is None:
            if hasattr(root_logger, "_uo_mdr_process_role"):
                delattr(root_logger, "_uo_mdr_process_role")
        else:
            setattr(root_logger, "_uo_mdr_process_role", original_role)
        if original_configured is None:
            if hasattr(root_logger, "_uo_mdr_logging_configured"):
                delattr(root_logger, "_uo_mdr_logging_configured")
        else:
            setattr(root_logger, "_uo_mdr_logging_configured", original_configured)


def test_configure_process_logging_writes_web_log(tmp_path: Path, isolated_root_logger) -> None:
    log_path = configure_process_logging(
        tmp_path,
        role="web",
        config={
            "APP_LOG_DIR": str(tmp_path),
            "APP_LOG_TO_FILE": True,
            "APP_LOG_STDOUT": False,
            "APP_LOG_LEVEL": "INFO",
        },
    )

    logging.getLogger("tests.logging.web").info("web log entry")

    assert log_path == tmp_path / "app-web.log"
    assert log_path.is_file()
    assert "web log entry" in log_path.read_text(encoding="utf-8")


def test_configure_process_logging_writes_worker_log(tmp_path: Path, isolated_root_logger) -> None:
    log_path = configure_process_logging(
        tmp_path,
        role="worker",
        config={
            "APP_LOG_DIR": str(tmp_path),
            "APP_LOG_TO_FILE": True,
            "APP_LOG_STDOUT": False,
            "APP_LOG_LEVEL": "INFO",
        },
    )

    logging.getLogger("tests.logging.worker").info("worker log entry")

    assert log_path == tmp_path / "app-worker.log"
    assert log_path.is_file()
    assert "worker log entry" in log_path.read_text(encoding="utf-8")
