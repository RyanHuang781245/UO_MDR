from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.models.auth import SystemErrorLog
from app.services.audit_service import record_system_error
from app.services.execution_service import run_worker_loop


def test_record_system_error_persists_separate_system_error_log(app):
    with app.app_context():
        ok = record_system_error(
            "system_settings.init",
            "System settings initialization failed",
            exc=RuntimeError("db unavailable"),
            detail={"phase": "bootstrap"},
            task_id="task-1",
        )

        assert ok is True

        rows = SystemErrorLog.query.all()
        assert len(rows) == 1
        row = rows[0]
        assert row.component == "system_settings.init"
        assert row.message == "System settings initialization failed"
        assert row.error_type == "RuntimeError"
        assert row.task_id == "task-1"
        assert '"phase": "bootstrap"' in (row.detail or "")
        assert '"exception_message": "db unavailable"' in (row.detail or "")


def test_system_error_logs_template_compiles(app):
    with app.app_context():
        template = app.jinja_env.get_template("admin/system_error_logs.html")

    assert template is not None


def test_record_system_error_normalizes_warn_to_warning(app):
    app.config["SYSTEM_ERROR_DB_MIN_LEVEL"] = "WARNING"
    with app.app_context():
        ok = record_system_error(
            "manual.warn",
            "Manual warning system error test",
            level="WARN",
            exc=RuntimeError("warn test"),
        )

        assert ok is True

        row = SystemErrorLog.query.order_by(SystemErrorLog.id.desc()).first()
        assert row is not None
        assert row.level == "WARNING"


def test_system_error_test_cli_supports_separate_levels(app):
    app.config["SYSTEM_ERROR_DB_MIN_LEVEL"] = "INFO"
    runner = app.test_cli_runner()
    result = runner.invoke(args=["system-error-test", "--level", "INFO", "--component", "manual.info", "--task-id", "task-info"])

    assert result.exit_code == 0
    assert "recorded=1" in result.output
    assert "level=INFO" in result.output

    with app.app_context():
        row = SystemErrorLog.query.order_by(SystemErrorLog.id.desc()).first()

    assert row is not None
    assert row.level == "INFO"
    assert row.component == "manual.info"
    assert row.task_id == "task-info"


def test_record_system_error_respects_db_min_level(app):
    app.config["SYSTEM_ERROR_DB_MIN_LEVEL"] = "ERROR"

    with app.app_context():
        ok = record_system_error(
            "manual.info.skip",
            "Info system error test",
            level="INFO",
            exc=RuntimeError("info test"),
        )

        assert ok is False
        assert SystemErrorLog.query.count() == 0


def test_system_error_test_cli_reports_skipped_by_level(app):
    app.config["SYSTEM_ERROR_DB_MIN_LEVEL"] = "ERROR"

    runner = app.test_cli_runner()
    result = runner.invoke(args=["system-error-test", "--level", "INFO", "--component", "manual.info.skip"])

    assert result.exit_code == 0
    assert "recorded=0" in result.output
    assert "skipped_by_level=1" in result.output


def test_record_system_error_falls_back_to_jsonl_when_db_write_fails(app, monkeypatch, tmp_path):
    fallback_path = Path(tmp_path) / "system-error-fallback.jsonl"
    app.config["APP_LOG_DIR"] = str(tmp_path)

    with app.app_context():
        original_commit = type(app.extensions["sqlalchemy"].session).commit

        def failing_commit(_session):
            raise RuntimeError("db down")

        monkeypatch.setattr(type(app.extensions["sqlalchemy"].session), "commit", failing_commit)
        try:
            ok = record_system_error(
                "manual.fallback",
                "Fallback system error test",
                detail={"source": "fallback-test"},
                exc=RuntimeError("db failure"),
                task_id="task-fallback",
            )
        finally:
            monkeypatch.setattr(type(app.extensions["sqlalchemy"].session), "commit", original_commit)

    assert ok is True
    assert fallback_path.is_file()
    payload = json.loads(fallback_path.read_text(encoding="utf-8").splitlines()[-1])
    assert payload["component"] == "manual.fallback"
    assert payload["fallback"] is True
    assert payload["level"] == "ERROR"


def test_system_error_fallback_jsonl_truncates_when_size_limit_exceeded(app, monkeypatch, tmp_path):
    fallback_path = Path(tmp_path) / "system-error-fallback.jsonl"
    fallback_path.write_text("old-entry\n" * 20, encoding="utf-8")
    app.config["APP_LOG_DIR"] = str(tmp_path)
    app.config["SYSTEM_ERROR_FALLBACK_MAX_BYTES"] = fallback_path.stat().st_size

    with app.app_context():
        original_commit = type(app.extensions["sqlalchemy"].session).commit

        def failing_commit(_session):
            raise RuntimeError("db down")

        monkeypatch.setattr(type(app.extensions["sqlalchemy"].session), "commit", failing_commit)
        try:
            ok = record_system_error(
                "manual.fallback.limit",
                "Fallback system error test after limit",
                detail={"source": "fallback-limit-test"},
                exc=RuntimeError("db failure"),
            )
        finally:
            monkeypatch.setattr(type(app.extensions["sqlalchemy"].session), "commit", original_commit)

    assert ok is True
    lines = fallback_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert "old-entry" not in lines[0]
    payload = json.loads(lines[0])
    assert payload["component"] == "manual.fallback.limit"
    assert payload["fallback"] is True


def test_unhandled_web_exception_records_system_error_log(app):
    app.config["PROPAGATE_EXCEPTIONS"] = False

    @app.route("/_test/system-error")
    def _test_system_error():
        raise RuntimeError("boom")

    client = app.test_client()
    response = client.get("/_test/system-error?case=1")

    assert response.status_code == 500

    with app.app_context():
        row = SystemErrorLog.query.order_by(SystemErrorLog.id.desc()).first()

    assert row is not None
    assert row.component == "web.unhandled_exception"
    assert row.message == "Unhandled web exception"
    assert row.error_type == "RuntimeError"
    assert '"path": "/_test/system-error"' in (row.detail or "")


def test_worker_loop_records_unhandled_exception_in_once_mode(app, monkeypatch):
    def failing_process_next_job(_app, worker_id=None):
        raise RuntimeError("worker loop boom")

    monkeypatch.setattr("app.services.execution_service.process_next_job", failing_process_next_job)

    with pytest.raises(RuntimeError, match="worker loop boom"):
        run_worker_loop(app, once=True)

    with app.app_context():
        row = SystemErrorLog.query.order_by(SystemErrorLog.id.desc()).first()

    assert row is not None
    assert row.component == "worker.loop"
    assert row.message == "Unhandled worker loop exception"
    assert row.error_type == "RuntimeError"
    assert '"once": true' in (row.detail or "").lower()


def test_worker_loop_records_and_recovers_from_unhandled_exception(app, monkeypatch):
    state = {"calls": 0}

    def flaky_process_next_job(_app, worker_id=None):
        state["calls"] += 1
        if state["calls"] == 1:
            raise RuntimeError("transient worker failure")
        return True

    monkeypatch.setattr("app.services.execution_service.process_next_job", flaky_process_next_job)
    monkeypatch.setattr("app.services.execution_service.time.sleep", lambda _seconds: None)

    processed = run_worker_loop(app, max_jobs=1)

    assert processed == 1

    with app.app_context():
        row = SystemErrorLog.query.order_by(SystemErrorLog.id.desc()).first()

    assert row is not None
    assert row.component == "worker.loop"
    assert row.error_type == "RuntimeError"
