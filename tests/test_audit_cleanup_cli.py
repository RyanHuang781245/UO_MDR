from __future__ import annotations

from datetime import datetime, timedelta

from app.extensions import db
from app.models.auth import AuditLog, ensure_schema
from app.services.audit_service import cleanup_audit_logs


def _insert_audit_log(*, action: str, created_at: datetime) -> None:
    db.session.add(
        AuditLog(
            action=action,
            work_id="NE025",
            detail='{"status":"completed"}',
            task_id="task-1",
            created_at=created_at,
        )
    )
    db.session.commit()


def test_cleanup_audit_logs_dry_run_keeps_rows(app):
    with app.app_context():
        ensure_schema()
        now = datetime.now()
        _insert_audit_log(action="old_action", created_at=now - timedelta(days=200))
        _insert_audit_log(action="recent_action", created_at=now - timedelta(days=5))

        result = cleanup_audit_logs(retention_days=180, dry_run=True)

        assert result["matched_count"] == 1
        assert result["deleted_count"] == 0
        assert AuditLog.query.count() == 2


def test_audit_cleanup_cli_deletes_expired_rows(app):
    with app.app_context():
        ensure_schema()
        now = datetime.now()
        _insert_audit_log(action="old_action", created_at=now - timedelta(days=200))
        _insert_audit_log(action="recent_action", created_at=now - timedelta(days=5))

    runner = app.test_cli_runner()
    result = runner.invoke(args=["audit-cleanup", "--days", "180"])

    assert result.exit_code == 0
    assert "matched=1" in result.output
    assert "deleted=1" in result.output

    with app.app_context():
        remaining_actions = [log.action for log in AuditLog.query.order_by(AuditLog.action).all()]

    assert remaining_actions == ["recent_action"]
