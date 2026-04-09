from __future__ import annotations

import json
from pathlib import Path

from app.extensions import db
from app.models.auth import User
from app.models.settings import SystemSetting
from app.services.notification_service import send_batch_notification


def test_send_batch_notification_formats_mapping_results(app, monkeypatch) -> None:
    task_id = "mapping-notify-task"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "meta.json").write_text(
        json.dumps({"name": "Mapping 通知測試任務"}, ensure_ascii=False),
        encoding="utf-8",
    )

    setting = SystemSetting.query.order_by(SystemSetting.id).first()
    assert setting is not None
    setting.email_batch_notify_enabled = True
    db.session.add(User(work_id="A123", display_name="Tester", email="tester@example.com", active=True))
    db.session.commit()

    sent: dict[str, object] = {}

    def fake_send_email(to_addrs, subject, body):
        sent["to_addrs"] = list(to_addrs)
        sent["subject"] = subject
        sent["body"] = body
        return True

    monkeypatch.setattr("app.services.notification_service._send_email", fake_send_email)

    send_batch_notification(
        task_id=task_id,
        batch_id="batch-001",
        status="failed",
        results=[
            {
                "scheme_name": "CH2 摘要擷取",
                "run_id": "run-1",
                "ok": True,
                "output_count": 3,
                "error": "",
            },
            {
                "mapping_file": "附錄圖片擷取.xlsx",
                "run_id": "run-2",
                "ok": False,
                "error": "Mapping execution failed",
            },
        ],
        actor_work_id="A123",
        actor_label="Tester",
        completed_at="2026-04-09 10:00:00",
        error="1 mapping scheme(s) failed",
    )

    assert sent["to_addrs"] == ["tester@example.com"]
    assert sent["subject"] == "[法規文件轉換系統] 批次執行失敗 - Mapping 通知測試任務 (batch-001)"
    assert "成功：1，失敗：1" in sent["body"]
    assert "執行結果：" in sent["body"]
    assert "CH2 摘要擷取" in sent["body"]
    assert "(run: run-1)" in sent["body"]
    assert "附錄圖片擷取.xlsx" in sent["body"]
    assert "Mapping execution failed" in sent["body"]


def test_send_batch_notification_counts_flow_results_from_status(app, monkeypatch) -> None:
    task_id = "flow-notify-task"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "meta.json").write_text(
        json.dumps({"name": "Flow 通知測試任務"}, ensure_ascii=False),
        encoding="utf-8",
    )

    setting = SystemSetting.query.order_by(SystemSetting.id).first()
    assert setting is not None
    setting.email_batch_notify_enabled = True
    if not User.query.filter_by(work_id="B456").first():
        db.session.add(User(work_id="B456", display_name="Flow Tester", email="flow@example.com", active=True))
    db.session.commit()

    sent: dict[str, object] = {}

    def fake_send_email(to_addrs, subject, body):
        sent["to_addrs"] = list(to_addrs)
        sent["subject"] = subject
        sent["body"] = body
        return True

    monkeypatch.setattr("app.services.notification_service._send_email", fake_send_email)

    send_batch_notification(
        task_id=task_id,
        batch_id="batch-002",
        status="failed",
        results=[
            {"flow": "flow", "job_id": "job-1", "status": "completed"},
            {"flow": "flow2", "job_id": "job-2", "status": "completed"},
        ],
        actor_work_id="B456",
        actor_label="Flow Tester",
        completed_at="2026-04-09 10:30:00",
        error="1 flow(s) failed",
    )

    assert sent["to_addrs"] == ["flow@example.com"]
    assert "成功：2，失敗：0" in sent["body"]
    assert "- flow: 成功 (job: job-1)" in sent["body"]
    assert "- flow2: 成功 (job: job-2)" in sent["body"]
