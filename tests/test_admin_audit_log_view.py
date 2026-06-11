from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from app.services.auth_admin_service import _build_audit_entry


def _fake_log(*, action: str, task_id: str = "task-1", work_id: str = "NE025"):
    return SimpleNamespace(
        created_at=datetime(2026, 5, 27, 9, 30, 0),
        action=action,
        work_id=work_id,
        task_id=task_id,
    )


def test_build_audit_entry_deduplicates_mapping_run_id_badges(app):
    detail = {
        "status": "queued",
        "job_id": "b25baa17",
        "run_id": "b25baa17",
        "mapping_display_name": "Mapping2.xlsx",
        "reference_ok": True,
        "extract_ok": False,
    }

    with app.test_request_context():
        entry = _build_audit_entry(_fake_log(action="task_mapping_check_extract"), detail)

    badge_texts = [badge["text"] for badge in entry["badges"]]

    assert entry["action_label"] == "Mapping 擷取條件檢查"
    assert badge_texts.count("b25baa17") == 1
    assert "Mapping2.xlsx" in badge_texts
    assert "引用檢查通過" in badge_texts
    assert "擷取檢查未通過" in badge_texts


def test_build_audit_entry_suppresses_standard_update_download_summary(app):
    detail = {
        "status": "completed",
        "task_name": "標準更新 A",
        "target_chapter_ref": "5.3",
        "updated_count": 3,
        "same_count": 1,
        "missing_count": 2,
        "output_path": "/tmp/result.xlsx",
    }

    with app.test_request_context():
        entry = _build_audit_entry(_fake_log(action="standard_update_mapping_download"), detail)

    assert entry["action_label"] == "標準更新下載結果"
    assert entry["summary_lines"] == []
    assert entry["status_badge"]["text"] == "COMPLETED"


def test_build_audit_entry_includes_harmonised_fallback_count_in_preview_summary(app):
    detail = {
        "updated_count": 3,
        "same_count": 1,
        "missing_count": 2,
        "harmonised_fallback_count": 5,
    }

    with app.test_request_context():
        entry = _build_audit_entry(_fake_log(action="task_standard_mapping_preview"), detail)

    summary_texts = [line["text"] for line in entry["summary_lines"]]

    assert "統計：更新 3、相同 1、缺漏 2、EU YES 退選更新 5" in summary_texts


def test_build_audit_entry_uses_chinese_labels_for_task_and_flow_actions(app):
    with app.test_request_context():
        task_entry = _build_audit_entry(_fake_log(action="task_create"), {})
        flow_entry = _build_audit_entry(_fake_log(action="flow_run_single_completed"), {"status": "completed"})

    assert task_entry["action_label"] == "建立任務"
    assert flow_entry["action_label"] == "流程執行完成"
