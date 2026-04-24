import os

from app.jobs import adoption_standard_update as update_job
from app.models.settings import (
    REGULATION_SYNC_SOURCE_KEY,
    RegulationSyncState,
    get_regulation_sync_state,
    upsert_regulation_sync_state,
)
from app.services.standard_update_service import get_latest_harmonised_release_in_dir


def test_upsert_regulation_sync_state_updates_existing_row(app):
    with app.app_context():
        first = upsert_regulation_sync_state(
            last_filename="old.xlsx",
            last_uuid="uuid-1",
            last_url="https://example.com/old",
        )
        second = upsert_regulation_sync_state(
            last_filename="new.xlsx",
            last_uuid="uuid-2",
            last_url="https://example.com/new",
        )

        rows = RegulationSyncState.query.filter_by(source_key=REGULATION_SYNC_SOURCE_KEY).all()

        assert len(rows) == 1
        assert first.id == second.id
        assert rows[0].last_filename == "new.xlsx"
        assert rows[0].last_uuid == "uuid-2"
        assert rows[0].last_url == "https://example.com/new"
        assert get_regulation_sync_state().id == first.id


def test_build_update_decision_marks_matching_state_as_not_updated(monkeypatch):
    current = {
        "filename": "same.xlsx",
        "uuid": "uuid-same",
        "url": "https://example.com/same",
    }

    monkeypatch.setattr(
        update_job,
        "load_last_state",
        lambda: (
            {
                "filename": "same.xlsx",
                "uuid": "uuid-same",
                "url": "https://example.com/same",
            },
            "database",
        ),
    )

    decision = update_job.build_update_decision(current)

    assert decision.last_source == "database"
    assert decision.should_download is False
    assert decision.reasons == []


def test_build_update_decision_reports_forced_download_reason(monkeypatch):
    current = {
        "filename": "same.xlsx",
        "uuid": "uuid-same",
        "url": "https://example.com/same",
    }

    monkeypatch.setattr(
        update_job,
        "load_last_state",
        lambda: (
            {
                "filename": "same.xlsx",
                "uuid": "uuid-same",
                "url": "https://example.com/same",
            },
            "database",
        ),
    )

    decision = update_job.build_update_decision(current, force_download=True)

    assert decision.should_download is True
    assert decision.reasons == ["強制下載模式啟用"]


def test_check_for_update_records_audit_even_when_no_update(monkeypatch):
    audits: list[tuple[str, dict, dict | None]] = []

    monkeypatch.setattr(
        update_job,
        "get_download_link",
        lambda page_url, link_text: "https://example.com/files/uuid-same?filename=same.xlsx",
    )
    monkeypatch.setattr(
        update_job,
        "load_last_state",
        lambda: (
            {
                "filename": "same.xlsx",
                "uuid": "uuid-same",
                "url": "https://example.com/files/uuid-same?filename=same.xlsx",
            },
            "database",
        ),
    )
    monkeypatch.setattr(
        update_job,
        "_record_regulation_audit",
        lambda action, detail, actor=None: audits.append((action, detail, actor)),
    )

    result = update_job.check_for_update(
        page_url="https://example.com/page",
        link_text="Summary list as xls file",
        actor={"work_id": "A123", "label": "Admin"},
    )

    assert result["should_download"] is False
    assert len(audits) == 1
    action, detail, actor = audits[0]
    assert action == "regulation_release_update_check"
    assert detail["status"] == "completed"
    assert detail["should_download"] is False
    assert detail["current"]["filename"] == "same.xlsx"
    assert actor == {"work_id": "A123", "label": "Admin"}


def test_run_update_force_download_records_manual_download_audit(monkeypatch, tmp_path):
    audits: list[tuple[str, dict, dict | None]] = []
    current = {
        "filename": "same.xlsx",
        "uuid": "uuid-same",
        "url": "https://example.com/files/uuid-same?filename=same.xlsx",
    }
    sync_result = {
        "id": 1,
        "file_name": "same.xlsx",
        "path": str(tmp_path / "same.xlsx"),
        "version_label": "20260424-1300",
        "downloaded_at": "2026-04-24 13:00",
        "source_url": current["url"],
    }

    monkeypatch.setattr(update_job, "resolve_save_dir", lambda: (tmp_path, "primary"))
    monkeypatch.setattr(update_job, "get_download_link", lambda page_url, link_text: current["url"])
    monkeypatch.setattr(
        update_job,
        "load_last_state",
        lambda: (
            {
                "filename": "same.xlsx",
                "uuid": "uuid-same",
                "url": current["url"],
            },
            "database",
        ),
    )
    monkeypatch.setattr(
        update_job,
        "perform_download",
        lambda current_payload, save_dir: {
            "downloaded": True,
            "path": str(tmp_path / current_payload["filename"]),
            "sync_result": sync_result,
            "current": current_payload,
        },
    )
    monkeypatch.setattr(
        update_job,
        "_record_regulation_audit",
        lambda action, detail, actor=None: audits.append((action, detail, actor)),
    )

    result = update_job.run_update(
        force_download=True,
        page_url="https://example.com/page",
        link_text="Summary list as xls file",
        actor={"work_id": "A123", "label": "Admin"},
    )

    assert result["downloaded"] is True
    assert len(audits) == 1
    action, detail, actor = audits[0]
    assert action == "regulation_release_manual_download"
    assert detail["status"] == "completed"
    assert detail["downloaded"] is True
    assert detail["current"]["filename"] == "same.xlsx"
    assert detail["sync_result"]["id"] == 1
    assert actor == {"work_id": "A123", "label": "Admin"}


def test_get_latest_harmonised_release_in_dir_returns_latest_excel(tmp_path):
    older = tmp_path / "older.xlsx"
    newer = tmp_path / "newer.xlsx"
    ignored = tmp_path / "notes.txt"

    older.write_text("old", encoding="utf-8")
    newer.write_text("new", encoding="utf-8")
    ignored.write_text("ignore", encoding="utf-8")

    older_ts = 1_700_000_000
    newer_ts = 1_700_000_100
    os.utime(older, (older_ts, older_ts))
    os.utime(newer, (newer_ts, newer_ts))

    result = get_latest_harmonised_release_in_dir(str(tmp_path))

    assert result["file_name"] == "newer.xlsx"
    assert result["path"] == str(newer)
    assert result["version_label"]
    assert result["downloaded_at"]
