from app.jobs import adoption_standard_update as update_job
from app.models.settings import (
    REGULATION_SYNC_SOURCE_KEY,
    RegulationSyncState,
    get_regulation_sync_state,
    upsert_regulation_sync_state,
)


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
