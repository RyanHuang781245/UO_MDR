from __future__ import annotations

import json
from pathlib import Path

from app.extensions import db
from app.models.standard_update import StandardUpdateRecord
from app.services.standard_update_service import (
    HARMONISED_SOURCE_CUSTOM,
    acquire_standard_update_lock,
    create_standard_update,
    list_standard_updates,
    load_standard_update,
    release_standard_update_lock,
    save_standard_update,
)


def test_save_standard_update_writes_meta_atomically(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("Atomic Save", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)
    meta = load_standard_update(task_id)
    meta["description"] = "updated"

    save_standard_update(task_id, meta)

    meta_path = standard_update_root / task_id / "meta.json"
    saved = json.loads(meta_path.read_text(encoding="utf-8"))
    assert saved["description"] == "updated"
    assert list(meta_path.parent.glob("meta.*.tmp")) == []


def test_load_standard_update_returns_empty_dict_for_invalid_json(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    task_dir = standard_update_root / "broken123"
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text("", encoding="utf-8")

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)

    assert load_standard_update("broken123") == {}


def test_load_standard_update_prefers_db_and_keeps_lock_in_db(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("DB First", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)
    meta_path = standard_update_root / task_id / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["regulation_excel_path"] = "legacy_regulation.xlsx"
    meta["lock"] = {
        "locked_by_actor_id": "actor-1",
        "locked_by_work_id": "A001",
        "locked_by_name": "Tester",
        "locked_at": "2026-05-26 18:00:00",
        "lock_expires_at": "2026-05-26 18:05:00",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    record = db.session.get(StandardUpdateRecord, task_id)
    record.name = "DB Updated Name"
    record.description = "db description"
    record.regulation_excel_path = None
    record.locked_by_actor_id = None
    record.locked_by_work_id = None
    record.locked_by_name = None
    record.locked_at = None
    record.lock_expires_at = None
    db.session.commit()

    loaded = load_standard_update(task_id)

    assert loaded["name"] == "DB Updated Name"
    assert loaded["description"] == "db description"
    assert loaded["regulation_excel_path"] == "legacy_regulation.xlsx"
    assert loaded["lock"]["locked_by_actor_id"] == ""
    assert loaded["lock"]["locked_by_name"] == ""


def test_list_standard_updates_includes_db_records_without_meta_file(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    record = StandardUpdateRecord(
        id="dbonly01",
        name="DB Only Task",
        status="draft",
        harmonised_source_mode="system",
    )
    db.session.add(record)
    db.session.commit()

    items = list_standard_updates()

    assert any(item["id"] == "dbonly01" and item["name"] == "DB Only Task" for item in items)


def test_load_standard_update_uses_db_lock_even_when_file_has_different_lock(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("DB Lock Source", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)
    meta_path = standard_update_root / task_id / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["lock"] = {
        "locked_by_actor_id": "file-actor",
        "locked_by_work_id": "FILE01",
        "locked_by_name": "File User",
        "locked_at": "2026-05-26 18:00:00",
        "lock_expires_at": "2026-05-26 18:05:00",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    record = db.session.get(StandardUpdateRecord, task_id)
    record.locked_by_actor_id = "db-actor"
    record.locked_by_work_id = "DB01"
    record.locked_by_name = "DB User"
    db.session.commit()

    loaded = load_standard_update(task_id)

    assert loaded["lock"]["locked_by_actor_id"] == "db-actor"
    assert loaded["lock"]["locked_by_work_id"] == "DB01"
    assert loaded["lock"]["locked_by_name"] == "DB User"


def test_acquire_and_release_standard_update_lock_updates_db_row(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("Lock DB Row", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)

    ok, task = acquire_standard_update_lock(task_id, "actor-1", work_id="A001", actor_name="Tester")
    assert ok is True
    assert task["lock"]["locked_by_actor_id"] == "actor-1"

    record = db.session.get(StandardUpdateRecord, task_id)
    assert record is not None
    assert record.locked_by_actor_id == "actor-1"
    assert record.locked_by_work_id == "A001"
    assert record.locked_by_name == "Tester"
    assert record.locked_at is not None
    assert record.lock_expires_at is not None

    ok, task = release_standard_update_lock(task_id, "actor-1")
    assert ok is True
    assert task["lock"]["locked_by_actor_id"] == ""

    db.session.refresh(record)
    assert record.locked_by_actor_id is None
    assert record.locked_by_work_id is None
    assert record.locked_by_name is None
    assert record.locked_at is None
    assert record.lock_expires_at is None


def test_acquire_standard_update_lock_creates_db_row_for_legacy_file_only_task(app, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    task_dir = standard_update_root / "legacy001"
    task_dir.mkdir(parents=True)
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    (task_dir / "meta.json").write_text(
        json.dumps(
            {
                "id": "legacy001",
                "name": "Legacy Task",
                "description": "",
                "creator_name": "Legacy User",
                "creator_work_id": "L001",
                "created": "2026-05-26 18:00",
                "updated": "2026-05-26 18:00",
                "status": "draft",
                "harmonised_source_mode": "custom",
                "word_file_path": "",
                "standard_excel_path": "",
                "regulation_excel_path": "legacy_reg.xlsx",
                "harmonised_snapshot_path": "",
                "harmonised_snapshot_version": "",
                "custom_harmonised_path": "",
                "custom_harmonised_version": "",
                "last_output_path": "",
                "last_run_at": "",
                "last_run_status": "",
                "lock": {
                    "locked_by_actor_id": "",
                    "locked_by_work_id": "",
                    "locked_by_name": "",
                    "locked_at": "",
                    "lock_expires_at": "",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    ok, task = acquire_standard_update_lock("legacy001", "actor-2", work_id="A002", actor_name="Legacy Tester")

    assert ok is True
    assert task["lock"]["locked_by_actor_id"] == "actor-2"
    record = db.session.get(StandardUpdateRecord, "legacy001")
    assert record is not None
    assert record.name == "Legacy Task"
    assert record.regulation_excel_path == "legacy_reg.xlsx"
    assert record.locked_by_actor_id == "actor-2"
