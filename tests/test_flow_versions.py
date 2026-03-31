import json
import shutil
from pathlib import Path

import pytest
from flask import url_for

from app import create_app
from app.blueprints.flows.routes import _flow_version_count
from app.extensions import ldap_manager


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(ldap_manager, "init_app", lambda app: None)
    app = create_app("testing")
    ctx = app.app_context()
    ctx.push()
    try:
        yield app
    finally:
        ctx.pop()


@pytest.fixture
def client(app):
    return app.test_client()


def _write_flow(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _version_metadata(flow_dir: Path, flow_name: str) -> dict:
    meta_path = flow_dir / "_versions" / flow_name / "metadata.json"
    if not meta_path.exists():
        return {"versions": []}
    return json.loads(meta_path.read_text(encoding="utf-8"))


def test_flow_save_overwrites_without_creating_version_snapshot(app, client) -> None:
    task_id = "flow-version-save"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    original_payload = {
        "created": "2026-03-24 11:00",
        "steps": [],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", original_payload)

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save",
            "flow_name": "版本流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "changed-output",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 302
    current_payload = json.loads((flow_dir / "版本流程.json").read_text(encoding="utf-8"))
    assert current_payload["output_filename"] == "changed-output.docx"

    metadata = _version_metadata(flow_dir, "版本流程")
    assert metadata["versions"] == []


def test_flow_save_skips_version_when_content_is_unchanged(app, client) -> None:
    task_id = "flow-version-nochange"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save",
            "flow_name": "版本流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 302
    metadata = _version_metadata(flow_dir, "版本流程")
    assert metadata["versions"] == []


def test_flow_restore_uses_selected_version_and_backs_up_current(app, client) -> None:
    task_id = "flow-version-restore"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    current_payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    restore_payload = {
        "created": "2026-03-24 10:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "restored"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", current_payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_restore_target"
    (versions_dir / f"{base_name}.json").write_text(
        json.dumps(restore_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "version-1",
                        "name": "目標版本",
                        "slug": "restore_target",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "dummy",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_bp.restore_flow_version", task_id=task_id, flow_name="版本流程", version_id="version-1")

    response = client.post(url)
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True

    restored_current = json.loads((flow_dir / "版本流程.json").read_text(encoding="utf-8"))
    assert restored_current == restore_payload

    metadata = _version_metadata(flow_dir, "版本流程")
    assert len(metadata["versions"]) == 2
    assert any(item["source"] == "before_restore" for item in metadata["versions"])
    backup_entry = next(item for item in metadata["versions"] if item["source"] == "before_restore")
    assert backup_entry["name"] == "回復前備份（目標：目標版本）"
    assert backup_entry["restored_to_version_id"] == "version-1"
    assert backup_entry["restored_to_version_name"] == "目標版本"
    backup_payload = json.loads((versions_dir / f"{backup_entry['base_name']}.json").read_text(encoding="utf-8"))
    assert backup_payload == current_payload
    with client.session_transaction() as sess:
        flashes = sess.get("_flashes", [])
    assert ("success", "已成功回復版本「目標版本」。") in flashes


def test_flow_restore_only_keeps_latest_before_restore_backup(app, client) -> None:
    task_id = "flow-version-restore-single-backup"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    original_payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "original"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    target_payload = {
        "created": "2026-03-24 10:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "target"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", original_payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_restore_target"
    (versions_dir / f"{base_name}.json").write_text(
        json.dumps(target_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "version-1",
                        "name": "目標版本",
                        "slug": "restore_target",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "dummy",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_bp.restore_flow_version", task_id=task_id, flow_name="版本流程", version_id="version-1")

    first = client.post(url)
    assert first.status_code == 200
    second = client.post(url)
    assert second.status_code == 200

    metadata = _version_metadata(flow_dir, "版本流程")
    backups = [item for item in metadata["versions"] if item["source"] == "before_restore"]
    assert len(backups) == 1
    backup_payload = json.loads((versions_dir / f"{backups[0]['base_name']}.json").read_text(encoding="utf-8"))
    assert backup_payload == target_payload


def test_flow_version_list_and_count_only_include_manual_snapshots(app, client) -> None:
    task_id = "flow-version-visible-list"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    manual_base = "20260324110000_manual_snapshot"
    backup_base = "20260324110100_before_restore"
    (versions_dir / f"{manual_base}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / f"{backup_base}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "backup-1",
                        "name": "回復前備份（目標：送審前）",
                        "slug": "before_restore",
                        "base_name": backup_base,
                        "created_at": "2026-03-24T11:01:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "before_restore",
                        "content_hash": "backup-hash",
                    },
                    {
                        "id": "manual-1",
                        "name": "送審前",
                        "slug": "manual_snapshot",
                        "base_name": manual_base,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash",
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_api_bp.list_flow_versions", task_id=task_id, flow_name="版本流程")

    response = client.get(url)
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert len(data["versions"]) == 1
    assert data["versions"][0]["id"] == "manual-1"
    assert data["versions"][0]["name"] == "送審前"
    assert f"/tasks/{task_id}/flows?flow=%E7%89%88%E6%9C%AC%E6%B5%81%E7%A8%8B&version_id=manual-1" in data["versions"][0]["view_url"]
    assert _flow_version_count(str(flow_dir), "版本流程") == 1


def test_create_flow_version_endpoint_creates_manual_snapshot(app, client) -> None:
    task_id = "flow-version-create-endpoint"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    with app.test_request_context():
        url = url_for("flow_version_api_bp.create_flow_version", task_id=task_id, flow_name="版本流程")

    response = client.post(url, data={"version_name": "送審前"})
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["version"]["name"] == "送審前"
    assert data["version_count"] == 1
    assert len(data["versions"]) == 1
    assert data["versions"][0]["name"] == "送審前"

    metadata = _version_metadata(flow_dir, "版本流程")
    assert len(metadata["versions"]) == 1
    assert metadata["versions"][0]["source"] == "manual_snapshot"
    assert metadata["versions"][0]["name"] == "送審前"


def test_create_flow_version_endpoint_rejects_duplicate_manual_name(app, client) -> None:
    task_id = "flow-version-create-duplicate"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_manual_snapshot"
    (versions_dir / f"{base_name}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "manual-1",
                        "name": "送審前",
                        "slug": "manual_snapshot",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_api_bp.create_flow_version", task_id=task_id, flow_name="版本流程")

    response = client.post(url, data={"version_name": "送審前"})
    assert response.status_code == 400
    data = response.get_json()
    assert data["ok"] is False
    assert data["error"] == "版本名稱已存在"


def test_delete_flow_version_endpoint_removes_manual_snapshot(app, client) -> None:
    task_id = "flow-version-delete-endpoint"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_manual_snapshot"
    version_path = versions_dir / f"{base_name}.json"
    version_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "manual-1",
                        "name": "送審前",
                        "slug": "manual_snapshot",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_api_bp.delete_flow_version", task_id=task_id, flow_name="版本流程", version_id="manual-1")

    response = client.post(url)
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["deleted_version"]["name"] == "送審前"
    assert data["version_count"] == 0
    assert data["versions"] == []
    assert not version_path.exists()

    metadata = _version_metadata(flow_dir, "版本流程")
    assert metadata["versions"] == []


def test_rename_flow_version_endpoint_updates_manual_snapshot_name(app, client) -> None:
    task_id = "flow-version-rename-endpoint"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_manual_snapshot"
    (versions_dir / f"{base_name}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "manual-1",
                        "name": "送審前",
                        "slug": "manual_snapshot",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_api_bp.rename_flow_version", task_id=task_id, flow_name="版本流程", version_id="manual-1")

    response = client.post(url, data={"version_name": "送審後"})
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["renamed_version"]["name"] == "送審後"
    assert data["versions"][0]["name"] == "送審後"

    metadata = _version_metadata(flow_dir, "版本流程")
    assert metadata["versions"][0]["name"] == "送審後"


def test_rename_flow_version_endpoint_rejects_duplicate_manual_name(app, client) -> None:
    task_id = "flow-version-rename-duplicate"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_one = "20260324110000_manual_snapshot"
    base_two = "20260324110100_manual_snapshot"
    (versions_dir / f"{base_one}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / f"{base_two}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "manual-1",
                        "name": "v1",
                        "slug": "manual_snapshot",
                        "base_name": base_one,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash-1",
                    },
                    {
                        "id": "manual-2",
                        "name": "v2",
                        "slug": "manual_snapshot",
                        "base_name": base_two,
                        "created_at": "2026-03-24T11:01:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash-2",
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_api_bp.rename_flow_version", task_id=task_id, flow_name="版本流程", version_id="manual-2")

    response = client.post(url, data={"version_name": "v1"})
    assert response.status_code == 400
    data = response.get_json()
    assert data["ok"] is False
    assert data["error"] == "版本名稱已存在"


def test_flow_builder_can_preview_version_in_readonly_mode(app, client) -> None:
    task_id = "flow-version-preview"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    current_payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "current-output",
    }
    preview_payload = {
        "created": "2026-03-24 10:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "preview content"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "preview-output",
    }
    _write_flow(flow_dir / "版本流程.json", current_payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_preview_target"
    (versions_dir / f"{base_name}.json").write_text(
        json.dumps(preview_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "preview-1",
                        "name": "送審前",
                        "slug": "preview_target",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "preview-hash",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_builder_bp.flow_builder", task_id=task_id, flow="版本流程", version_id="preview-1")

    response = client.get(url)
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "唯讀模式" in html
    assert "送審前" in html
    assert "preview-output" in html
    assert "current-output" not in html
    assert 'id="flowNameInput"' in html and "disabled" in html
    assert 'id="outputFilenameInput"' in html and "disabled" in html
    assert "回復此版本" in html
    assert f"/tasks/{task_id}/flows/%E7%89%88%E6%9C%AC%E6%B5%81%E7%A8%8B/versions/preview-1/restore" in html
    assert "const IS_VERSION_PREVIEW = true;" in html


def test_flow_builder_shows_undo_last_restore_only_once(app, client) -> None:
    task_id = "flow-version-undo-banner"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    backup_base = "20260324110100_before_restore"
    (versions_dir / f"{backup_base}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "backup-1",
                        "name": "回復前備份（目標：送審前）",
                        "slug": "before_restore",
                        "base_name": backup_base,
                        "created_at": "2026-03-24T11:01:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "before_restore",
                        "content_hash": "backup-hash",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    with app.test_request_context():
        url = url_for("flow_builder_bp.flow_builder", task_id=task_id, flow="版本流程", show_restore_notice="1")
        clean_url = url_for("flow_builder_bp.flow_builder", task_id=task_id, flow="版本流程")

    response = client.get(url)
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "撤銷版本回復" in html
    assert "撤銷" in html
    assert f"/tasks/{task_id}/flows/%E7%89%88%E6%9C%AC%E6%B5%81%E7%A8%8B/versions/backup-1/restore" in html

    second_response = client.get(clean_url)
    assert second_response.status_code == 200
    second_html = second_response.get_data(as_text=True)
    assert "撤銷版本回復" not in second_html


def test_restore_before_restore_sets_undo_success_flash(app, client) -> None:
    task_id = "flow-version-undo-flash"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    current_payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "current"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    undo_payload = {
        "created": "2026-03-24 10:30",
        "steps": [{"type": "insert_text_after", "params": {"text": "undo target"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", current_payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_before_restore"
    (versions_dir / f"{base_name}.json").write_text(
        json.dumps(undo_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "backup-1",
                        "name": "回復前備份（目標：送審前）",
                        "slug": "before_restore",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "before_restore",
                        "content_hash": "dummy",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_version_bp.restore_flow_version", task_id=task_id, flow_name="版本流程", version_id="backup-1")

    response = client.post(url)
    assert response.status_code == 200
    with client.session_transaction() as sess:
        flashes = sess.get("_flashes", [])
    assert ("success", "已成功撤銷上次回復。") in flashes


def test_flow_save_version_creates_named_manual_snapshot(app, client) -> None:
    task_id = "flow-version-manual"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save_version",
            "flow_name": "版本流程",
            "version_name": "送審前",
            "ordered_ids": "s1",
            "step_s1_type": "insert_text_after",
            "step_s1_text": "manual content",
            "step_s1_template_index": "",
            "step_s1_template_mode": "insert_after",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 302
    metadata = _version_metadata(flow_dir, "版本流程")
    assert len(metadata["versions"]) == 1
    assert metadata["versions"][0]["name"] == "送審前"
    assert metadata["versions"][0]["source"] == "manual_snapshot"
    base_name = metadata["versions"][0]["base_name"]
    saved_snapshot = json.loads((flow_dir / "_versions" / "版本流程" / f"{base_name}.json").read_text(encoding="utf-8"))
    assert isinstance(saved_snapshot, dict)
    assert (flow_dir / "版本流程.json").is_file()


def test_flow_save_version_rejects_duplicate_manual_name(app, client) -> None:
    task_id = "flow-version-manual-duplicate"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        shutil.rmtree(tdir)
    flow_dir = tdir / "flows"
    (tdir / "files").mkdir(parents=True, exist_ok=True)

    existing_payload = {
        "created": "2026-03-24 11:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "existing"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    _write_flow(flow_dir / "版本流程.json", existing_payload)

    versions_dir = flow_dir / "_versions" / "版本流程"
    versions_dir.mkdir(parents=True, exist_ok=True)
    base_name = "20260324110000_manual_snapshot"
    (versions_dir / f"{base_name}.json").write_text(json.dumps(existing_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (versions_dir / "metadata.json").write_text(
        json.dumps(
            {
                "versions": [
                    {
                        "id": "manual-1",
                        "name": "送審前",
                        "slug": "manual_snapshot",
                        "base_name": base_name,
                        "created_at": "2026-03-24T11:00:00",
                        "created_by": "",
                        "flow_name": "版本流程",
                        "source": "manual_snapshot",
                        "content_hash": "manual-hash",
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save_version",
            "flow_name": "版本流程",
            "version_name": "送審前",
            "ordered_ids": "s1",
            "step_s1_type": "insert_text_after",
            "step_s1_text": "manual content",
            "step_s1_template_index": "",
            "step_s1_template_mode": "insert_after",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 400
    assert "版本名稱已存在" in response.get_data(as_text=True)
