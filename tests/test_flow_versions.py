import json
import shutil
from pathlib import Path

import pytest
from flask import url_for

from app import create_app
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


def test_flow_save_creates_version_snapshot_before_overwrite(app, client) -> None:
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
        url = url_for("flows_bp.run_flow", task_id=task_id)

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
    assert len(metadata["versions"]) == 1
    base_name = metadata["versions"][0]["base_name"]
    saved_snapshot = json.loads((flow_dir / "_versions" / "版本流程" / f"{base_name}.json").read_text(encoding="utf-8"))
    assert saved_snapshot == original_payload
    assert metadata["versions"][0]["source"] == "auto_save"


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
        url = url_for("flows_bp.run_flow", task_id=task_id)

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
        url = url_for("flows_bp.restore_flow_version", task_id=task_id, flow_name="版本流程", version_id="version-1")

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
    backup_payload = json.loads((versions_dir / f"{backup_entry['base_name']}.json").read_text(encoding="utf-8"))
    assert backup_payload == current_payload
