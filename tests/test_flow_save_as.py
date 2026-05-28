import json
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


def test_flow_save_as_creates_new_flow_without_overwriting_original(app, client) -> None:
    task_id = "flow-save-as"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        import shutil
        shutil.rmtree(tdir)
    files_dir = tdir / "files"
    flow_dir = tdir / "flows"
    files_dir.mkdir(parents=True, exist_ok=True)
    flow_dir.mkdir(parents=True, exist_ok=True)

    original_path = flow_dir / "原流程.json"
    original_payload = {
        "created": "2026-03-17 12:00",
        "steps": [{"type": "insert_text_after", "params": {"text": "original"}}],
        "template_file": "",
        "document_format": "default",
        "line_spacing": "1.5",
        "apply_formatting": True,
        "output_filename": "",
    }
    original_path.write_text(json.dumps(original_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save_as",
            "flow_name": "原流程",
            "save_as_name": "原流程_副本",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 302
    assert "flow=%E5%8E%9F%E6%B5%81%E7%A8%8B_%E5%89%AF%E6%9C%AC" in response.headers["Location"]

    copied_path = flow_dir / "原流程_副本.json"
    assert copied_path.is_file()

    original_after = json.loads(original_path.read_text(encoding="utf-8"))
    copied_after = json.loads(copied_path.read_text(encoding="utf-8"))
    assert original_after == original_payload
    assert copied_after["steps"] == []
    assert copied_after["document_format"] == "default"
    assert copied_after["line_spacing"] == "1.5"


def test_flow_save_as_rejects_existing_name(app, client) -> None:
    task_id = "flow-save-as-duplicate"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        import shutil
        shutil.rmtree(tdir)
    files_dir = tdir / "files"
    flow_dir = tdir / "flows"
    files_dir.mkdir(parents=True, exist_ok=True)
    flow_dir.mkdir(parents=True, exist_ok=True)

    (flow_dir / "既有流程.json").write_text("{}", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save_as",
            "flow_name": "原流程",
            "save_as_name": "既有流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 400
    assert "流程名稱已存在" in response.get_data(as_text=True)


def test_flow_save_redirect_keeps_saved_flow_loaded(app, client) -> None:
    task_id = "flow-save-redirect"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        import shutil
        shutil.rmtree(tdir)
    files_dir = tdir / "files"
    flow_dir = tdir / "flows"
    files_dir.mkdir(parents=True, exist_ok=True)
    flow_dir.mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flow_execution_bp.run_flow", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "save",
            "flow_name": "預防性存檔流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 302
    assert "flow=%E9%A0%90%E9%98%B2%E6%80%A7%E5%AD%98%E6%AA%94%E6%B5%81%E7%A8%8B" in response.headers["Location"]
