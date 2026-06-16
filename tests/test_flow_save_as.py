import json
from pathlib import Path

import pytest
from flask import url_for

from app import create_app
import app.blueprints.flows.execution_routes as execution_routes
from app.extensions import ldap_manager
from app.models.execution import JobRecord


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
    assert "save_status=saved" in response.headers["Location"]
    assert JobRecord.query.count() == 0


def test_flow_submit_without_action_saves_without_executing(app, client) -> None:
    task_id = "flow-save-default-no-execute"
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
            "flow_name": "預設保存流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": "default",
            "line_spacing": "1.5",
            "apply_formatting": "true",
        },
    )

    assert response.status_code == 302
    assert (flow_dir / "預設保存流程.json").is_file()
    assert JobRecord.query.count() == 0


def test_flow_save_defaults_format_settings_to_none(app, client) -> None:
    task_id = "flow-save-default-format-none"
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
            "flow_name": "預設格式流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
        },
    )

    assert response.status_code == 302
    payload = json.loads((flow_dir / "預設格式流程.json").read_text(encoding="utf-8"))
    assert payload["document_format"] == "none"
    assert payload["line_spacing"] == "none"
    assert payload["apply_formatting"] is False


def test_flow_save_derives_apply_formatting_from_selects(app, client) -> None:
    task_id = "flow-save-derived-format"
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
            "flow_name": "下拉決定格式",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": "modern",
            "line_spacing": "2",
        },
    )

    assert response.status_code == 302
    payload = json.loads((flow_dir / "下拉決定格式.json").read_text(encoding="utf-8"))
    assert payload["document_format"] == "modern"
    assert payload["line_spacing"] == "2"
    assert payload["apply_formatting"] is True


@pytest.mark.parametrize(
    ("document_format", "line_spacing"),
    [
        ("modern", "none"),
        ("none", "2"),
    ],
)
def test_flow_save_applies_formatting_when_either_select_is_set(
    app,
    client,
    document_format,
    line_spacing,
) -> None:
    task_id = f"flow-save-partial-format-{document_format}-{line_spacing}"
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
            "flow_name": "部分格式流程",
            "ordered_ids": "",
            "template_file": "",
            "output_filename": "",
            "document_format": document_format,
            "line_spacing": line_spacing,
        },
    )

    assert response.status_code == 302
    payload = json.loads((flow_dir / "部分格式流程.json").read_text(encoding="utf-8"))
    assert payload["document_format"] == document_format
    assert payload["line_spacing"] == line_spacing
    assert payload["apply_formatting"] is True


def test_execute_saved_flow_uses_saved_format_settings(app, client, monkeypatch) -> None:
    task_id = "flow-execute-saved-format"
    tdir = Path(app.config["TASK_FOLDER"]) / task_id
    if tdir.exists():
        import shutil
        shutil.rmtree(tdir)
    files_dir = tdir / "files"
    flow_dir = tdir / "flows"
    files_dir.mkdir(parents=True, exist_ok=True)
    flow_dir.mkdir(parents=True, exist_ok=True)

    flow_path = flow_dir / "格式流程.json"
    flow_path.write_text(
        json.dumps(
            {
                "created": "2026-03-17 12:00",
                "steps": [],
                "template_file": "",
                "document_format": "modern",
                "line_spacing": "2",
                "apply_formatting": True,
                "output_filename": "",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    captured = {}

    def fake_queue_single_flow_job(**kwargs):
        captured.update(kwargs)
        return "job-format"

    monkeypatch.setattr(execution_routes, "_queue_single_flow_job", fake_queue_single_flow_job)

    with app.test_request_context():
        url = url_for("flow_execution_bp.execute_flow", task_id=task_id, flow_name="格式流程")

    response = client.post(url, data={"fpage": "1"})

    assert response.status_code == 302
    assert captured["document_format"] == "modern"
    assert captured["line_spacing"] == 2.0
    assert captured["apply_formatting"] is True
