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


def test_flow_create_task_folder_endpoint_creates_child_directory(app, client) -> None:
    task_id = "flow-create-folder"
    files_dir = Path(app.config["TASK_FOLDER"]) / task_id / "files" / "輸出-USTAR II System"
    files_dir.mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flows_bp.api_flow_create_task_folder", task_id=task_id)

    response = client.post(url, data={"parent": "輸出-USTAR II System", "name": "IFU_New"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["path"] == "輸出-USTAR II System/IFU_New"
    assert (files_dir / "IFU_New").is_dir()


def test_flow_create_task_folder_endpoint_rejects_nested_name(app, client) -> None:
    task_id = "flow-create-folder-invalid"
    (Path(app.config["TASK_FOLDER"]) / task_id / "files").mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flows_bp.api_flow_create_task_folder", task_id=task_id)

    response = client.post(url, data={"parent": "", "name": "A/B"})

    assert response.status_code == 400
    data = response.get_json()
    assert data["ok"] is False
    assert "資料夾名稱不可包含" in data["error"]
