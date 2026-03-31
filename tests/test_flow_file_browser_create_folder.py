from pathlib import Path

import pytest
import shutil
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
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    files_dir = task_root / "files" / "輸出-USTAR II System"
    files_dir.mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_create_task_folder", task_id=task_id)

    response = client.post(url, data={"parent": "輸出-USTAR II System", "name": "IFU_New"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["path"] == "輸出-USTAR II System/IFU_New"
    assert (files_dir / "IFU_New").is_dir()


def test_flow_create_task_folder_endpoint_rejects_nested_name(app, client) -> None:
    task_id = "flow-create-folder-invalid"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    (task_root / "files").mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_create_task_folder", task_id=task_id)

    response = client.post(url, data={"parent": "", "name": "A/B"})

    assert response.status_code == 400
    data = response.get_json()
    assert data["ok"] is False
    assert "資料夾名稱不可包含" in data["error"]


def test_flow_rename_task_folder_endpoint_renames_child_directory(app, client) -> None:
    task_id = "flow-rename-folder"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    files_dir = task_root / "files" / "輸出區"
    target_dir = files_dir / "舊名稱"
    target_dir.mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_rename_task_folder", task_id=task_id)

    response = client.post(url, data={"path": "輸出區/舊名稱", "name": "新名稱"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["path"] == "輸出區/新名稱"
    assert not target_dir.exists()
    assert (files_dir / "新名稱").is_dir()


def test_flow_delete_task_folder_endpoint_deletes_child_directory(app, client) -> None:
    task_id = "flow-delete-folder"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    target_dir = task_root / "files" / "輸出區" / "待刪除"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "x.txt").write_text("x", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_delete_task_folder", task_id=task_id)

    response = client.post(url, data={"path": "輸出區/待刪除"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["deleted"] == "輸出區/待刪除"
    assert not target_dir.exists()
