from pathlib import Path

import pytest
import shutil
import zipfile
from io import BytesIO
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


def test_flow_list_task_files_endpoint_reads_output_scope(app, client) -> None:
    task_id = "flow-output-browser"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    output_dir = task_root / "output" / "pkg" / "files"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "result.txt").write_text("ok", encoding="utf-8")
    (task_root / "output" / ".uo_flow_copy_registry.json").write_text("{}", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_list_task_files", task_id=task_id)

    response = client.get(url, query_string={"scope": "output", "path": "pkg"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["scope"] == "output"
    assert data["path"] == "pkg"
    assert any(item["path"] == "pkg/files" for item in data["dirs"])
    assert all(item["name"] != ".uo_flow_copy_registry.json" for item in data["files"])


def test_flow_create_task_folder_endpoint_creates_output_scope_directory(app, client) -> None:
    task_id = "flow-output-create-folder"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    output_dir = task_root / "output" / "pkg"
    output_dir.mkdir(parents=True, exist_ok=True)

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_create_task_folder", task_id=task_id)

    response = client.post(url, data={"scope": "output", "parent": "pkg", "name": "attachments"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["scope"] == "output"
    assert data["path"] == "pkg/attachments"
    assert (output_dir / "attachments").is_dir()


def test_flow_output_scope_downloads_single_file(app, client) -> None:
    task_id = "flow-output-download-file"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    target_file = task_root / "output" / "pkg" / "report.txt"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("report", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_download_task_file", task_id=task_id)

    response = client.get(url, query_string={"scope": "output", "path": "pkg/report.txt"})

    assert response.status_code == 200
    assert response.data == b"report"
    assert "attachment" in response.headers.get("Content-Disposition", "")


def test_flow_output_scope_downloads_zip(app, client) -> None:
    task_id = "flow-output-download-zip"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    target_file = task_root / "output" / "pkg" / "report.txt"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("report", encoding="utf-8")
    (task_root / "output" / ".uo_flow_copy_registry.json").write_text("{}", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_download_task_scope_zip", task_id=task_id)

    response = client.get(url, query_string={"scope": "output"})

    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    assert len(response.data) > 20
    with zipfile.ZipFile(BytesIO(response.data), "r") as archive:
        assert ".uo_flow_copy_registry.json" not in archive.namelist()
        assert "pkg/report.txt" in archive.namelist()


def test_flow_output_scope_downloads_subfolder_zip(app, client) -> None:
    task_id = "flow-output-download-subfolder-zip"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    (task_root / "output" / "pkg").mkdir(parents=True, exist_ok=True)
    (task_root / "output" / "other").mkdir(parents=True, exist_ok=True)
    (task_root / "output" / "pkg" / "a.txt").write_text("a", encoding="utf-8")
    (task_root / "output" / "other" / "b.txt").write_text("b", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_download_task_scope_zip", task_id=task_id)

    response = client.get(url, query_string={"scope": "output", "path": "pkg"})

    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.data), "r") as archive:
        assert "a.txt" in archive.namelist()
        assert "other/b.txt" not in archive.namelist()


def test_flow_output_scope_renames_entry(app, client) -> None:
    task_id = "flow-output-rename-entry"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    target_file = task_root / "output" / "pkg" / "report.txt"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("report", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_rename_task_entry", task_id=task_id)

    response = client.post(url, data={"scope": "output", "path": "pkg/report.txt", "name": "renamed.txt"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["path"] == "pkg/renamed.txt"
    assert not target_file.exists()
    assert (task_root / "output" / "pkg" / "renamed.txt").is_file()


def test_flow_output_scope_deletes_entry(app, client) -> None:
    task_id = "flow-output-delete-entry"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    target_file = task_root / "output" / "pkg" / "report.txt"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("report", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_delete_task_entry", task_id=task_id)

    response = client.post(url, data={"scope": "output", "path": "pkg/report.txt"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["deleted"] == "pkg/report.txt"
    assert not target_file.exists()


def test_flow_output_scope_clear_removes_all_entries(app, client) -> None:
    task_id = "flow-output-clear"
    task_root = Path(app.config["TASK_FOLDER"]) / task_id
    if task_root.exists():
        shutil.rmtree(task_root)
    output_dir = task_root / "output"
    (output_dir / "pkg").mkdir(parents=True, exist_ok=True)
    (output_dir / "pkg" / "report.txt").write_text("report", encoding="utf-8")
    (output_dir / ".uo_flow_copy_registry.json").write_text("{}", encoding="utf-8")

    with app.test_request_context():
        url = url_for("flow_file_bp.api_flow_clear_task_scope", task_id=task_id)

    response = client.post(url, data={"scope": "output"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["cleared"] is True
    assert list(output_dir.iterdir()) == []
