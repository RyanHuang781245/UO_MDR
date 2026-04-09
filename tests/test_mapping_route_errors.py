import os
import json
import shutil
from io import BytesIO
from pathlib import Path

import pytest
from flask import url_for

from app import create_app
from app.extensions import ldap_manager

os.environ["LDAP_HOST"] = "localhost"


def _mapping_session_dirs(task_dir: Path) -> list[Path]:
    base_dir = task_dir / "_mapping_sessions"
    if not base_dir.is_dir():
        return []
    return sorted(
        [
            client_dir
            for owner_dir in base_dir.iterdir()
            if owner_dir.is_dir()
            for client_dir in owner_dir.iterdir()
            if client_dir.is_dir()
        ]
    )


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


def test_mapping_route_renders_error_steps_even_when_all_rows_fail(app, client, monkeypatch) -> None:
    task_id = "mapping-all-error"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        assert validate_only is True
        assert validate_extract_only is False
        log_path = Path(log_dir or output_dir) / "mapping_log_test.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        messages = [
            "ERROR: (Row 3) Extract chapter :: source.docx (chapter 1.1.1, title General description) :: 來源檔案解析失敗: file not found: FolderA/source.docx",
            "ERROR: (Row 4) Extract chapter :: source.docx (chapter 1.1.2, title Principles) :: 來源檔案解析失敗: file not found: FolderB/source.docx",
        ]
        log_path.write_text(
            json.dumps({"messages": messages, "runs": []}, ensure_ascii=False),
            encoding="utf-8",
        )
        return {"logs": messages, "outputs": [], "log_file": log_path.name, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "check",
            "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "(Row 3) Extract chapter" in html
    assert "(Row 4) Extract chapter" in html
    assert "來源檔案解析失敗: file not found: FolderA/source.docx" in html
    assert "檢查過程中發現錯誤" not in html


def test_mapping_route_accepts_chinese_mapping_filename(app, client, monkeypatch) -> None:
    task_id = "mapping-chinese-name"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    captured: dict[str, str] = {}

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        captured["mapping_name"] = Path(mapping_path).name
        captured["validate_only"] = validate_only
        captured["validate_extract_only"] = validate_extract_only
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "check",
            "mapping_file": (BytesIO(b"dummy"), "全中文測試.xlsx"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    assert captured["mapping_name"] == "全中文測試.xlsx"
    assert captured["validate_only"] is True
    assert captured["validate_extract_only"] is False
    session_dirs = _mapping_session_dirs(task_dir)
    assert len(session_dirs) == 1
    assert (session_dirs[0] / "全中文測試.xlsx").is_file()


def test_mapping_route_displays_original_mixed_language_filename(app, client, monkeypatch) -> None:
    task_id = "mapping-mixed-display-name"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    original_name = "Mapping_ch1 - 複製.xlsx"
    safe_name = "Mapping_ch1_-_.xlsx"

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        assert Path(mapping_path).name == safe_name
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "check",
            "mapping_file": (BytesIO(b"dummy"), original_name),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert original_name in html
    session_dirs = _mapping_session_dirs(task_dir)
    assert len(session_dirs) == 1
    assert (session_dirs[0] / safe_name).is_file()
    state = json.loads((session_dirs[0] / "mapping_validation_state.json").read_text(encoding="utf-8"))
    assert state["mapping_file"] == safe_name
    assert state["mapping_display_name"] == original_name


def test_mapping_route_uses_isolated_workspace_per_client(app, monkeypatch) -> None:
    task_id = "mapping-session-isolation"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    client_a = app.test_client()
    client_b = app.test_client()
    calls: list[dict[str, str]] = []

    def fake_process_mapping_excel(mapping_path, task_files_dir, output_dir, log_dir=None, validate_only=False, validate_extract_only=False):
        calls.append(
            {
                "mapping_name": Path(mapping_path).name,
                "workspace_dir": str(Path(mapping_path).parent),
                "mode": "check" if validate_only else ("check_extract" if validate_extract_only else "run"),
            }
        )
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response_a = client_a.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"client-a"), "client-a.xlsx")},
        content_type="multipart/form-data",
    )
    response_b = client_b.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"client-b"), "client-b.xlsx")},
        content_type="multipart/form-data",
    )
    response_b_refresh = client_b.get(url)
    response_a_extract = client_a.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
    )

    assert response_a.status_code == 200
    assert response_b.status_code == 200
    assert response_b_refresh.status_code == 200
    assert response_a_extract.status_code == 200
    assert [call["mapping_name"] for call in calls] == ["client-a.xlsx", "client-b.xlsx", "client-a.xlsx"]

    session_dirs = _mapping_session_dirs(task_dir)
    assert len(session_dirs) == 2
    client_a_workspace = Path(calls[0]["workspace_dir"])
    client_b_workspace = Path(calls[1]["workspace_dir"])
    assert client_a_workspace != client_b_workspace
    assert (client_a_workspace / "client-a.xlsx").is_file()
    assert (client_a_workspace / "mapping_validation_state.json").is_file()
    assert not (client_b_workspace / "client-b.xlsx").exists()


def test_mapping_route_uses_separate_workspace_when_user_changes_in_same_session(app, monkeypatch) -> None:
    task_id = "mapping-user-session-isolation"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    client = app.test_client()
    calls: list[dict[str, str]] = []
    actor_state = {"work_id": "A123"}

    def fake_actor_info():
        work_id = actor_state["work_id"]
        return work_id, work_id

    def fake_process_mapping_excel(mapping_path, task_files_dir, output_dir, log_dir=None, validate_only=False, validate_extract_only=False):
        calls.append(
            {
                "mapping_name": Path(mapping_path).name,
                "workspace_dir": str(Path(mapping_path).parent),
            }
        )
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes._get_actor_info", fake_actor_info)
    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    first_response = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"user-a"), "user-a.xlsx")},
        content_type="multipart/form-data",
    )
    actor_state["work_id"] = "B456"
    second_response = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"user-b"), "user-b.xlsx")},
        content_type="multipart/form-data",
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert [call["mapping_name"] for call in calls] == ["user-a.xlsx", "user-b.xlsx"]
    assert Path(calls[0]["workspace_dir"]).parts[-2] == "A123"
    assert Path(calls[1]["workspace_dir"]).parts[-2] == "B456"
    assert Path(calls[0]["workspace_dir"]) != Path(calls[1]["workspace_dir"])


def test_mapping_route_check_extract_requires_reference_check_first(app, client, monkeypatch) -> None:
    task_id = "mapping-check-extract"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}

    def fake_process_mapping_excel(mapping_path, task_files_dir, output_dir, log_dir=None, validate_only=False, validate_extract_only=False):
        captured["validate_only"] = validate_only
        captured["validate_extract_only"] = validate_extract_only
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "check_extract",
            "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    assert captured == {}
    assert "請先通過檢查引用文件" in response.get_data(as_text=True)


def test_mapping_route_check_extract_can_reuse_cached_mapping_after_reference_check(app, client, monkeypatch) -> None:
    task_id = "mapping-check-sequence"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    calls: list[dict[str, object]] = []

    def fake_process_mapping_excel(mapping_path, task_files_dir, output_dir, log_dir=None, validate_only=False, validate_extract_only=False):
        calls.append(
            {
                "mapping_name": Path(mapping_path).name,
                "validate_only": validate_only,
                "validate_extract_only": validate_extract_only,
            }
        )
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response_check = client.post(
        url,
        data={
            "action": "check",
            "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx"),
        },
        content_type="multipart/form-data",
    )
    assert response_check.status_code == 200
    assert calls[-1]["validate_only"] is True

    response_extract = client.post(
        url,
        data={
            "action": "check_extract",
        },
        content_type="multipart/form-data",
    )
    assert response_extract.status_code == 200
    assert calls[-1]["mapping_name"] == "mapping.xlsx"
    assert calls[-1]["validate_only"] is False
    assert calls[-1]["validate_extract_only"] is True


def test_mapping_route_only_shows_generate_after_extract_check(app, client, monkeypatch) -> None:
    task_id = "mapping-generate-gate"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(mapping_path, task_files_dir, output_dir, log_dir=None, validate_only=False, validate_extract_only=False):
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response_check = client.post(
        url,
        data={
            "action": "check",
            "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx"),
        },
        content_type="multipart/form-data",
    )
    html_check = response_check.get_data(as_text=True)
    assert response_check.status_code == 200
    assert 'value="run_cached"' not in html_check

    response_extract = client.post(
        url,
        data={
            "action": "check_extract",
        },
        content_type="multipart/form-data",
    )
    html_extract = response_extract.get_data(as_text=True)
    assert response_extract.status_code == 200
    assert 'value="run_cached"' in html_extract


def test_mapping_route_renders_copy_steps_with_row_labels(app, client, monkeypatch) -> None:
    task_id = "mapping-copy-rows"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        log_path = Path(log_dir or output_dir) / "mapping_log_copy_rows.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "messages": [],
            "runs": [
                {
                    "output": "pkg",
                    "workflow_log": [
                        {
                            "step": 1,
                            "type": "copy_folder",
                            "params": {
                                "mapping_row": 3,
                                "source": r"C:\tmp\IFU",
                                "destination": r"C:\dest\pkg\folders",
                            },
                            "status": "ok",
                            "error": "",
                        },
                        {
                            "step": 2,
                            "type": "copy_file",
                            "params": {
                                "mapping_row": 4,
                                "source": r"C:\tmp\labeling.pdf",
                                "destination": r"C:\dest\pkg\files",
                            },
                            "status": "ok",
                            "error": "",
                        },
                    ],
                }
            ],
        }
        log_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return {"logs": [], "outputs": [], "log_file": log_path.name, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={
            "action": "check",
            "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "(Row 3) 複製資料夾" in html
    assert "(Row 4) 複製檔案" in html
    assert r"C:/dest/pkg/folders" in html or r"C:\dest\pkg\folders" in html
    assert r"C:/dest/pkg/files" in html or r"C:\dest\pkg\files" in html
