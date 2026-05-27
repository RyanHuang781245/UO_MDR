import shutil
import os
from pathlib import Path

from io import BytesIO

import pytest
from flask import url_for

from app import create_app
from app.extensions import ldap_manager
from app.extensions import db
from app.models.execution import JobRecord
from datetime import datetime, timedelta


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


def test_mapping_output_query_download_supports_chinese_path(app, client) -> None:
    task_id = "mapping-download-cn"
    base_dir = Path(app.config["TASK_FOLDER"]) / task_id / "mapping_job" / "中文資料夾"
    base_dir.mkdir(parents=True, exist_ok=True)
    target = base_dir / "測試文件.docx"
    target.write_bytes(b"test-content")

    with app.test_request_context():
        url = url_for(
            "tasks_bp.task_download_output_query",
            task_id=task_id,
            filename="中文資料夾/測試文件.docx",
        )

    response = client.get(url, follow_redirects=True)

    assert response.status_code == 200
    assert response.data == b"test-content"


def _set_tree_mtime(target: Path, timestamp: float) -> None:
    for path in sorted(target.rglob("*"), reverse=True):
        os.utime(path, (timestamp, timestamp))
    os.utime(target, (timestamp, timestamp))


def test_mapping_route_cleans_up_stale_workspaces(app, client) -> None:
    task_id = "mapping-workspace-cleanup"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    old_workspace = task_dir / "_mapping_sessions" / "anonymous" / "oldclient"
    (old_workspace / "_validation" / "run12345").mkdir(parents=True, exist_ok=True)
    (old_workspace / "mapping.xlsx").write_bytes(b"dummy")
    (old_workspace / "_validation" / "run12345" / "mapping_check_log.json").write_text("{}", encoding="utf-8")
    stale_ts = (datetime.now() - timedelta(days=8)).timestamp()
    _set_tree_mtime(old_workspace, stale_ts)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.get(url)

    assert response.status_code == 200
    assert not old_workspace.exists()


def test_mapping_route_keeps_stale_workspace_with_active_ops(app, client) -> None:
    task_id = "mapping-workspace-keep-active"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    old_workspace = task_dir / "_mapping_sessions" / "anonymous" / "oldclient"
    (old_workspace / "_ops").mkdir(parents=True, exist_ok=True)
    (old_workspace / "_ops" / "run12345.json").write_text(
        '{"op_id":"run12345","status":"running","action":"check"}',
        encoding="utf-8",
    )
    stale_ts = (datetime.now() - timedelta(days=8)).timestamp()
    _set_tree_mtime(old_workspace, stale_ts)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.get(url)

    assert response.status_code == 200
    assert old_workspace.exists()


def test_mapping_route_uses_per_run_output_subdirectory(app, client, monkeypatch) -> None:
    task_id = "mapping-run-subdir"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
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
        captured["mapping_path"] = str(mapping_path)
        captured["output_dir"] = str(output_dir)
        captured["log_dir"] = str(log_dir or "")
        out_path = Path(output_dir) / "pkg" / "result.docx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"docx")
        log_path = Path(log_dir or output_dir) / "mapping_log.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        zip_path = Path(output_dir) / "mapping_outputs.zip"
        zip_path.write_bytes(b"zip")
        return {
            "logs": [],
            "outputs": [str(out_path)],
            "log_file": "mapping_log.json",
            "zip_file": "mapping_outputs.zip",
        }

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
        follow_redirects=True,
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    mapping_path = Path(captured["mapping_path"])
    output_dir = Path(captured["output_dir"])
    log_dir = Path(captured["log_dir"])
    assert output_dir.parent.name == "_validation"
    assert log_dir == output_dir
    assert mapping_path.parent == output_dir
    assert mapping_path.name == "source.xlsx"
    run_id = output_dir.name
    assert len(run_id) == 8
    assert not (task_dir / "mapping_job" / run_id).exists()
    assert "生成結果" not in html
    assert f"{run_id}/pkg/result.docx" not in html


def test_mapping_route_preserves_unicode_uploaded_filename_in_workspace(app, client, monkeypatch) -> None:
    task_id = "mapping-upload-unicode-name"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        return {
            "logs": [],
            "outputs": [],
            "log_file": "mapping_log.json",
            "zip_file": None,
        }

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    upload_name = "Mapping_多段落擷取 Round 3.xlsx"
    response = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), upload_name)},
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert response.status_code == 200
    owner_dir = task_dir / "_mapping_sessions" / "anonymous"
    stored_files = [path for path in owner_dir.rglob("*.xlsx") if path.name == upload_name]
    assert stored_files
    assert stored_files[0].name == upload_name
    validation_state = stored_files[0].parent / "mapping_validation_state.json"
    assert validation_state.is_file()
    assert upload_name in validation_state.read_text(encoding="utf-8")


def test_mapping_route_localizes_english_error_messages(app, client, monkeypatch) -> None:
    task_id = "mapping-localized-errors"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        return {
            "logs": ["ERROR: file not found: missing-source.docx"],
            "outputs": [],
            "log_file": "mapping_log.json",
            "zip_file": None,
        }

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "找不到檔案： missing-source.docx" in html
    assert "ERROR: file not found: missing-source.docx" not in html


def test_mapping_check_records_completed_audit_with_status_details(app, client, monkeypatch) -> None:
    task_id = "mapping-audit-check-success"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    audits: list[dict] = []

    def fake_record_audit(action, actor=None, detail=None, task_id=None):
        audits.append(
            {
                "action": action,
                "actor": dict(actor or {}),
                "detail": dict(detail or {}),
                "task_id": task_id,
            }
        )

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        return {
            "logs": [],
            "outputs": [],
            "log_file": "mapping_log.json",
            "zip_file": None,
        }

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.record_audit", fake_record_audit)
    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert response.status_code == 200
    actions = [item["action"] for item in audits]
    assert "task_mapping_check" in actions
    assert "task_mapping_check_completed" in actions
    queued_audit = next(item for item in audits if item["action"] == "task_mapping_check")
    completed_audit = next(item for item in audits if item["action"] == "task_mapping_check_completed")
    assert queued_audit["detail"]["status"] == "queued"
    assert queued_audit["detail"]["mapping_file"] == "mapping.xlsx"
    assert completed_audit["detail"]["status"] == "completed"
    assert completed_audit["detail"]["mapping_file"] == "mapping.xlsx"
    assert completed_audit["detail"]["reference_ok"] is True
    assert completed_audit["detail"]["extract_ok"] is False
    assert completed_audit["detail"]["log_file"] == "mapping_check_log.json"


def test_mapping_check_extract_records_failed_audit_with_error_details(app, client, monkeypatch) -> None:
    task_id = "mapping-audit-check-extract-failed"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    audits: list[dict] = []

    def fake_record_audit(action, actor=None, detail=None, task_id=None):
        audits.append(
            {
                "action": action,
                "actor": dict(actor or {}),
                "detail": dict(detail or {}),
                "task_id": task_id,
            }
        )

    calls = {"count": 0}

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        calls["count"] += 1
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        if calls["count"] == 1:
            (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
            return {
                "logs": [],
                "outputs": [],
                "log_file": "mapping_log.json",
                "zip_file": None,
            }
        raise RuntimeError("extract check failed")

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.record_audit", fake_record_audit)
    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    first = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    second = client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    actions = [item["action"] for item in audits]
    assert "task_mapping_check" in actions
    assert "task_mapping_check_completed" in actions
    assert "task_mapping_check_extract" in actions
    assert "task_mapping_check_extract_failed" in actions
    failed_audit = next(item for item in audits if item["action"] == "task_mapping_check_extract_failed")
    queued_audit = next(item for item in audits if item["action"] == "task_mapping_check_extract")
    assert queued_audit["detail"]["status"] == "queued"
    assert failed_audit["detail"]["status"] == "failed"
    assert failed_audit["detail"]["mapping_file"] == "mapping.xlsx"
    assert failed_audit["detail"]["extract_ok"] is False
    assert failed_audit["detail"]["error"] == "extract check failed"


def test_mapping_route_blocks_workspace_reset_when_active_op_exists(app, client, monkeypatch) -> None:
    task_id = "mapping-workspace-active-op-lock"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    with client.session_transaction() as sess:
        sess["mapping_client_id"] = "a" * 32

    workspace_dir = task_dir / "_mapping_sessions" / "anonymous" / ("a" * 32)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    existing_file = workspace_dir / "既有_mapping.xlsx"
    existing_file.write_bytes(b"old")
    (workspace_dir / "mapping_last.txt").write_text(existing_file.name, encoding="utf-8")
    (workspace_dir / "mapping_validation_state.json").write_text(
        '{"mapping_file":"既有_mapping.xlsx","mapping_display_name":"既有_mapping.xlsx","reference_ok":false,"extract_ok":false,"run_id":"runlock01"}',
        encoding="utf-8",
    )
    ops_dir = workspace_dir / "_ops"
    ops_dir.mkdir(parents=True, exist_ok=True)
    (ops_dir / "runlock01.json").write_text(
        '{"op_id":"runlock01","status":"running","action":"check","mapping_display_name":"既有_mapping.xlsx","resume_url":"/tasks/'
        + task_id
        + '/mapping?mapping_tab=create&mapping_job=runlock01"}',
        encoding="utf-8",
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("process_mapping_excel should not run when workspace has an active op")

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fail_if_called)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"new"), "新_mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert existing_file.is_file()
    assert not (workspace_dir / "新_mapping.xlsx").exists()
    assert "mapping_job=runlock01" in response.headers["Location"]
    assert "mapping_notice=active_op" in response.headers["Location"]


def test_mapping_route_reuses_same_run_id_across_check_extract_and_run(app, client, monkeypatch) -> None:
    task_id = "mapping-run-reuse"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    calls: list[dict[str, str]] = []

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        calls.append(
            {
                "output_dir": str(output_dir),
                "log_dir": str(log_dir or ""),
                "mode": "check" if validate_only else ("check_extract" if validate_extract_only else "run"),
            }
        )
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        return {
            "logs": [],
            "outputs": [],
            "log_file": "mapping_log.json",
            "zip_file": None,
        }

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response_check = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert response_check.status_code == 200

    response_extract = client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert response_extract.status_code == 200

    response_run = client.post(
        url,
        data={"action": "run_cached"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert response_run.status_code == 200

    run_ids = [Path(call["output_dir"]).name for call in calls]
    assert len(calls) == 3
    assert run_ids[0] == run_ids[1] == run_ids[2]
    assert Path(calls[0]["output_dir"]).parent.name == "_validation"
    assert Path(calls[1]["output_dir"]).parent.name == "_validation"
    assert Path(calls[2]["output_dir"]).parent.name == "mapping_job"
    run_dirs = [p for p in (task_dir / "mapping_job").iterdir() if p.is_dir()]
    assert [p.name for p in run_dirs] == [run_ids[0]]
    run_dir = task_dir / "mapping_job" / run_ids[0]
    assert (run_dir / "mapping_run_log.json").is_file()
    assert not (run_dir / "mapping_check_log.json").exists()
    assert not (run_dir / "mapping_check_extract_log.json").exists()
    workspace_dirs = list((task_dir / "_mapping_sessions").glob("*/*"))
    assert workspace_dirs == []


def test_mapping_validation_log_download_uses_workspace_store(app, client, monkeypatch) -> None:
    task_id = "mapping-validation-log-download"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        filename = "mapping_check_extract_log.json" if validate_extract_only else "mapping_check_log.json"
        (Path(log_dir or output_dir) / filename).write_text(f'{{"kind":"{filename}"}}', encoding="utf-8")
        return {
            "logs": [],
            "outputs": [],
            "log_file": filename,
            "zip_file": None,
        }

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    assert client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    response_extract = client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    html = response_extract.get_data(as_text=True)
    run_id = next((task_dir / "_mapping_sessions").glob("*/*/_validation/*")).name

    with app.test_request_context():
        download_url = url_for(
            "tasks_bp.task_download_mapping_validation_log",
            task_id=task_id,
            run_id=run_id,
            kind="check_extract",
        )

    download_response = client.get(download_url)

    assert response_extract.status_code == 200
    assert "擷取檢查 Log" in html
    assert download_response.status_code == 200
    assert download_response.data == b'{"kind":"mapping_check_extract_log.json"}'


def test_mapping_route_starts_new_run_id_when_rechecking_after_completed_run(app, client, monkeypatch) -> None:
    task_id = "mapping-run-recheck-after-complete"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    calls: list[dict[str, str]] = []

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        mode = "check" if validate_only else ("check_extract" if validate_extract_only else "run")
        calls.append({"output_dir": str(output_dir), "mode": mode})
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        if mode == "run":
            out_path = Path(output_dir) / "pkg" / "result.docx"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(b"docx")
            return {"logs": [], "outputs": [str(out_path)], "log_file": "mapping_log.json", "zip_file": None}
        return {"logs": [], "outputs": [], "log_file": "mapping_log.json", "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    assert client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "run_cached"},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200

    run_ids = [Path(call["output_dir"]).name for call in calls]
    assert run_ids[0] == run_ids[1] == run_ids[2]
    assert run_ids[3] != run_ids[2]


def test_mapping_route_new_upload_creates_new_run_id(app, client, monkeypatch) -> None:
    task_id = "mapping-run-new-upload"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    calls: list[str] = []

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        calls.append(str(output_dir))
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        return {"logs": [], "outputs": [], "log_file": None, "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    first = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy-1"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    second = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy-2"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(calls) == 2
    assert Path(calls[0]).name != Path(calls[1]).name


def test_mapping_run_cached_writes_result_meta(app, client, monkeypatch) -> None:
    task_id = "mapping-run-meta"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    calls: list[dict[str, object]] = []

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        mode = "check" if validate_only else ("check_extract" if validate_extract_only else "run")
        calls.append({"mode": mode, "output_dir": str(output_dir)})
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        if mode == "run":
            out_path = Path(output_dir) / "pkg" / "result.docx"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(b"docx")
            (Path(output_dir) / "mapping_outputs.zip").write_bytes(b"zip")
            return {
                "logs": [],
                "outputs": [str(out_path)],
                "log_file": "mapping_log.json",
                "zip_file": "mapping_outputs.zip",
            }
        return {"logs": [], "outputs": [], "log_file": "mapping_log.json", "zip_file": None}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    original_name = "Mapping_ch1 - 複製.xlsx"

    assert client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), original_name)},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "run_cached"},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200

    run_dir = Path(calls[-1]["output_dir"])
    meta = (run_dir / "meta.json").read_text(encoding="utf-8")
    payload = __import__("json").loads(meta)
    assert payload["record_type"] == "mapping_run"
    assert payload["mapping_file"] == original_name
    assert payload["mapping_display_name"] == original_name
    assert payload["status"] == "completed"
    assert payload["reference_ok"] is True
    assert payload["extract_ok"] is True
    assert payload["output_count"] == 1
    assert payload["zip_file"] == "mapping_outputs.zip"
    assert payload["log_file"] == "mapping_run_log.json"


def test_mapping_run_cached_does_not_show_processing_status(app, client, monkeypatch) -> None:
    task_id = "mapping-run-hide-status"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        (Path(log_dir or output_dir) / "mapping_log.json").write_text(
            '{"messages":[],"runs":[{"output":"pkg","workflow_log":[{"step":1,"type":"copy_file","params":{"mapping_row":3,"source":"C:\\\\tmp\\\\labeling.pdf","destination":"C:\\\\dest\\\\pkg\\\\files"},"status":"ok","error":""}]}]}',
            encoding="utf-8",
        )
        if validate_only or validate_extract_only:
            return {"logs": [], "outputs": [], "log_file": "mapping_log.json", "zip_file": None}
        out_path = Path(output_dir) / "pkg" / "result.docx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"docx")
        (Path(output_dir) / "mapping_outputs.zip").write_bytes(b"zip")
        return {
            "logs": [],
            "outputs": [str(out_path)],
            "log_file": "mapping_log.json",
            "zip_file": "mapping_outputs.zip",
        }

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    assert client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200

    response_run = client.post(
        url,
        data={"action": "run_cached"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    html = response_run.get_data(as_text=True)

    assert response_run.status_code == 200
    assert "處理狀態" not in html
    assert "生成結果" in html
    assert "處理步驟" in html
    assert "(第 3 列) 複製檔案" in html
    assert "下載 ZIP" in html
    assert "下載 Log" in html
    assert "產出文件" in html


def test_mapping_create_page_prefers_run_meta_over_stale_running_status(app, client) -> None:
    task_id = "mapping-create-page-stale-running-status"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    run_dir = task_dir / "mapping_job" / "runmeta01"
    if task_dir.exists():
        shutil.rmtree(task_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "mapping_outputs.zip").write_bytes(b"zip")
    (run_dir / "mapping_run_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
    (run_dir / "pkg").mkdir(parents=True, exist_ok=True)
    (run_dir / "pkg" / "result.docx").write_bytes(b"docx")
    (run_dir / "meta.json").write_text(
        __import__("json").dumps(
            {
                "record_type": "mapping_run",
                "run_id": "runmeta01",
                "mapping_file": "Mapping.xlsx",
                "mapping_display_name": "Mapping.xlsx",
                "status": "completed",
                "started_at": "2026-05-20 10:00:00",
                "completed_at": "2026-05-20 10:00:05",
                "reference_ok": True,
                "extract_ok": True,
                "outputs": ["pkg/result.docx"],
                "output_count": 1,
                "zip_file": "mapping_outputs.zip",
                "log_file": "mapping_run_log.json",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    db.session.add(
        JobRecord(
            job_id="runmeta01",
            job_type="mapping_operation",
            queue_name="heavy",
            task_id=task_id,
            target_name="Mapping.xlsx",
            status="running",
            payload_json='{"action":"run_cached"}',
            result_json=None,
            started_at=datetime(2026, 5, 20, 10, 0, 0),
            completed_at=None,
        )
    )
    db.session.commit()

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="create", mapping_job="runmeta01")

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "生成結果" in html
    assert "runmeta01/mapping_outputs.zip" in html
    assert "runmeta01/mapping_run_log.json" in html
    assert "pkg/result.docx" in html
    assert "disabled" not in html.split('name="mapping_file"', 1)[1].split(">", 1)[0]


def test_mapping_op_status_falls_back_to_run_meta_after_workspace_cleanup(app, client) -> None:
    task_id = "mapping-op-status-meta-fallback"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    run_dir = task_dir / "mapping_job" / "runmeta02"
    if task_dir.exists():
        shutil.rmtree(task_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(
        __import__("json").dumps(
            {
                "record_type": "mapping_run",
                "run_id": "runmeta02",
                "mapping_file": "Mapping.xlsx",
                "mapping_display_name": "Mapping.xlsx",
                "status": "completed",
                "started_at": "2026-05-20 10:00:00",
                "completed_at": "2026-05-20 10:00:05",
                "reference_ok": True,
                "extract_ok": True,
                "outputs": ["pkg/result.docx"],
                "output_count": 1,
                "zip_file": "mapping_outputs.zip",
                "log_file": "mapping_run_log.json",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping_op_status", task_id=task_id, op_id="runmeta02")

    response = client.get(url)
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["status"] == "completed"
    assert payload["action"] == "run_cached"
    assert payload["mapping_display_name"] == "Mapping.xlsx"
    assert "mapping_job=runmeta02" in payload["resume_url"]


def test_flow_results_mapping_tab_renders_mapping_runs(app, client) -> None:
    task_id = "mapping-results-tab"
    run_dir = Path(app.config["TASK_FOLDER"]) / task_id / "mapping_job" / "run12345"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "mapping_outputs.zip").write_bytes(b"zip")
    (run_dir / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
    (run_dir / "meta.json").write_text(
        __import__("json").dumps(
            {
                "record_type": "mapping_run",
                "run_id": "run12345",
                "mapping_file": "sample_mapping.xlsx",
                "mapping_display_name": "Mapping_ch1 - 複製.xlsx",
                "status": "completed",
                "started_at": "2026-03-17 10:00:00",
                "completed_at": "2026-03-17 10:00:10",
                "reference_ok": True,
                "extract_ok": True,
                "output_count": 2,
                "zip_file": "mapping_outputs.zip",
                "log_file": "mapping_log.json",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    db.session.add(
        JobRecord(
            job_id="run12345",
            job_type="mapping_operation",
            queue_name="heavy",
            task_id=task_id,
            target_name="Mapping_ch1 - 複製.xlsx",
            status="completed",
            payload_json='{"action":"run_cached"}',
            result_json=__import__("json").dumps(
                {
                    "mapping_file": "Mapping_ch1 - 複製.xlsx",
                    "output_count": 2,
                    "zip_file": "mapping_outputs.zip",
                    "log_file": "mapping_log.json",
                    "reference_ok": True,
                    "extract_ok": True,
                    "source": "manual",
                },
                ensure_ascii=False,
            ),
            started_at=datetime(2026, 3, 17, 10, 0, 0),
            completed_at=datetime(2026, 3, 17, 10, 0, 10),
        )
    )
    db.session.commit()

    with app.test_request_context():
        url = url_for("flow_results_bp.flow_results", task_id=task_id, tab="mapping")

    response = client.get(url, follow_redirects=True)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Mapping 執行結果" in html
    assert "Mapping_ch1 - 複製.xlsx" in html
    assert "run12345" in html
    assert "下載 ZIP" in html
    assert "run12345/mapping_outputs.zip" in html


def test_mapping_results_tab_shows_actions_for_manual_run_without_result_json(app, client) -> None:
    task_id = "mapping-results-actions-fallback"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    run_dir = task_dir / "mapping_job" / "run45678"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "mapping_outputs.zip").write_bytes(b"zip")
    (run_dir / "mapping_run_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
    (run_dir / "meta.json").write_text(
        __import__("json").dumps(
            {
                "record_type": "mapping_run",
                "run_id": "run45678",
                "mapping_file": "Mapping_test.xlsx",
                "mapping_display_name": "Mapping_test.xlsx",
                "status": "completed",
                "started_at": "2026-05-19 18:09:08",
                "completed_at": "2026-05-19 18:09:20",
                "reference_ok": True,
                "extract_ok": True,
                "output_count": 1,
                "zip_file": "mapping_outputs.zip",
                "log_file": "mapping_run_log.json",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    db.session.add(
        JobRecord(
            job_id="run45678",
            job_type="mapping_operation",
            queue_name="heavy",
            task_id=task_id,
            target_name="Mapping_test.xlsx",
            status="completed",
            payload_json='{"action":"run_cached"}',
            result_json=None,
            started_at=datetime(2026, 5, 19, 18, 9, 8),
            completed_at=datetime(2026, 5, 19, 18, 9, 20),
        )
    )
    db.session.commit()

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Mapping_test.xlsx" in html
    assert "run45678" in html
    assert "run45678/mapping_outputs.zip" in html
    assert "run45678/mapping_run_log.json" in html
    assert ">ZIP<" in html
    assert ">Log<" in html
