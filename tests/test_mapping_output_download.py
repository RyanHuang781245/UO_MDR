from pathlib import Path

from io import BytesIO

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

    response = client.get(url)

    assert response.status_code == 200
    assert response.data == b"test-content"


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
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    output_dir = Path(captured["output_dir"])
    log_dir = Path(captured["log_dir"])
    assert output_dir.parent.name == "mapping_job"
    assert log_dir == output_dir
    run_id = output_dir.name
    assert len(run_id) == 8
    assert f"{run_id}/pkg/result.docx" in html
    assert f"{run_id}/mapping_log.json" in html
    assert f"{run_id}/mapping_outputs.zip" in html


def test_mapping_route_reuses_same_run_id_across_check_extract_and_run(app, client, monkeypatch) -> None:
    task_id = "mapping-run-reuse"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
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
    )
    assert response_check.status_code == 200

    response_extract = client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
    )
    assert response_extract.status_code == 200

    response_run = client.post(
        url,
        data={"action": "run_cached"},
        content_type="multipart/form-data",
    )
    assert response_run.status_code == 200

    run_ids = [Path(call["output_dir"]).name for call in calls]
    assert len(calls) == 3
    assert run_ids[0] == run_ids[1] == run_ids[2]


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
    )
    second = client.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy-2"), "mapping.xlsx")},
        content_type="multipart/form-data",
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
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "run_cached"},
        content_type="multipart/form-data",
    ).status_code == 200

    run_dir = Path(calls[-1]["output_dir"])
    meta = (run_dir / "meta.json").read_text(encoding="utf-8")
    payload = __import__("json").loads(meta)
    assert payload["record_type"] == "mapping_run"
    assert payload["mapping_file"] == "Mapping_ch1_-_.xlsx"
    assert payload["mapping_display_name"] == original_name
    assert payload["status"] == "completed"
    assert payload["reference_ok"] is True
    assert payload["extract_ok"] is True
    assert payload["output_count"] == 1
    assert payload["zip_file"] == "mapping_outputs.zip"
    assert payload["log_file"] == "mapping_log.json"


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

    with app.test_request_context():
        url = url_for("flows_bp.flow_results", task_id=task_id, tab="mapping")

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Mapping 執行結果" in html
    assert "Mapping_ch1 - 複製.xlsx" in html
    assert "run12345" in html
    assert "下載 ZIP" in html
    assert "run12345/mapping_outputs.zip" in html
