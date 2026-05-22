from __future__ import annotations

import json
import shutil
from io import BytesIO
from pathlib import Path

import pytest
from flask import url_for

from app import create_app
from app.blueprints.tasks.mapping_scheme_helpers import save_mapping_scheme, set_scheduled_mapping_scheme
from app.extensions import ldap_manager
from app.services.global_batch_items import encode_batch_item
from app.services.execution_service import MAPPING_OPERATION_JOB


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


def test_mapping_route_can_save_validated_scheme(app, client, monkeypatch) -> None:
    task_id = "mapping-scheme-save"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)

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

    response = client.post(
        url,
        data={"action": "save_scheme", "scheme_name": "CH1 章節擷取"},
        content_type="multipart/form-data",
    )

    scheme_dirs = [p for p in (task_dir / "mappings").glob("*") if p.is_dir() and (p / "meta.json").is_file()]

    assert response.status_code == 200
    assert scheme_dirs
    matched_meta = None
    for scheme_dir in scheme_dirs:
        meta = json.loads((scheme_dir / "meta.json").read_text(encoding="utf-8"))
        if meta.get("name") == "CH1 章節擷取":
            matched_meta = meta
            assert (scheme_dir / "source.xlsx").is_file()
            break
    assert matched_meta is not None
    assert matched_meta["extract_ok"] is True
    matched_dir = next(
        scheme_dir
        for scheme_dir in scheme_dirs
        if json.loads((scheme_dir / "meta.json").read_text(encoding="utf-8")).get("name") == "CH1 章節擷取"
    )
    assert matched_meta["check_log_file"] == "mapping_check_log.json"
    assert matched_meta["check_extract_log_file"] == "mapping_check_extract_log.json"
    assert (matched_dir / "mapping_check_log.json").is_file()
    assert (matched_dir / "mapping_check_extract_log.json").is_file()
    workspace_dirs = list((task_dir / "_mapping_sessions").glob("*/*"))
    assert workspace_dirs == []


def test_mapping_route_auto_saved_scheme_copies_validation_logs(app, client, monkeypatch) -> None:
    task_id = "mapping-scheme-auto-save-logs"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)

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
        (Path(log_dir or output_dir) / filename).write_text('{"messages":[],"runs":[]}', encoding="utf-8")
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
    response = client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert response.status_code == 200
    scheme_dirs = [p for p in (task_dir / "mappings").glob("*") if p.is_dir() and (p / "meta.json").is_file()]
    assert scheme_dirs
    auto_meta = json.loads((scheme_dirs[0] / "meta.json").read_text(encoding="utf-8"))
    assert auto_meta["check_log_file"] == "mapping_check_log.json"
    assert auto_meta["check_extract_log_file"] == "mapping_check_extract_log.json"
    assert (scheme_dirs[0] / "mapping_check_log.json").is_file()
    assert (scheme_dirs[0] / "mapping_check_extract_log.json").is_file()


def test_save_mapping_scheme_defaults_to_original_display_name_when_filename_is_sanitized(app) -> None:
    task_id = "mapping-scheme-display-name"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    source_path = task_dir / "Mapping_ch1_-_.xlsx"
    source_path.write_bytes(b"dummy")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "",
        {
            "mapping_file": "Mapping_ch1_-_.xlsx",
            "mapping_display_name": "Mapping_ch1 - 複製.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    assert scheme["display_name"] == "Mapping_ch1 - 複製"
    meta = json.loads((task_dir / "mappings" / scheme["id"] / "meta.json").read_text(encoding="utf-8"))
    assert meta["name"] == "Mapping_ch1 - 複製"
    assert meta["mapping_display_name"] == "Mapping_ch1 - 複製.xlsx"


def test_global_batch_page_accepts_saved_mapping_scheme(app, client) -> None:
    task_id = "mapping-scheme-queue"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    save_mapping_scheme(
        task_id,
        str(source_path),
        "附錄圖片擷取",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    with app.test_request_context():
        url = url_for("global_batch_bp.global_batch_page", task_ids=task_id)

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "全部 Mapping" in html
    assert "已保存 Mapping" in html
    assert "1 個" in html
    assert "流程" in html
    assert task_id in html
    assert "附錄圖片擷取" not in html


def test_global_batch_run_executes_saved_mapping_scheme(app, client, monkeypatch) -> None:
    task_id = "mapping-scheme-run"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    save_mapping_scheme(
        task_id,
        str(source_path),
        "CH2 摘要擷取",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )
    save_mapping_scheme(
        task_id,
        str(source_path),
        "附錄圖片擷取",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )
    scheme_ids = sorted(p.name for p in (task_dir / "mappings").iterdir() if p.is_dir())
    executed_scheme_ids: list[str] = []
    notification_calls: list[dict] = []

    monkeypatch.setattr(
        "app.blueprints.tasks.mapping_scheme_helpers.run_saved_mapping_scheme_job",
        lambda job_id, payload: (
            executed_scheme_ids.append(payload["scheme_id"]) or {
                "result_payload": {
                    "run_id": job_id,
                    "mapping_file": payload.get("mapping_display_name") or "",
                    "scheme_name": payload.get("scheme_name") or "",
                    "status": "completed",
                    "output_count": 3,
                    "zip_file": "mapping_outputs.zip",
                    "log_file": "mapping_log.json",
                    "zip_relpath": f"{job_id}/mapping_outputs.zip",
                    "log_relpath": f"{job_id}/mapping_log.json",
                    "reference_ok": True,
                    "extract_ok": True,
                    "source": "global_batch",
                    "error": "",
                }
            }
        ),
    )
    monkeypatch.setattr(
        "app.blueprints.flows.global_batch_routes._get_actor_info",
        lambda: ("A123", "Tester"),
    )
    monkeypatch.setattr(
        "app.blueprints.flows.global_batch_routes.send_batch_notification",
        lambda **kwargs: notification_calls.append(kwargs),
    )

    token = encode_batch_item({"kind": "mapping_scheme", "task_id": task_id, "scheme_id": ""})

    with app.test_request_context():
        run_url = url_for("global_batch_bp.run_global_batch")

    response = client.post(run_url, data={"batch_items": token}, follow_redirects=False)

    assert response.status_code == 302
    batch_id = response.headers["Location"].rsplit("batch=", 1)[-1]

    with app.test_request_context():
        status_url = url_for("global_batch_bp.global_batch_status", batch_id=batch_id)

    status_response = client.get(status_url)
    payload = status_response.get_json()

    assert status_response.status_code == 200
    assert payload["ok"] is True
    assert payload["status"]["results"][0]["kind"] == "mapping_scheme"
    assert len(payload["status"]["results"][0]["mapping_runs"]) == 2
    assert payload["status"]["results"][0]["ok"] is True
    assert len(notification_calls) == 1
    assert notification_calls[0]["task_id"] == task_id
    assert notification_calls[0]["batch_id"] == batch_id
    assert notification_calls[0]["status"] == "completed"
    assert notification_calls[0]["actor_work_id"] == "A123"
    assert len(notification_calls[0]["results"]) == 2


def test_can_download_saved_mapping_scheme_source_file(app, client) -> None:
    task_id = "mapping-scheme-download"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "Mapping_download.xlsx"
    source_path.write_bytes(b"dummy-download")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "下載測試方案",
        {
            "mapping_file": "Mapping_download.xlsx",
            "mapping_display_name": "Mapping_download.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    with app.test_request_context():
        download_url = url_for("tasks_bp.task_download_mapping_scheme", task_id=task_id, scheme_id=scheme["id"])

    response = client.get(download_url)

    assert response.status_code == 200
    assert response.data == b"dummy-download"
    assert "attachment; filename=Mapping_download.xlsx" in response.headers["Content-Disposition"]


def test_can_download_saved_mapping_scheme_validation_logs(app, client) -> None:
    task_id = "mapping-scheme-download-logs"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "Mapping_download_logs.xlsx"
    source_path.write_bytes(b"dummy-download")

    validation_dir = task_dir / "mapping_job" / "run12345"
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / "mapping_check_log.json").write_text('{"kind":"check"}', encoding="utf-8")
    (validation_dir / "mapping_check_extract_log.json").write_text('{"kind":"check_extract"}', encoding="utf-8")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "下載 Log 測試方案",
        {
            "mapping_file": "Mapping_download_logs.xlsx",
            "mapping_display_name": "Mapping_download_logs.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
        validation_log_dir=str(validation_dir),
    )

    with app.test_request_context():
        check_url = url_for(
            "tasks_bp.task_download_mapping_scheme_log",
            task_id=task_id,
            scheme_id=scheme["id"],
            kind="check",
        )
        check_extract_url = url_for(
            "tasks_bp.task_download_mapping_scheme_log",
            task_id=task_id,
            scheme_id=scheme["id"],
            kind="check_extract",
        )

    check_response = client.get(check_url)
    check_extract_response = client.get(check_extract_url)

    assert check_response.status_code == 200
    assert check_response.data == b'{"kind":"check"}'
    assert "attachment; filename=mapping_check_log.json" in check_response.headers["Content-Disposition"]
    assert check_extract_response.status_code == 200
    assert check_extract_response.data == b'{"kind":"check_extract"}'
    assert "attachment; filename=mapping_check_extract_log.json" in check_extract_response.headers["Content-Disposition"]


def test_can_delete_saved_mapping_scheme(app, client) -> None:
    task_id = "mapping-scheme-delete"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "Mapping_delete.xlsx"
    source_path.write_bytes(b"dummy-delete")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "刪除測試方案",
        {
            "mapping_file": "Mapping_delete.xlsx",
            "mapping_display_name": "Mapping_delete.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="saved")

    response = client.post(url, data={"action": "delete_scheme", "scheme_id": scheme["id"]})
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "刪除測試方案" not in html
    assert "尚未保存任何 Mapping 方案" in html
    assert not (task_dir / "mappings" / scheme["id"]).exists()


def test_can_rename_saved_mapping_scheme(app, client) -> None:
    task_id = "mapping-scheme-rename"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "Mapping_rename.xlsx"
    source_path.write_bytes(b"dummy-rename")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "舊名稱方案",
        {
            "mapping_file": "Mapping_rename.xlsx",
            "mapping_display_name": "Mapping_rename.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="saved")

    response = client.post(
        url,
        data={"action": "rename_scheme", "scheme_id": scheme["id"], "scheme_name": "新名稱方案"},
    )
    html = response.get_data(as_text=True)
    meta = json.loads((task_dir / "mappings" / scheme["id"] / "meta.json").read_text(encoding="utf-8"))

    assert response.status_code == 200
    assert "新名稱方案" in html
    assert "舊名稱方案" not in html
    assert meta["name"] == "新名稱方案"


def test_saved_mapping_schemes_list_supports_pagination(app, client) -> None:
    task_id = "mapping-scheme-pagination"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "Mapping_page.xlsx"
    source_path.write_bytes(b"dummy-page")

    for idx in range(11):
        save_mapping_scheme(
            task_id,
            str(source_path),
            f"方案{idx + 1:02d}",
            {
                "mapping_file": "Mapping_page.xlsx",
                "mapping_display_name": "Mapping_page.xlsx",
                "reference_ok": True,
                "extract_ok": True,
            },
            actor={"work_id": "A123", "label": "Tester"},
        )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mpage=2)

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "共 <span>11</span> 筆 Mapping (第 2 / 2 頁)" in html
    assert "#saved-schemes-pane" in html


def test_saved_mapping_scheme_list_shows_validation_log_download_actions(app, client) -> None:
    task_id = "mapping-scheme-list-log-actions"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    validation_dir = task_dir / "mapping_job" / "run12345"
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / "mapping_check_log.json").write_text("{}", encoding="utf-8")
    (validation_dir / "mapping_check_extract_log.json").write_text("{}", encoding="utf-8")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "列表 Log 按鈕方案",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
        validation_log_dir=str(validation_dir),
    )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="saved")

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert scheme["id"] in html
    assert "引用檢查 Log" in html
    assert "擷取檢查 Log" in html


def test_saved_mapping_scheme_run_does_not_show_processing_status(app, client, monkeypatch) -> None:
    task_id = "mapping-scheme-run-hide-status"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "執行測試方案",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_saved_mapping_scheme_run", lambda *args, **kwargs: "run-hide-status")

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(url, data={"action": "run_scheme", "scheme_id": scheme["id"], "mpage": "1"})
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "處理狀態" not in html
    assert "生成結果" not in html


def test_saved_mapping_scheme_general_run_forwards_disable_figure_reference(
    app, client, monkeypatch
) -> None:
    task_id = "mapping-scheme-figure-ref-setting"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "一般執行方案",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    captured_job_payload: dict[str, object] = {}

    def fake_enqueue_job(job_type, payload, **kwargs):
        captured_job_payload.clear()
        captured_job_payload.update(payload)
        return "run-figure-ref-flag"

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_job", fake_enqueue_job)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    run_response = client.post(
        url,
        data={
            "action": "run_scheme",
            "scheme_id": scheme["id"],
            "mpage": "1",
        },
    )
    assert run_response.status_code == 200
    assert captured_job_payload["mapping_path"].endswith("source.xlsx")
    assert captured_job_payload["enable_figure_reference"] is False


def test_saved_mapping_scheme_run_reuses_current_validated_mapping_run(
    app, client, monkeypatch
) -> None:
    task_id = "mapping-scheme-reuse-current-run"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

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
        data={"action": "save_scheme", "scheme_name": "重用目前驗證方案"},
        content_type="multipart/form-data",
    ).status_code == 200

    scheme_dirs = [p for p in (task_dir / "mappings").glob("*") if p.is_dir() and (p / "meta.json").is_file()]
    assert scheme_dirs
    scheme_meta = next(
        json.loads((scheme_dir / "meta.json").read_text(encoding="utf-8"))
        for scheme_dir in scheme_dirs
        if json.loads((scheme_dir / "meta.json").read_text(encoding="utf-8")).get("name") == "重用目前驗證方案"
    )
    scheme_id = str(scheme_meta["id"])

    expected_validation_run_id = str(scheme_meta["validated_run_id"])

    captured_enqueue: dict[str, object] = {}
    scheme_enqueue_called = {"called": False}

    def fake_enqueue_job(job_type, payload, **kwargs):
        captured_enqueue.clear()
        captured_enqueue.update(
            {
                "job_type": job_type,
                "payload": dict(payload),
                "kwargs": dict(kwargs),
            }
        )
        return str(kwargs.get("job_id") or "queued-run")

    def fail_saved_scheme_enqueue(*args, **kwargs):
        scheme_enqueue_called["called"] = True
        raise AssertionError("should not enqueue saved mapping scheme run")

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_saved_mapping_scheme_run", fail_saved_scheme_enqueue)

    response = client.post(
        url,
        data={"action": "run_scheme", "scheme_id": scheme_id, "mpage": "1"},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    assert scheme_enqueue_called["called"] is False
    assert captured_enqueue["job_type"] == MAPPING_OPERATION_JOB
    assert captured_enqueue["payload"]["action"] == "run_cached"
    assert captured_enqueue["payload"]["mapping_path"].endswith("source.xlsx")
    assert captured_enqueue["payload"]["validation_state_snapshot"]["run_id"] == expected_validation_run_id
    assert captured_enqueue["kwargs"]["job_id"] != expected_validation_run_id


def test_saved_mapping_scheme_run_reuses_validated_mapping_from_other_workspace(
    app, monkeypatch
) -> None:
    task_id = "mapping-scheme-reuse-other-workspace"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    client_a = app.test_client()
    client_b = app.test_client()

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

    assert client_a.post(
        url,
        data={"action": "check", "mapping_file": (BytesIO(b"dummy"), "mapping.xlsx")},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client_a.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
        follow_redirects=True,
    ).status_code == 200
    assert client_a.post(
        url,
        data={"action": "save_scheme", "scheme_name": "跨工作區重用方案"},
        content_type="multipart/form-data",
    ).status_code == 200

    scheme_dirs = [p for p in (task_dir / "mappings").glob("*") if p.is_dir() and (p / "meta.json").is_file()]
    assert scheme_dirs
    scheme_meta = next(
        json.loads((scheme_dir / "meta.json").read_text(encoding="utf-8"))
        for scheme_dir in scheme_dirs
        if json.loads((scheme_dir / "meta.json").read_text(encoding="utf-8")).get("name") == "跨工作區重用方案"
    )
    scheme_id = str(scheme_meta["id"])

    expected_validation_run_id = str(scheme_meta["validated_run_id"])

    captured_enqueue: dict[str, object] = {}
    scheme_enqueue_called = {"called": False}

    def fake_enqueue_job(job_type, payload, **kwargs):
        captured_enqueue.clear()
        captured_enqueue.update(
            {
                "job_type": job_type,
                "payload": dict(payload),
                "kwargs": dict(kwargs),
            }
        )
        return str(kwargs.get("job_id") or "queued-run")

    def fail_saved_scheme_enqueue(*args, **kwargs):
        scheme_enqueue_called["called"] = True
        raise AssertionError("should not enqueue saved mapping scheme run")

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_saved_mapping_scheme_run", fail_saved_scheme_enqueue)

    response = client_b.post(
        url,
        data={"action": "run_scheme", "scheme_id": scheme_id, "mpage": "1"},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    assert scheme_enqueue_called["called"] is False
    assert captured_enqueue["job_type"] == MAPPING_OPERATION_JOB
    assert captured_enqueue["payload"]["action"] == "run_cached"
    assert captured_enqueue["payload"]["validation_state_snapshot"]["run_id"] == expected_validation_run_id
    assert captured_enqueue["kwargs"]["job_id"] != expected_validation_run_id


def test_saved_mapping_scheme_run_creates_new_run_id_each_time(
    app, client, monkeypatch
) -> None:
    task_id = "mapping-scheme-new-run-each-time"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

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
        data={"action": "save_scheme", "scheme_name": "每次新 Run 方案"},
        content_type="multipart/form-data",
    ).status_code == 200

    scheme_dirs = [p for p in (task_dir / "mappings").glob("*") if p.is_dir() and (p / "meta.json").is_file()]
    assert scheme_dirs
    scheme_meta = json.loads((scheme_dirs[0] / "meta.json").read_text(encoding="utf-8"))
    scheme_id = str(scheme_meta["id"])

    queued_job_ids: list[str] = []

    def fake_enqueue_job(job_type, payload, **kwargs):
        queued_job_ids.append(str(kwargs.get("job_id") or ""))
        return queued_job_ids[-1]

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(
        "app.blueprints.tasks.mapping_routes.enqueue_saved_mapping_scheme_run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not enqueue saved mapping scheme run")),
    )

    first = client.post(
        url,
        data={"action": "run_scheme", "scheme_id": scheme_id, "mpage": "1"},
        content_type="multipart/form-data",
    )
    second = client.post(
        url,
        data={"action": "run_scheme", "scheme_id": scheme_id, "mpage": "1"},
        content_type="multipart/form-data",
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(queued_job_ids) == 2
    assert queued_job_ids[0]
    assert queued_job_ids[1]
    assert queued_job_ids[0] != queued_job_ids[1]


def test_saved_mapping_scheme_run_does_not_touch_existing_create_workspace(
    app, client, monkeypatch
) -> None:
    task_id = "mapping-scheme-run-keeps-create-workspace"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "不碰草稿方案",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    with client.session_transaction() as sess:
        sess["mapping_client_id"] = "b" * 32

    workspace_dir = task_dir / "_mapping_sessions" / "anonymous" / ("b" * 32)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    draft_file = workspace_dir / "create-draft.xlsx"
    draft_file.write_bytes(b"draft")
    (workspace_dir / "mapping_last.txt").write_text("create-draft.xlsx", encoding="utf-8")
    (workspace_dir / "mapping_validation_state.json").write_text(
        '{"mapping_file":"create-draft.xlsx","mapping_display_name":"create-draft.xlsx","reference_ok":false,"extract_ok":false,"run_id":""}',
        encoding="utf-8",
    )

    captured_payload: dict[str, object] = {}

    def fake_enqueue_job(job_type, payload, **kwargs):
        captured_payload.clear()
        captured_payload.update(payload)
        return str(kwargs.get("job_id") or "queued-run")

    monkeypatch.setattr("app.blueprints.tasks.mapping_routes.enqueue_job", fake_enqueue_job)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(
        url,
        data={"action": "run_scheme", "scheme_id": scheme["id"], "mpage": "1"},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    assert "workspace_dir" not in captured_payload
    assert draft_file.is_file()
    assert (workspace_dir / "mapping_last.txt").is_file()
    assert captured_payload["mapping_path"].endswith("source.xlsx")


def test_saved_mapping_scheme_run_results_show_original_mapping_name(app, client, monkeypatch) -> None:
    task_id = "mapping-scheme-results-original-name"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "Mapping_ch1_-_.xlsx"
    source_path.write_bytes(b"dummy")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "顯示名稱方案",
        {
            "mapping_file": "Mapping_ch1_-_.xlsx",
            "mapping_display_name": "Mapping_ch1 - 複製.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
        **kwargs,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir or output_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(output_dir) / "pkg" / "result.docx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"docx")
        (Path(log_dir or output_dir) / "mapping_log.json").write_text('{"messages":[],"runs":[]}', encoding="utf-8")
        (Path(output_dir) / "mapping_outputs.zip").write_bytes(b"zip")
        return {
            "logs": [],
            "outputs": [str(out_path)],
            "log_file": "mapping_log.json",
            "zip_file": "mapping_outputs.zip",
        }

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    with app.test_request_context():
        run_url = url_for("tasks_bp.task_mapping", task_id=task_id)
        results_url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")

    run_response = client.post(
        run_url,
        data={"action": "run_scheme", "scheme_id": scheme["id"], "mpage": "1"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    results_response = client.get(results_url)
    html = results_response.get_data(as_text=True)

    assert run_response.status_code == 200
    assert results_response.status_code == 200
    assert "Mapping_ch1 - 複製.xlsx" in html
    assert "source.xlsx" not in html


def test_saved_mapping_scheme_figure_reference_run_forwards_enable_figure_reference(
    app, client, monkeypatch
) -> None:
    task_id = "mapping-scheme-figure-ref-run"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")

    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "圖表參照執行方案",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    captured_job_payload: dict[str, object] = {}

    def fake_enqueue_job(job_type, payload, **kwargs):
        captured_job_payload.clear()
        captured_job_payload.update(payload)
        return "run-figure-ref-flag"

    monkeypatch.setattr("app.blueprints.tasks.mapping_scheme_helpers.enqueue_job", fake_enqueue_job)

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    run_response = client.post(
        url,
        data={
            "action": "run_scheme_figure_reference",
            "scheme_id": scheme["id"],
            "mpage": "1",
        },
    )
    assert run_response.status_code == 200
    assert captured_job_payload["scheme_id"] == scheme["id"]
    assert captured_job_payload["enable_figure_reference"] is True


def test_saved_mapping_scheme_list_shows_two_run_actions(app, client) -> None:
    task_id = "mapping-scheme-list-run-actions"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)

    source_path = task_dir / "mapping.xlsx"
    source_path.write_bytes(b"dummy")
    scheme = save_mapping_scheme(
        task_id,
        str(source_path),
        "列表按鈕方案",
        {
            "mapping_file": "mapping.xlsx",
            "mapping_display_name": "mapping.xlsx",
            "reference_ok": True,
            "extract_ok": True,
        },
        actor={"work_id": "A123", "label": "Tester"},
    )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id, mapping_tab="saved")

    response = client.get(url)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert scheme["id"] in html
    assert 'value="run_scheme"' in html
    assert 'value="run_scheme_figure_reference"' in html
