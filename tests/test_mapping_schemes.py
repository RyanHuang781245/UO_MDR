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
    ).status_code == 200
    assert client.post(
        url,
        data={"action": "check_extract"},
        content_type="multipart/form-data",
    ).status_code == 200

    response = client.post(
        url,
        data={"action": "save_scheme", "scheme_name": "CH1 章節擷取"},
        content_type="multipart/form-data",
    )

    html = response.get_data(as_text=True)
    scheme_dirs = [p for p in (task_dir / "mappings").glob("*") if p.is_dir() and (p / "meta.json").is_file()]

    assert response.status_code == 200
    assert "CH1 章節擷取" in html
    assert "生成" in html
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

    def fake_execute_saved_mapping_scheme(task_id_arg, scheme_id_arg, *args, **kwargs):
        executed_scheme_ids.append(scheme_id_arg)
        return {
            "run_id": f"run-{scheme_id_arg}",
            "ok": True,
            "status": "completed",
            "output_count": 3,
            "zip_file": "mapping_outputs.zip",
            "log_file": "mapping_log.json",
            "zip_relpath": f"run-{scheme_id_arg}/mapping_outputs.zip",
            "log_relpath": f"run-{scheme_id_arg}/mapping_log.json",
            "error": "",
        }

    monkeypatch.setattr(
        "app.blueprints.flows.global_batch_routes.execute_saved_mapping_scheme",
        fake_execute_saved_mapping_scheme,
    )

    class ImmediateThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None):
            self.target = target
            self.args = args
            self.kwargs = kwargs or {}

        def start(self):
            if self.target:
                self.target(*self.args, **self.kwargs)

    monkeypatch.setattr("app.blueprints.flows.global_batch_routes.threading.Thread", ImmediateThread)

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
    assert sorted(executed_scheme_ids) == scheme_ids
    assert payload["status"]["results"][0]["ok"] is True


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
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

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
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

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

    monkeypatch.setattr(
        "app.blueprints.tasks.mapping_routes.execute_saved_mapping_scheme",
        lambda *args, **kwargs: {
            "run_id": "run-hide-status",
            "messages": [],
            "outputs": ["pkg/result.docx"],
            "log_file": "mapping_log.json",
            "zip_file": "mapping_outputs.zip",
            "log_relpath": "run-hide-status/mapping_log.json",
            "zip_relpath": "run-hide-status/mapping_outputs.zip",
        },
    )

    with app.test_request_context():
        url = url_for("tasks_bp.task_mapping", task_id=task_id)

    response = client.post(url, data={"action": "run_scheme", "scheme_id": scheme["id"], "mpage": "1"})
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "處理狀態" not in html
    assert "生成結果" not in html
