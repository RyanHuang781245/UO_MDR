import json
from pathlib import Path

import pytest
from docx import Document as DocxDocument

from app import create_app
from app.extensions import db
from app.extensions import ldap_manager
from app.models.execution import JobRecord
from modules.workflow import run_workflow


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setattr(ldap_manager, "init_app", lambda app: None)
    app = create_app("testing")
    app.config["TASK_FOLDER"] = str(tmp_path)
    ctx = app.app_context()
    ctx.push()
    try:
        yield app
    finally:
        ctx.pop()


@pytest.fixture
def client(app):
    return app.test_client()


def _create_job(tmp_path: Path, task_id: str, job_id: str) -> Path:
    src = tmp_path / "source.docx"
    doc = DocxDocument()
    doc.add_paragraph("Sample body")
    doc.save(src)
    job_dir = tmp_path / task_id / "jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    run_workflow(
        [{"type": "extract_word_all_content", "params": {"input_file": str(src)}}],
        str(job_dir),
    )
    db.session.add(
        JobRecord(
            job_id=job_id,
            job_type="flow_single",
            queue_name="default",
            task_id=task_id,
            target_name="Demo Flow",
            status="completed",
            payload_json=json.dumps({"flow_name": "Demo Flow"}),
        )
    )
    db.session.commit()
    return job_dir


def _prepare_task_files(tmp_path: Path, task_id: str) -> Path:
    task_dir = tmp_path / task_id
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    meta_path = task_dir / "meta.json"
    meta_path.write_text(json.dumps({"name": task_id}, ensure_ascii=False), encoding="utf-8")
    return task_dir


def _create_failed_flow_run(tmp_path: Path, task_id: str, job_id: str) -> Path:
    task_dir = _prepare_task_files(tmp_path, task_id)
    job_dir = task_dir / "jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "meta.json").write_text(
        json.dumps({"status": "failed", "flow_name": "Failed Flow"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (job_dir / "log.json").write_text(
        json.dumps([{"step_type": "extract_word_all_content", "status": "error", "error": "boom"}], ensure_ascii=False),
        encoding="utf-8",
    )
    db.session.add(
        JobRecord(
            job_id=job_id,
            job_type="flow_single",
            queue_name="default",
            task_id=task_id,
            target_name="Failed Flow",
            status="failed",
            payload_json=json.dumps({"flow_name": "Failed Flow"}),
        )
    )
    db.session.commit()
    return job_dir


def _create_failed_mapping_run(tmp_path: Path, task_id: str, run_id: str) -> Path:
    task_dir = _prepare_task_files(tmp_path, task_id)
    run_dir = task_dir / "mapping_job" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(
        json.dumps(
            {
                "status": "failed",
                "mapping_name": "Failed Mapping",
                "zip_file": "",
                "log_file": "run.log",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (run_dir / "run.log").write_text("failed", encoding="utf-8")
    db.session.add(
        JobRecord(
            job_id=run_id,
            job_type="mapping_scheme_run",
            queue_name="default",
            task_id=task_id,
            target_name="Failed Mapping",
            status="failed",
            payload_json=json.dumps({"scheme_name": "Default Scheme"}),
        )
    )
    db.session.commit()
    return run_dir


def test_flow_run_detail_returns_log_entries(app, client, tmp_path: Path) -> None:
    task_id = "task-flow-results"
    job_id = "job1234"
    _prepare_task_files(tmp_path, task_id)
    _create_job(tmp_path, task_id, job_id)

    resp = client.get(f"/tasks/{task_id}/flows/runs/{job_id}/detail")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["job_id"] == job_id
    assert isinstance(payload["log_entries"], list)
    assert payload["docx_url"].endswith(f"/tasks/{task_id}/download/{job_id}/docx")
    assert payload["compare_url"].endswith(f"/tasks/{task_id}/compare/{job_id}")


def test_task_result_redirects_to_flow_results_tab(app, client, tmp_path: Path) -> None:
    task_id = "task-flow-result-redirect"
    job_id = "job5678"
    _prepare_task_files(tmp_path, task_id)
    _create_job(tmp_path, task_id, job_id)

    resp = client.get(f"/tasks/{task_id}/result/{job_id}", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith(f"/tasks/{task_id}/flows?flow_tab=results")


def test_flow_run_status_does_not_return_retry_url(app, client, tmp_path: Path) -> None:
    task_id = "task-flow-status-no-retry"
    job_id = "job-failed-1"
    _create_failed_flow_run(tmp_path, task_id, job_id)

    resp = client.get(f"/tasks/{task_id}/flows/runs/{job_id}/status")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["status"] == "failed"
    assert "retry_url" not in payload
    assert payload["has_log"] is True


def test_flow_results_page_hides_retry_for_failed_flow(app, client, tmp_path: Path) -> None:
    task_id = "task-flow-failed-no-retry"
    job_id = "job-failed-2"
    _create_failed_flow_run(tmp_path, task_id, job_id)

    resp = client.get(f"/tasks/{task_id}/flows?flow_tab=results")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Failed Flow" in html
    assert "job-failed-2" in html
    assert f"/tasks/{task_id}/flows/runs/{job_id}/retry" not in html
    assert f"/tasks/{task_id}/download/{job_id}/log" in html


def test_mapping_results_page_hides_retry_for_failed_run(app, client, tmp_path: Path) -> None:
    task_id = "task-mapping-failed-no-retry"
    run_id = "mapping-failed-1"
    _create_failed_mapping_run(tmp_path, task_id, run_id)

    resp = client.get(f"/tasks/{task_id}/mapping?mapping_tab=results")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Failed Mapping" in html
    assert "mapping-failed-1" in html
    assert f"/tasks/{task_id}/mapping/{run_id}/retry" not in html
