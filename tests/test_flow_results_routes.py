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


def test_flow_run_detail_returns_log_entries(app, client, tmp_path: Path) -> None:
    task_id = "task-flow-results"
    job_id = "job1234"
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
    _create_job(tmp_path, task_id, job_id)

    resp = client.get(f"/tasks/{task_id}/result/{job_id}", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith(f"/tasks/{task_id}/flows?flow_tab=results")
