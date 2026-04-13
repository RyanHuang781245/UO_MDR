from __future__ import annotations

import json
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest

from app import create_app
from app.extensions import ldap_manager
from app.extensions import db
from app.jobs.executor import enqueue_single_flow_job
from app.models.execution import JobArtifactRecord, JobRecord
from app.services.execution_service import MAPPING_OPERATION_JOB, cancel_job, enqueue_job, retry_job, run_job_by_id


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


def test_enqueue_single_flow_job_runs_inline_and_persists_job_metadata(app, monkeypatch) -> None:
    task_id = "flow-inline-job"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)
    (task_dir / "meta.json").write_text(json.dumps({"name": "Flow Inline"}, ensure_ascii=False), encoding="utf-8")

    def fake_run_workflow(runtime_steps, workdir, template=None):
        assert runtime_steps == [{"type": "fake_step", "params": {"value": "ok"}}]
        job_dir = Path(workdir)
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / "result.docx").write_bytes(b"docx-output")
        (job_dir / "log.json").write_text('[{"status":"ok"}]', encoding="utf-8")
        return {
            "result_docx": str(job_dir / "result.docx"),
            "log_json": [{"status": "ok"}],
        }

    monkeypatch.setattr("app.jobs.executor.run_workflow", fake_run_workflow)
    monkeypatch.setattr("app.jobs.executor.remove_hidden_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.hide_paragraphs_with_text", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.apply_basic_style", lambda *args, **kwargs: None)

    job_id = enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=[{"type": "fake_step", "params": {"value": "ok"}}],
        template_cfg=None,
        document_format="none",
        line_spacing=1.5,
        apply_formatting=False,
        actor={"work_id": "A123", "label": "Tester"},
        flow_name="Inline Flow",
        output_filename="",
    )

    record = db.session.get(JobRecord, job_id)
    assert record is not None
    assert record.status == "completed"
    assert record.job_type == "flow_single"
    assert record.task_id == task_id

    job_dir = task_dir / "jobs" / job_id
    meta = json.loads((job_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["status"] == "completed"
    assert (job_dir / "result.docx").is_file()

    artifacts = JobArtifactRecord.query.filter_by(job_id=job_id).all()
    artifact_types = {item.artifact_type for item in artifacts}
    assert "result_docx" in artifact_types
    assert "log_json" in artifact_types


def test_enqueue_single_flow_job_publishes_copy_steps_to_task_output_path(app, monkeypatch, tmp_path) -> None:
    task_id = "flow-copy-output-root"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    source_dir = files_dir / "src"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "report.txt"
    source_file.write_text("copy-me", encoding="utf-8")
    output_root = tmp_path / "published-output"
    (task_dir / "meta.json").write_text(
        json.dumps({"name": "Flow Copy Output", "output_path": str(output_root)}, ensure_ascii=False),
        encoding="utf-8",
    )

    def fake_run_workflow(runtime_steps, workdir, template=None):
        assert runtime_steps[0]["type"] == "copy_files"
        assert runtime_steps[0]["params"]["source_dir"] == str(source_file)
        assert runtime_steps[0]["params"]["dest_dir"] == str(output_root / "pkg" / "files")
        job_dir = Path(workdir)
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / "result.docx").write_bytes(b"docx-output")
        (job_dir / "log.json").write_text('[{"status":"ok"}]', encoding="utf-8")
        return {
            "result_docx": str(job_dir / "result.docx"),
            "log_json": [{"status": "ok"}],
        }

    monkeypatch.setattr("app.jobs.executor.run_workflow", fake_run_workflow)
    monkeypatch.setattr("app.jobs.executor.remove_hidden_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.hide_paragraphs_with_text", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.apply_basic_style", lambda *args, **kwargs: None)

    job_id = enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=[
            {
                "type": "copy_files",
                "params": {"source_dir": str(source_file), "dest_dir": "pkg/files", "keywords": "", "target_name": "", "recursive_search": "true"},
            }
        ],
        template_cfg=None,
        document_format="none",
        line_spacing=1.5,
        apply_formatting=False,
        actor={"work_id": "A123", "label": "Tester"},
        flow_name="Copy Flow",
        output_filename="",
    )

    meta = json.loads((task_dir / "jobs" / job_id / "meta.json").read_text(encoding="utf-8"))
    assert meta["output_root"] == str(output_root)
    assert meta["has_copy_output"] is True


def test_load_task_context_defaults_output_path_to_task_output_dir(app) -> None:
    task_id = "task-output-default"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "meta.json").write_text(
        json.dumps({"name": "Task Output Default", "nas_path": r"\\nas\demo"}, ensure_ascii=False),
        encoding="utf-8",
    )

    from app.services.task_service import load_task_context

    context = load_task_context(task_id)
    assert context["output_path"] == str(task_dir / "output")


def test_execute_saved_flow_remaps_copy_dest_to_task_output_path(app, monkeypatch, tmp_path) -> None:
    task_id = "saved-flow-copy-output"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    files_dir = task_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    source_file = files_dir / "report.txt"
    source_file.write_text("copy-me", encoding="utf-8")
    flow_dir = task_dir / "flows"
    flow_dir.mkdir(parents=True, exist_ok=True)
    output_root = tmp_path / "saved-flow-output"
    (task_dir / "meta.json").write_text(
        json.dumps({"name": "Saved Flow Copy", "output_path": str(output_root)}, ensure_ascii=False),
        encoding="utf-8",
    )
    (flow_dir / "CopyFlow.json").write_text(
        json.dumps(
            {
                "steps": [
                    {
                        "type": "copy_files",
                        "params": {
                            "source_dir": "report.txt",
                            "dest_dir": "pkg/files",
                            "keywords": "",
                            "target_name": "",
                            "recursive_search": "true",
                        },
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_run_workflow(runtime_steps, workdir, template=None):
        assert runtime_steps[0]["params"]["source_dir"] == str(source_file)
        assert runtime_steps[0]["params"]["dest_dir"] == str(output_root / "pkg" / "files")
        job_dir = Path(workdir)
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / "result.docx").write_bytes(b"docx-output")
        return {"result_docx": str(job_dir / "result.docx"), "log_json": [{"status": "ok"}]}

    monkeypatch.setattr("app.blueprints.flows.run_helpers.run_workflow", fake_run_workflow)
    monkeypatch.setattr("app.blueprints.flows.run_helpers.remove_hidden_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.blueprints.flows.run_helpers.hide_paragraphs_with_text", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.blueprints.flows.run_helpers.apply_basic_style", lambda *args, **kwargs: None)

    from app.blueprints.flows.run_helpers import _execute_saved_flow

    job_id = _execute_saved_flow(task_id, "CopyFlow")
    meta = json.loads((task_dir / "jobs" / job_id / "meta.json").read_text(encoding="utf-8"))
    assert meta["output_root"] == str(output_root)


def test_cancel_job_marks_queued_job_as_canceled(app, monkeypatch) -> None:
    app.config["JOB_EXECUTOR_MODE"] = "worker"
    task_id = "flow-cancel-job"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("app.jobs.executor.run_workflow", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not run")))

    job_id = enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=[{"type": "fake_step", "params": {"value": "ok"}}],
        template_cfg=None,
        document_format="none",
        line_spacing=1.5,
        apply_formatting=False,
        actor={"work_id": "A123", "label": "Tester"},
        flow_name="Queued Flow",
        output_filename="",
    )

    record = db.session.get(JobRecord, job_id)
    assert record is not None
    assert record.status == "queued"

    ok, message = cancel_job(job_id)
    assert ok is True
    assert "canceled" in message.lower()
    assert db.session.get(JobRecord, job_id).status == "canceled"


def test_retry_job_requeues_failed_job_with_new_id(app, monkeypatch) -> None:
    task_id = "flow-retry-job"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)

    calls = {"count": 0}

    def fake_run_workflow(runtime_steps, workdir, template=None):
        calls["count"] += 1
        job_dir = Path(workdir)
        job_dir.mkdir(parents=True, exist_ok=True)
        if calls["count"] == 1:
            raise RuntimeError("first failure")
        (job_dir / "result.docx").write_bytes(b"docx-output")
        (job_dir / "log.json").write_text('[{"status":"ok"}]', encoding="utf-8")
        return {
            "result_docx": str(job_dir / "result.docx"),
            "log_json": [{"status": "ok"}],
        }

    monkeypatch.setattr("app.jobs.executor.run_workflow", fake_run_workflow)
    monkeypatch.setattr("app.jobs.executor.remove_hidden_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.hide_paragraphs_with_text", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.apply_basic_style", lambda *args, **kwargs: None)

    failed_job_id = enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=[{"type": "fake_step", "params": {"value": "ok"}}],
        template_cfg=None,
        document_format="none",
        line_spacing=1.5,
        apply_formatting=False,
        actor={"work_id": "A123", "label": "Tester"},
        flow_name="Retry Flow",
        output_filename="",
    )

    assert db.session.get(JobRecord, failed_job_id).status == "failed"

    ok, message, retried_job_id = retry_job(failed_job_id, actor={"work_id": "A123", "label": "Tester"})
    assert ok is True
    assert "retried" in message.lower()
    assert retried_job_id
    assert retried_job_id != failed_job_id
    assert db.session.get(JobRecord, retried_job_id).status == "completed"


def test_worker_heartbeat_touches_running_job(app, monkeypatch) -> None:
    task_id = "flow-heartbeat-job"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)
    app.config["JOB_HEARTBEAT_INTERVAL_SECONDS"] = 0.05

    heartbeats: list[str] = []

    def fake_touch_job_heartbeat(job_id):
        heartbeats.append(job_id)
        record = db.session.get(JobRecord, job_id)
        if record:
            record.heartbeat_at = datetime.now()
            db.session.add(record)
            db.session.commit()

    def fake_run_workflow(runtime_steps, workdir, template=None):
        job_dir = Path(workdir)
        job_dir.mkdir(parents=True, exist_ok=True)
        time.sleep(0.18)
        (job_dir / "result.docx").write_bytes(b"docx-output")
        (job_dir / "log.json").write_text('[{"status":"ok"}]', encoding="utf-8")
        return {
            "result_docx": str(job_dir / "result.docx"),
            "log_json": [{"status": "ok"}],
        }

    monkeypatch.setattr("app.services.execution_service.touch_job_heartbeat", fake_touch_job_heartbeat)
    monkeypatch.setattr("app.jobs.executor.run_workflow", fake_run_workflow)
    monkeypatch.setattr("app.jobs.executor.remove_hidden_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.hide_paragraphs_with_text", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.apply_basic_style", lambda *args, **kwargs: None)

    job_id = enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=[{"type": "fake_step", "params": {"value": "ok"}}],
        template_cfg=None,
        document_format="none",
        line_spacing=1.5,
        apply_formatting=False,
        actor={"work_id": "A123", "label": "Tester"},
        flow_name="Heartbeat Flow",
        output_filename="",
    )

    assert db.session.get(JobRecord, job_id).status == "completed"
    assert heartbeats


def test_cancel_running_flow_job_marks_job_and_meta_as_canceled(app, monkeypatch) -> None:
    app.config["JOB_EXECUTOR_MODE"] = "worker"
    task_id = "flow-cancel-running"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)

    def fake_run_workflow(runtime_steps, workdir, template=None, cancel_check=None):
        job_dir = Path(workdir)
        job_dir.mkdir(parents=True, exist_ok=True)
        for _ in range(40):
            time.sleep(0.02)
            if cancel_check:
                cancel_check()
        (job_dir / "result.docx").write_bytes(b"docx-output")
        (job_dir / "log.json").write_text('[{"status":"ok"}]', encoding="utf-8")
        return {
            "result_docx": str(job_dir / "result.docx"),
            "log_json": [{"status": "ok"}],
        }

    monkeypatch.setattr("app.jobs.executor.run_workflow", fake_run_workflow)
    monkeypatch.setattr("app.jobs.executor.remove_hidden_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.hide_paragraphs_with_text", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.jobs.executor.apply_basic_style", lambda *args, **kwargs: None)

    job_id = enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=[{"type": "fake_step", "params": {"value": "ok"}}],
        template_cfg=None,
        document_format="none",
        line_spacing=1.5,
        apply_formatting=False,
        actor={"work_id": "A123", "label": "Tester"},
        flow_name="Cancel Running Flow",
        output_filename="",
    )

    worker = threading.Thread(target=run_job_by_id, args=(app, job_id), daemon=True)
    worker.start()

    deadline = time.time() + 5.0
    while time.time() < deadline:
        db.session.expire_all()
        record = db.session.get(JobRecord, job_id)
        if record and record.status == "running":
            break
        time.sleep(0.02)
    else:
        raise AssertionError("job did not reach running state")

    ok, message = cancel_job(job_id)
    assert ok is True
    assert "requested" in message.lower()

    worker.join(timeout=5.0)
    assert not worker.is_alive()

    db.session.expire_all()
    record = db.session.get(JobRecord, job_id)
    assert record is not None
    assert record.status == "canceled"
    meta = json.loads((task_dir / "jobs" / job_id / "meta.json").read_text(encoding="utf-8"))
    assert meta["status"] == "canceled"


def test_cancel_running_mapping_job_marks_job_and_op_as_canceled(app, monkeypatch) -> None:
    app.config["JOB_EXECUTOR_MODE"] = "worker"
    task_id = "mapping-cancel-running"
    task_dir = Path(app.config["TASK_FOLDER"]) / task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    workspace_dir = task_dir / "_mapping_sessions" / "tester" / "client"
    workspace_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "files").mkdir(parents=True, exist_ok=True)
    mapping_path = workspace_dir / "mapping.xlsx"
    mapping_path.write_bytes(b"xlsx")

    def fake_process_mapping_excel(
        mapping_path,
        task_files_dir,
        output_dir,
        log_dir=None,
        validate_only=False,
        validate_extract_only=False,
        cancel_check=None,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        for _ in range(40):
            time.sleep(0.02)
            if cancel_check:
                cancel_check()
        return {"logs": [], "outputs": [], "log_file": "", "zip_file": ""}

    monkeypatch.setattr("modules.mapping_processor.process_mapping_excel", fake_process_mapping_excel)

    job_id = enqueue_job(
        MAPPING_OPERATION_JOB,
        {
            "task_id": task_id,
            "workspace_dir": str(workspace_dir),
            "action": "run_cached",
            "mapping_path": str(mapping_path),
            "current_mapping_display_name": "Cancel Mapping",
            "validation_state_snapshot": {"reference_ok": True, "extract_ok": True},
            "actor": {"work_id": "A123", "label": "Tester"},
        },
        task_id=task_id,
        target_name="Cancel Mapping",
        actor={"work_id": "A123", "label": "Tester"},
        queue_name="heavy",
        job_id="mapcan01",
    )

    worker = threading.Thread(target=run_job_by_id, args=(app, job_id), daemon=True)
    worker.start()

    deadline = time.time() + 5.0
    while time.time() < deadline:
        db.session.expire_all()
        record = db.session.get(JobRecord, job_id)
        if record and record.status == "running":
            break
        time.sleep(0.02)
    else:
        raise AssertionError("mapping job did not reach running state")

    ok, message = cancel_job(job_id)
    assert ok is True
    assert "requested" in message.lower()

    worker.join(timeout=5.0)
    assert not worker.is_alive()

    db.session.expire_all()
    record = db.session.get(JobRecord, job_id)
    assert record is not None
    assert record.status == "canceled"
    op_payload = json.loads((workspace_dir / "_ops" / f"{job_id}.json").read_text(encoding="utf-8"))
    assert op_payload["status"] == "canceled"
