from __future__ import annotations

from app.models.auth import Role, ensure_schema as ensure_auth_schema
from app.models.execution import JobArtifactRecord, JobEventRecord, JobRecord
from app.models.mapping_metadata import MappingRunRecord, MappingSchemeRecord
from app.models.settings import RegulationSyncState, SystemSetting
from app.services.execution_service import FLOW_SINGLE_JOB, MAPPING_OPERATION_JOB
from app.services import operations_service
from app.extensions import db


def test_schema_preflight_fails_when_required_tables_missing(app, monkeypatch):
    monkeypatch.setattr(
        operations_service,
        "required_schema_groups",
        lambda _app: {"ops": ("__missing_table__",)},
    )

    runner = app.test_cli_runner()
    result = runner.invoke(args=["schema-preflight"])

    assert result.exit_code != 0
    assert "__missing_table__" in result.output


def test_seed_bootstrap_populates_defaults(app, monkeypatch):
    app.config["AUTH_ENABLED"] = True
    monkeypatch.setenv("BOOTSTRAP_ADMIN", "NE025")

    with app.app_context():
        ensure_auth_schema()

    runner = app.test_cli_runner()
    result = runner.invoke(args=["seed-bootstrap"])

    assert result.exit_code == 0
    assert "roles=2" in result.output
    assert "admins=1" in result.output
    assert "system_settings=1" in result.output
    assert "regulation_sync_states=1" in result.output

    with app.app_context():
        assert Role.query.count() == 2
        assert SystemSetting.query.count() == 1
        assert RegulationSyncState.query.count() == 1


def test_cleanup_mapping_metadata_cli_removes_only_mapping_records(app):
    with app.app_context():
        db.session.add(
            MappingSchemeRecord(
                scheme_id="scheme-001",
                task_id="task-001",
                name="Mapping",
                reference_ok=True,
                extract_ok=True,
                status_key="ready",
            )
        )
        db.session.add(MappingRunRecord(run_id="map-job-001", task_id="task-001", status="completed"))
        db.session.add(JobRecord(job_id="map-job-001", job_type=MAPPING_OPERATION_JOB, queue_name="heavy", task_id="task-001"))
        db.session.add(JobArtifactRecord(job_id="map-job-001", artifact_type="result_zip", rel_path="task-001/mapping_job/map-job-001/out.zip"))
        db.session.add(JobEventRecord(job_id="map-job-001", event_type="completed"))
        db.session.add(JobRecord(job_id="flow-job-001", job_type=FLOW_SINGLE_JOB, queue_name="heavy", task_id="task-001"))
        db.session.commit()

    runner = app.test_cli_runner()
    dry_run = runner.invoke(args=["cleanup-mapping-metadata"])

    assert dry_run.exit_code == 0
    assert "dry_run=1" in dry_run.output
    with app.app_context():
        assert MappingSchemeRecord.query.count() == 1
        assert JobRecord.query.filter_by(job_id="map-job-001").count() == 1

    result = runner.invoke(args=["cleanup-mapping-metadata", "--yes"])

    assert result.exit_code == 0
    assert "commit=1" in result.output
    with app.app_context():
        assert MappingSchemeRecord.query.count() == 0
        assert MappingRunRecord.query.count() == 0
        assert JobRecord.query.filter_by(job_id="map-job-001").count() == 0
        assert JobArtifactRecord.query.filter_by(job_id="map-job-001").count() == 0
        assert JobEventRecord.query.filter_by(job_id="map-job-001").count() == 0
        assert JobRecord.query.filter_by(job_id="flow-job-001").count() == 1
