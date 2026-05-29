from __future__ import annotations

from app import create_app
from app.config import ProductionConfig
from app.services import execution_service, mapping_metadata_service, nas_service, standard_update_service, system_service, task_service


def test_production_forces_worker_mode_when_inline_configured(monkeypatch):
    monkeypatch.setattr(ProductionConfig, "SQLALCHEMY_DATABASE_URI", "sqlite:///:memory:")
    monkeypatch.setattr(ProductionConfig, "AUTH_ENABLED", False)
    monkeypatch.setattr(ProductionConfig, "APP_ENV", "production")
    monkeypatch.setattr(ProductionConfig, "JOB_EXECUTOR_MODE", "inline")

    app = create_app("production", init_auth=False)

    assert app.config["JOB_EXECUTOR_MODE"] == "worker"


def test_production_disables_startup_schema_bootstrap(monkeypatch):
    monkeypatch.setattr(ProductionConfig, "SQLALCHEMY_DATABASE_URI", "sqlite:///:memory:")
    monkeypatch.setattr(ProductionConfig, "AUTH_ENABLED", False)
    monkeypatch.setattr(ProductionConfig, "APP_ENV", "production")
    monkeypatch.setattr(ProductionConfig, "AUTO_SCHEMA_MANAGEMENT", False)

    def fail_if_called() -> None:
        raise AssertionError("ensure_schema should not run during production startup")

    monkeypatch.setattr(task_service, "ensure_task_schema", fail_if_called)
    monkeypatch.setattr(standard_update_service, "ensure_standard_update_schema", fail_if_called)
    monkeypatch.setattr(system_service, "ensure_schema", fail_if_called)
    monkeypatch.setattr(mapping_metadata_service, "ensure_schema", fail_if_called)
    monkeypatch.setattr(execution_service, "ensure_schema", fail_if_called)
    monkeypatch.setattr(nas_service, "ensure_nas_schema", fail_if_called)

    app = create_app("production", init_auth=False)

    assert app.config["AUTO_SCHEMA_MANAGEMENT"] is False
