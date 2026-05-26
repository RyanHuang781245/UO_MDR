from __future__ import annotations

from app import create_app
from app.config import ProductionConfig


def test_production_forces_worker_mode_when_inline_configured(monkeypatch):
    monkeypatch.setattr(ProductionConfig, "SQLALCHEMY_DATABASE_URI", "sqlite:///:memory:")
    monkeypatch.setattr(ProductionConfig, "AUTH_ENABLED", False)
    monkeypatch.setattr(ProductionConfig, "APP_ENV", "production")
    monkeypatch.setattr(ProductionConfig, "JOB_EXECUTOR_MODE", "inline")

    app = create_app("production", init_auth=False)

    assert app.config["JOB_EXECUTOR_MODE"] == "worker"
