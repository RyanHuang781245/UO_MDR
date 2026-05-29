from __future__ import annotations

from app.models.auth import Role, ensure_schema as ensure_auth_schema
from app.models.settings import RegulationSyncState, SystemSetting
from app.services import operations_service


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
