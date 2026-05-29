from __future__ import annotations

import click
from flask import current_app

from app.models.auth import Role, count_admins, seed_roles
from app.models.settings import (
    RegulationSyncState,
    SystemSetting,
    ensure_default_regulation_sync_state,
    ensure_default_settings,
)
from app.services.authn_service import bootstrap_admins
from app.services.schema_control import missing_schema_groups, required_schema_groups


def run_schema_preflight(app) -> dict[str, object]:
    with app.app_context():
        groups = required_schema_groups(app)
        missing = missing_schema_groups(app, groups)
        return {
            "ok": not missing,
            "groups": groups,
            "missing": missing,
        }


def run_seed_bootstrap(app, *, include_auth: bool | None = None, include_system: bool = True) -> dict[str, object]:
    with app.app_context():
        requested_groups: dict[str, tuple[str, ...]] = {}
        if include_auth is None:
            include_auth = bool(app.config.get("AUTH_ENABLED", True))
        all_groups = required_schema_groups(app)
        if include_auth:
            requested_groups["auth"] = all_groups["auth"]
        if include_system:
            requested_groups["system"] = all_groups["system"]
        missing = missing_schema_groups(app, requested_groups)
        if missing:
            raise click.ClickException(
                "Missing required tables for seed bootstrap: "
                + ", ".join(f"{group}=[{', '.join(tables)}]" for group, tables in sorted(missing.items()))
            )

        result: dict[str, object] = {
            "auth_enabled": bool(include_auth),
            "system_enabled": bool(include_system),
        }
        if include_auth:
            seed_roles()
            bootstrap_admins()
            result["role_count"] = Role.query.count()
            result["admin_count"] = count_admins()
        if include_system:
            ensure_default_settings()
            ensure_default_regulation_sync_state()
            result["system_setting_count"] = SystemSetting.query.count()
            result["regulation_sync_state_count"] = RegulationSyncState.query.count()
        return result


def register_operations_cli(app) -> None:
    @app.cli.command("schema-preflight")
    def schema_preflight_command() -> None:
        result = run_schema_preflight(current_app._get_current_object())
        if not result["ok"]:
            missing = result["missing"]
            details = " ".join(
                f"{group}=[{', '.join(tables)}]" for group, tables in sorted(missing.items())  # type: ignore[arg-type]
            )
            raise click.ClickException(f"Missing required tables: {details}")
        click.echo(
            "schema_preflight "
            f"ok=1 groups={len(result['groups'])} "
            f"auto_schema_management={'1' if current_app.config.get('AUTO_SCHEMA_MANAGEMENT') else '0'}"
        )

    @app.cli.command("seed-bootstrap")
    @click.option("--skip-auth", is_flag=True, help="Skip auth role/admin bootstrap.")
    @click.option("--skip-system", is_flag=True, help="Skip default system settings bootstrap.")
    def seed_bootstrap_command(skip_auth: bool, skip_system: bool) -> None:
        app_obj = current_app._get_current_object()
        result = run_seed_bootstrap(
            app_obj,
            include_auth=False if skip_auth else None,
            include_system=not skip_system,
        )
        click.echo(
            "seed_bootstrap "
            f"auth={'1' if result['auth_enabled'] else '0'} "
            f"system={'1' if result['system_enabled'] else '0'} "
            f"roles={result.get('role_count', 0)} "
            f"admins={result.get('admin_count', 0)} "
            f"system_settings={result.get('system_setting_count', 0)} "
            f"regulation_sync_states={result.get('regulation_sync_state_count', 0)}"
        )
