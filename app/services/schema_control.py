from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import inspect

from app.extensions import db

SCHEMA_GROUPS: dict[str, tuple[str, ...]] = {
    "auth": ("users", "roles", "user_roles", "audit_logs", "system_error_logs"),
    "system": ("system_settings", "regulation_sync_states"),
    "tasks": ("tasks",),
    "nas": ("nas_roots",),
    "standard_updates": ("standard_update_tasks", "harmonised_releases"),
    "mapping": ("task_file_states", "mapping_schemes", "mapping_runs"),
    "execution": ("job_records", "job_artifacts", "job_events", "task_execution_locks"),
}


def auto_schema_management_enabled(app) -> bool:
    return bool(app.config.get("AUTO_SCHEMA_MANAGEMENT", True))


def tables_exist(*table_names: str) -> bool:
    if not table_names:
        return True
    inspector = inspect(db.engine)
    existing_tables = set(inspector.get_table_names())
    return all(name in existing_tables for name in table_names)


def existing_tables() -> set[str]:
    inspector = inspect(db.engine)
    return set(inspector.get_table_names())


def required_schema_groups(app) -> dict[str, tuple[str, ...]]:
    groups = {
        "system": SCHEMA_GROUPS["system"],
        "tasks": SCHEMA_GROUPS["tasks"],
        "nas": SCHEMA_GROUPS["nas"],
        "standard_updates": SCHEMA_GROUPS["standard_updates"],
        "mapping": SCHEMA_GROUPS["mapping"],
        "execution": SCHEMA_GROUPS["execution"],
    }
    if app.config.get("AUTH_ENABLED", True):
        groups["auth"] = SCHEMA_GROUPS["auth"]
    return groups


def missing_schema_groups(app, table_groups: Mapping[str, tuple[str, ...]] | None = None) -> dict[str, list[str]]:
    groups = dict(table_groups or required_schema_groups(app))
    tables = existing_tables()
    missing: dict[str, list[str]] = {}
    for group_name, required_tables in groups.items():
        absent = [table for table in required_tables if table not in tables]
        if absent:
            missing[group_name] = absent
    return missing
