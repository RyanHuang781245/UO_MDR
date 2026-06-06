"""add local auth password hash

Revision ID: 0002_add_user_password_hash
Revises: 0001_baseline_schema
Create Date: 2026-06-04 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "0002_add_user_password_hash"
down_revision = "0001_baseline_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if "users" not in inspector.get_table_names():
        return
    existing_columns = {column["name"].lower() for column in inspector.get_columns("users")}
    if "password_hash" not in existing_columns:
        op.add_column("users", sa.Column("password_hash", sa.String(length=255), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if "users" not in inspector.get_table_names():
        return
    existing_columns = {column["name"].lower() for column in inspector.get_columns("users")}
    if "password_hash" in existing_columns:
        op.drop_column("users", "password_hash")
