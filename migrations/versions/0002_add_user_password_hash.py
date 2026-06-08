"""remove deprecated local auth password migration

Revision ID: 0002_add_user_password_hash
Revises: 0001_baseline_schema
Create Date: 2026-06-04 00:00:00
"""
from __future__ import annotations

# revision identifiers, used by Alembic.
revision = "0002_add_user_password_hash"
down_revision = "0001_baseline_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
