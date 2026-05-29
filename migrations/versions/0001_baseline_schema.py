"""baseline schema

Revision ID: 0001_baseline_schema
Revises:
Create Date: 2026-05-29 00:00:00
"""
from __future__ import annotations

from alembic import op

from app.extensions import db
import app.models  # noqa: F401

# revision identifiers, used by Alembic.
revision = "0001_baseline_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    db.metadata.create_all(bind=bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    db.metadata.drop_all(bind=bind, checkfirst=True)
