"""init

Revision ID: 20260206_0001
Revises: 
Create Date: 2026-02-06 00:00:00
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260206_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS postgis")
