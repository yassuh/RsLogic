"""create model tables

Revision ID: b7912b7a0663
Revises: 3ef61f5cfa17
Create Date: 2026-02-06 20:21:14.940484
"""
from pathlib import Path
import sys

from alembic import op


# revision identifiers, used by Alembic.
revision = 'b7912b7a0663'
down_revision = '3ef61f5cfa17'
branch_labels = None
depends_on = None


def _load_base():
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from models import Base

    return Base


def upgrade() -> None:
    Base = _load_base()
    bind = op.get_bind()
    tables = [table for table in Base.metadata.sorted_tables if table.name != "realityscan_jobs"]
    Base.metadata.create_all(bind=bind, tables=tables, checkfirst=True)


def downgrade() -> None:
    Base = _load_base()
    bind = op.get_bind()
    tables = [table for table in reversed(Base.metadata.sorted_tables) if table.name != "realityscan_jobs"]
    Base.metadata.drop_all(bind=bind, tables=tables, checkfirst=True)
