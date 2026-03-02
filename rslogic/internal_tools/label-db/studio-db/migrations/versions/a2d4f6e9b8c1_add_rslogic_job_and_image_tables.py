"""add rslogic image and processing job tables"""

from pathlib import Path
import sys

from alembic import op


# revision identifiers, used by Alembic.
revision = "a2d4f6e9b8c1"
down_revision = "b8b660afba04"
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
    if "image_assets" in Base.metadata.tables:
        Base.metadata.tables["image_assets"].create(bind=bind, checkfirst=True)
    if "processing_jobs" in Base.metadata.tables:
        Base.metadata.tables["processing_jobs"].create(bind=bind, checkfirst=True)


def downgrade() -> None:
    Base = _load_base()
    bind = op.get_bind()
    if "processing_jobs" in Base.metadata.tables:
        Base.metadata.tables["processing_jobs"].drop(bind=bind, checkfirst=True)
    if "image_assets" in Base.metadata.tables:
        Base.metadata.tables["image_assets"].drop(bind=bind, checkfirst=True)
