"""add rslogic image and processing job tables"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "a2d4f6e9b8c1"
down_revision = "b8b660afba04"
branch_labels = None
depends_on = None


def _has_table(bind, table_name: str) -> bool:
    inspector = sa.inspect(bind)
    return inspector.has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    inspector = sa.inspect(bind)
    return {column["name"] for column in inspector.get_columns(table_name)}


def _has_index(bind, table_name: str, index_name: str) -> bool:
    query = sa.text(
        """
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = :table_name
          AND indexname = :index_name
        LIMIT 1
        """
    )
    return bind.execute(query, {"table_name": table_name, "index_name": index_name}).scalar() is not None


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_table(bind, "image_assets"):
        raise RuntimeError("Expected image_assets to exist before adding RsLogic job tables.")

    image_columns = _column_names(bind, "image_assets")
    if "dataset_id" not in image_columns:
        op.add_column("image_assets", sa.Column("dataset_id", sa.String(length=255), nullable=True))
    if not _has_index(bind, "image_assets", "ix_rslogic_image_assets_dataset_id"):
        op.create_index("ix_rslogic_image_assets_dataset_id", "image_assets", ["dataset_id"], unique=False)

    if _has_table(bind, "processing_jobs"):
        return

    op.create_table(
        "processing_jobs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("dataset_id", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("progress", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.Column("filters", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("result_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("progress >= 0 AND progress <= 100", name="ck_processing_jobs_progress_range"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_rslogic_processing_jobs_dataset_id", "processing_jobs", ["dataset_id"], unique=False)
    op.create_index("ix_rslogic_processing_jobs_status", "processing_jobs", ["status"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    if _has_index(bind, "processing_jobs", "ix_rslogic_processing_jobs_status"):
        op.drop_index("ix_rslogic_processing_jobs_status", table_name="processing_jobs")
    if _has_index(bind, "processing_jobs", "ix_rslogic_processing_jobs_dataset_id"):
        op.drop_index("ix_rslogic_processing_jobs_dataset_id", table_name="processing_jobs")
    if _has_table(bind, "processing_jobs"):
        op.drop_table("processing_jobs")

    if _has_index(bind, "image_assets", "ix_rslogic_image_assets_dataset_id"):
        op.drop_index("ix_rslogic_image_assets_dataset_id", table_name="image_assets")
    if _has_table(bind, "image_assets") and "dataset_id" in _column_names(bind, "image_assets"):
        op.drop_column("image_assets", "dataset_id")
