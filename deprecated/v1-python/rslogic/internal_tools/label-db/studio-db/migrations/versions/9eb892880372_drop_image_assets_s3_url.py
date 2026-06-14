"""drop image_assets s3_url

Revision ID: 9eb892880372
Revises: e31fb594c8bb
Create Date: 2026-02-22 23:32:52.315154
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9eb892880372'
down_revision = 'e31fb594c8bb'
branch_labels = None
depends_on = None


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

    if _has_index(bind, "image_assets", "ix_rslogic_image_assets_s3_url"):
        op.drop_index("ix_rslogic_image_assets_s3_url", table_name="image_assets")

    if "s3_url" in _column_names(bind, "image_assets"):
        op.drop_column("image_assets", "s3_url")


def downgrade() -> None:
    bind = op.get_bind()

    if "s3_url" not in _column_names(bind, "image_assets"):
        op.add_column("image_assets", sa.Column("s3_url", sa.String(length=2048), nullable=True))

    op.execute(
        """
        UPDATE image_assets
        SET s3_url = uri
        WHERE s3_url IS NULL
          AND uri LIKE 's3://%'
        """
    )

    if not _has_index(bind, "image_assets", "ix_rslogic_image_assets_s3_url"):
        op.create_index("ix_rslogic_image_assets_s3_url", "image_assets", ["s3_url"], unique=False)
