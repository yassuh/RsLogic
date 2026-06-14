"""add image asset metadata and postgis location

Revision ID: b5be93bc118d
Revises: 7500ca4b1731
Create Date: 2026-02-23 23:39:21.226764
"""
from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'b5be93bc118d'
down_revision = '7500ca4b1731'
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
    columns = _column_names(bind, "image_assets")

    if "location" not in columns:
        op.add_column(
            "image_assets",
            sa.Column("location", Geometry(geometry_type="POINT", srid=4326), nullable=True),
        )

    if "metadata" not in columns:
        op.add_column(
            "image_assets",
            sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        )

        # Keep existing free-form payload in sync when adding metadata.
        if "extra" in columns:
            op.execute("UPDATE image_assets SET metadata = COALESCE(extra, '{}'::jsonb) WHERE metadata IS NULL")
        else:
            op.execute("UPDATE image_assets SET metadata = '{}'::jsonb WHERE metadata IS NULL")

        op.alter_column(
            "image_assets",
            "metadata",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        )

    if "altitude" in columns:
        op.drop_column("image_assets", "altitude")

    if not _has_index(bind, "image_assets", "ix_rslogic_image_assets_location_gist"):
        op.create_index(
            "ix_rslogic_image_assets_location_gist",
            "image_assets",
            ["location"],
            unique=False,
            postgresql_using="gist",
        )


def downgrade() -> None:
    bind = op.get_bind()
    columns = _column_names(bind, "image_assets")

    if _has_index(bind, "image_assets", "ix_rslogic_image_assets_location_gist"):
        op.drop_index("ix_rslogic_image_assets_location_gist", table_name="image_assets")

    if "metadata" in columns:
        op.drop_column("image_assets", "metadata")

    if "location" in columns:
        op.drop_column("image_assets", "location")
