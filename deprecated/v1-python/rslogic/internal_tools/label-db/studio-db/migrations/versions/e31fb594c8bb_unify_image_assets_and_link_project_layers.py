"""unify image assets and link project layers

Revision ID: e31fb594c8bb
Revises: c0e0c2f4d9ab
Create Date: 2026-02-22 00:00:00
"""

from pathlib import PurePosixPath
from uuid import uuid4

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e31fb594c8bb"
down_revision = "c0e0c2f4d9ab"
branch_labels = None
depends_on = None


def _has_constraint(bind, name: str) -> bool:
    query = sa.text("SELECT 1 FROM pg_constraint WHERE conname = :name LIMIT 1")
    return bind.execute(query, {"name": name}).scalar() is not None


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


def _column_names(bind, table_name: str) -> set[str]:
    insp = sa.inspect(bind)
    return {col["name"] for col in insp.get_columns(table_name)}


def _foreign_key_names(bind, table_name: str) -> set[str]:
    insp = sa.inspect(bind)
    return {fk["name"] for fk in insp.get_foreign_keys(table_name) if fk.get("name")}


def _s3_parts(uri: str) -> tuple[str | None, str | None]:
    if not uri.startswith("s3://"):
        return None, None

    rest = uri[5:]
    if "/" not in rest:
        return rest or None, None

    bucket, object_key = rest.split("/", 1)
    return bucket or None, object_key or None


def _filename_from_uri(uri: str) -> str | None:
    if "://" in uri:
        path = uri.split("://", 1)[1]
        if "/" in path:
            path = path.split("/", 1)[1]
    else:
        path = uri

    name = PurePosixPath(path).name
    return name or None


def upgrade() -> None:
    bind = op.get_bind()

    image_columns = _column_names(bind, "image_assets")
    if "uri" not in image_columns:
        op.add_column("image_assets", sa.Column("uri", sa.String(length=2048), nullable=True))

    op.execute(
        """
        UPDATE image_assets
        SET uri = COALESCE(
            uri,
            s3_url,
            CASE
                WHEN bucket_name IS NOT NULL AND object_key IS NOT NULL
                THEN 's3://' || bucket_name || '/' || object_key
                ELSE NULL
            END
        )
        WHERE uri IS NULL
        """
    )

    missing_uri = bind.execute(sa.text("SELECT count(*) FROM image_assets WHERE uri IS NULL")).scalar_one()
    if missing_uri > 0:
        raise RuntimeError("Cannot enforce image_assets.uri: existing rows are missing a resolvable URI.")

    op.alter_column("image_assets", "uri", existing_type=sa.String(length=2048), nullable=False)
    op.alter_column("image_assets", "dataset_id", existing_type=sa.String(length=255), nullable=True)
    op.alter_column("image_assets", "bucket_name", existing_type=sa.String(length=255), nullable=True)
    op.alter_column("image_assets", "object_key", existing_type=sa.String(length=1024), nullable=True)
    op.alter_column("image_assets", "filename", existing_type=sa.String(length=255), nullable=True)
    op.alter_column("image_assets", "extra", server_default=sa.text("'{}'::jsonb"))

    if _has_constraint(bind, "ck_image_assets_uri_scheme"):
        op.drop_constraint("ck_image_assets_uri_scheme", "image_assets", type_="check")
    op.create_check_constraint(
        "ck_image_assets_uri_scheme",
        "image_assets",
        "(uri ~* '^(s3://|file://).+')",
    )

    if _has_constraint(bind, "uq_image_assets_uri"):
        op.drop_constraint("uq_image_assets_uri", "image_assets", type_="unique")
    op.create_unique_constraint("uq_image_assets_uri", "image_assets", ["uri"])

    if not _has_index(bind, "image_assets", "ix_rslogic_image_assets_uri"):
        op.create_index("ix_rslogic_image_assets_uri", "image_assets", ["uri"], unique=False)

    project_layer_columns = _column_names(bind, "project_layers")
    if "image_asset_id" not in project_layer_columns:
        op.add_column("project_layers", sa.Column("image_asset_id", sa.String(length=36), nullable=True))

    if not _has_index(bind, "project_layers", "ix_project_layers_image_asset_id"):
        op.create_index("ix_project_layers_image_asset_id", "project_layers", ["image_asset_id"], unique=False)

    fk_names = _foreign_key_names(bind, "project_layers")
    if "fk_project_layers_image_asset_id" not in fk_names:
        op.create_foreign_key(
            "fk_project_layers_image_asset_id",
            "project_layers",
            "image_assets",
            ["image_asset_id"],
            ["id"],
            ondelete="SET NULL",
        )

    op.execute(
        """
        UPDATE project_layers p
        SET image_asset_id = a.id
        FROM image_assets a
        WHERE p.image_asset_id IS NULL
          AND p.image_url IS NOT NULL
          AND a.uri = p.image_url
        """
    )

    missing_asset_uris = bind.execute(
        sa.text(
            """
            SELECT DISTINCT p.image_url
            FROM project_layers p
            LEFT JOIN image_assets a ON a.uri = p.image_url
            WHERE p.source_type = 'image'
              AND p.image_url IS NOT NULL
              AND p.image_asset_id IS NULL
              AND a.id IS NULL
            """
        )
    ).scalars().all()

    for uri in missing_asset_uris:
        bucket_name, object_key = _s3_parts(uri)
        bind.execute(
            sa.text(
                """
                INSERT INTO image_assets (
                    id,
                    uri,
                    dataset_id,
                    bucket_name,
                    object_key,
                    s3_url,
                    filename,
                    extra
                )
                VALUES (
                    :id,
                    :uri,
                    NULL,
                    :bucket_name,
                    :object_key,
                    :s3_url,
                    :filename,
                    '{}'::jsonb
                )
                """
            ),
            {
                "id": str(uuid4()),
                "uri": uri,
                "bucket_name": bucket_name,
                "object_key": object_key,
                "s3_url": uri if uri.startswith("s3://") else None,
                "filename": _filename_from_uri(uri),
            },
        )

    op.execute(
        """
        UPDATE project_layers p
        SET image_asset_id = a.id
        FROM image_assets a
        WHERE p.image_asset_id IS NULL
          AND p.image_url IS NOT NULL
          AND a.uri = p.image_url
        """
    )

    missing_image_assets = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM project_layers
            WHERE source_type = 'image'
              AND image_asset_id IS NULL
            """
        )
    ).scalar_one()
    if missing_image_assets > 0:
        raise RuntimeError("Cannot enforce project_layers image->asset link: some image layers have no mapped image_asset_id.")

    for name in (
        "ck_project_layers_image_requires_url",
        "ck_project_layers_non_image_no_url",
        "ck_project_layers_image_requires_asset",
        "ck_project_layers_non_image_no_asset",
        "ck_project_layers_image_url_scheme",
    ):
        if _has_constraint(bind, name):
            op.drop_constraint(name, "project_layers", type_="check")

    op.create_check_constraint(
        "ck_project_layers_image_requires_asset",
        "project_layers",
        "(source_type != 'image') OR (image_asset_id IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_project_layers_non_image_no_asset",
        "project_layers",
        "(source_type = 'image') OR (image_asset_id IS NULL)",
    )
    op.create_check_constraint(
        "ck_project_layers_image_url_scheme",
        "project_layers",
        "(image_url IS NULL) OR (image_url ~* '^(s3://|file://).+')",
    )


def downgrade() -> None:
    bind = op.get_bind()

    op.execute(
        """
        UPDATE project_layers p
        SET image_url = a.uri
        FROM image_assets a
        WHERE p.image_asset_id = a.id
          AND p.image_url IS NULL
        """
    )

    missing_urls = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM project_layers
            WHERE source_type = 'image'
              AND image_url IS NULL
            """
        )
    ).scalar_one()
    if missing_urls > 0:
        raise RuntimeError("Cannot downgrade: image layers without image_url would violate previous schema.")

    for name in (
        "ck_project_layers_image_requires_asset",
        "ck_project_layers_non_image_no_asset",
        "ck_project_layers_image_url_scheme",
    ):
        if _has_constraint(bind, name):
            op.drop_constraint(name, "project_layers", type_="check")

    op.create_check_constraint(
        "ck_project_layers_image_requires_url",
        "project_layers",
        "(source_type != 'image') OR (image_url IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_project_layers_image_url_scheme",
        "project_layers",
        "(source_type != 'image') OR (image_url ~* '^(s3://|file://).+')",
    )
    op.create_check_constraint(
        "ck_project_layers_non_image_no_url",
        "project_layers",
        "(source_type = 'image') OR (image_url IS NULL)",
    )

    fk_names = _foreign_key_names(bind, "project_layers")
    if "fk_project_layers_image_asset_id" in fk_names:
        op.drop_constraint("fk_project_layers_image_asset_id", "project_layers", type_="foreignkey")

    if _has_index(bind, "project_layers", "ix_project_layers_image_asset_id"):
        op.drop_index("ix_project_layers_image_asset_id", table_name="project_layers")

    if "image_asset_id" in _column_names(bind, "project_layers"):
        op.drop_column("project_layers", "image_asset_id")

    op.execute(
        """
        UPDATE image_assets
        SET s3_url = uri
        WHERE s3_url IS NULL
          AND uri LIKE 's3://%'
        """
    )

    null_required = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM image_assets
            WHERE dataset_id IS NULL
               OR bucket_name IS NULL
               OR object_key IS NULL
               OR filename IS NULL
            """
        )
    ).scalar_one()
    if null_required > 0:
        raise RuntimeError("Cannot downgrade: nullable image_assets columns now contain NULL values.")

    if _has_index(bind, "image_assets", "ix_rslogic_image_assets_uri"):
        op.drop_index("ix_rslogic_image_assets_uri", table_name="image_assets")

    if _has_constraint(bind, "uq_image_assets_uri"):
        op.drop_constraint("uq_image_assets_uri", "image_assets", type_="unique")

    if _has_constraint(bind, "ck_image_assets_uri_scheme"):
        op.drop_constraint("ck_image_assets_uri_scheme", "image_assets", type_="check")

    op.alter_column("image_assets", "extra", server_default=None)
    op.alter_column("image_assets", "filename", existing_type=sa.String(length=255), nullable=False)
    op.alter_column("image_assets", "object_key", existing_type=sa.String(length=1024), nullable=False)
    op.alter_column("image_assets", "bucket_name", existing_type=sa.String(length=255), nullable=False)
    op.alter_column("image_assets", "dataset_id", existing_type=sa.String(length=255), nullable=False)

    if "uri" in _column_names(bind, "image_assets"):
        op.drop_column("image_assets", "uri")
