"""replace dataset_id with image groups

Revision ID: 7500ca4b1731
Revises: 9eb892880372
Create Date: 2026-02-22 23:47:38.759456
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from uuid import uuid4


# revision identifiers, used by Alembic.
revision = '7500ca4b1731'
down_revision = '9eb892880372'
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


def _foreign_key_names(bind, table_name: str) -> set[str]:
    inspector = sa.inspect(bind)
    return {fk["name"] for fk in inspector.get_foreign_keys(table_name) if fk.get("name")}


def upgrade() -> None:
    bind = op.get_bind()

    if not _has_table(bind, "image_groups"):
        op.create_table(
            "image_groups",
            sa.Column("id", sa.String(length=36), nullable=False),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column(
                "extra",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
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
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("name", name="uq_image_groups_name"),
        )

    if not _has_table(bind, "image_group_items"):
        op.create_table(
            "image_group_items",
            sa.Column("group_id", sa.String(length=36), nullable=False),
            sa.Column("image_id", sa.String(length=36), nullable=False),
            sa.Column("role", sa.String(length=64), nullable=True),
            sa.Column(
                "added_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.ForeignKeyConstraint(
                ["group_id"],
                ["image_groups.id"],
                name="fk_image_group_items_group",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["image_id"],
                ["image_assets.id"],
                name="fk_image_group_items_image",
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("group_id", "image_id", name="pk_image_group_items"),
        )

    if not _has_index(bind, "image_group_items", "ix_image_group_items_image_id"):
        op.create_index("ix_image_group_items_image_id", "image_group_items", ["image_id"], unique=False)

    processing_columns = _column_names(bind, "processing_jobs")
    if "image_group_id" not in processing_columns:
        op.add_column("processing_jobs", sa.Column("image_group_id", sa.String(length=36), nullable=True))

    if not _has_index(bind, "processing_jobs", "ix_rslogic_processing_jobs_image_group_id"):
        op.create_index("ix_rslogic_processing_jobs_image_group_id", "processing_jobs", ["image_group_id"], unique=False)

    processing_fks = _foreign_key_names(bind, "processing_jobs")
    if "fk_processing_jobs_image_group_id" not in processing_fks:
        op.create_foreign_key(
            "fk_processing_jobs_image_group_id",
            "processing_jobs",
            "image_groups",
            ["image_group_id"],
            ["id"],
            ondelete="SET NULL",
        )

    image_columns = _column_names(bind, "image_assets")
    processing_columns = _column_names(bind, "processing_jobs")
    image_has_dataset = "dataset_id" in image_columns
    processing_has_dataset = "dataset_id" in processing_columns

    dataset_values: set[str] = set()
    if image_has_dataset:
        dataset_values.update(
            bind.execute(
                sa.text("SELECT DISTINCT dataset_id FROM image_assets WHERE dataset_id IS NOT NULL")
            ).scalars().all()
        )
    if processing_has_dataset:
        dataset_values.update(
            bind.execute(
                sa.text("SELECT DISTINCT dataset_id FROM processing_jobs WHERE dataset_id IS NOT NULL")
            ).scalars().all()
        )

    for dataset_id in sorted(dataset_values):
        bind.execute(
            sa.text(
                """
                INSERT INTO image_groups (id, name, description, extra)
                VALUES (:id, :name, NULL, '{}'::jsonb)
                ON CONFLICT (name) DO NOTHING
                """
            ),
            {"id": str(uuid4()), "name": dataset_id},
        )

    if processing_has_dataset:
        op.execute(
            """
            UPDATE processing_jobs p
            SET image_group_id = g.id
            FROM image_groups g
            WHERE p.image_group_id IS NULL
              AND p.dataset_id IS NOT NULL
              AND g.name = p.dataset_id
            """
        )

        missing_job_groups = bind.execute(
            sa.text(
                """
                SELECT count(*)
                FROM processing_jobs
                WHERE dataset_id IS NOT NULL
                  AND image_group_id IS NULL
                """
            )
        ).scalar_one()
        if missing_job_groups > 0:
            raise RuntimeError("Unable to map all processing_jobs.dataset_id values to image groups.")

    if image_has_dataset:
        op.execute(
            """
            INSERT INTO image_group_items (group_id, image_id, role, added_at)
            SELECT g.id, i.id, NULL, now()
            FROM image_assets i
            JOIN image_groups g ON g.name = i.dataset_id
            WHERE i.dataset_id IS NOT NULL
            ON CONFLICT (group_id, image_id) DO NOTHING
            """
        )

    if _has_index(bind, "image_assets", "ix_rslogic_image_assets_dataset_id"):
        op.drop_index("ix_rslogic_image_assets_dataset_id", table_name="image_assets")
    if _has_index(bind, "processing_jobs", "ix_rslogic_processing_jobs_dataset_id"):
        op.drop_index("ix_rslogic_processing_jobs_dataset_id", table_name="processing_jobs")

    if image_has_dataset:
        op.drop_column("image_assets", "dataset_id")
    if processing_has_dataset:
        op.drop_column("processing_jobs", "dataset_id")


def downgrade() -> None:
    bind = op.get_bind()

    image_columns = _column_names(bind, "image_assets")
    if "dataset_id" not in image_columns:
        op.add_column("image_assets", sa.Column("dataset_id", sa.String(length=255), nullable=True))

    processing_columns = _column_names(bind, "processing_jobs")
    if "dataset_id" not in processing_columns:
        op.add_column("processing_jobs", sa.Column("dataset_id", sa.String(length=255), nullable=True))

    if not _has_index(bind, "image_assets", "ix_rslogic_image_assets_dataset_id"):
        op.create_index("ix_rslogic_image_assets_dataset_id", "image_assets", ["dataset_id"], unique=False)
    if not _has_index(bind, "processing_jobs", "ix_rslogic_processing_jobs_dataset_id"):
        op.create_index("ix_rslogic_processing_jobs_dataset_id", "processing_jobs", ["dataset_id"], unique=False)

    # dataset_id is scalar in old schema; refuse downgrade if any image belongs to multiple groups.
    multi_group_images = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM (
              SELECT image_id
              FROM image_group_items
              GROUP BY image_id
              HAVING count(*) > 1
            ) t
            """
        )
    ).scalar_one()
    if multi_group_images > 0:
        raise RuntimeError("Cannot downgrade: one or more images belong to multiple groups.")

    op.execute(
        """
        UPDATE image_assets i
        SET dataset_id = g.name
        FROM image_group_items gi
        JOIN image_groups g ON g.id = gi.group_id
        WHERE gi.image_id = i.id
        """
    )

    op.execute(
        """
        UPDATE processing_jobs p
        SET dataset_id = g.name
        FROM image_groups g
        WHERE p.image_group_id = g.id
          AND p.dataset_id IS NULL
        """
    )

    missing_job_dataset = bind.execute(
        sa.text("SELECT count(*) FROM processing_jobs WHERE dataset_id IS NULL")
    ).scalar_one()
    if missing_job_dataset > 0:
        raise RuntimeError("Cannot downgrade: processing_jobs rows without image_group_id cannot restore dataset_id.")

    op.alter_column("processing_jobs", "dataset_id", existing_type=sa.String(length=255), nullable=False)

    processing_fks = _foreign_key_names(bind, "processing_jobs")
    if "fk_processing_jobs_image_group_id" in processing_fks:
        op.drop_constraint("fk_processing_jobs_image_group_id", "processing_jobs", type_="foreignkey")

    if _has_index(bind, "processing_jobs", "ix_rslogic_processing_jobs_image_group_id"):
        op.drop_index("ix_rslogic_processing_jobs_image_group_id", table_name="processing_jobs")

    if "image_group_id" in _column_names(bind, "processing_jobs"):
        op.drop_column("processing_jobs", "image_group_id")

    if _has_index(bind, "image_group_items", "ix_image_group_items_image_id"):
        op.drop_index("ix_image_group_items_image_id", table_name="image_group_items")

    if _has_table(bind, "image_group_items"):
        op.drop_table("image_group_items")
    if _has_table(bind, "image_groups"):
        op.drop_table("image_groups")
