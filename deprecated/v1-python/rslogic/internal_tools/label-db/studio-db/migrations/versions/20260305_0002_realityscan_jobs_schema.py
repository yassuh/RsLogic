"""rename processing jobs to realityscan jobs and store explicit job definition

Revision ID: 20260305_0002
Revises: 7500ca4b1731
Create Date: 2026-03-05 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260305_0002"
down_revision = "7500ca4b1731"
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


def _constraint_names(bind, table_name: str) -> set[str]:
    query = sa.text(
        """
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = to_regclass(:table_name)
        """
    )
    return {row[0] for row in bind.execute(query, {"table_name": table_name}).fetchall()}


def upgrade() -> None:
    bind = op.get_bind()

    if _has_table(bind, "processing_jobs") and not _has_table(bind, "realityscan_jobs"):
        op.rename_table("processing_jobs", "realityscan_jobs")

    if not _has_table(bind, "realityscan_jobs"):
        return

    columns = _column_names(bind, "realityscan_jobs")
    if "filters" in columns and "job_definition" not in columns:
        op.alter_column("realityscan_jobs", "filters", new_column_name="job_definition")
    columns = _column_names(bind, "realityscan_jobs")

    if "job_name" not in columns:
        op.add_column("realityscan_jobs", sa.Column("job_name", sa.String(length=255), nullable=True))

    if "job_definition" not in columns:
        op.add_column(
            "realityscan_jobs",
            sa.Column(
                "job_definition",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
        )
    else:
        op.execute("UPDATE realityscan_jobs SET job_definition = '{}'::jsonb WHERE job_definition IS NULL")
        op.alter_column(
            "realityscan_jobs",
            "job_definition",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        )

    constraints = _constraint_names(bind, "realityscan_jobs")
    if "ck_processing_jobs_progress_range" in constraints and "ck_realityscan_jobs_progress_range" not in constraints:
        op.execute(
            "ALTER TABLE realityscan_jobs RENAME CONSTRAINT "
            "ck_processing_jobs_progress_range TO ck_realityscan_jobs_progress_range"
        )
        constraints = _constraint_names(bind, "realityscan_jobs")

    if "fk_processing_jobs_image_group_id" in constraints and "fk_realityscan_jobs_image_group_id" not in constraints:
        op.execute(
            "ALTER TABLE realityscan_jobs RENAME CONSTRAINT "
            "fk_processing_jobs_image_group_id TO fk_realityscan_jobs_image_group_id"
        )

    if _has_index(bind, "realityscan_jobs", "ix_rslogic_processing_jobs_image_group_id") and not _has_index(
        bind,
        "realityscan_jobs",
        "ix_rslogic_realityscan_jobs_image_group_id",
    ):
        op.execute(
            "ALTER INDEX ix_rslogic_processing_jobs_image_group_id "
            "RENAME TO ix_rslogic_realityscan_jobs_image_group_id"
        )
    elif not _has_index(bind, "realityscan_jobs", "ix_rslogic_realityscan_jobs_image_group_id"):
        op.create_index(
            "ix_rslogic_realityscan_jobs_image_group_id",
            "realityscan_jobs",
            ["image_group_id"],
            unique=False,
        )

    if _has_index(bind, "realityscan_jobs", "ix_rslogic_processing_jobs_status") and not _has_index(
        bind,
        "realityscan_jobs",
        "ix_rslogic_realityscan_jobs_status",
    ):
        op.execute(
            "ALTER INDEX ix_rslogic_processing_jobs_status "
            "RENAME TO ix_rslogic_realityscan_jobs_status"
        )
    elif not _has_index(bind, "realityscan_jobs", "ix_rslogic_realityscan_jobs_status"):
        op.create_index(
            "ix_rslogic_realityscan_jobs_status",
            "realityscan_jobs",
            ["status"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()

    if not _has_table(bind, "realityscan_jobs"):
        return

    constraints = _constraint_names(bind, "realityscan_jobs")
    if "ck_realityscan_jobs_progress_range" in constraints and "ck_processing_jobs_progress_range" not in constraints:
        op.execute(
            "ALTER TABLE realityscan_jobs RENAME CONSTRAINT "
            "ck_realityscan_jobs_progress_range TO ck_processing_jobs_progress_range"
        )
        constraints = _constraint_names(bind, "realityscan_jobs")

    if "fk_realityscan_jobs_image_group_id" in constraints and "fk_processing_jobs_image_group_id" not in constraints:
        op.execute(
            "ALTER TABLE realityscan_jobs RENAME CONSTRAINT "
            "fk_realityscan_jobs_image_group_id TO fk_processing_jobs_image_group_id"
        )

    if _has_index(bind, "realityscan_jobs", "ix_rslogic_realityscan_jobs_image_group_id") and not _has_index(
        bind,
        "realityscan_jobs",
        "ix_rslogic_processing_jobs_image_group_id",
    ):
        op.execute(
            "ALTER INDEX ix_rslogic_realityscan_jobs_image_group_id "
            "RENAME TO ix_rslogic_processing_jobs_image_group_id"
        )

    if _has_index(bind, "realityscan_jobs", "ix_rslogic_realityscan_jobs_status") and not _has_index(
        bind,
        "realityscan_jobs",
        "ix_rslogic_processing_jobs_status",
    ):
        op.execute(
            "ALTER INDEX ix_rslogic_realityscan_jobs_status "
            "RENAME TO ix_rslogic_processing_jobs_status"
        )

    columns = _column_names(bind, "realityscan_jobs")
    if "job_name" in columns:
        op.drop_column("realityscan_jobs", "job_name")

    if "job_definition" in columns and "filters" not in columns:
        op.alter_column(
            "realityscan_jobs",
            "job_definition",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            server_default=None,
        )
        op.alter_column("realityscan_jobs", "job_definition", new_column_name="filters")

    if _has_table(bind, "realityscan_jobs") and not _has_table(bind, "processing_jobs"):
        op.rename_table("realityscan_jobs", "processing_jobs")
