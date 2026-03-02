"""add label edge curve handles

Revision ID: b8b660afba04
Revises: 7423fd1523d5
Create Date: 2026-02-06 20:45:41.014513
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'b8b660afba04'
down_revision = '7423fd1523d5'
branch_labels = None
depends_on = None


def _has_constraint(bind, name: str) -> bool:
    query = sa.text("SELECT 1 FROM pg_constraint WHERE conname = :name LIMIT 1")
    return bind.execute(query, {"name": name}).scalar() is not None


def _column_names(bind, table_name: str) -> set[str]:
    insp = sa.inspect(bind)
    return {col["name"] for col in insp.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()

    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'curve_type') THEN
                CREATE TYPE curve_type AS ENUM ('line', 'quadratic_bezier', 'cubic_bezier', 'spline');
            END IF;
        END
        $$;
        """
    )

    columns = _column_names(bind, "label_edges")
    if "curve_type" not in columns:
        op.add_column(
            "label_edges",
            sa.Column(
                "curve_type",
                sa.Enum("line", "quadratic_bezier", "cubic_bezier", "spline", name="curve_type"),
                nullable=False,
                server_default="line",
            ),
        )
    if "from_handle_px" not in columns:
        op.add_column("label_edges", sa.Column("from_handle_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if "to_handle_px" not in columns:
        op.add_column("label_edges", sa.Column("to_handle_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if "controls_px" not in columns:
        op.add_column("label_edges", sa.Column("controls_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    op.alter_column("label_edges", "curve_type", server_default=sa.text("'line'::curve_type"))

    if _has_constraint(bind, "ck_label_edges_from_handle_shape"):
        op.drop_constraint("ck_label_edges_from_handle_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_to_handle_shape"):
        op.drop_constraint("ck_label_edges_to_handle_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_controls_shape"):
        op.drop_constraint("ck_label_edges_controls_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_line_no_controls"):
        op.drop_constraint("ck_label_edges_line_no_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_quadratic_controls"):
        op.drop_constraint("ck_label_edges_quadratic_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_cubic_controls"):
        op.drop_constraint("ck_label_edges_cubic_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_spline_controls"):
        op.drop_constraint("ck_label_edges_spline_controls", "label_edges", type_="check")

    op.create_check_constraint(
        "ck_label_edges_from_handle_shape",
        "label_edges",
        "(from_handle_px IS NULL) OR (jsonb_typeof(from_handle_px) = 'object')",
    )
    op.create_check_constraint(
        "ck_label_edges_to_handle_shape",
        "label_edges",
        "(to_handle_px IS NULL) OR (jsonb_typeof(to_handle_px) = 'object')",
    )
    op.create_check_constraint(
        "ck_label_edges_controls_shape",
        "label_edges",
        "(controls_px IS NULL) OR (jsonb_typeof(controls_px) = 'array')",
    )
    op.create_check_constraint(
        "ck_label_edges_line_no_controls",
        "label_edges",
        "(curve_type != 'line') OR (from_handle_px IS NULL AND to_handle_px IS NULL AND controls_px IS NULL)",
    )
    op.create_check_constraint(
        "ck_label_edges_quadratic_controls",
        "label_edges",
        """
        (curve_type != 'quadratic_bezier') OR
        (
            (controls_px IS NOT NULL AND jsonb_array_length(controls_px) = 1 AND from_handle_px IS NULL AND to_handle_px IS NULL)
            OR
            (
                controls_px IS NULL
                AND
                (
                    (CASE WHEN from_handle_px IS NULL THEN 0 ELSE 1 END)
                    + (CASE WHEN to_handle_px IS NULL THEN 0 ELSE 1 END)
                ) = 1
            )
        )
        """,
    )
    op.create_check_constraint(
        "ck_label_edges_cubic_controls",
        "label_edges",
        "(curve_type != 'cubic_bezier') OR (from_handle_px IS NOT NULL AND to_handle_px IS NOT NULL AND controls_px IS NULL)",
    )
    op.create_check_constraint(
        "ck_label_edges_spline_controls",
        "label_edges",
        "(curve_type != 'spline') OR (controls_px IS NOT NULL AND jsonb_array_length(controls_px) >= 2)",
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_label_edges_curve_type ON label_edges (curve_type)")


def downgrade() -> None:
    bind = op.get_bind()

    has_curve_data = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM label_edges
            WHERE curve_type != 'line'
               OR from_handle_px IS NOT NULL
               OR to_handle_px IS NOT NULL
               OR controls_px IS NOT NULL
            """
        )
    ).scalar_one()
    if has_curve_data > 0:
        raise RuntimeError("Cannot downgrade while curve handle data exists in label_edges.")

    op.execute("DROP INDEX IF EXISTS ix_label_edges_curve_type")

    if _has_constraint(bind, "ck_label_edges_spline_controls"):
        op.drop_constraint("ck_label_edges_spline_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_cubic_controls"):
        op.drop_constraint("ck_label_edges_cubic_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_quadratic_controls"):
        op.drop_constraint("ck_label_edges_quadratic_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_line_no_controls"):
        op.drop_constraint("ck_label_edges_line_no_controls", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_controls_shape"):
        op.drop_constraint("ck_label_edges_controls_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_to_handle_shape"):
        op.drop_constraint("ck_label_edges_to_handle_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_from_handle_shape"):
        op.drop_constraint("ck_label_edges_from_handle_shape", "label_edges", type_="check")

    columns = _column_names(bind, "label_edges")
    if "controls_px" in columns:
        op.drop_column("label_edges", "controls_px")
    if "to_handle_px" in columns:
        op.drop_column("label_edges", "to_handle_px")
    if "from_handle_px" in columns:
        op.drop_column("label_edges", "from_handle_px")
    if "curve_type" in columns:
        op.drop_column("label_edges", "curve_type")

    op.execute("DROP TYPE IF EXISTS curve_type")
