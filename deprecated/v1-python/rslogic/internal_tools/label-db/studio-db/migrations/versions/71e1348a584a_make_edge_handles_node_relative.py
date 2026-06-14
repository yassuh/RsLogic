"""make edge handles node-relative

Revision ID: 71e1348a584a
Revises: b8b660afba04
Create Date: 2026-02-06 21:19:48.651560
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '71e1348a584a'
down_revision = 'b8b660afba04'
branch_labels = None
depends_on = None


def _has_constraint(bind, name: str) -> bool:
    query = sa.text("SELECT 1 FROM pg_constraint WHERE conname = :name LIMIT 1")
    return bind.execute(query, {"name": name}).scalar() is not None


def upgrade() -> None:
    bind = op.get_bind()

    # Best-effort conversion from absolute endpoint handles {"x","y"} to node-relative
    # vectors {"dx","dy"} when the corresponding node pixel position is available.
    op.execute(
        """
        UPDATE label_edges e
        SET from_handle_px = jsonb_strip_nulls(
            jsonb_build_object(
                'dx', ((e.from_handle_px->>'x')::double precision - (n.point_px->>'x')::double precision),
                'dy', ((e.from_handle_px->>'y')::double precision - (n.point_px->>'y')::double precision),
                'mode', e.from_handle_px->>'mode'
            )
        )
        FROM label_nodes n
        WHERE e.from_handle_px IS NOT NULL
          AND (e.from_handle_px ? 'x')
          AND (e.from_handle_px ? 'y')
          AND NOT (e.from_handle_px ? 'dx')
          AND n.id = e.from_node_id
          AND n.label_id = e.label_id
          AND n.project_id = e.project_id
          AND n.project_layer_id = e.project_layer_id
          AND n.account_id = e.account_id
          AND n.point_px IS NOT NULL
          AND (n.point_px ? 'x')
          AND (n.point_px ? 'y');
        """
    )
    op.execute(
        """
        UPDATE label_edges e
        SET to_handle_px = jsonb_strip_nulls(
            jsonb_build_object(
                'dx', ((e.to_handle_px->>'x')::double precision - (n.point_px->>'x')::double precision),
                'dy', ((e.to_handle_px->>'y')::double precision - (n.point_px->>'y')::double precision),
                'mode', e.to_handle_px->>'mode'
            )
        )
        FROM label_nodes n
        WHERE e.to_handle_px IS NOT NULL
          AND (e.to_handle_px ? 'x')
          AND (e.to_handle_px ? 'y')
          AND NOT (e.to_handle_px ? 'dx')
          AND n.id = e.to_node_id
          AND n.label_id = e.label_id
          AND n.project_id = e.project_id
          AND n.project_layer_id = e.project_layer_id
          AND n.account_id = e.account_id
          AND n.point_px IS NOT NULL
          AND (n.point_px ? 'x')
          AND (n.point_px ? 'y');
        """
    )

    if _has_constraint(bind, "ck_label_edges_from_handle_shape"):
        op.drop_constraint("ck_label_edges_from_handle_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_to_handle_shape"):
        op.drop_constraint("ck_label_edges_to_handle_shape", "label_edges", type_="check")

    op.create_check_constraint(
        "ck_label_edges_from_handle_shape",
        "label_edges",
        """
        (from_handle_px IS NULL) OR
        (
            jsonb_typeof(from_handle_px) = 'object'
            AND (from_handle_px ? 'dx')
            AND (from_handle_px ? 'dy')
            AND jsonb_typeof(from_handle_px->'dx') = 'number'
            AND jsonb_typeof(from_handle_px->'dy') = 'number'
            AND ((NOT (from_handle_px ? 'mode')) OR jsonb_typeof(from_handle_px->'mode') = 'string')
        )
        """,
    )
    op.create_check_constraint(
        "ck_label_edges_to_handle_shape",
        "label_edges",
        """
        (to_handle_px IS NULL) OR
        (
            jsonb_typeof(to_handle_px) = 'object'
            AND (to_handle_px ? 'dx')
            AND (to_handle_px ? 'dy')
            AND jsonb_typeof(to_handle_px->'dx') = 'number'
            AND jsonb_typeof(to_handle_px->'dy') = 'number'
            AND ((NOT (to_handle_px ? 'mode')) OR jsonb_typeof(to_handle_px->'mode') = 'string')
        )
        """,
    )


def downgrade() -> None:
    bind = op.get_bind()

    if _has_constraint(bind, "ck_label_edges_from_handle_shape"):
        op.drop_constraint("ck_label_edges_from_handle_shape", "label_edges", type_="check")
    if _has_constraint(bind, "ck_label_edges_to_handle_shape"):
        op.drop_constraint("ck_label_edges_to_handle_shape", "label_edges", type_="check")

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
