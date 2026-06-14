"""add pixel-space aoi labels

Revision ID: 7423fd1523d5
Revises: b7912b7a0663
Create Date: 2026-02-06 20:34:33.016073
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '7423fd1523d5'
down_revision = 'b7912b7a0663'
branch_labels = None
depends_on = None


def _has_constraint(bind, name: str) -> bool:
    query = sa.text(
        "SELECT 1 FROM pg_constraint WHERE conname = :name LIMIT 1"
    )
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
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'coordinate_space') THEN
                CREATE TYPE coordinate_space AS ENUM ('geographic', 'pixel');
            END IF;
        END
        $$;
        """
    )

    project_columns = _column_names(bind, "projects")
    if "area_of_interest_px" not in project_columns:
        op.add_column("projects", sa.Column("area_of_interest_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if "coordinate_space" not in project_columns:
        op.add_column(
            "projects",
            sa.Column(
                "coordinate_space",
                sa.Enum("geographic", "pixel", name="coordinate_space"),
                server_default="geographic",
                nullable=False,
            ),
        )
    op.alter_column("projects", "area_of_interest", nullable=True)
    op.alter_column("projects", "coordinate_space", server_default=sa.text("'geographic'::coordinate_space"))

    if _has_constraint(bind, "ck_projects_geographic_aoi"):
        op.drop_constraint("ck_projects_geographic_aoi", "projects", type_="check")
    if _has_constraint(bind, "ck_projects_pixel_aoi"):
        op.drop_constraint("ck_projects_pixel_aoi", "projects", type_="check")
    op.create_check_constraint(
        "ck_projects_geographic_aoi",
        "projects",
        "(coordinate_space != 'geographic') OR (area_of_interest IS NOT NULL AND area_of_interest_px IS NULL)",
    )
    op.create_check_constraint(
        "ck_projects_pixel_aoi",
        "projects",
        "(coordinate_space != 'pixel') OR (area_of_interest IS NULL AND area_of_interest_px IS NOT NULL)",
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_projects_coordinate_space ON projects (coordinate_space)")

    label_columns = _column_names(bind, "labels")
    if "geometry_px" not in label_columns:
        op.add_column("labels", sa.Column("geometry_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if _has_constraint(bind, "ck_labels_polygon_requires_geometry"):
        op.drop_constraint("ck_labels_polygon_requires_geometry", "labels", type_="check")
    if _has_constraint(bind, "ck_labels_single_geometry_space"):
        op.drop_constraint("ck_labels_single_geometry_space", "labels", type_="check")
    op.create_check_constraint(
        "ck_labels_polygon_requires_geometry",
        "labels",
        "(style != 'polygon') OR (geometry IS NOT NULL OR geometry_px IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_labels_single_geometry_space",
        "labels",
        "(geometry IS NULL) OR (geometry_px IS NULL)",
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_labels_geometry_px_gin ON labels USING gin (geometry_px)")

    label_node_columns = _column_names(bind, "label_nodes")
    if "point_px" not in label_node_columns:
        op.add_column("label_nodes", sa.Column("point_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.alter_column("label_nodes", "point", nullable=True)
    if _has_constraint(bind, "ck_label_nodes_point_required"):
        op.drop_constraint("ck_label_nodes_point_required", "label_nodes", type_="check")
    if _has_constraint(bind, "ck_label_nodes_single_point_space"):
        op.drop_constraint("ck_label_nodes_single_point_space", "label_nodes", type_="check")
    op.create_check_constraint(
        "ck_label_nodes_point_required",
        "label_nodes",
        "(point IS NOT NULL) OR (point_px IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_label_nodes_single_point_space",
        "label_nodes",
        "(point IS NULL) OR (point_px IS NULL)",
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_label_nodes_point_px_gin ON label_nodes USING gin (point_px)")

    label_edge_columns = _column_names(bind, "label_edges")
    if "geometry_px" not in label_edge_columns:
        op.add_column("label_edges", sa.Column("geometry_px", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if _has_constraint(bind, "ck_label_edges_single_geometry_space"):
        op.drop_constraint("ck_label_edges_single_geometry_space", "label_edges", type_="check")
    op.create_check_constraint(
        "ck_label_edges_single_geometry_space",
        "label_edges",
        "(geometry IS NULL) OR (geometry_px IS NULL)",
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_label_edges_geometry_px_gin ON label_edges USING gin (geometry_px)")


def downgrade() -> None:
    bind = op.get_bind()

    pixel_projects = bind.execute(
        sa.text("SELECT count(*) FROM projects WHERE coordinate_space = 'pixel' OR area_of_interest IS NULL")
    ).scalar_one()
    if pixel_projects > 0:
        raise RuntimeError("Cannot downgrade with pixel-space projects present.")

    pixel_labels = bind.execute(
        sa.text("SELECT count(*) FROM labels WHERE geometry_px IS NOT NULL")
    ).scalar_one()
    if pixel_labels > 0:
        raise RuntimeError("Cannot downgrade with pixel-space labels present.")

    pixel_nodes = bind.execute(
        sa.text("SELECT count(*) FROM label_nodes WHERE point_px IS NOT NULL OR point IS NULL")
    ).scalar_one()
    if pixel_nodes > 0:
        raise RuntimeError("Cannot downgrade with pixel-space label nodes present.")

    pixel_edges = bind.execute(
        sa.text("SELECT count(*) FROM label_edges WHERE geometry_px IS NOT NULL")
    ).scalar_one()
    if pixel_edges > 0:
        raise RuntimeError("Cannot downgrade with pixel-space label edges present.")

    op.execute("DROP INDEX IF EXISTS ix_label_edges_geometry_px_gin")
    if _has_constraint(bind, "ck_label_edges_single_geometry_space"):
        op.drop_constraint("ck_label_edges_single_geometry_space", "label_edges", type_="check")
    if "geometry_px" in _column_names(bind, "label_edges"):
        op.drop_column("label_edges", "geometry_px")

    op.execute("DROP INDEX IF EXISTS ix_label_nodes_point_px_gin")
    if _has_constraint(bind, "ck_label_nodes_single_point_space"):
        op.drop_constraint("ck_label_nodes_single_point_space", "label_nodes", type_="check")
    if _has_constraint(bind, "ck_label_nodes_point_required"):
        op.drop_constraint("ck_label_nodes_point_required", "label_nodes", type_="check")
    if "point_px" in _column_names(bind, "label_nodes"):
        op.drop_column("label_nodes", "point_px")
    op.alter_column("label_nodes", "point", nullable=False)

    op.execute("DROP INDEX IF EXISTS ix_labels_geometry_px_gin")
    if _has_constraint(bind, "ck_labels_single_geometry_space"):
        op.drop_constraint("ck_labels_single_geometry_space", "labels", type_="check")
    if _has_constraint(bind, "ck_labels_polygon_requires_geometry"):
        op.drop_constraint("ck_labels_polygon_requires_geometry", "labels", type_="check")
    op.create_check_constraint(
        "ck_labels_polygon_requires_geometry",
        "labels",
        "(style != 'polygon') OR (geometry IS NOT NULL)",
    )
    if "geometry_px" in _column_names(bind, "labels"):
        op.drop_column("labels", "geometry_px")

    op.execute("DROP INDEX IF EXISTS ix_projects_coordinate_space")
    if _has_constraint(bind, "ck_projects_pixel_aoi"):
        op.drop_constraint("ck_projects_pixel_aoi", "projects", type_="check")
    if _has_constraint(bind, "ck_projects_geographic_aoi"):
        op.drop_constraint("ck_projects_geographic_aoi", "projects", type_="check")
    if "coordinate_space" in _column_names(bind, "projects"):
        op.drop_column("projects", "coordinate_space")
    if "area_of_interest_px" in _column_names(bind, "projects"):
        op.drop_column("projects", "area_of_interest_px")
    op.alter_column("projects", "area_of_interest", nullable=False)

    op.execute("DROP TYPE IF EXISTS coordinate_space")
