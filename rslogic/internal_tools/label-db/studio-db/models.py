from __future__ import annotations

import enum
from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""


class TimestampMixin:
    """Common timestamps for row creation/update tracking."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class LayerSelectionMode(str, enum.Enum):
    LOCKED = "locked"
    MULTI = "multi"


class LayerSourceType(str, enum.Enum):
    XYZ = "xyz"
    IMAGE = "image"


class WorkOrderStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    BLOCKED = "blocked"


class WorkOrderSubmissionStatus(str, enum.Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    REJECTED = "rejected"


class LabelGeometryStyle(str, enum.Enum):
    POLYGON = "polygon"
    GRAPH = "graph"


class CurveType(str, enum.Enum):
    LINE = "line"
    QUADRATIC_BEZIER = "quadratic_bezier"
    CUBIC_BEZIER = "cubic_bezier"
    SPLINE = "spline"


class CoordinateSpace(str, enum.Enum):
    GEOGRAPHIC = "geographic"
    PIXEL = "pixel"


def enum_type(enum_cls: type[enum.Enum], *, name: str) -> Enum:
    return Enum(
        enum_cls,
        name=name,
        values_callable=lambda members: [member.value for member in members],
    )


class Account(TimestampMixin, Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )

    profiles: Mapped[list["Profile"]] = relationship(
        back_populates="account",
        cascade="all, delete-orphan",
    )
    projects: Mapped[list["Project"]] = relationship(back_populates="account")
    layers: Mapped[list["ProjectLayer"]] = relationship(back_populates="account")
    work_orders: Mapped[list["WorkOrder"]] = relationship(back_populates="account")
    labels: Mapped[list["Label"]] = relationship(back_populates="account")
    work_order_submissions: Mapped[list["WorkOrderSubmission"]] = relationship(
        back_populates="account"
    )


class Profile(TimestampMixin, Base):
    __tablename__ = "profiles"
    __table_args__ = (
        UniqueConstraint("account_id", "handle", name="uq_profiles_account_handle"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    handle: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str | None] = mapped_column(Text, nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    account: Mapped[Account] = relationship(back_populates="profiles")


class Project(TimestampMixin, Base):
    __tablename__ = "projects"
    __table_args__ = (
        UniqueConstraint("id", "account_id", name="uq_projects_id_account"),
        UniqueConstraint(
            "id",
            "account_id",
            "layer_source_type",
            name="uq_projects_id_account_source_type",
        ),
        CheckConstraint(
            "(coordinate_space != 'geographic') OR (area_of_interest IS NOT NULL AND area_of_interest_px IS NULL)",
            name="ck_projects_geographic_aoi",
        ),
        CheckConstraint(
            "(coordinate_space != 'pixel') OR (area_of_interest IS NULL AND area_of_interest_px IS NOT NULL)",
            name="ck_projects_pixel_aoi",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    area_of_interest: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326),
        nullable=True,
    )
    area_of_interest_px: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    coordinate_space: Mapped[CoordinateSpace] = mapped_column(
        enum_type(CoordinateSpace, name="coordinate_space"),
        nullable=False,
        server_default=CoordinateSpace.GEOGRAPHIC.value,
    )
    layer_selection_mode: Mapped[LayerSelectionMode] = mapped_column(
        enum_type(LayerSelectionMode, name="layer_selection_mode"),
        nullable=False,
        server_default=LayerSelectionMode.LOCKED.value,
    )
    layer_source_type: Mapped[LayerSourceType] = mapped_column(
        enum_type(LayerSourceType, name="layer_source_type"),
        nullable=False,
    )

    account: Mapped[Account] = relationship(back_populates="projects")
    created_by_profile: Mapped[Profile | None] = relationship()
    layers: Mapped[list["ProjectLayer"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )
    work_orders: Mapped[list["WorkOrder"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )
    labels: Mapped[list["Label"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )


class ProjectLayer(TimestampMixin, Base):
    __tablename__ = "project_layers"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "account_id", "source_type"],
            ["projects.id", "projects.account_id", "projects.layer_source_type"],
            ondelete="CASCADE",
            name="fk_project_layers_project_account_source_type",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "account_id",
            name="uq_project_layers_id_project_account",
        ),
        UniqueConstraint("project_id", "name", name="uq_project_layers_project_name"),
        CheckConstraint(
            "(source_type != 'xyz') OR (tile_url_template IS NOT NULL)",
            name="ck_project_layers_xyz_requires_template",
        ),
        CheckConstraint(
            "(source_type = 'xyz') OR (tile_url_template IS NULL)",
            name="ck_project_layers_non_xyz_no_template",
        ),
        CheckConstraint(
            "(source_type = 'xyz') OR (min_zoom IS NULL AND max_zoom IS NULL)",
            name="ck_project_layers_non_xyz_no_zoom",
        ),
        CheckConstraint(
            "(source_type != 'image') OR (image_asset_id IS NOT NULL)",
            name="ck_project_layers_image_requires_asset",
        ),
        CheckConstraint(
            "(image_url IS NULL) OR (image_url ~* '^(s3://|file://).+')",
            name="ck_project_layers_image_url_scheme",
        ),
        CheckConstraint(
            "(source_type = 'image') OR (image_asset_id IS NULL)",
            name="ck_project_layers_non_image_no_asset",
        ),
        CheckConstraint(
            "(source_type = 'image') OR (image_footprint IS NULL)",
            name="ck_project_layers_non_image_no_footprint",
        ),
        CheckConstraint(
            "(min_zoom IS NULL OR max_zoom IS NULL) OR (min_zoom <= max_zoom)",
            name="ck_project_layers_zoom_range",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[LayerSourceType] = mapped_column(
        enum_type(LayerSourceType, name="layer_source_type"),
        nullable=False,
    )
    is_default: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    sort_order: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )

    tile_url_template: Mapped[str | None] = mapped_column(Text, nullable=True)
    min_zoom: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_zoom: Mapped[int | None] = mapped_column(Integer, nullable=True)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    image_asset_id: Mapped[str | None] = mapped_column(
        ForeignKey("image_assets.id", ondelete="SET NULL"),
        nullable=True,
    )
    image_footprint: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326),
        nullable=True,
    )
    capture_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    attribution: Mapped[str | None] = mapped_column(Text, nullable=True)

    account: Mapped[Account] = relationship(back_populates="layers")
    created_by_profile: Mapped[Profile | None] = relationship()
    project: Mapped[Project] = relationship(back_populates="layers")
    image_asset: Mapped["RsLogicImageAsset | None"] = relationship(back_populates="project_layers")
    work_orders: Mapped[list["WorkOrder"]] = relationship(
        back_populates="layer",
        cascade="all, delete-orphan",
    )
    labels: Mapped[list["Label"]] = relationship(
        back_populates="layer",
        cascade="all, delete-orphan",
    )


class WorkOrder(TimestampMixin, Base):
    __tablename__ = "work_orders"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "account_id"],
            ["projects.id", "projects.account_id"],
            ondelete="CASCADE",
            name="fk_work_orders_project_account",
        ),
        ForeignKeyConstraint(
            ["project_layer_id", "project_id", "account_id"],
            ["project_layers.id", "project_layers.project_id", "project_layers.account_id"],
            ondelete="CASCADE",
            name="fk_work_orders_layer_project_account",
        ),
        UniqueConstraint(
            "project_layer_id",
            "z",
            "x",
            "y",
            name="uq_work_orders_layer_zxy",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "project_layer_id",
            "account_id",
            name="uq_work_orders_id_scope",
        ),
        CheckConstraint("z >= 0", name="ck_work_orders_z_non_negative"),
        CheckConstraint("x >= 0", name="ck_work_orders_x_non_negative"),
        CheckConstraint("y >= 0", name="ck_work_orders_y_non_negative"),
        CheckConstraint("x < power(2, z)", name="ck_work_orders_x_within_zoom"),
        CheckConstraint("y < power(2, z)", name="ck_work_orders_y_within_zoom"),
        CheckConstraint("required_submissions >= 1", name="ck_work_orders_required_submissions"),
        CheckConstraint(
            "(status != 'completed') OR (completed_at IS NOT NULL)",
            name="ck_work_orders_completed_has_timestamp",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(nullable=False)
    project_layer_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    z: Mapped[int] = mapped_column(Integer, nullable=False)
    x: Mapped[int] = mapped_column(Integer, nullable=False)
    y: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[WorkOrderStatus] = mapped_column(
        enum_type(WorkOrderStatus, name="work_order_status"),
        nullable=False,
        server_default=WorkOrderStatus.PENDING.value,
    )
    required_submissions: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completion_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    account: Mapped[Account] = relationship(back_populates="work_orders")
    created_by_profile: Mapped[Profile | None] = relationship()
    project: Mapped[Project] = relationship(back_populates="work_orders")
    layer: Mapped[ProjectLayer] = relationship(back_populates="work_orders")
    submissions: Mapped[list["WorkOrderSubmission"]] = relationship(
        back_populates="work_order",
        cascade="all, delete-orphan",
    )
    label_links: Mapped[list["WorkOrderLabel"]] = relationship(
        back_populates="work_order",
        cascade="all, delete-orphan",
    )


class WorkOrderSubmission(TimestampMixin, Base):
    __tablename__ = "work_order_submissions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["work_order_id", "project_id", "project_layer_id", "account_id"],
            [
                "work_orders.id",
                "work_orders.project_id",
                "work_orders.project_layer_id",
                "work_orders.account_id",
            ],
            ondelete="CASCADE",
            name="fk_work_order_submissions_work_order_scope",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "project_layer_id",
            "account_id",
            name="uq_work_order_submissions_id_scope",
        ),
        UniqueConstraint(
            "work_order_id",
            "submitted_by_profile_id",
            "submission_round",
            name="uq_work_order_submissions_actor_round",
        ),
        CheckConstraint("submission_round >= 1", name="ck_work_order_submissions_round"),
        CheckConstraint(
            "(status != 'completed') OR (completed_at IS NOT NULL)",
            name="ck_work_order_submissions_completed_has_timestamp",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    work_order_id: Mapped[int] = mapped_column(nullable=False)
    project_id: Mapped[int] = mapped_column(nullable=False)
    project_layer_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    submitted_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    submission_round: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
    )
    status: Mapped[WorkOrderSubmissionStatus] = mapped_column(
        enum_type(WorkOrderSubmissionStatus, name="work_order_submission_status"),
        nullable=False,
        server_default=WorkOrderSubmissionStatus.IN_PROGRESS.value,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    account: Mapped[Account] = relationship(back_populates="work_order_submissions")
    submitted_by_profile: Mapped[Profile | None] = relationship()
    work_order: Mapped[WorkOrder] = relationship(back_populates="submissions")
    labels: Mapped[list["Label"]] = relationship(back_populates="origin_submission")


class Label(TimestampMixin, Base):
    __tablename__ = "labels"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "account_id"],
            ["projects.id", "projects.account_id"],
            ondelete="CASCADE",
            name="fk_labels_project_account",
        ),
        ForeignKeyConstraint(
            ["project_layer_id", "project_id", "account_id"],
            ["project_layers.id", "project_layers.project_id", "project_layers.account_id"],
            ondelete="CASCADE",
            name="fk_labels_layer_project_account",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "project_layer_id",
            "account_id",
            name="uq_labels_id_scope",
        ),
        CheckConstraint(
            "(style != 'polygon') OR (geometry IS NOT NULL OR geometry_px IS NOT NULL)",
            name="ck_labels_polygon_requires_geometry",
        ),
        CheckConstraint(
            "(geometry IS NULL) OR (geometry_px IS NULL)",
            name="ck_labels_single_geometry_space",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(nullable=False)
    project_layer_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    origin_submission_id: Mapped[int | None] = mapped_column(
        ForeignKey("work_order_submissions.id", ondelete="SET NULL"),
        nullable=True,
    )
    style: Mapped[LabelGeometryStyle] = mapped_column(
        enum_type(LabelGeometryStyle, name="label_geometry_style"),
        nullable=False,
        server_default=LabelGeometryStyle.POLYGON.value,
    )
    geometry: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326),
        nullable=True,
    )
    geometry_px: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    label_class: Mapped[str] = mapped_column(Text, nullable=False)
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    account: Mapped[Account] = relationship(back_populates="labels")
    created_by_profile: Mapped[Profile | None] = relationship()
    origin_submission: Mapped[WorkOrderSubmission | None] = relationship(back_populates="labels")
    project: Mapped[Project] = relationship(back_populates="labels")
    layer: Mapped[ProjectLayer] = relationship(back_populates="labels")
    nodes: Mapped[list["LabelNode"]] = relationship(
        back_populates="label",
        cascade="all, delete-orphan",
    )
    edges: Mapped[list["LabelEdge"]] = relationship(
        back_populates="label",
        cascade="all, delete-orphan",
    )
    work_order_links: Mapped[list["WorkOrderLabel"]] = relationship(
        back_populates="label",
        cascade="all, delete-orphan",
    )


class LabelNode(TimestampMixin, Base):
    __tablename__ = "label_nodes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["label_id", "project_id", "project_layer_id", "account_id"],
            ["labels.id", "labels.project_id", "labels.project_layer_id", "labels.account_id"],
            ondelete="CASCADE",
            name="fk_label_nodes_label_scope",
        ),
        UniqueConstraint(
            "id",
            "label_id",
            "project_id",
            "project_layer_id",
            "account_id",
            name="uq_label_nodes_id_scope",
        ),
        CheckConstraint(
            "width_px > 0",
            name="ck_label_nodes_width_px_positive",
        ),
        CheckConstraint(
            "(point IS NOT NULL) OR (point_px IS NOT NULL)",
            name="ck_label_nodes_point_required",
        ),
        CheckConstraint(
            "(point IS NULL) OR (point_px IS NULL)",
            name="ck_label_nodes_single_point_space",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    label_id: Mapped[int] = mapped_column(nullable=False)
    project_id: Mapped[int] = mapped_column(nullable=False)
    project_layer_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    point: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="POINT", srid=4326),
        nullable=True,
    )
    point_px: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    width_px: Mapped[float] = mapped_column(Float, nullable=False)
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    label: Mapped[Label] = relationship(back_populates="nodes")
    created_by_profile: Mapped[Profile | None] = relationship()


class LabelEdge(TimestampMixin, Base):
    __tablename__ = "label_edges"
    __table_args__ = (
        ForeignKeyConstraint(
            ["label_id", "project_id", "project_layer_id", "account_id"],
            ["labels.id", "labels.project_id", "labels.project_layer_id", "labels.account_id"],
            ondelete="CASCADE",
            name="fk_label_edges_label_scope",
        ),
        ForeignKeyConstraint(
            ["from_node_id", "label_id", "project_id", "project_layer_id", "account_id"],
            [
                "label_nodes.id",
                "label_nodes.label_id",
                "label_nodes.project_id",
                "label_nodes.project_layer_id",
                "label_nodes.account_id",
            ],
            ondelete="CASCADE",
            name="fk_label_edges_from_node_scope",
        ),
        ForeignKeyConstraint(
            ["to_node_id", "label_id", "project_id", "project_layer_id", "account_id"],
            [
                "label_nodes.id",
                "label_nodes.label_id",
                "label_nodes.project_id",
                "label_nodes.project_layer_id",
                "label_nodes.account_id",
            ],
            ondelete="CASCADE",
            name="fk_label_edges_to_node_scope",
        ),
        UniqueConstraint(
            "label_id",
            "from_node_id",
            "to_node_id",
            name="uq_label_edges_label_nodes",
        ),
        CheckConstraint(
            "from_node_id != to_node_id",
            name="ck_label_edges_distinct_nodes",
        ),
        CheckConstraint(
            "(geometry IS NULL) OR (geometry_px IS NULL)",
            name="ck_label_edges_single_geometry_space",
        ),
        CheckConstraint(
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
            name="ck_label_edges_from_handle_shape",
        ),
        CheckConstraint(
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
            name="ck_label_edges_to_handle_shape",
        ),
        CheckConstraint(
            "(controls_px IS NULL) OR (jsonb_typeof(controls_px) = 'array')",
            name="ck_label_edges_controls_shape",
        ),
        CheckConstraint(
            "(curve_type != 'line') OR (from_handle_px IS NULL AND to_handle_px IS NULL AND controls_px IS NULL)",
            name="ck_label_edges_line_no_controls",
        ),
        CheckConstraint(
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
            name="ck_label_edges_quadratic_controls",
        ),
        CheckConstraint(
            "(curve_type != 'cubic_bezier') OR (from_handle_px IS NOT NULL AND to_handle_px IS NOT NULL AND controls_px IS NULL)",
            name="ck_label_edges_cubic_controls",
        ),
        CheckConstraint(
            "(curve_type != 'spline') OR (controls_px IS NOT NULL AND jsonb_array_length(controls_px) >= 2)",
            name="ck_label_edges_spline_controls",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    label_id: Mapped[int] = mapped_column(nullable=False)
    project_id: Mapped[int] = mapped_column(nullable=False)
    project_layer_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profiles.id", ondelete="SET NULL"),
        nullable=True,
    )
    from_node_id: Mapped[int] = mapped_column(nullable=False)
    to_node_id: Mapped[int] = mapped_column(nullable=False)
    curve_type: Mapped[CurveType] = mapped_column(
        enum_type(CurveType, name="curve_type"),
        nullable=False,
        server_default=CurveType.LINE.value,
    )
    geometry: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="LINESTRING", srid=4326),
        nullable=True,
    )
    # Pixel-space endpoint handles relative to their node position.
    # Payload shape: {"dx": 12.3, "dy": -8.7, "mode": "free|aligned|mirrored|vector"}.
    from_handle_px: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    to_handle_px: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    # Additional pixel-space controls for spline or optional explicit quadratic control.
    # These are absolute pixel points; endpoint handles above are node-relative vectors.
    # Payload shape: [{"x": 10.0, "y": 20.0}, {"x": 15.0, "y": 22.0}, ...].
    controls_px: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)
    geometry_px: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    label: Mapped[Label] = relationship(back_populates="edges")
    created_by_profile: Mapped[Profile | None] = relationship()
    from_node: Mapped[LabelNode] = relationship(foreign_keys=[from_node_id])
    to_node: Mapped[LabelNode] = relationship(foreign_keys=[to_node_id])


class WorkOrderLabel(Base):
    __tablename__ = "work_order_labels"
    __table_args__ = (
        PrimaryKeyConstraint(
            "work_order_id",
            "label_id",
            name="pk_work_order_labels",
        ),
        ForeignKeyConstraint(
            ["work_order_id", "project_id", "project_layer_id", "account_id"],
            [
                "work_orders.id",
                "work_orders.project_id",
                "work_orders.project_layer_id",
                "work_orders.account_id",
            ],
            ondelete="CASCADE",
            name="fk_work_order_labels_work_order",
        ),
        ForeignKeyConstraint(
            ["label_id", "project_id", "project_layer_id", "account_id"],
            ["labels.id", "labels.project_id", "labels.project_layer_id", "labels.account_id"],
            ondelete="CASCADE",
            name="fk_work_order_labels_label",
        ),
        CheckConstraint(
            "overlap_area_m2 IS NULL OR overlap_area_m2 >= 0",
            name="ck_work_order_labels_overlap_non_negative",
        ),
    )

    work_order_id: Mapped[int] = mapped_column(nullable=False)
    label_id: Mapped[int] = mapped_column(nullable=False)
    project_id: Mapped[int] = mapped_column(nullable=False)
    project_layer_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(nullable=False)
    intersection_geom: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326),
        nullable=True,
    )
    overlap_area_m2: Mapped[float | None] = mapped_column(Float, nullable=True)

    work_order: Mapped[WorkOrder] = relationship(back_populates="label_links")
    label: Mapped[Label] = relationship(back_populates="work_order_links")


class RsLogicImageAsset(TimestampMixin, Base):
    __tablename__ = "image_assets"
    __table_args__ = (
        CheckConstraint("(uri ~* '^(s3://|file://).+')", name="ck_image_assets_uri_scheme"),
        CheckConstraint("latitude IS NULL OR (latitude BETWEEN -90 AND 90)", name="ck_image_assets_latitude_range"),
        CheckConstraint("longitude IS NULL OR (longitude BETWEEN -180 AND 180)", name="ck_image_assets_longitude_range"),
        UniqueConstraint("uri", name="uq_image_assets_uri"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    uri: Mapped[str] = mapped_column(String(2048), nullable=False)
    location: Mapped[object | None] = mapped_column(
        Geometry(geometry_type="POINT", srid=4326),
        nullable=True,
    )
    bucket_name: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    object_key: Mapped[str | None] = mapped_column(String(1024), index=True, nullable=True)
    filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sha256: Mapped[str | None] = mapped_column(String(128), nullable=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    captured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    altitude_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    drone_model: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    camera_make: Mapped[str | None] = mapped_column(String(255), nullable=True)
    camera_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    focal_length_mm: Mapped[float | None] = mapped_column(Float, nullable=True)
    image_width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    image_height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    software: Mapped[str | None] = mapped_column(String(255), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    extra: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))

    project_layers: Mapped[list[ProjectLayer]] = relationship(back_populates="image_asset")
    group_items: Mapped[list["ImageGroupItem"]] = relationship(
        back_populates="image",
        cascade="all, delete-orphan",
    )


class ImageGroup(TimestampMixin, Base):
    __tablename__ = "image_groups"
    __table_args__ = (
        UniqueConstraint("name", name="uq_image_groups_name"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    extra: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))

    image_items: Mapped[list["ImageGroupItem"]] = relationship(
        back_populates="group",
        cascade="all, delete-orphan",
    )
    processing_jobs: Mapped[list["RsLogicProcessingJob"]] = relationship(back_populates="image_group")


class ImageGroupItem(Base):
    __tablename__ = "image_group_items"
    __table_args__ = (
        PrimaryKeyConstraint(
            "group_id",
            "image_id",
            name="pk_image_group_items",
        ),
        ForeignKeyConstraint(
            ["group_id"],
            ["image_groups.id"],
            ondelete="CASCADE",
            name="fk_image_group_items_group",
        ),
        ForeignKeyConstraint(
            ["image_id"],
            ["image_assets.id"],
            ondelete="CASCADE",
            name="fk_image_group_items_image",
        ),
    )

    group_id: Mapped[str] = mapped_column(String(36), nullable=False)
    image_id: Mapped[str] = mapped_column(String(36), nullable=False)
    role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    group: Mapped[ImageGroup] = relationship(back_populates="image_items")
    image: Mapped[RsLogicImageAsset] = relationship(back_populates="group_items")


class RsLogicProcessingJob(TimestampMixin, Base):
    __tablename__ = "processing_jobs"
    __table_args__ = (
        CheckConstraint("progress >= 0 AND progress <= 100", name="ck_processing_jobs_progress_range"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    image_group_id: Mapped[str | None] = mapped_column(
        ForeignKey("image_groups.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(String(50), index=True)
    progress: Mapped[float] = mapped_column(Float, default=0.0)
    filters: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_summary: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    image_group: Mapped[ImageGroup | None] = relationship(back_populates="processing_jobs")


ImageAsset = RsLogicImageAsset
ProcessingJob = RsLogicProcessingJob


Index("ix_accounts_name", Account.name)
Index("ix_profiles_account_id", Profile.account_id)
Index("ix_profiles_deleted_at", Profile.deleted_at)
Index("ix_projects_account_id", Project.account_id)
Index("ix_projects_coordinate_space", Project.coordinate_space)
Index("ix_projects_area_of_interest_gist", Project.area_of_interest, postgresql_using="gist")
Index("ix_project_layers_project_id", ProjectLayer.project_id)
Index("ix_project_layers_account_id", ProjectLayer.account_id)
Index("ix_project_layers_source_type", ProjectLayer.source_type)
Index("ix_project_layers_image_asset_id", ProjectLayer.image_asset_id)
Index(
    "ix_project_layers_image_footprint_gist",
    ProjectLayer.image_footprint,
    postgresql_using="gist",
)
Index(
    "ix_project_layers_source_metadata_gin",
    ProjectLayer.source_metadata,
    postgresql_using="gin",
)
Index(
    "ux_project_layers_default_per_project",
    ProjectLayer.project_id,
    unique=True,
    postgresql_where=text("is_default"),
)
Index("ix_work_orders_project_id", WorkOrder.project_id)
Index("ix_work_orders_project_layer_id", WorkOrder.project_layer_id)
Index("ix_work_orders_account_id", WorkOrder.account_id)
Index("ix_work_orders_status", WorkOrder.status)
Index("ix_work_order_submissions_work_order_id", WorkOrderSubmission.work_order_id)
Index("ix_work_order_submissions_account_id", WorkOrderSubmission.account_id)
Index("ix_work_order_submissions_status", WorkOrderSubmission.status)
Index(
    "ix_work_order_submissions_submitted_by_profile_id",
    WorkOrderSubmission.submitted_by_profile_id,
)
Index("ix_labels_project_id", Label.project_id)
Index("ix_labels_project_layer_id", Label.project_layer_id)
Index("ix_labels_account_id", Label.account_id)
Index("ix_labels_created_by_profile_id", Label.created_by_profile_id)
Index("ix_labels_origin_submission_id", Label.origin_submission_id)
Index("ix_labels_style", Label.style)
Index("ix_labels_geometry_gist", Label.geometry, postgresql_using="gist")
Index("ix_labels_geometry_px_gin", Label.geometry_px, postgresql_using="gin")
Index("ix_label_nodes_label_id", LabelNode.label_id)
Index("ix_label_nodes_account_id", LabelNode.account_id)
Index("ix_label_nodes_created_by_profile_id", LabelNode.created_by_profile_id)
Index("ix_label_nodes_point_gist", LabelNode.point, postgresql_using="gist")
Index("ix_label_nodes_point_px_gin", LabelNode.point_px, postgresql_using="gin")
Index("ix_label_edges_label_id", LabelEdge.label_id)
Index("ix_label_edges_account_id", LabelEdge.account_id)
Index("ix_label_edges_created_by_profile_id", LabelEdge.created_by_profile_id)
Index("ix_label_edges_from_node_id", LabelEdge.from_node_id)
Index("ix_label_edges_to_node_id", LabelEdge.to_node_id)
Index("ix_label_edges_curve_type", LabelEdge.curve_type)
Index("ix_label_edges_geometry_gist", LabelEdge.geometry, postgresql_using="gist")
Index("ix_label_edges_geometry_px_gin", LabelEdge.geometry_px, postgresql_using="gin")
Index("ix_work_order_labels_work_order_id", WorkOrderLabel.work_order_id)
Index("ix_work_order_labels_label_id", WorkOrderLabel.label_id)
Index("ix_work_order_labels_account_id", WorkOrderLabel.account_id)
Index(
    "ix_work_order_labels_intersection_geom_gist",
    WorkOrderLabel.intersection_geom,
    postgresql_using="gist",
)
Index("ix_rslogic_image_assets_bucket_name", RsLogicImageAsset.bucket_name)
Index("ix_rslogic_image_assets_object_key", RsLogicImageAsset.object_key)
Index("ix_rslogic_image_assets_uri", RsLogicImageAsset.uri)
Index("ix_rslogic_image_assets_location_gist", RsLogicImageAsset.location, postgresql_using="gist")
Index("ix_rslogic_image_assets_drone_model", RsLogicImageAsset.drone_model)
Index("ix_rslogic_image_assets_created_at", RsLogicImageAsset.created_at)
Index("ix_image_group_items_image_id", ImageGroupItem.image_id)
Index("ix_rslogic_processing_jobs_image_group_id", RsLogicProcessingJob.image_group_id)
Index("ix_rslogic_processing_jobs_status", RsLogicProcessingJob.status)
