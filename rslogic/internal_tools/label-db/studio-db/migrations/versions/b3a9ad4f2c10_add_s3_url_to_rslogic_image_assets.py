"""add s3_url to rslogic image assets"""

from alembic import op
from sqlalchemy import Column, String, inspect

# revision identifiers, used by Alembic.
revision = "b3a9ad4f2c10"
down_revision = "a2d4f6e9b8c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    if not inspector.has_table("image_assets"):
        return

    columns = {column["name"] for column in inspector.get_columns("image_assets")}
    if "s3_url" not in columns:
        op.add_column(
            "image_assets",
            Column("s3_url", String(2048), nullable=True),
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    if not inspector.has_table("image_assets"):
        return

    columns = {column["name"] for column in inspector.get_columns("image_assets")}
    if "s3_url" in columns:
        op.drop_column("image_assets", "s3_url")
