"""Create all database tables from SQLAlchemy models."""

from __future__ import annotations

from rslogic.config import CONFIG
from sqlalchemy import create_engine
import studio_db  # noqa: F401 - ensures all models are registered on Base
from studio_db import Base


def main() -> None:
    engine = create_engine(CONFIG.label_db.database_url)
    Base.metadata.create_all(engine)
    print("Database tables created.")
