from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from relevance.infrastructure.orm import Base, SchemaMigrationModel


LATEST_VERSION = "0001_initial"


class MigrationRunner:
    def __init__(self, engine) -> None:
        self._engine = engine

    def run(self) -> None:
        Base.metadata.create_all(self._engine)
        with self._engine.begin() as conn:
            try:
                existing = conn.execute(
                    select(SchemaMigrationModel).where(
                        SchemaMigrationModel.version == LATEST_VERSION
                    )
                ).first()
            except OperationalError:
                existing = None
            if not existing:
                conn.execute(
                    SchemaMigrationModel.__table__.insert().values(
                        version=LATEST_VERSION,
                        applied_at=datetime.now(timezone.utc),
                    )
                )
