from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker


@dataclass(frozen=True)
class DatabaseConfig:
    url: str
    echo: bool = False


class Database:
    def __init__(self, config: DatabaseConfig) -> None:
        self._engine = create_engine(config.url, echo=config.echo, future=True)
        @event.listens_for(self._engine, "connect")
        def _configure_sqlite(dbapi_connection, connection_record):
            try:
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                cursor.execute("PRAGMA temp_store=MEMORY;")
                cursor.close()
            except Exception:
                pass
        self._session_factory = sessionmaker(self._engine, expire_on_commit=False)

    @property
    def engine(self):
        return self._engine

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
