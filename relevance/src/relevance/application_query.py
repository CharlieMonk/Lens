from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from relevance.infrastructure_orm import (
    AgencyModel,
    AggregateCfrCountModel,
    CitationModel,
    DocumentCitationModel,
    DocumentModel,
)


class QueryService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_agencies(self) -> list[AgencyModel]:
        return list(self._session.execute(select(AgencyModel)).scalars())

    def top_cfr(
        self,
        limit: int,
        agency_id: int | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[tuple[str, int, int]]:
        stmt = select(
            AggregateCfrCountModel.normalized_citation,
            func.sum(AggregateCfrCountModel.document_count),
            func.sum(AggregateCfrCountModel.occurrence_count),
        )
        if agency_id is not None:
            stmt = stmt.where(AggregateCfrCountModel.agency_id == agency_id)
        if since is not None:
            stmt = stmt.where(AggregateCfrCountModel.period_start >= since)
        if until is not None:
            stmt = stmt.where(AggregateCfrCountModel.period_start <= until)
        stmt = (
            stmt.group_by(AggregateCfrCountModel.normalized_citation)
            .order_by(func.sum(AggregateCfrCountModel.occurrence_count).desc())
            .limit(limit)
        )
        return list(self._session.execute(stmt).all())

    def trend(
        self, normalized: str, granularity: str, agency_id: int | None = None
    ) -> list[tuple[datetime, int]]:
        stmt = select(
            AggregateCfrCountModel.period_start,
            func.sum(AggregateCfrCountModel.occurrence_count),
        ).where(AggregateCfrCountModel.normalized_citation == normalized)
        if agency_id is not None:
            stmt = stmt.where(AggregateCfrCountModel.agency_id == agency_id)
        stmt = stmt.group_by(AggregateCfrCountModel.period_start).order_by(
            AggregateCfrCountModel.period_start
        )
        return list(self._session.execute(stmt).all())

    def documents_for_cfr(
        self, normalized: str, limit: int
    ) -> list[tuple[str, str, datetime]]:
        stmt = (
            select(DocumentModel.title, DocumentModel.url, DocumentModel.published_at)
            .join(DocumentCitationModel, DocumentModel.id == DocumentCitationModel.document_id)
            .join(CitationModel, CitationModel.id == DocumentCitationModel.citation_id)
            .where(CitationModel.normalized == normalized)
            .order_by(DocumentModel.published_at.desc())
            .limit(limit)
        )
        return list(self._session.execute(stmt).all())
