from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from relevance.domain import models
from relevance.infrastructure.orm import (
    AgencyModel,
    AggregateCfrCountModel,
    CitationModel,
    DocumentCitationModel,
    DocumentModel,
    SourceModel,
)


class AgencyRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_name(self, name: str) -> AgencyModel | None:
        return self._session.execute(
            select(AgencyModel).where(AgencyModel.name == name)
        ).scalar_one_or_none()

    def list_all(self) -> list[AgencyModel]:
        return list(self._session.execute(select(AgencyModel)).scalars())

    def add(self, agency: models.Agency) -> AgencyModel:
        row = AgencyModel(name=agency.name, aliases=agency.aliases)
        self._session.add(row)
        self._session.flush()
        return row


class SourceRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, source: models.Source) -> SourceModel:
        row = SourceModel(
            agency_id=source.agency_id,
            source_type=source.source_type.value,
            base_url=source.base_url,
            config_json=source.config_json,
        )
        self._session.add(row)
        self._session.flush()
        return row

    def list_by_agency(self, agency_id: int) -> list[SourceModel]:
        return list(
            self._session.execute(
                select(SourceModel).where(SourceModel.agency_id == agency_id)
            ).scalars()
        )

    def list_all(self) -> list[SourceModel]:
        return list(self._session.execute(select(SourceModel)).scalars())


class DocumentRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_url(self, url: str) -> DocumentModel | None:
        return self._session.execute(
            select(DocumentModel).where(DocumentModel.url == url)
        ).scalar_one_or_none()

    def add(self, document: models.Document) -> DocumentModel:
        row = DocumentModel(
            source_id=document.source_id,
            agency_id=document.agency_id,
            title=document.title,
            url=document.url,
            published_at=document.published_at,
            retrieved_at=document.retrieved_at,
            raw_html=document.raw_html,
            text=document.text,
            content_hash=document.content_hash,
        )
        self._session.add(row)
        self._session.flush()
        return row

    def update(self, existing: DocumentModel, document: models.Document) -> DocumentModel:
        existing.title = document.title
        existing.published_at = document.published_at
        existing.retrieved_at = document.retrieved_at
        existing.raw_html = document.raw_html
        existing.text = document.text
        existing.content_hash = document.content_hash
        self._session.add(existing)
        self._session.flush()
        return existing

    def list_by_agency(self, agency_id: int) -> list[DocumentModel]:
        return list(
            self._session.execute(
                select(DocumentModel).where(DocumentModel.agency_id == agency_id)
            ).scalars()
        )


class CitationRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_normalized(self, normalized: str) -> CitationModel | None:
        return self._session.execute(
            select(CitationModel).where(CitationModel.normalized == normalized)
        ).scalar_one_or_none()

    def add(self, citation: models.Citation) -> CitationModel:
        row = CitationModel(
            title_number=citation.title_number,
            part=citation.part,
            section=citation.section,
            raw_text=citation.raw_text,
            normalized=citation.normalized,
            citation_type=citation.citation_type.value,
        )
        self._session.add(row)
        self._session.flush()
        return row


class DocumentCitationRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_many(self, document_id: int, entries: Iterable[models.DocumentCitation]) -> None:
        for entry in entries:
            row = DocumentCitationModel(
                document_id=document_id,
                citation_id=entry.citation_id,
                context_snippet=entry.context_snippet,
                match_start=entry.match_start,
                match_end=entry.match_end,
                occurrence_count=entry.occurrence_count,
            )
            self._session.merge(row)

    def clear_for_document(self, document_id: int) -> None:
        self._session.query(DocumentCitationModel).filter(
            DocumentCitationModel.document_id == document_id
        ).delete()


class AggregateRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def clear(self) -> None:
        self._session.query(AggregateCfrCountModel).delete()

    def add(
        self,
        agency_id: int,
        normalized_citation: str,
        period_start: datetime,
        period_end: datetime,
        document_count: int,
        occurrence_count: int,
    ) -> None:
        row = AggregateCfrCountModel(
            agency_id=agency_id,
            normalized_citation=normalized_citation,
            period_start=period_start,
            period_end=period_end,
            document_count=document_count,
            occurrence_count=occurrence_count,
        )
        self._session.add(row)

    def top_cfr(
        self, agency_id: int | None, limit: int
    ) -> list[tuple[str, int, int]]:
        stmt = select(
            AggregateCfrCountModel.normalized_citation,
            func.sum(AggregateCfrCountModel.document_count),
            func.sum(AggregateCfrCountModel.occurrence_count),
        )
        if agency_id is not None:
            stmt = stmt.where(AggregateCfrCountModel.agency_id == agency_id)
        stmt = (
            stmt.group_by(AggregateCfrCountModel.normalized_citation)
            .order_by(func.sum(AggregateCfrCountModel.occurrence_count).desc())
            .limit(limit)
        )
        return list(self._session.execute(stmt).all())
