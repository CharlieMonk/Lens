from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from relevance.infrastructure_orm import CitationModel, DocumentCitationModel, DocumentModel
from relevance.infrastructure_repositories import CitationCountRepository


class CitationCountService:
    def rebuild(self, session: Session) -> int:
        repo = CitationCountRepository(session)
        repo.clear()
        rows = session.execute(
            select(
                DocumentModel.agency_id,
                CitationModel.normalized,
                func.sum(DocumentCitationModel.occurrence_count),
            )
            .join(DocumentCitationModel, DocumentModel.id == DocumentCitationModel.document_id)
            .join(CitationModel, CitationModel.id == DocumentCitationModel.citation_id)
            .group_by(DocumentModel.agency_id, CitationModel.normalized)
        ).all()
        for agency_id, normalized, occ in rows:
            repo.add(
                agency_id=agency_id,
                normalized_citation=normalized,
                occurrence_count=int(occ),
            )
        return len(rows)
