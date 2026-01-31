from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from relevance.infrastructure_orm import CitationModel, DocumentCitationModel, DocumentModel
from relevance.infrastructure_repositories import AggregateRepository


class AggregationService:
    def rebuild(self, session: Session, granularity: str = "month") -> int:
        repo = AggregateRepository(session)
        repo.clear()
        rows = session.execute(
            select(
                DocumentModel.id,
                DocumentModel.agency_id,
                DocumentModel.published_at,
                CitationModel.normalized,
                DocumentCitationModel.occurrence_count,
            )
            .join(DocumentCitationModel, DocumentModel.id == DocumentCitationModel.document_id)
            .join(CitationModel, CitationModel.id == DocumentCitationModel.citation_id)
        ).all()
        grouped: dict[tuple[int, str, datetime], dict[str, int]] = defaultdict(
            lambda: {"documents": 0, "occurrences": 0}
        )
        seen_docs: set[tuple[int, str, datetime, int]] = set()
        for doc_id, agency_id, published_at, normalized, occ in rows:
            period_start, period_end = self._period_bounds(published_at, granularity)
            key = (agency_id, normalized, period_start)
            grouped[key]["occurrences"] += occ
            doc_key = (agency_id, normalized, period_start, doc_id)
            if doc_key not in seen_docs:
                grouped[key]["documents"] += 1
                seen_docs.add(doc_key)
        for (agency_id, normalized, period_start), counts in grouped.items():
            period_end = self._period_bounds(period_start, granularity)[1]
            repo.add(
                agency_id=agency_id,
                normalized_citation=normalized,
                period_start=period_start,
                period_end=period_end,
                document_count=counts["documents"],
                occurrence_count=counts["occurrences"],
            )
        return len(grouped)

    def _period_bounds(self, date: datetime, granularity: str) -> tuple[datetime, datetime]:
        date = date.astimezone(timezone.utc)
        if granularity == "day":
            start = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
            end = start.replace(hour=23, minute=59, second=59)
            return start, end
        if granularity == "month":
            start = datetime(date.year, date.month, 1, tzinfo=timezone.utc)
            if date.month == 12:
                end = datetime(date.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(date.year, date.month + 1, 1, tzinfo=timezone.utc)
            end = end.replace(day=1) - self._delta_second()
            return start, end
        raise ValueError("granularity must be day or month")

    def _delta_second(self):
        from datetime import timedelta

        return timedelta(seconds=1)
