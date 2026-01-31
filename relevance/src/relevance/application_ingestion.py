from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from relevance.adapters_registry import AdapterRegistry
from relevance.application_citation_extractor import CitationExtractor
from relevance.application_fetcher import Fetcher
import relevance.domain_models as models
from relevance.infrastructure_repositories import (
    CitationRepository,
    DocumentCitationRepository,
    DocumentRepository,
    SourceRepository,
)


class IngestionService:
    def __init__(
        self,
        adapter_registry: AdapterRegistry,
        extractor: CitationExtractor,
    ) -> None:
        self._adapters = adapter_registry
        self._extractor = extractor

    def ingest_sources(
        self,
        fetcher: Fetcher,
        source_repo: SourceRepository,
        document_repo: DocumentRepository,
        citation_repo: CitationRepository,
        doc_citation_repo: DocumentCitationRepository,
        agency_filter: str | None = None,
        base_url_override: dict[int, str] | None = None,
        continue_on_error: bool = False,
    ) -> dict[str, int]:
        sources = source_repo.list_all()
        stats = {"documents": 0, "citations": 0}
        for source in sources:
            adapter = self._adapters.get(source.agency.name)
            if agency_filter and agency_filter.lower() not in source.agency.name.lower():
                continue
            base_url = source.base_url
            if base_url_override and source.id in base_url_override:
                base_url = base_url_override[source.id]
            try:
                parsed_docs = adapter.fetch_documents(
                    fetcher, base_url, config=source.config_json
                )
            except Exception as exc:
                if continue_on_error:
                    if _is_robots_blocked(exc):
                        continue
                    continue
                raise
            for parsed in parsed_docs:
                existing = document_repo.get_by_url(parsed.url)
                doc_model = None
                document = models.Document(
                    id=existing.id if existing else None,
                    agency_id=source.agency_id,
                    title=parsed.title,
                    url=parsed.url,
                    published_at=parsed.published_at,
                    retrieved_at=datetime.now(timezone.utc),
                )
                if existing:
                    doc_model = document_repo.update(existing, document)
                    doc_citation_repo.clear_for_document(doc_model.id)
                else:
                    doc_model = document_repo.add(document)
                extracted = self._extractor.extract(parsed.text)
                grouped: dict[str, list] = defaultdict(list)
                for item in extracted:
                    grouped[item.normalized].append(item)
                doc_citations: list[models.DocumentCitation] = []
                for normalized, items in grouped.items():
                    first = items[0]
                    citation = citation_repo.get_by_normalized(normalized)
                    if not citation:
                        citation = citation_repo.add(
                            models.Citation(
                                id=None,
                                title_number=first.title_number,
                                part=first.part,
                                section=first.section,
                                raw_text=first.raw_text,
                                normalized=normalized,
                                citation_type=first.citation_type,
                            )
                        )
                    doc_citations.append(
                        models.DocumentCitation(
                            document_id=doc_model.id,
                            citation_id=citation.id,
                            context_snippet=first.context_snippet,
                            match_start=first.match_start,
                            match_end=first.match_end,
                            occurrence_count=len(items),
                        )
                    )
                doc_citation_repo.add_many(doc_model.id, doc_citations)
                stats["documents"] += 1
                stats["citations"] += len(doc_citations)
        return stats


def _is_robots_blocked(exc: Exception) -> bool:
    if "robots.txt" in str(exc):
        return True
    if isinstance(exc, RetryError) and exc.last_attempt:
        inner = exc.last_attempt.exception()
        if inner and "robots.txt" in str(inner):
            return True
    return False
from tenacity import RetryError
