from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from relevance.adapters_registry import AdapterRegistry
from relevance.application_counts import CitationCountService
from relevance.application_bootstrap import add_source, ensure_agency
from relevance.application_citation_extractor import CitationExtractor
from relevance.application_ingestion import IngestionService
from relevance.domain_models import SourceType
from relevance.infrastructure_db import Database, DatabaseConfig
from relevance.infrastructure_fixture_fetcher import FixtureFetcher, FixtureRegistry
from relevance.infrastructure_http_fetcher import HttpFetcher, HttpFetcherConfig
from relevance.infrastructure_migrations import MigrationRunner
from relevance.infrastructure_repositories import (
    AgencyRepository,
    CitationCountRepository,
    CitationRepository,
    DocumentCitationRepository,
    DocumentRepository,
    SourceRepository,
)


@dataclass(frozen=True)
class SourceConfig:
    agency_name: str
    aliases: list[str]
    source_type: SourceType
    base_url: str
    fixture_base_url: str | None = None


class CitationDatabaseBuilder:
    def __init__(self, database_url: str) -> None:
        self._db = Database(DatabaseConfig(url=database_url))
        MigrationRunner(self._db.engine).run()

    def register_sources(self, sources: list[SourceConfig]) -> list[int]:
        with self._db.session() as session:
            agencies = AgencyRepository(session)
            sources_repo = SourceRepository(session)
            source_ids: list[int] = []
            for source in sources:
                agency_id = ensure_agency(agencies, source.agency_name, source.aliases)
                config = {}
                if source.fixture_base_url:
                    config["fixture_base_url"] = source.fixture_base_url
                source_id = add_source(
                    sources_repo,
                    agency_id=agency_id,
                    source_type=source.source_type,
                    base_url=source.base_url,
                    config_json=config,
                )
                source_ids.append(source_id)
            return source_ids

    def ingest(
        self,
        offline: bool = False,
        fixtures_path: Path | None = None,
        agency_filter: str | None = None,
    ) -> dict[str, int]:
        fetcher = self._build_fetcher(offline, fixtures_path)
        ingestion = IngestionService(AdapterRegistry(), CitationExtractor())
        with self._db.session() as session:
            sources = SourceRepository(session)
            documents = DocumentRepository(session)
            citations = CitationRepository(session)
            doc_citations = DocumentCitationRepository(session)
            override = None
            if offline:
                override = {}
                for source in sources.list_all():
                    fixture_url = source.config_json.get("fixture_base_url")
                    if not fixture_url:
                        raise ValueError(f"fixture_base_url missing for source {source.id}")
                    override[source.id] = fixture_url
            return ingestion.ingest_sources(
                fetcher,
                sources,
                documents,
                citations,
                doc_citations,
                agency_filter=agency_filter,
                base_url_override=override,
            )

    def rebuild_counts(self) -> int:
        with self._db.session() as session:
            counter = CitationCountService()
            return counter.rebuild(session)

    def top_citations(self, limit: int = 10, agency_name: str | None = None):
        with self._db.session() as session:
            agencies = AgencyRepository(session)
            agency_id = None
            if agency_name:
                agency = agencies.get_by_name(agency_name)
                if not agency:
                    for row in agencies.list_all():
                        if agency_name.lower() in [alias.lower() for alias in row.aliases]:
                            agency = row
                            break
                if not agency:
                    raise ValueError(f"Unknown agency: {agency_name}")
                agency_id = agency.id
            repo = CitationCountRepository(session)
            return repo.top_cfr(agency_id=agency_id, limit=limit)

    def build_offline_starter_db(self, fixtures_path: Path) -> dict[str, int]:
        sources = [
            SourceConfig(
                agency_name="Securities and Exchange Commission",
                aliases=["SEC"],
                source_type=SourceType.ENFORCEMENT,
                base_url="fixture://sec/index",
                fixture_base_url="fixture://sec/index",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.ENFORCEMENT,
                base_url="fixture://epa/index",
                fixture_base_url="fixture://epa/index",
            ),
            SourceConfig(
                agency_name="Department of Labor",
                aliases=["DOL"],
                source_type=SourceType.PRESS,
                base_url="fixture://dol/index",
                fixture_base_url="fixture://dol/index",
            ),
        ]
        self.register_sources(sources)
        stats = self.ingest(offline=True, fixtures_path=fixtures_path)
        self.rebuild_counts()
        return stats

    def counts_by_agency(self):
        with self._db.session() as session:
            repo = CitationCountRepository(session)
            agencies = AgencyRepository(session)
            result = {}
            for agency in agencies.list_all():
                result[agency.name] = repo.top_cfr(agency_id=agency.id, limit=1000)
            return result

    def _build_fetcher(self, offline: bool, fixtures_path: Path | None):
        if offline:
            if not fixtures_path:
                raise ValueError("fixtures_path required for offline ingestion")
            return FixtureFetcher(FixtureRegistry(fixtures_path))
        return HttpFetcher(HttpFetcherConfig())
