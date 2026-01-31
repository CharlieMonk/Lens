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
    max_links: int | None = None
    use_pdf: bool | None = None
    listing_urls: list[str] | None = None
    link_regex: str | None = None
    link_selector: str | None = None


class CitationDatabaseBuilder:
    def __init__(self, database_url: str) -> None:
        self._db = Database(DatabaseConfig(url=database_url))
        MigrationRunner(self._db.engine).run()

    @staticmethod
    def default_live_sources() -> list[SourceConfig]:
        sec_archive_urls = []
        for year in range(2018, 2027):
            for page in range(0, 7):
                sec_archive_urls.append(
                    f"https://www.sec.gov/enforcement-litigation/litigation-releases?populate=&year={year}&month=All&page={page}"
                )
        return [
            SourceConfig(
                agency_name="Securities and Exchange Commission",
                aliases=["SEC"],
                source_type=SourceType.ENFORCEMENT,
                base_url="https://www.sec.gov/enforcement-litigation/litigation-releases/rss",
                max_links=400,
                use_pdf=True,
            ),
            SourceConfig(
                agency_name="Securities and Exchange Commission",
                aliases=["SEC"],
                source_type=SourceType.PRESS,
                base_url="https://www.sec.gov/news/pressreleases.rss",
                max_links=400,
                use_pdf=True,
            ),
            SourceConfig(
                agency_name="Securities and Exchange Commission",
                aliases=["SEC"],
                source_type=SourceType.LITIGATION,
                base_url="https://www.sec.gov/rss/litigation/litreleases.xml",
                max_links=400,
                use_pdf=True,
            ),
            SourceConfig(
                agency_name="Securities and Exchange Commission",
                aliases=["SEC"],
                source_type=SourceType.LITIGATION,
                base_url="https://www.sec.gov/enforcement-litigation/litigation-releases",
                max_links=1000,
                use_pdf=True,
                listing_urls=sec_archive_urls,
                link_regex=r"/litigation-releases/lr-",
                link_selector="a[href]",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.ENFORCEMENT,
                base_url="https://www.epa.gov/newsreleases/search?f%5B0%5D=subject%3A226191",
                max_links=400,
                use_pdf=True,
                link_regex=r"/newsreleases/[^?]+$",
                link_selector="a[href]",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.PRESS,
                base_url="https://www.epa.gov/newsreleases/search",
                max_links=400,
                use_pdf=True,
                link_regex=r"/newsreleases/[^?]+$",
                link_selector="a[href]",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.ENFORCEMENT,
                base_url="https://www.epa.gov/enforcement/compliance-advisories-and-enforcement-alerts",
                max_links=400,
                use_pdf=True,
                link_selector="table a",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.ENFORCEMENT,
                base_url="https://yosemite.epa.gov/oarm/alj/alj_web_docket.nsf//All%20Dockets%20by%20Statute?SearchView&Query=%22C.%20F.%20R.%22&SearchMax=0&SearchWV=TRUE",
                max_links=500,
                use_pdf=True,
                link_regex=r"OpenDocument",
                link_selector="a[href]",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.ENFORCEMENT,
                base_url="https://yosemite.epa.gov/oarm/alj/alj_web_docket.nsf//All%20Dockets%20by%20Statute?SearchView&Query=%22C.F.R.%22&SearchMax=0&SearchWV=TRUE",
                max_links=500,
                use_pdf=True,
                link_regex=r"OpenDocument",
                link_selector="a[href]",
            ),
            SourceConfig(
                agency_name="Environmental Protection Agency",
                aliases=["EPA"],
                source_type=SourceType.ENFORCEMENT,
                base_url="https://yosemite.epa.gov/oarm/alj/alj_web_docket.nsf//All%20Dockets%20by%20Statute?SearchView&Query=%22CFR%22&SearchMax=0&SearchWV=TRUE",
                max_links=500,
                use_pdf=True,
                link_regex=r"OpenDocument",
                link_selector="a[href]",
            ),
            SourceConfig(
                agency_name="Department of Labor",
                aliases=["DOL"],
                source_type=SourceType.PRESS,
                base_url="https://www.osha.gov/news/newsreleases.xml",
                max_links=400,
                use_pdf=True,
            ),
            SourceConfig(
                agency_name="Department of Labor",
                aliases=["DOL"],
                source_type=SourceType.PRESS,
                base_url="https://www.dol.gov/newsroom/releases/rss",
                max_links=400,
                use_pdf=True,
            ),
            SourceConfig(
                agency_name="Department of Labor",
                aliases=["DOL"],
                source_type=SourceType.PRESS,
                base_url="https://www.osha.gov/news/newsreleases/search",
                max_links=400,
                use_pdf=True,
                link_regex=r"/news/newsreleases/",
                link_selector="a[href]",
            ),
        ]

    def build_live_db(self, respect_robots: bool = True) -> dict[str, int]:
        self.register_sources(self.default_live_sources())
        stats = self.ingest(
            offline=False, continue_on_error=True, respect_robots=respect_robots
        )
        self.rebuild_counts()
        return stats

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
                if source.max_links:
                    config["max_links"] = source.max_links
                if source.use_pdf is not None:
                    config["use_pdf"] = source.use_pdf
                if source.listing_urls:
                    config["listing_urls"] = source.listing_urls
                if source.link_regex:
                    config["link_regex"] = source.link_regex
                if source.link_selector:
                    config["link_selector"] = source.link_selector
                config["max_workers"] = 6
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
        continue_on_error: bool = False,
        respect_robots: bool = True,
    ) -> dict[str, int]:
        fetcher = self._build_fetcher(offline, fixtures_path, respect_robots)
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
                continue_on_error=continue_on_error,
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

    def _build_fetcher(
        self, offline: bool, fixtures_path: Path | None, respect_robots: bool
    ):
        if offline:
            if not fixtures_path:
                raise ValueError("fixtures_path required for offline ingestion")
            return FixtureFetcher(FixtureRegistry(fixtures_path))
        return HttpFetcher(
            HttpFetcherConfig(respect_robots=respect_robots, rate_limit_per_domain=0.2)
        )
