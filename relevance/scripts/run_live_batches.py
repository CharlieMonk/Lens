from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from relevance.adapters_registry import AdapterRegistry
from sqlalchemy import text

from relevance.application_bootstrap import add_source, ensure_agency
from relevance.application_citation_extractor import CitationExtractor
from relevance.domain_models import Citation, Document, DocumentCitation, SourceType
from relevance.infrastructure_db import Database, DatabaseConfig
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
class LiveSource:
    agency_name: str
    aliases: list[str]
    source_type: SourceType
    base_url: str
    max_links: int
    use_pdf: bool
    listing_urls: list[str] | None = None
    link_regex: str | None = None
    link_selector: str | None = None


SEC_ARCHIVE_URLS = [
    f"https://www.sec.gov/enforcement-litigation/litigation-releases?populate=&year={year}&month=All&page={page}"
    for year in range(2018, 2027)
    for page in range(0, 7)
]

LIVE_SOURCES = [
    LiveSource(
        "Securities and Exchange Commission",
        ["SEC"],
        SourceType.ENFORCEMENT,
        "https://www.sec.gov/enforcement-litigation/litigation-releases/rss",
        max_links=150,
        use_pdf=True,
    ),
    LiveSource(
        "Securities and Exchange Commission",
        ["SEC"],
        SourceType.PRESS,
        "https://www.sec.gov/news/pressreleases.rss",
        max_links=150,
        use_pdf=True,
    ),
    LiveSource(
        "Environmental Protection Agency",
        ["EPA"],
        SourceType.ENFORCEMENT,
        "https://www.epa.gov/newsreleases/search?f%5B0%5D=subject%3A226191",
        max_links=150,
        use_pdf=True,
        link_regex=r"/newsreleases/[^?]+$",
        link_selector="a[href]",
    ),
    LiveSource(
        "Environmental Protection Agency",
        ["EPA"],
        SourceType.PRESS,
        "https://www.epa.gov/newsreleases/search",
        max_links=150,
        use_pdf=True,
        link_regex=r"/newsreleases/[^?]+$",
        link_selector="a[href]",
    ),
    LiveSource(
        "Environmental Protection Agency",
        ["EPA"],
        SourceType.ENFORCEMENT,
        "https://nepis.epa.gov/RSS/ECA.xml",
        max_links=50,
        use_pdf=True,
    ),
    LiveSource(
        "Environmental Protection Agency",
        ["EPA"],
        SourceType.ENFORCEMENT,
        "https://www.epa.gov/enforcement/compliance-advisories-and-enforcement-alerts",
        max_links=200,
        use_pdf=True,
        link_selector="table a",
    ),
    LiveSource(
        "Department of Labor",
        ["DOL"],
        SourceType.PRESS,
        "https://www.osha.gov/news/newsreleases.xml",
        max_links=150,
        use_pdf=True,
    ),
    LiveSource(
        "Department of Labor",
        ["DOL"],
        SourceType.PRESS,
        "https://www.dol.gov/newsroom/releases/rss",
        max_links=150,
        use_pdf=True,
    ),
    LiveSource(
        "Securities and Exchange Commission",
        ["SEC"],
        SourceType.LITIGATION,
        "https://www.sec.gov/enforcement-litigation/litigation-releases",
        max_links=800,
        use_pdf=True,
        listing_urls=SEC_ARCHIVE_URLS,
        link_regex=r"/litigation-releases/lr-",
        link_selector="a[href]",
    ),
    LiveSource(
        "Environmental Protection Agency",
        ["EPA"],
        SourceType.ENFORCEMENT,
        "https://www.epa.gov/enforcement",
        max_links=400,
        use_pdf=True,
        link_regex=r"/newsreleases/",
        link_selector="a[href]",
    ),
    LiveSource(
        "Environmental Protection Agency",
        ["EPA"],
        SourceType.PRESS,
        "https://www.epa.gov/newsroom/browse-news-releases",
        max_links=400,
        use_pdf=True,
        link_regex=r"/newsreleases/",
        link_selector="a[href]",
    ),
    LiveSource(
        "Department of Labor",
        ["DOL"],
        SourceType.PRESS,
        "https://www.osha.gov/news/newsreleases/search",
        max_links=400,
        use_pdf=True,
        link_regex=r"/news/newsreleases/",
        link_selector="a[href]",
    ),
]


def build_sources(session, sources: list[LiveSource]) -> list[int]:
    agencies = AgencyRepository(session)
    repo = SourceRepository(session)
    ids = []
    for source in sources:
        agency_id = ensure_agency(agencies, source.agency_name, source.aliases)
        config = {"max_links": source.max_links, "use_pdf": source.use_pdf, "max_workers": 6}
        if source.listing_urls:
            config["listing_urls"] = source.listing_urls
        if source.link_regex:
            config["link_regex"] = source.link_regex
        if source.link_selector:
            config["link_selector"] = source.link_selector
        if "epa.gov/newsreleases/search" in source.base_url:
            config["use_playwright_listing"] = True
        source_id = add_source(
            repo,
            agency_id=agency_id,
            source_type=source.source_type,
            base_url=source.base_url,
            config_json=config,
        )
        ids.append(source_id)
    return ids


def ingest_source(session, fetcher, adapter, source_row, extractor, progress_every: int = 10):
    sources_repo = SourceRepository(session)
    documents_repo = DocumentRepository(session)
    citations_repo = CitationRepository(session)
    doc_citations_repo = DocumentCitationRepository(session)

    print(f"  fetching listing: {source_row.base_url}")
    parsed = adapter.fetch_documents(fetcher, source_row.base_url, config=source_row.config_json)
    total = len(parsed)
    print(f"  fetched {total} docs from {source_row.base_url}")
    doc_count = 0
    citation_count = 0
    for idx, parsed_doc in enumerate(parsed, start=1):
        existing = documents_repo.get_by_url(parsed_doc.url)
        document = Document(
            id=existing.id if existing else None,
            agency_id=source_row.agency_id,
            title=parsed_doc.title,
            url=parsed_doc.url,
            published_at=parsed_doc.published_at,
            retrieved_at=datetime.now(timezone.utc),
        )
        if existing:
            doc_model = documents_repo.update(existing, document)
            doc_citations_repo.clear_for_document(doc_model.id)
        else:
            doc_model = documents_repo.add(document)

        extracted = extractor.extract(parsed_doc.text)
        grouped = {}
        for item in extracted:
            grouped.setdefault(item.normalized, []).append(item)

        doc_links = []
        for normalized, items in grouped.items():
            first = items[0]
            citation = citations_repo.get_by_normalized(normalized)
            if not citation:
                citation = citations_repo.add(
                    Citation(
                        id=None,
                        title_number=first.title_number,
                        part=first.part,
                        section=first.section,
                        raw_text=first.raw_text,
                        normalized=normalized,
                        citation_type=first.citation_type,
                    )
                )
            doc_links.append(
                DocumentCitation(
                    document_id=doc_model.id,
                    citation_id=citation.id,
                    context_snippet=first.context_snippet,
                    match_start=first.match_start,
                    match_end=first.match_end,
                    occurrence_count=len(items),
                )
            )
        doc_citations_repo.add_many(doc_model.id, doc_links)

        doc_count += 1
        citation_count += len(doc_links)
        if idx % progress_every == 0 or idx == total:
            print(f"    progress {idx}/{total}: docs={doc_count}, citations={citation_count}")
    print(f"  completed {doc_count} docs, {citation_count} doc-citation links")
    return doc_count, citation_count


def rebuild_counts(session):
    repo = CitationCountRepository(session)
    repo.clear()
    rows = session.execute(
        text(
            """
        select d.agency_id, c.normalized, sum(dc.occurrence_count)
        from documents d
        join document_citations dc on d.id = dc.document_id
        join citations c on c.id = dc.citation_id
        group by d.agency_id, c.normalized
        """
        )
    ).all()
    for agency_id, normalized, occ in rows:
        repo.add(agency_id=agency_id, normalized_citation=normalized, occurrence_count=int(occ))


def _reset_db(session):
    session.execute(text("delete from document_citations"))
    session.execute(text("delete from citation_counts"))
    session.execute(text("delete from citations"))
    session.execute(text("delete from documents"))
    session.execute(text("delete from sources"))
    session.execute(text("delete from agencies"))


def main():
    db_url = "sqlite:///data/relevance.sqlite"
    Path("data").mkdir(exist_ok=True)
    db = Database(DatabaseConfig(url=db_url))
    MigrationRunner(db.engine).run()

    fetcher = HttpFetcher(HttpFetcherConfig(respect_robots=False, rate_limit_per_domain=0.2))
    registry = AdapterRegistry()
    extractor = CitationExtractor()

    with db.session() as session:
        print("Resetting DB...")
        _reset_db(session)
        print("Registering sources...")
        build_sources(session, LIVE_SOURCES)
        sources = SourceRepository(session).list_all()
        print(f"Sources: {len(sources)}")

        print("Starting ingestion...")
        total_docs = 0
        total_links = 0
        for i, source in enumerate(sources, start=1):
            print(f"Source {i}/{len(sources)}: {source.base_url}")
            adapter = registry.get(source.agency.name)
            try:
                docs_added, links_added = ingest_source(
                    session, fetcher, adapter, source, extractor, progress_every=10
                )
            except Exception as exc:
                print(f"  error: {exc}")
                continue
            total_docs += docs_added
            total_links += links_added
            print(
                f"Status after source {i}: docs_added={docs_added}, "
                f"links_added={links_added}, running_docs={total_docs}, "
                f"running_links={total_links}"
            )

        print("Rebuilding counts at end...")
        rebuild_counts(session)
        session.flush()
        docs = session.execute(text("select count(*) from documents")).fetchone()[0]
        occ = session.execute(
            text("select coalesce(sum(occurrence_count), 0) from citation_counts")
        ).fetchone()[0]
        print(f"Final status: documents={docs}, total_citations={occ}")


if __name__ == "__main__":
    main()
