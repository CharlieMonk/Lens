from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich import print
from rich.table import Table

from relevance.adapters.registry import AdapterRegistry
from relevance.application.aggregation import AggregationService
from relevance.application.bootstrap import add_source, ensure_agency
from relevance.application.citation_extractor import CitationExtractor
from relevance.application.ingestion import IngestionService
from relevance.application.query import QueryService
from relevance.infrastructure.config import AppSettings
from relevance.infrastructure.db import Database, DatabaseConfig
from relevance.infrastructure.fixture_fetcher import FixtureFetcher, FixtureRegistry
from relevance.infrastructure.http_fetcher import HttpFetcher, HttpFetcherConfig
from relevance.infrastructure.migrations import MigrationRunner
from relevance.infrastructure.repositories import (
    AgencyRepository,
    CitationRepository,
    DocumentCitationRepository,
    DocumentRepository,
    SourceRepository,
)
from sqlalchemy import text

app = typer.Typer(no_args_is_help=True)


def _build_db(settings: AppSettings | None = None) -> Database:
    settings = settings or AppSettings()
    db = Database(DatabaseConfig(url=settings.database_url))
    MigrationRunner(db.engine).run()
    return db


def _fetcher(offline: bool, fixtures_path: Path | None = None):
    if offline:
        if not fixtures_path:
            raise typer.BadParameter("fixtures path required for offline mode")
        registry = FixtureRegistry(fixtures_path)
        return FixtureFetcher(registry)
    return HttpFetcher(HttpFetcherConfig())


@app.command()
def init_db() -> None:
    _build_db()
    print("Database initialized")


@app.command()
def add_source_cmd(
    agency: str = typer.Option(..., "--agency"),
    source_type: str = typer.Option(..., "--type"),
    base_url: str = typer.Option(..., "--base-url"),
    fixture_base_url: Optional[str] = typer.Option(None, "--fixture-base-url"),
    aliases: Optional[str] = typer.Option(None, "--aliases"),
) -> None:
    db = _build_db()
    with db.session() as session:
        agencies = AgencyRepository(session)
        sources = SourceRepository(session)
        alias_list = [a.strip() for a in aliases.split(",")] if aliases else []
        agency_id = ensure_agency(agencies, agency, alias_list)
        config = {}
        if fixture_base_url:
            config["fixture_base_url"] = fixture_base_url
        source_id = add_source(
            sources,
            agency_id=agency_id,
            source_type=_source_type(source_type),
            base_url=base_url,
            config_json=config,
        )
        print(f"Source added: {source_id}")


@app.command()
def ingest(
    all: bool = typer.Option(True, "--all/--no-all"),
    agency: Optional[str] = typer.Option(None, "--agency"),
    offline: bool = typer.Option(False, "--offline"),
    fixtures: Optional[Path] = typer.Option(None, "--fixtures"),
) -> None:
    db = _build_db()
    fetcher = _fetcher(offline, fixtures)
    adapters = AdapterRegistry()
    ingestion = IngestionService(adapters, CitationExtractor())
    if not all and not agency:
        raise typer.BadParameter("agency is required when --all is false")
    with db.session() as session:
        sources = SourceRepository(session)
        docs = DocumentRepository(session)
        citations = CitationRepository(session)
        doc_citations = DocumentCitationRepository(session)
        override = None
        if offline:
            override = {}
            for source in sources.list_all():
                fixture_url = source.config_json.get("fixture_base_url")
                if not fixture_url:
                    raise typer.BadParameter(
                        f"fixture_base_url missing for source {source.id}"
                    )
                override[source.id] = fixture_url
        stats = ingestion.ingest_sources(
            fetcher,
            sources,
            docs,
            citations,
            doc_citations,
            agency_filter=None if all else agency,
            base_url_override=override,
        )
        print(f"Documents ingested: {stats['documents']}")
        print(f"Citations extracted: {stats['citations']}")


@app.command("rebuild-aggregates")
def rebuild_aggregates() -> None:
    db = _build_db()
    aggregation = AggregationService()
    with db.session() as session:
        count = aggregation.rebuild(session, granularity="month")
    print(f"Aggregate rows: {count}")


@app.command("top-cfr")
def top_cfr(
    limit: int = typer.Option(10, "--limit"),
    agency: Optional[str] = typer.Option(None, "--agency"),
    since: Optional[str] = typer.Option(None, "--since"),
    until: Optional[str] = typer.Option(None, "--until"),
) -> None:
    db = _build_db()
    with db.session() as session:
        agencies = AgencyRepository(session)
        agency_id = _agency_id(agencies, agency) if agency else None
        query = QueryService(session)
        since_dt = datetime.fromisoformat(since) if since else None
        until_dt = datetime.fromisoformat(until) if until else None
        results = query.top_cfr(limit, agency_id=agency_id, since=since_dt, until=until_dt)
        table = Table(title="Top CFR citations")
        table.add_column("Citation")
        table.add_column("Documents", justify="right")
        table.add_column("Occurrences", justify="right")
        for normalized, doc_count, occ_count in results:
            table.add_row(normalized, str(doc_count), str(occ_count))
        print(table)


@app.command()
def trend(
    cfr: str = typer.Option(..., "--cfr"),
    granularity: str = typer.Option("month", "--granularity"),
    agency: Optional[str] = typer.Option(None, "--agency"),
) -> None:
    db = _build_db()
    with db.session() as session:
        agencies = AgencyRepository(session)
        agency_id = _agency_id(agencies, agency) if agency else None
        query = QueryService(session)
        results = query.trend(cfr, granularity, agency_id=agency_id)
        table = Table(title=f"Trend for {cfr}")
        table.add_column("Period start")
        table.add_column("Occurrences", justify="right")
        for period_start, occ in results:
            table.add_row(period_start.isoformat(), str(occ))
        print(table)


@app.command()
def docs(
    cfr: str = typer.Option(..., "--cfr"),
    limit: int = typer.Option(10, "--limit"),
) -> None:
    db = _build_db()
    with db.session() as session:
        query = QueryService(session)
        results = query.documents_for_cfr(cfr, limit)
        table = Table(title=f"Documents citing {cfr}")
        table.add_column("Title")
        table.add_column("URL")
        table.add_column("Published")
        for title, url, published in results:
            table.add_row(title, url, published.isoformat())
        print(table)


@app.command("build-starter-db")
def build_starter_db(
    out: Path = typer.Option(Path("data/starter.sqlite"), "--out"),
    fixtures: Path = typer.Option(Path("tests/fixtures"), "--fixtures"),
    rebuild: bool = typer.Option(False, "--rebuild"),
) -> None:
    if out.exists() and not rebuild:
        raise typer.BadParameter("output file exists; use --rebuild to overwrite")
    if out.exists() and rebuild:
        out.unlink()
    db = Database(DatabaseConfig(url=f"sqlite:///{out}"))
    MigrationRunner(db.engine).run()
    registry = FixtureRegistry(fixtures)
    fetcher = FixtureFetcher(registry)
    adapters = AdapterRegistry()
    ingestion = IngestionService(adapters, CitationExtractor())
    with db.session() as session:
        agencies = AgencyRepository(session)
        sources = SourceRepository(session)
        docs = DocumentRepository(session)
        citations = CitationRepository(session)
        doc_citations = DocumentCitationRepository(session)

        sec_id = ensure_agency(agencies, "Securities and Exchange Commission", ["SEC"])
        epa_id = ensure_agency(agencies, "Environmental Protection Agency", ["EPA"])
        dol_id = ensure_agency(agencies, "Department of Labor", ["DOL"])
        sec_source = add_source(
            sources,
            sec_id,
            _source_type("enforcement"),
            base_url="fixture://sec/index",
            config_json={"fixture_base_url": "fixture://sec/index"},
        )
        epa_source = add_source(
            sources,
            epa_id,
            _source_type("enforcement"),
            base_url="fixture://epa/index",
            config_json={"fixture_base_url": "fixture://epa/index"},
        )
        dol_source = add_source(
            sources,
            dol_id,
            _source_type("press"),
            base_url="fixture://dol/index",
            config_json={"fixture_base_url": "fixture://dol/index"},
        )
        override = {
            sec_source: "fixture://sec/index",
            epa_source: "fixture://epa/index",
            dol_source: "fixture://dol/index",
        }
        stats = ingestion.ingest_sources(
            fetcher, sources, docs, citations, doc_citations, base_url_override=override
        )
        aggregation = AggregationService()
        aggregation.rebuild(session, granularity="month")
        query = QueryService(session)
        print(f"documents ingested: {stats['documents']}")
        unique_citations = session.execute(text("select count(*) from citations")).scalar_one()
        print(f"unique citations: {unique_citations}")
        top = query.top_cfr(limit=10)
        print("top 10 citations overall:")
        for normalized, doc_count, occ in top:
            print(f"- {normalized}: {doc_count} docs, {occ} mentions")
        for agency_name in ["Securities and Exchange Commission", "Environmental Protection Agency", "Department of Labor"]:
            agency_id = _agency_id(agencies, agency_name)
            top_agency = query.top_cfr(limit=5, agency_id=agency_id)
            print(f"top 5 for {agency_name}:")
            for normalized, doc_count, occ in top_agency:
                print(f"- {normalized}: {doc_count} docs, {occ} mentions")


def _agency_id(repo: AgencyRepository, name: str | None) -> int:
    if not name:
        raise typer.BadParameter("agency name required")
    agency = repo.get_by_name(name)
    if agency:
        return agency.id
    for row in repo.list_all():
        if name.lower() in [alias.lower() for alias in row.aliases]:
            return row.id
    raise typer.BadParameter(f"unknown agency: {name}")


def _source_type(value: str):
    from relevance.domain.models import SourceType

    try:
        return SourceType(value)
    except ValueError:
        raise typer.BadParameter("source_type must be enforcement|litigation|press")


if __name__ == "__main__":
    app()
