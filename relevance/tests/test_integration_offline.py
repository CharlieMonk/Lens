from relevance.adapters_registry import AdapterRegistry
from relevance.application_counts import CitationCountService
from relevance.application_bootstrap import add_source, ensure_agency
from relevance.application_citation_extractor import CitationExtractor
from relevance.application_ingestion import IngestionService
from relevance.infrastructure_fixture_fetcher import FixtureFetcher, FixtureRegistry
from relevance.infrastructure_repositories import (
    AgencyRepository,
    CitationCountRepository,
    CitationRepository,
    DocumentCitationRepository,
    DocumentRepository,
    SourceRepository,
)
from relevance.domain_models import SourceType


def test_offline_ingestion_and_counts(session, fixtures_path):
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
        SourceType.ENFORCEMENT,
        base_url="fixture://sec/index",
        config_json={"fixture_base_url": "fixture://sec/index"},
    )
    epa_source = add_source(
        sources,
        epa_id,
        SourceType.ENFORCEMENT,
        base_url="fixture://epa/index",
        config_json={"fixture_base_url": "fixture://epa/index"},
    )
    dol_source = add_source(
        sources,
        dol_id,
        SourceType.PRESS,
        base_url="fixture://dol/index",
        config_json={"fixture_base_url": "fixture://dol/index"},
    )

    fetcher = FixtureFetcher(FixtureRegistry(fixtures_path))
    ingestion = IngestionService(AdapterRegistry(), CitationExtractor())
    stats = ingestion.ingest_sources(
        fetcher,
        sources,
        docs,
        citations,
        doc_citations,
        base_url_override={
            sec_source: "fixture://sec/index",
            epa_source: "fixture://epa/index",
            dol_source: "fixture://dol/index",
        },
    )
    assert stats["documents"] == 9
    assert stats["citations"] > 0

    counter = CitationCountService()
    counter.rebuild(session)

    top = CitationCountRepository(session).top_cfr(agency_id=None, limit=5)
    assert len(top) > 0
    normalized = [row[0] for row in top]
    assert "17 CFR 240.10b-5" in normalized
