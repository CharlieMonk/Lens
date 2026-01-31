from __future__ import annotations

from datetime import datetime

from fastapi import FastAPI, Query

from relevance.application_query import QueryService
from relevance.infrastructure_config import AppSettings
from relevance.interface_wiring import build_app
from relevance.infrastructure_repositories import AgencyRepository

app = FastAPI(title="CFR Relevance API")

settings = AppSettings()
_db, _ingestion, _aggregation = build_app(settings)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/agencies")
def agencies():
    with _db.session() as session:
        repo = AgencyRepository(session)
        rows = repo.list_all()
        return [{"id": r.id, "name": r.name, "aliases": r.aliases} for r in rows]


@app.get("/top-cfr")
def top_cfr(
    limit: int = Query(10, ge=1, le=100),
    agency: str | None = None,
    since: str | None = None,
    until: str | None = None,
):
    with _db.session() as session:
        agencies = AgencyRepository(session)
        agency_id = None
        if agency:
            for row in agencies.list_all():
                if row.name == agency or agency.lower() in [a.lower() for a in row.aliases]:
                    agency_id = row.id
                    break
        query = QueryService(session)
        since_dt = datetime.fromisoformat(since) if since else None
        until_dt = datetime.fromisoformat(until) if until else None
        results = query.top_cfr(limit, agency_id=agency_id, since=since_dt, until=until_dt)
        return [
            {"normalized": normalized, "document_count": doc_count, "occurrences": occ}
            for normalized, doc_count, occ in results
        ]


@app.get("/trend")
def trend(
    cfr: str = Query(...),
    granularity: str = Query("month"),
    agency: str | None = None,
):
    with _db.session() as session:
        agencies = AgencyRepository(session)
        agency_id = None
        if agency:
            for row in agencies.list_all():
                if row.name == agency or agency.lower() in [a.lower() for a in row.aliases]:
                    agency_id = row.id
                    break
        query = QueryService(session)
        results = query.trend(cfr, granularity, agency_id=agency_id)
        return [
            {"period_start": period.isoformat(), "occurrences": occ}
            for period, occ in results
        ]


@app.get("/documents")
def documents(cfr: str = Query(...), limit: int = Query(10, ge=1, le=100)):
    with _db.session() as session:
        query = QueryService(session)
        results = query.documents_for_cfr(cfr, limit)
        return [
            {"title": title, "url": url, "published_at": published.isoformat()}
            for title, url, published in results
        ]
