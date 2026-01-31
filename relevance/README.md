# CFR Enforcement Relevance

Production-grade Python system for identifying which CFR sections are most commonly enforced by U.S. federal agencies.

## Features
- Ingests enforcement actions, litigation releases, and press releases via agency adapters
- Extracts and normalizes CFR citations from text
- Stores raw documents, citations, and aggregates in SQLite
- Deterministic offline ingestion via HTML fixtures
- CLI and FastAPI API for querying frequency and trends
- Pluggable adapters to add agencies without touching core logic

## Project layout
- `src/relevance/` flat modules (domain, services, adapters, persistence, CLI, API)
- `tests/fixtures` offline HTML fixtures

## Setup (parent project .venv)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## CLI usage
Initialize DB:
```bash
relevance init-db
```

Add a source:
```bash
relevance add-source --agency "Securities and Exchange Commission" \
  --type enforcement \
  --base-url "https://www.sec.gov/litigation/litreleases.htm" \
  --fixture-base-url "fixture://sec/index" \
  --aliases "SEC"
```

Offline ingestion:
```bash
relevance ingest --all --offline --fixtures tests/fixtures
```

Rebuild aggregates:
```bash
relevance rebuild-aggregates
```

Query top CFR citations:
```bash
relevance top-cfr --limit 10 --agency SEC
```

Trend query:
```bash
relevance trend --cfr "17 CFR 240.10b-5" --granularity month
```

Documents for CFR:
```bash
relevance docs --cfr "29 CFR 1910.147" --limit 5
```

Build starter DB:
```bash
relevance build-starter-db --out data/starter.sqlite --fixtures tests/fixtures --rebuild
```

## API
Run:
```bash
uvicorn relevance.api_app:app --reload
```

Endpoints:
- `GET /health`
- `GET /agencies`
- `GET /top-cfr`
- `GET /trend`
- `GET /documents`

## Testing
```bash
pytest
```

## Adding a new agency
1. Create a new adapter in `src/relevance/adapters` implementing `AgencyAdapter`.
2. Register it in `AdapterRegistry`.
3. Add a source via CLI.

## Offline fixtures
Fixtures live under `tests/fixtures/{sec,epa,dol}` and are consumed via `fixture://<agency>/<doc>` URLs.
