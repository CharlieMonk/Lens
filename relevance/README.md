# CFR Enforcement Relevance

Production-grade Python system for identifying which CFR sections are most commonly enforced by U.S. federal agencies.

## Features
- Ingests enforcement actions, litigation releases, and press releases via agency adapters
- Extracts and normalizes CFR citations from text
- Stores citations, per-agency citation counts, and links/titles for each enforcement action in SQLite
- Deterministic offline ingestion via HTML fixtures
- Pluggable adapters to add agencies without touching core logic

## Project layout
- `src/relevance/` flat modules (domain, services, adapters, persistence, builder)
- `tests/fixtures` offline HTML fixtures

## Setup (parent project .venv)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Usage (Python)
```python
from pathlib import Path
from relevance import CitationDatabaseBuilder

builder = CitationDatabaseBuilder("sqlite:///data/relevance.sqlite")
builder.build_offline_starter_db(Path("relevance/tests/fixtures"))
builder.rebuild_counts()
top = builder.top_citations(limit=10)
```

## Live sources
```python
from relevance import CitationDatabaseBuilder

builder = CitationDatabaseBuilder("sqlite:///data/relevance_live.sqlite")
stats = builder.build_live_db(respect_robots=True)
print(stats)
```

The default live sources use multiple RSS/Atom feeds per agency for broader coverage (SEC litigation + press releases, EPA enforcement + general news releases, DOL OSHA + DOL newsroom releases).
If a source is blocked by robots.txt, live ingestion skips it and continues.
Set `respect_robots=False` if you are permitted to crawl blocked sources.

## Testing
```bash
pytest
```

## Adding a new agency
1. Create a new adapter in `src/relevance/` implementing `AgencyAdapter`.
2. Register it in `AdapterRegistry`.
3. Register a `SourceConfig` via `CitationDatabaseBuilder.register_sources`.

## Offline fixtures
Fixtures live under `tests/fixtures/{sec,epa,dol}` and are consumed via `fixture://<agency>/<doc>` URLs.
