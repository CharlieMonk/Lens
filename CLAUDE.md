# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

eCFR is a Python tool for fetching and processing Code of Federal Regulations (CFR) data from ecfr.gov. It has three main components:

1. **CFR Data Fetcher** (`fetch_titles.py`) - Downloads and processes all 50 CFR titles from eCFR and govinfo bulk data
2. **CFR Web Viewer** (`cfr_viewer/`) - Flask web application for browsing CFR data
3. **Enforcement Relevance** (`relevance/`) - Identifies most-enforced CFR sections by ingesting agency enforcement actions

## Commands

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -e .                    # Install with relevance subproject
pip install -e ".[dev]"             # Include dev dependencies

# Fetch CFR data
python fetch_titles.py              # Fetch current + historical (default)
python fetch_titles.py --current    # Fetch only current data
python fetch_titles.py --historical # Fetch only historical years

# Run web viewer
cfr-viewer                          # Starts Flask at localhost:5000

# Run tests
pytest                              # All tests
pytest tests/                       # Core ecfr tests
pytest cfr_viewer/tests/            # Web viewer tests
pytest relevance/tests/             # Relevance tests only
pytest -m "not integration"         # Skip slow integration tests
pytest tests/test_file.py::TestClass::test_method -v  # Run single test
```

## Architecture

### CFR Fetcher (`fetch_titles.py`)

Four classes handle data fetching:

- **ECFRDatabase** (`ecfr/database.py`): SQLite persistence and query interface. Handles titles, agencies, sections, and word counts. Stores in `ecfr/ecfr_data/ecfr.db`. Also provides all read operations (navigate, search, get_structure, get_section, get_similar_sections). Similarity search uses TF-IDF computed on-demand per chapter.
- **ECFRClient**: Async HTTP requests to eCFR API and govinfo bulk endpoints. Uses exponential backoff retry (max 7 retries, 3s base delay). Races both sources in parallel, taking first success.
- **XMLExtractor**: Extracts section data directly from eCFR/govinfo XML. Tracks word counts and extracts section data (title/chapter/part/section/text).
- **ECFRFetcher**: Main orchestrator coordinating parallel fetching. Processes current and historical years sequentially to manage memory.

Data flow:
1. Fetch titles metadata from eCFR API
2. Fetch agencies metadata (for chapter-to-agency mapping)
3. Race eCFR and govinfo endpoints for each title XML
4. Extract sections from XML and save to SQLite

### CFR Web Viewer (`cfr_viewer/`)

Flask application for browsing CFR data:
- `app.py` - Flask app factory, registers blueprints, stores database instance on app
- `services.py` - Service layer wrapping ECFRDatabase
- `routes_browse.py` - Browse views: titles index, title structure, section detail
- `routes_statistics.py` - Word count statistics by agencies and titles (URL: `/statistics`)
- `routes_compare.py` - Compare sections across years
- `routes_api.py` - HTMX partials for similar sections

### Relevance Subproject (`relevance/src/relevance/`)

Flat module structure using domain-driven design:

- **domain_models.py**: Core types (Agency, Source, Document, Citation, CitationType)
- **builder.py**: `CitationDatabaseBuilder` orchestrates database construction from sources
- **adapters_*.py**: Agency-specific HTML/RSS parsers (SEC, EPA, DOL)
- **adapters_registry.py**: Maps agencies to their adapters
- **application_*.py**: Services (ingestion, citation extraction, counting)
- **infrastructure_*.py**: Database, ORM, HTTP fetching, fixtures

Usage:
```python
from relevance import CitationDatabaseBuilder

builder = CitationDatabaseBuilder("sqlite:///data/relevance.sqlite")
builder.build_live_db(respect_robots=True)  # Live from agency RSS feeds
# or
builder.build_offline_starter_db(Path("relevance/tests/fixtures"))  # From fixtures
top = builder.top_citations(limit=10)
```

## eCFR API

Base URL: `https://www.ecfr.gov/api`

Key endpoints:
- `versioner/v1/titles.json` - Titles metadata
- `versioner/v1/full/{date}/title-{n}.xml` - Full title XML
- `admin/v1/agencies.json` - Agency metadata

Govinfo bulk (faster for historical):
- `https://www.govinfo.gov/bulkdata/CFR/{year}/title-{n}/CFR-{year}-title{n}-vol{vol}.xml`

## Database Schema

Main tables in `ecfr/ecfr_data/ecfr.db`:
- `titles` - CFR title metadata
- `agencies` - Agency names and relationships
- `cfr_references` - Maps agencies to CFR chapters
- `sections` - Full section text with hierarchy (year, title, chapter, part, section)
- `agency_word_counts` - Denormalized word counts per agency-title-chapter

## Testing

The project uses pytest with Playwright for verification tests:
- `tests/` - Unit tests for ecfr package (client, database, extractor, fetcher)
- `tests/test_fetcher_integration.py` - Integration test comparing fetched data against production DB (marked `@integration`, `@slow`)
- `cfr_viewer/tests/` - Web viewer route tests
- `cfr_viewer/tests/test_user_stories.py` - Playwright E2E tests (require running server)
- `relevance/tests/` - Unit and integration tests using offline fixtures
