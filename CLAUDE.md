# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

eCFR is a Python tool for fetching and processing Code of Federal Regulations (CFR) data from ecfr.gov. It has three main components:

1. **CFR Data Fetcher** (`ecfr/fetcher.py`) - Downloads and processes all 50 CFR titles from eCFR and govinfo bulk data
2. **CFR Web Viewer** (`cfr_viewer/`) - Flask web application for browsing CFR data
3. **Enforcement Relevance** (`relevance/`) - Identifies most-enforced CFR sections by ingesting agency enforcement actions

## Commands

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -e .                    # Install package
pip install -e ".[dev]"             # Include dev dependencies

# Fetch CFR data (standalone)
python fetch_titles.py              # Fetch current + historical (default)
python fetch_titles.py --current    # Fetch only current data
python fetch_titles.py --historical # Fetch only historical years
python fetch_titles.py --build-index # Build FAISS similarity index only

# Run web viewer (auto-fetches data on startup)
cfr-viewer                          # Starts Flask at localhost:5000

# Run tests
pytest                              # All tests
pytest ecfr/tests/                  # Core ecfr tests
pytest cfr_viewer/tests/            # Web viewer tests
pytest -m "not integration"         # Skip slow integration tests
pytest ecfr/tests/test_database.py::TestClass::test_method -v  # Single test
```

## Architecture

### CFR Fetcher (`ecfr/`)

Four classes handle data fetching:

- **ECFRDatabase** (`database.py`): SQLite persistence and query interface. Handles titles, agencies, sections, word counts. Stores in `~/ecfr_data/ecfr.db`. Provides read operations (navigate, search, get_structure, get_section). Includes FAISS-based global similarity search.
- **ECFRClient** (`client.py`): Async HTTP requests to eCFR API and govinfo bulk endpoints. Uses exponential backoff retry. Races both sources in parallel, taking first success.
- **XMLExtractor** (`extractor.py`): Extracts section data from eCFR/govinfo XML. Tracks word counts and hierarchy.
- **ECFRFetcher** (`fetcher.py`): Main orchestrator coordinating parallel fetching.

Data flow:
1. Fetch titles metadata from eCFR API
2. Fetch agencies metadata (for chapter-to-agency mapping)
3. Race eCFR and govinfo endpoints for each title XML
4. Extract sections from XML and save to SQLite

### CFR Web Viewer (`cfr_viewer/src/cfr_viewer/`)

Flask application for browsing CFR data:
- `app.py` - Flask app factory, registers blueprints, stores database on app
- `services.py` - Service layer with shared helpers:
  - `get_validated_year()` - Extract/validate year from request
  - `get_title_name()` - Title name with fallback
  - `compute_change_vs_baseline()` - Directional change percentage
  - `node_label()` - Display label for structure nodes
  - `navigate_to_path()` - Structure tree navigation
- `routes_browse.py` - Browse views: titles, title structure, section detail
- `routes_agencies.py` - Agency word count statistics (`/agencies/`)
- `routes_compare.py` - Compare sections across years (`/compare/`)
- `routes_chart.py` - Word count trends over time (`/chart/`)
- `routes_api.py` - HTMX partials for similar sections

The `cfr-viewer` entry point runs `python -m ecfr.fetcher` before starting Flask.

### Relevance Subproject (`relevance/src/relevance/`)

Flat module structure using domain-driven design:

- **domain_models.py**: Core types (Agency, Source, Document, Citation, CitationType)
- **builder.py**: `CitationDatabaseBuilder` orchestrates database construction
- **adapters_*.py**: Agency-specific HTML/RSS parsers (SEC, EPA, DOL)
- **application_*.py**: Services (ingestion, citation extraction, counting)
- **infrastructure_*.py**: Database, ORM, HTTP fetching, fixtures

## Configuration

Settings in `config.yaml` with environment variable overrides (prefix `ECFR_`):

- `database.path` - SQLite database location (default: `~/ecfr_data/ecfr.db`)
- `fetcher.historical_years` - Years to fetch: [2025, 2020, 2015, 2010, 2005, 2000]
- `viewer.baseline_year` - Reference year for statistics (default: 2010)
- `similar_sections.global_search` - Enable FAISS global search (default: true)

## eCFR API

Base URL: `https://www.ecfr.gov/api`

Key endpoints:
- `versioner/v1/titles.json` - Titles metadata
- `versioner/v1/full/{date}/title-{n}.xml` - Full title XML
- `admin/v1/agencies.json` - Agency metadata

Govinfo bulk (faster for historical):
- `https://www.govinfo.gov/bulkdata/CFR/{year}/title-{n}/CFR-{year}-title{n}-vol{vol}.xml`

## Database Schema

Main tables in SQLite:
- `titles` - CFR title metadata
- `agencies` - Agency names and relationships
- `cfr_references` - Maps agencies to CFR chapters
- `sections` - Full section text with hierarchy (year, title, chapter, part, section)
- `agency_word_counts` - Denormalized word counts per agency-title-chapter

## Testing

- `ecfr/tests/` - Unit tests for ecfr package (client, database, extractor, fetcher)
- `ecfr/tests/test_fetcher_integration.py` - Integration test (marked `@integration`, `@slow`)
- `cfr_viewer/tests/test_routes.py` - Web viewer route tests (37 tests)
- `cfr_viewer/tests/test_user_stories.py` - Playwright E2E tests (require running server)
- `relevance/tests/` - Unit and integration tests using offline fixtures
