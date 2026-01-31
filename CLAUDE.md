# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

eFCR is a Python tool for fetching and processing Code of Federal Regulations (CFR) data from ecfr.gov. It has two main components:

1. **CFR Data Fetcher** (`fetch_titles.py`) - Downloads and processes all 50 CFR titles from eCFR and govinfo bulk data
2. **Enforcement Relevance** (`relevance/`) - Identifies most-enforced CFR sections by ingesting agency enforcement actions

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
python fetch_titles.py --similarities  # Compute TF-IDF similarities

# Run tests
pytest                              # All tests
pytest relevance/tests/             # Relevance tests only
pytest test_ecfr_verification.py    # Playwright verification tests
```

## Architecture

### CFR Fetcher (`fetch_titles.py`)

Four classes handle data fetching:

- **ECFRDatabase**: SQLite persistence for titles, agencies, sections, word counts, and TF-IDF similarities. Stores in `ecfr/ecfr_data/ecfr.db`.
- **ECFRClient**: Async HTTP requests to eCFR API and govinfo bulk endpoints. Uses exponential backoff retry (max 7 retries, 3s base delay). Races both sources in parallel, taking first success.
- **MarkdownConverter**: Converts eCFR/govinfo XML to Markdown. Tracks word counts and extracts section data (title/chapter/part/section/text).
- **ECFRFetcher**: Main orchestrator coordinating parallel fetching. Processes current and historical years sequentially to manage memory.

Data flow:
1. Fetch titles metadata from eCFR API
2. Fetch agencies metadata (for chapter-to-agency mapping)
3. Race eCFR and govinfo endpoints for each title XML
4. Convert XML to sections, save to SQLite, delete intermediate Markdown

### CFR Reader (`ecfr_reader.py`)

Query interface for the SQLite database:
- `navigate(title, section, year)` - Navigate to specific CFR location
- `search(query, title, year)` - Full-text search across sections
- `get_structure(title)` - Hierarchy tree (parts/sections)
- `get_section(title, section)` - Full section data with text

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
- `section_similarities` - TF-IDF cosine similarities between sections

## Testing

The project uses pytest with Playwright for verification tests:
- `test_ecfr_verification.py` - Compares local data against ecfr.gov website
- `relevance/tests/` - Unit and integration tests using offline fixtures
