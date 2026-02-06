# Lens

A Python toolkit for fetching, storing, and browsing Code of Federal Regulations (CFR) data from ecfr.gov and govinfo.gov.

**Lens: [https://lens.charlesmonk.org](https://lens.charlesmonk.org)**

## Features

- **Bulk Data Fetching**: Downloads all 50 CFR titles from both eCFR API and govinfo bulk endpoints, racing both sources for faster retrieval
- **Historical Data**: Stores CFR data across multiple years (2000, 2005, 2010, 2015, 2020, 2025) for historical comparison
- **Web Viewer**: Flask-based interface for browsing titles, sections, and agency statistics
- **Similar Sections**: FAISS-powered similarity search to find related CFR sections using TF-IDF vectors
- **Word Count Analytics**: Track regulation growth by agency, title, and chapter over time
- **Section Comparison**: Side-by-side diff view comparing sections across years

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/eCFR.git
cd eCFR

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install package
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

## Quick Start

```bash
# Start the web viewer (auto-fetches data on first run)
cfr-viewer
```

The web viewer will be available at http://localhost:5000.

## Usage

### Web Viewer

```bash
cfr-viewer                          # Start Flask server at localhost:5000
```

The viewer provides:

- **Browse**: Navigate CFR titles, chapters, parts, and sections
- **Agencies**: View word count statistics by regulatory agency
- **Compare**: Side-by-side section comparison across years with diff highlighting
- **Charts**: Word count trends over time for titles and agencies
- **Similar Sections**: Find related regulatory text using semantic similarity

### Data Fetching (Standalone)

```bash
python fetch_titles.py              # Fetch current + historical data
python fetch_titles.py --current    # Fetch only current data
python fetch_titles.py --historical # Fetch only historical years
python fetch_titles.py --build-index # Build FAISS similarity index only
```

## Architecture

### Core Package (`ecfr/`)

| Module | Description |
|--------|-------------|
| `database.py` | SQLite persistence and query interface with FAISS similarity search |
| `client.py` | Async HTTP client for eCFR API and govinfo bulk endpoints |
| `extractor.py` | XML parser for extracting sections and metadata |
| `fetcher.py` | Orchestrator for parallel data fetching |
| `config.py` | Configuration loading from YAML and environment variables |

### Web Viewer (`cfr_viewer/`)

| Module | Description |
|--------|-------------|
| `app.py` | Flask application factory |
| `services.py` | Shared helpers for year validation, navigation, and calculations |
| `routes_browse.py` | Title and section browsing |
| `routes_agencies.py` | Agency statistics views |
| `routes_compare.py` | Year-over-year comparison |
| `routes_chart.py` | Word count trend charts |
| `routes_api.py` | HTMX partials for dynamic content |

## Configuration

Settings are stored in `config.yaml` with environment variable overrides using the `ECFR_` prefix.

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `database.path` | `~/ecfr_data/ecfr.db` | SQLite database location |
| `flask.port` | `5000` | Web server port |
| `fetcher.max_workers` | `5` | Concurrent fetch operations |
| `viewer.baseline_year` | `2010` | Reference year for statistics |
| `similar_sections.global_search` | `true` | Enable FAISS global search |

### Environment Variables

```bash
ECFR_DATABASE_PATH=/path/to/ecfr.db
ECFR_FLASK_PORT=8080
ECFR_BASELINE_YEAR=2015
```

## Database Schema

| Table | Description |
|-------|-------------|
| `titles` | CFR title metadata (number, name, last updated) |
| `agencies` | Agency names and parent relationships |
| `cfr_references` | Maps agencies to CFR chapters |
| `sections` | Full section text with hierarchy (year, title, chapter, part, section) |
| `agency_word_counts` | Denormalized word counts per agency-title-chapter |

## Data Sources

### eCFR API

Base URL: `https://www.ecfr.gov/api`

- `versioner/v1/titles.json` - Titles metadata
- `versioner/v1/full/{date}/title-{n}.xml` - Full title XML
- `admin/v1/agencies.json` - Agency metadata

### GovInfo Bulk Data

Faster for historical data:

- `https://www.govinfo.gov/bulkdata/CFR/{year}/title-{n}/CFR-{year}-title{n}-vol{vol}.xml`

## Testing

```bash
# Run all tests
pytest

# Run specific test suites
pytest ecfr/tests/                  # Core package tests
pytest cfr_viewer/tests/            # Web viewer tests

# Skip slow integration tests
pytest -m "not integration"

# Run a single test
pytest ecfr/tests/test_database.py::TestClass::test_method -v
```

### Test Structure

- `ecfr/tests/` - Unit tests for client, database, extractor, and fetcher
- `cfr_viewer/tests/test_routes.py` - Route handler tests
- `cfr_viewer/tests/test_user_stories.py` - Playwright end-to-end tests

## Requirements

- Python 3.10+
- Dependencies: requests, lxml, flask, aiohttp, scikit-learn, faiss-cpu, pyyaml, playwright

## License

See LICENSE file for details.
