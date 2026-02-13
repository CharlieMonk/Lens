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
python -m ecfr.fetcher              # Fetch current + historical (default)
python -m ecfr.fetcher --current    # Fetch only current data
python -m ecfr.fetcher --historical # Fetch only historical years
python -m ecfr.fetcher --build-index # Build FAISS similarity index only

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
- **config** (`config.py`): YAML configuration with environment variable overrides (prefix `ECFR_`).

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
- `routes_search.py` - Full-text search (`/search/`)
- `routes_api.py` - HTMX partials for similar sections and previews

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

### Title Availability Notes

Not all CFR titles are available for all years:

| Title | Issue | Notes |
|-------|-------|-------|
| 35 | Reserved | No regulatory content in any year (reserved for future use) |
| 2, 3, 6 | Not in 2000 | Govinfo bulk data doesn't include these titles for year 2000 |
| 3 | Not in 2005 | Presidential documents, published separately from CFR bulk data |

**Very large titles** (40 EPA, 42 Health, etc.) may timeout when fetching the full XML. The Lambda fetcher handles these by:
1. Fetching the title structure from `/versioner/v1/structure/{date}/title-{n}.json`
2. Downloading each subchapter separately using `?subchapter={id}` parameter
3. Streaming results to /tmp to avoid memory issues

**Year 2025 data**: Govinfo bulk data for 2025 is not yet available. The fetcher falls back to the eCFR historical API, using subchapter-level fetching for large titles.

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

## AWS Deployment

Infrastructure is defined in Terraform under `terraform/`:

### Architecture
- **Elastic Beanstalk**: Flask app with ALB, auto-scaling 1-4 t3.small instances
- **ECS Fargate**: Weekly scheduled task fetches CFR data (Sundays 6 AM UTC)
- **EFS**: Shared filesystem at `/data` for SQLite database and FAISS index
- **Public subnets only**: No NAT Gateway; security groups control access

### Terraform Structure
```
terraform/
├── modules/
│   ├── network/          # VPC, subnets, security groups
│   ├── storage/          # EFS filesystem and access points
│   ├── fetcher/          # ECR, ECS cluster, task definition, EventBridge
│   └── elastic_beanstalk/ # EB app, environment, IAM roles
└── environments/
    └── dev/              # Dev environment configuration
```

### Deployment Commands
```bash
# Deploy infrastructure
cd terraform/environments/dev
terraform init
terraform plan -out=tfplan
terraform apply tfplan

# Build and push fetcher image
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker build -t ecfr-fetcher -f docker/Dockerfile.fetcher .
docker tag ecfr-fetcher:latest <ecr-url>:latest
docker push <ecr-url>:latest

# Run manual fetch
aws ecs run-task --cluster ecfr-dev-fetcher --task-definition ecfr-dev-fetcher \
  --launch-type FARGATE --network-configuration "awsvpcConfiguration={...}"

# Deploy app to EB
python3 -c "import zipfile; ..." # Create app.zip (see deploy.sh)
aws s3 cp app.zip s3://ecfr-dev-deployments/app-v1.zip
aws elasticbeanstalk create-application-version --application-name ecfr-dev --version-label v1 --source-bundle S3Bucket=...,S3Key=...
aws elasticbeanstalk update-environment --environment-name ecfr-dev --version-label v1
```

### Environment Variables
- `ECFR_DATABASE_PATH=/data/ecfr.db` - EFS-mounted database path
- `ECFR_OUTPUT_DIR=/data` - EFS-mounted output directory
- `ECFR_ATOMIC_WRITES=true` - Enable atomic file replacement (for containerized fetcher)

### Atomic Writes
The fetcher supports atomic file replacement for production deployments:
- Set `ECFR_ATOMIC_WRITES=true` to write to `.new` suffixes then rename
- FAISS index hot-reloads when file mtime changes (no restart needed)
