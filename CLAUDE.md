# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

eFCR is a Python tool for fetching and processing Code of Federal Regulations (CFR) data from the eCFR API (ecfr.gov). It downloads XML files for all 50 CFR titles, converts them to Markdown, and generates word count statistics.

## Commands

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run
python fetch_titles.py
```

## Architecture

The project uses `fetch_titles.py` with four OOP classes:

- **ECFRDatabase**: SQLite operations for titles, agencies, and word counts. Stores metadata in `data_cache/ecfr.db`.
- **ECFRClient**: API requests with exponential backoff retry logic (max 7 retries, 3s base delay).
- **MarkdownConverter**: Converts eCFR XML to Markdown, tracking word counts by hierarchy level.
- **ECFRFetcher**: Main orchestrator that coordinates fetching and processing with parallel workers.

**Data flow:**
1. Fetch titles metadata from `/api/versioner/v1/titles.json`
2. Fetch agencies metadata from `/api/admin/v1/agencies.json`
3. Download full XML for each title using `/api/versioner/v1/full/{date}/title-{n}.xml`
4. Convert XML to Markdown and count words by hierarchy (title/chapter/subchapter/part/subpart)
5. Output Markdown files and `word_counts.csv` to `data_cache/`

**Caching:** Files are considered fresh if modified today. Database and markdown files are cached to avoid re-fetching.

## Dependencies

- `requests` - HTTP client for API calls
- `lxml` - XML parsing

## eCFR API

Base URL: `https://www.ecfr.gov/api`

Endpoints used:
- `versioner/v1/titles.json` - metadata including `latest_issue_date` for each title
- `versioner/v1/full/{date}/title-{n}.xml` - full XML for a title on a specific date
- `admin/v1/agencies.json` - agency metadata with CFR references

See `docs/api-review-versioner-full-xml.md` for API documentation analysis.
