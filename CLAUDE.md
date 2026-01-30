# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

eFCR is a Python tool for fetching and processing Code of Federal Regulations (CFR) data from the eCFR API (ecfr.gov). It downloads XML files for all 50 CFR titles, converts them to YAML, and generates word count statistics.

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

The project consists of a single script (`fetch_titles.py`) that:
1. Fetches titles metadata from `/api/versioner/v1/titles.json` to get latest issue dates
2. Downloads full XML for each title using `/api/versioner/v1/full/{date}/title-{n}.xml`
3. Converts XML to YAML and counts words by hierarchy level (title/chapter/subchapter/part/subpart)
4. Outputs YAML files and a `word_counts.csv` to `xml_output/`

Key constants:
- `MAX_WORKERS = 5`: Parallel fetch threads
- `MAX_RETRIES = 7`: Retry count with exponential backoff
- `RETRY_DELAY = 3`: Base delay in seconds (doubles each retry)

## eCFR API

Base URL: `https://www.ecfr.gov/api/versioner/v1`

Endpoints used:
- `titles.json` - metadata including `latest_issue_date` for each title
- `full/{date}/title-{n}.xml` - full XML for a title on a specific date

See `docs/api-review-versioner-full-xml.md` for API documentation analysis.
