"""
Consolidator script for ECS Fargate.
Reads JSON files from S3 and writes to SQLite + builds FAISS index.

Optimizations:
- Parallel S3 reads with ThreadPoolExecutor
- SQLite bulk mode pragmas for fast inserts
- executemany() for batch inserts
"""

import json
import os
import urllib.request
import boto3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from ecfr.database import ECFRDatabase

s3 = boto3.client('s3')

S3_BUCKET = os.environ.get('S3_BUCKET')
DATABASE_PATH = os.environ.get('ECFR_DATABASE_PATH', '/data/ecfr.db')
OUTPUT_DIR = os.environ.get('ECFR_OUTPUT_DIR', '/data')

ECFR_API_URL = "https://www.ecfr.gov/api"


def fetch_titles_metadata():
    """Fetch titles metadata from eCFR API."""
    url = f"{ECFR_API_URL}/versioner/v1/titles.json"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'eCFR-Consolidator/1.0'})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read())
            return data.get("titles", [])
    except Exception as e:
        print(f"Warning: Could not fetch titles metadata: {e}")
        return []


def read_s3_file(key):
    """Read a single S3 file. Returns (key, year, sections)."""
    try:
        parts = key.split('/')
        year = int(parts[1]) if len(parts) >= 2 else 0
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        sections = json.loads(response['Body'].read())
        return key, year, sections
    except Exception as e:
        print(f"  Error reading {key}: {e}")
        return key, 0, None


def main():
    print("=" * 50)
    print("eCFR Consolidator")
    print("=" * 50)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Database: {DATABASE_PATH}")
    print(f"Output Dir: {OUTPUT_DIR}")

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    db = ECFRDatabase(DATABASE_PATH)

    # Fetch and save titles metadata
    print("\nFetching titles metadata from eCFR API...")
    titles = fetch_titles_metadata()
    if titles:
        print(f"  Found {len(titles)} titles")
        db.save_titles(titles)

    # List all S3 files
    print("\nListing S3 files...")
    all_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='sections/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                all_keys.append(obj['Key'])
    print(f"  Found {len(all_keys)} files")

    # Read S3 files in parallel
    print("\nReading S3 files (20 parallel workers)...")
    sections_by_year = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(read_s3_file, key) for key in all_keys]
        for i, future in enumerate(as_completed(futures)):
            key, year, sections = future.result()
            if sections:
                if year not in sections_by_year:
                    sections_by_year[year] = []
                sections_by_year[year].extend(sections)
            if (i + 1) % 50 == 0:
                print(f"  Read {i + 1}/{len(all_keys)} files...")

    print(f"\nFound sections for years: {sorted(sections_by_year.keys())}")

    # Enable bulk mode pragmas
    print("\nEnabling bulk insert mode...")
    db._execute("PRAGMA synchronous = OFF")
    db._execute("PRAGMA journal_mode = MEMORY")
    db._execute("PRAGMA cache_size = -64000")

    # Insert sections using bulk method
    total_sections = 0
    for year, sections in sorted(sections_by_year.items()):
        print(f"\nInserting {len(sections):,} sections for year {year}...")
        db.save_sections_bulk(sections, year=year)
        total_sections += len(sections)

    print(f"\nInserted {total_sections:,} total sections")

    # Restore safe pragmas
    db._execute("PRAGMA synchronous = FULL")
    db._execute("PRAGMA journal_mode = DELETE")

    # Build FAISS index
    print("\nBuilding similarity index...")
    try:
        db.build_similarity_index()
        print("Similarity index built successfully")
    except Exception as e:
        print(f"Warning: Could not build similarity index: {e}")

    # Compute word counts
    print("\nPopulating title word counts...")
    try:
        db.populate_title_word_counts()
        print("Title word counts populated successfully")
    except Exception as e:
        print(f"Warning: Could not populate word counts: {e}")

    print("\n" + "=" * 50)
    print("Consolidation complete!")
    print("=" * 50)


if __name__ == '__main__':
    main()
