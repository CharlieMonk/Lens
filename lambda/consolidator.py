"""
Consolidator script for ECS Fargate.
Reads JSON files from S3 and writes to SQLite + builds FAISS index.
"""

import json
import os
import urllib.request
import boto3
from pathlib import Path

# Import from ecfr package
from ecfr.database import ECFRDatabase
from ecfr.config import config

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


def main():
    print("=" * 50)
    print("eCFR Consolidator")
    print("=" * 50)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Database: {DATABASE_PATH}")
    print(f"Output Dir: {OUTPUT_DIR}")

    # Ensure output directory exists
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Initialize database
    db = ECFRDatabase(DATABASE_PATH)

    # Fetch and save titles metadata from eCFR API
    print("\nFetching titles metadata from eCFR API...")
    titles = fetch_titles_metadata()
    if titles:
        print(f"  Found {len(titles)} titles")
        db.save_titles(titles)
        print("  Saved titles metadata to database")
    else:
        print("  Warning: No titles metadata retrieved")

    # List all JSON files in S3
    sections_by_year = {}

    print("\nReading sections from S3...")
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='sections/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                print(f"  Reading {key}")

                response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                content = response['Body'].read()
                sections = json.loads(content)

                # Parse year from key: sections/{year}/title-{n}.json
                parts = key.split('/')
                if len(parts) >= 2:
                    year = int(parts[1])
                    if year not in sections_by_year:
                        sections_by_year[year] = []
                    sections_by_year[year].extend(sections)

    print(f"\nFound sections for years: {sorted(sections_by_year.keys())}")

    # Insert sections into database
    total_sections = 0
    for year, sections in sorted(sections_by_year.items()):
        print(f"\nInserting {len(sections)} sections for year {year}...")
        db.save_sections(sections, year=year)
        total_sections += len(sections)

    print(f"\nInserted {total_sections} total sections")

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
