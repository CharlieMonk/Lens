"""
Consolidator script for ECS Fargate.
Reads JSON files from S3 and writes to SQLite + builds FAISS index.
"""

import json
import os
import boto3
from pathlib import Path

# Import from ecfr package
from ecfr.database import ECFRDatabase
from ecfr.config import config

s3 = boto3.client('s3')

S3_BUCKET = os.environ.get('S3_BUCKET')
DATABASE_PATH = os.environ.get('ECFR_DATABASE_PATH', '/data/ecfr.db')
OUTPUT_DIR = os.environ.get('ECFR_OUTPUT_DIR', '/data')


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

        for section in sections:
            db.insert_section(
                year=section.get('year', year),
                title=section['title'],
                chapter=section.get('chapter', ''),
                part=section.get('part', ''),
                section=section['section'],
                heading=section.get('heading', ''),
                text=section.get('text', ''),
                word_count=section.get('word_count', 0)
            )
            total_sections += 1

        # Commit after each year
        db.commit()

    print(f"\nInserted {total_sections} total sections")

    # Build FAISS index
    print("\nBuilding similarity index...")
    try:
        db.build_similarity_index()
        print("Similarity index built successfully")
    except Exception as e:
        print(f"Warning: Could not build similarity index: {e}")

    # Compute word counts
    print("\nComputing agency word counts...")
    try:
        db.compute_agency_word_counts()
        print("Word counts computed successfully")
    except Exception as e:
        print(f"Warning: Could not compute word counts: {e}")

    print("\n" + "=" * 50)
    print("Consolidation complete!")
    print("=" * 50)


if __name__ == '__main__':
    main()
