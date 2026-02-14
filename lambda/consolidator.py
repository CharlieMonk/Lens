"""
Consolidator script for ECS Fargate.
Reads section files from S3 and writes to SQLite + builds FAISS index.

Memory-efficient streaming approach:
- Process one file at a time (no parallel accumulation)
- Stream gzip decompression
- Insert and free memory immediately
- SQLite bulk mode pragmas for fast inserts
"""

import json
import gzip
import io
import os
import time
import urllib.request
import boto3
from pathlib import Path

from ecfr.database import ECFRDatabase

s3 = boto3.client('s3')

S3_BUCKET = os.environ.get('S3_BUCKET')
DATABASE_PATH = os.environ.get('ECFR_DATABASE_PATH', '/data/ecfr.db')
OUTPUT_DIR = os.environ.get('ECFR_OUTPUT_DIR', '/data')

ECFR_API_URL = "https://www.ecfr.gov/api"

DB_BATCH_SIZE = 5000


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


def fetch_agencies_metadata():
    """Fetch agencies metadata from eCFR API."""
    url = f"{ECFR_API_URL}/admin/v1/agencies.json"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'eCFR-Consolidator/1.0'})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read())
            return data.get("agencies", [])
    except Exception as e:
        print(f"Warning: Could not fetch agencies metadata: {e}")
        return []


def stream_s3_json(key):
    """
    Stream and parse a JSON file from S3.
    Supports both plain JSON and gzipped JSON.
    Returns (year, sections_list) or (year, []) on error.
    """
    try:
        parts = key.split('/')
        year = int(parts[1]) if len(parts) >= 2 else 0

        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body_stream = response['Body']

        if key.endswith('.json.gz'):
            # Stream through gzip decompressor
            with gzip.GzipFile(fileobj=body_stream) as gz:
                sections = json.load(gz)
        else:
            sections = json.load(body_stream)

        return year, sections
    except Exception as e:
        print(f"  Error reading {key}: {e}")
        return 0, []


def process_files_streaming(db, all_keys):
    """
    Process S3 files one at a time with immediate DB insertion.
    Memory-efficient: only one file's data in memory at a time.
    Uses deferred commits (single commit at end) for better performance.
    """
    total_sections = 0
    s3_time = 0
    db_time = 0

    for i, key in enumerate(all_keys):
        # Read one file
        t0 = time.time()
        year, sections = stream_s3_json(key)
        s3_time += time.time() - t0

        if not sections:
            continue

        # Insert immediately in batches (no commit per batch)
        t0 = time.time()
        for j in range(0, len(sections), DB_BATCH_SIZE):
            batch = sections[j:j + DB_BATCH_SIZE]
            db.save_sections_bulk(batch, year=year, commit=False)
        db_time += time.time() - t0

        total_sections += len(sections)

        # Free memory
        del sections

        # Progress every 50 files
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(all_keys)} files ({total_sections:,} sections)")

    return {
        's3_read': s3_time,
        'db_insert': db_time,
        'total_sections': total_sections
    }


def main():
    timings = {}
    total_start = time.time()

    print("=" * 50)
    print("eCFR Consolidator (Streaming)")
    print("=" * 50)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Database: {DATABASE_PATH}")

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    db = ECFRDatabase(DATABASE_PATH)

    # Step 1: Fetch titles metadata
    t0 = time.time()
    print("\n[1/6] Fetching titles metadata...")
    titles = fetch_titles_metadata()
    if titles:
        db.save_titles(titles)
    timings['titles'] = time.time() - t0
    print(f"  {len(titles)} titles - {timings['titles']:.1f}s")

    # Step 1b: Fetch agencies metadata
    t0 = time.time()
    print("\n[2/6] Fetching agencies metadata...")
    agencies = fetch_agencies_metadata()
    if agencies:
        db.save_agencies(agencies)
    timings['agencies'] = time.time() - t0
    print(f"  {len(agencies)} agencies - {timings['agencies']:.1f}s")

    # Step 3: List S3 files
    t0 = time.time()
    print("\n[3/6] Listing S3 files...")
    all_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='sections/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json') or key.endswith('.json.gz'):
                all_keys.append(key)
    timings['list_s3'] = time.time() - t0
    print(f"  {len(all_keys)} files - {timings['list_s3']:.1f}s")

    # Step 3b: Delete all existing sections (to avoid duplicate primary keys)
    print("\n  Clearing existing sections...")
    db._execute("DELETE FROM sections")
    db._execute("DELETE FROM texts")
    print("  Done")

    # Step 4: Enable bulk mode and process files
    print("\n[4/6] Reading S3 and inserting (streaming)...")
    db.begin_bulk_transaction()

    t0 = time.time()
    stats = process_files_streaming(db, all_keys)
    timings['s3_read'] = stats['s3_read']
    timings['db_insert'] = stats['db_insert']
    timings['total_sections'] = stats['total_sections']

    # Commit and restore safe pragmas
    db.end_bulk_transaction()

    print(f"  S3 read: {timings['s3_read']:.1f}s, DB insert: {timings['db_insert']:.1f}s")
    print(f"  Total sections: {timings['total_sections']:,}")

    # Step 5: Build FAISS index
    t0 = time.time()
    print("\n[5/6] Building similarity index...")
    try:
        build_similarity_index(db)
        timings['faiss'] = time.time() - t0
        print(f"  Done - {timings['faiss']:.1f}s")
    except Exception as e:
        timings['faiss'] = time.time() - t0
        print(f"  Failed: {e} - {timings['faiss']:.1f}s")

    # Step 6: Word counts
    t0 = time.time()
    print("\n[6/6] Populating word counts...")
    try:
        db.populate_title_word_counts()
        timings['word_counts'] = time.time() - t0
        print(f"  Done - {timings['word_counts']:.1f}s")
    except Exception as e:
        timings['word_counts'] = time.time() - t0
        print(f"  Failed: {e} - {timings['word_counts']:.1f}s")

    total_time = time.time() - total_start

    print("\n" + "=" * 50)
    print("TIMING SUMMARY")
    print("=" * 50)
    for step, duration in timings.items():
        if step != 'total_sections':
            print(f"  {step}: {duration:.1f}s")
    print(f"  TOTAL: {total_time:.1f}s ({total_time/60:.1f}m)")
    print("=" * 50)


def build_similarity_index(db, year=0, max_features=2000):
    """
    Build FAISS index with memory-efficient batched processing.
    """
    import numpy as np
    import faiss
    from sklearn.feature_extraction.text import TfidfVectorizer
    from ecfr.config import config
    import pickle

    index_path = config.faiss_index_path
    index_path.parent.mkdir(parents=True, exist_ok=True)

    # Get count first to plan batching
    count_result = db._query("""SELECT COUNT(*) FROM sections s
        JOIN texts t ON s.text_hash = t.hash
        WHERE s.year=? AND s.section != '' AND s.text_hash != ''""", (year,))
    n_sections = count_result[0][0]

    if n_sections < 100:
        raise ValueError(f"Not enough sections: {n_sections}")

    print(f"  Processing {n_sections:,} sections...")

    # Fetch sections in batches to control memory
    FETCH_BATCH = 50000
    all_metadata = []
    all_texts = []

    print("  Loading sections from database...")
    offset = 0
    while offset < n_sections:
        rows = db._query("""SELECT s.title, s.chapter, s.section, s.heading, t.content
            FROM sections s JOIN texts t ON s.text_hash = t.hash
            WHERE s.year=? AND s.section != '' AND s.text_hash != ''
            ORDER BY s.title, s.chapter, s.section
            LIMIT ? OFFSET ?""", (year, FETCH_BATCH, offset))

        for title, chapter, section, heading, text in rows:
            all_metadata.append((title, chapter, section, heading))
            all_texts.append(text)

        offset += FETCH_BATCH
        if offset < n_sections:
            print(f"    Loaded {min(offset, n_sections):,}/{n_sections:,} sections")

    # TF-IDF vectorization
    print(f"  Building TF-IDF vectors (max {max_features} features)...")
    vectorizer = TfidfVectorizer(
        stop_words='english',
        max_features=max_features,
        dtype=np.float32,
    )
    tfidf_matrix = vectorizer.fit_transform(all_texts)
    del all_texts  # Free text memory
    dim = tfidf_matrix.shape[1]

    print("  Building FAISS index...")
    if n_sections < 10000:
        vectors = tfidf_matrix.toarray().astype(np.float32)
        faiss.normalize_L2(vectors)
        index = faiss.IndexFlatIP(dim)
        index.add(vectors)
    else:
        nlist = min(config.faiss_nlist, n_sections // 39)
        m = min(32, dim // 4)
        while dim % m != 0 and m > 1:
            m -= 1

        quantizer = faiss.IndexFlatIP(dim)
        index = faiss.IndexIVFPQ(quantizer, dim, nlist, m, 8, faiss.METRIC_INNER_PRODUCT)

        # Train on sample
        print("  Training index quantizers...")
        sample_size = min(20000, n_sections)
        sample_idx = np.random.choice(n_sections, sample_size, replace=False)
        train_vectors = tfidf_matrix[sample_idx].toarray().astype(np.float32)
        faiss.normalize_L2(train_vectors)
        index.train(train_vectors)
        del train_vectors

        # Add vectors in batches
        batch_size = 20000
        print(f"  Adding vectors in batches of {batch_size}...")
        for i in range(0, n_sections, batch_size):
            end = min(i + batch_size, n_sections)
            batch = tfidf_matrix[i:end].toarray().astype(np.float32)
            faiss.normalize_L2(batch)
            index.add(batch)
            del batch  # Free batch memory immediately

    del tfidf_matrix

    # Save index and metadata
    print("  Saving index to disk...")
    faiss.write_index(index, str(index_path))

    metadata_path = index_path.with_suffix('.pkl')
    with open(metadata_path, 'wb') as f:
        pickle.dump({
            'metadata': all_metadata,
            'vectorizer': vectorizer
        }, f)


if __name__ == '__main__':
    main()
