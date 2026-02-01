#!/usr/bin/env python3
"""Tests for embedding-based similarity computation."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from ecfr import ECFRDatabase, ECFRFetcher


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_ecfr.db"
        yield ECFRDatabase(db_path)


@pytest.fixture
def sample_sections():
    """Sample sections with enough text for similarity computation."""
    return [
        {
            "title": 1,
            "chapter": "I",
            "part": "1",
            "section": "1.1",
            "heading": "Definitions",
            "text": "This section defines terms used throughout this chapter. "
                    "Regulations apply to all persons subject to federal law.",
        },
        {
            "title": 1,
            "chapter": "I",
            "part": "1",
            "section": "1.2",
            "heading": "Scope",
            "text": "This section describes the scope of regulations. "
                    "Regulations apply to all persons subject to federal requirements.",
        },
        {
            "title": 1,
            "chapter": "I",
            "part": "2",
            "section": "2.1",
            "heading": "Purpose",
            "text": "The purpose of these regulations is to establish standards. "
                    "Federal law requires compliance with all applicable rules.",
        },
        {
            "title": 1,
            "chapter": "II",
            "part": "10",
            "section": "10.1",
            "heading": "General provisions",
            "text": "General provisions for administrative procedures. "
                    "All agencies must follow these guidelines for rulemaking.",
        },
        {
            "title": 1,
            "chapter": "II",
            "part": "10",
            "section": "10.2",
            "heading": "Procedures",
            "text": "Administrative procedures for notice and comment rulemaking. "
                    "Agencies must provide public notice before adopting rules.",
        },
    ]


class TestSimilarityComputation:
    """Test that embeddings are computed when data is added."""

    def test_save_sections_stores_data(self, temp_db, sample_sections):
        """Verify sections are saved to the database."""
        temp_db.save_sections(sample_sections, year=0)

        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sections WHERE year = 0")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == len(sample_sections)

    def test_compute_similarities_creates_embeddings(self, temp_db, sample_sections):
        """Verify compute_similarities creates embeddings for sections."""
        temp_db.save_sections(sample_sections, year=0)

        # Compute similarities (creates embeddings)
        count = temp_db.compute_similarities(title=1, year=0)

        assert count > 0, "Should create at least one embedding"

        # Verify embeddings are in the database
        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM section_embeddings WHERE year = 0 AND title = 1"
        )
        db_count = cursor.fetchone()[0]
        conn.close()

        assert db_count == count

    def test_embeddings_have_valid_format(self, temp_db, sample_sections):
        """Verify embeddings are stored as valid blobs."""
        temp_db.save_sections(sample_sections, year=0)
        temp_db.compute_similarities(title=1, year=0)

        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT embedding FROM section_embeddings WHERE year = 0 AND title = 1 LIMIT 1"
        )
        row = cursor.fetchone()
        conn.close()

        assert row is not None, "Should have at least one embedding"
        embedding_blob = row[0]
        assert isinstance(embedding_blob, bytes), "Embedding should be bytes"
        assert len(embedding_blob) > 0, "Embedding should not be empty"

    def test_compute_similarities_incremental(self, temp_db, sample_sections):
        """Verify compute_similarities only adds missing embeddings."""
        temp_db.save_sections(sample_sections, year=0)

        # First call should create all embeddings
        count1 = temp_db.compute_similarities(title=1, year=0)
        assert count1 > 0, "Should create embeddings on first call"

        # Second call should not create new embeddings (all exist)
        count2 = temp_db.compute_similarities(title=1, year=0)
        assert count2 == 0, "Should not recompute existing embeddings"

    def test_fetcher_compute_all_similarities(self, sample_sections):
        """Verify ECFRFetcher.compute_all_similarities processes all titles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            fetcher = ECFRFetcher(output_dir=output_dir)

            # Save sections
            fetcher.db.save_sections(sample_sections, year=0)

            # Compute all similarities
            results = fetcher.compute_all_similarities(year=0)

            assert 1 in results, "Title 1 should be processed"
            assert results[1] > 0, "Should create embeddings for title 1"

    def test_historical_year_embeddings(self, temp_db, sample_sections):
        """Verify embeddings work for historical years."""
        # Save sections for a historical year
        temp_db.save_sections(sample_sections, year=2020)

        count = temp_db.compute_similarities(title=1, year=2020)
        assert count > 0, "Should create embeddings for historical year"

        # Verify year is stored correctly
        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT year FROM section_embeddings WHERE title = 1"
        )
        years = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert 2020 in years


class TestECFRReaderSimilarity:
    """Test ECFRReader similarity methods."""

    @pytest.fixture
    def reader_with_data(self, sample_sections):
        """Create a reader with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_ecfr.db"
            db = ECFRDatabase(db_path)
            db.save_sections(sample_sections, year=0)
            db.compute_similarities(title=1, year=0)

            from ecfr import ECFRReader

            reader = ECFRReader(db_path=str(db_path))
            yield reader

    def test_get_similar_sections(self, reader_with_data):
        """Test finding similar sections for a given section."""
        similar = reader_with_data.get_similar_sections(title=1, section="1.1", limit=5)
        assert isinstance(similar, list)
        for item in similar:
            assert "title" in item
            assert "section" in item
            assert "similarity" in item
            assert 0 < item["similarity"] <= 1

    def test_get_similar_sections_respects_limit(self, reader_with_data):
        """Test that limit is respected."""
        similar = reader_with_data.get_similar_sections(title=1, section="1.1", limit=2)
        assert len(similar) <= 2

    def test_get_similar_sections_min_similarity(self, reader_with_data):
        """Test filtering by minimum similarity."""
        similar = reader_with_data.get_similar_sections(
            title=1, section="1.1", min_similarity=0.9, limit=10
        )
        for item in similar:
            assert item["similarity"] >= 0.9


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
