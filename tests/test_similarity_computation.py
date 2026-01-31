#!/usr/bin/env python3
"""Tests for TF-IDF similarity computation on data ingestion."""

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
    """Sample sections with enough text for TF-IDF to find similarities."""
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
    """Test that similarities are computed when data is added."""

    def test_save_sections_stores_data(self, temp_db, sample_sections):
        """Verify sections are saved to the database."""
        temp_db.save_sections(sample_sections, year=0)

        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sections WHERE year = 0")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == len(sample_sections)

    def test_compute_similarities_creates_pairs(self, temp_db, sample_sections):
        """Verify compute_similarities creates similarity pairs."""
        temp_db.save_sections(sample_sections, year=0)

        # Compute similarities
        count = temp_db.compute_similarities(title=1, year=0, top_n=3)

        assert count > 0, "Should create at least one similarity pair"

        # Verify pairs are in the database
        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM section_similarities WHERE year = 0 AND title = 1"
        )
        db_count = cursor.fetchone()[0]
        conn.close()

        assert db_count == count

    def test_similarities_have_valid_scores(self, temp_db, sample_sections):
        """Verify similarity scores are between 0 and 1."""
        temp_db.save_sections(sample_sections, year=0)
        temp_db.compute_similarities(title=1, year=0)

        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT similarity FROM section_similarities WHERE year = 0 AND title = 1"
        )
        scores = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert all(0 < s <= 1 for s in scores), "Similarity scores should be in (0, 1]"

    def test_compute_similarities_skips_large_titles(self, temp_db):
        """Verify large titles are skipped to avoid memory issues."""
        # Create many sections (more than default max_sections=2000)
        large_sections = [
            {
                "title": 99,
                "chapter": "I",
                "part": str(i // 100),
                "section": f"{i // 100}.{i % 100}",
                "heading": f"Section {i}",
                "text": f"This is section number {i} with some text content for testing.",
            }
            for i in range(2100)
        ]
        temp_db.save_sections(large_sections, year=0)

        # Should return -1 (skipped)
        count = temp_db.compute_similarities(title=99, year=0, max_sections=2000)
        assert count == -1, "Should skip titles with too many sections"

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
            assert results[1] > 0, "Should create similarity pairs for title 1"

    def test_historical_year_similarities(self, temp_db, sample_sections):
        """Verify similarities work for historical years."""
        # Save sections for a historical year
        temp_db.save_sections(sample_sections, year=2020)

        count = temp_db.compute_similarities(title=1, year=2020)
        assert count > 0, "Should create similarities for historical year"

        # Verify year is stored correctly
        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT year FROM section_similarities WHERE title = 1"
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

    def test_get_most_similar_pairs(self, reader_with_data):
        """Test getting most similar section pairs."""
        pairs = reader_with_data.get_most_similar_pairs(limit=10, min_similarity=0.1)
        assert isinstance(pairs, list)
        for pair in pairs:
            assert "title1" in pair
            assert "section1" in pair
            assert "title2" in pair
            assert "section2" in pair
            assert "similarity" in pair

    def test_find_duplicate_regulations(self, reader_with_data):
        """Test finding duplicate regulations."""
        dupes = reader_with_data.find_duplicate_regulations(min_similarity=0.1, limit=10)
        assert isinstance(dupes, list)
        for dupe in dupes:
            assert "title1" in dupe
            assert "text1" in dupe
            assert "title2" in dupe
            assert "text2" in dupe

    def test_similarity_stats(self, reader_with_data):
        """Test similarity statistics."""
        stats = reader_with_data.similarity_stats()
        assert "total_pairs" in stats
        assert "titles_with_similarities" in stats
        assert "distribution" in stats
        assert "avg_similarity" in stats
        assert stats["total_pairs"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
