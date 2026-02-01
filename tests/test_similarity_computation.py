#!/usr/bin/env python3
"""Tests for on-demand similarity computation."""

import tempfile
from pathlib import Path

import pytest

from ecfr import ECFRDatabase


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


class TestOnDemandSimilarity:
    """Test on-demand similarity computation."""

    def test_get_similar_sections_returns_results(self, temp_db, sample_sections):
        """Test finding similar sections for a given section."""
        temp_db.save_sections(sample_sections, year=0)

        similar = temp_db.get_similar_sections(title=1, section="1.1", limit=5)

        assert isinstance(similar, list)
        assert len(similar) > 0
        for item in similar:
            assert "title" in item
            assert "section" in item
            assert "similarity" in item
            assert 0 < item["similarity"] <= 1

    def test_get_similar_sections_excludes_self(self, temp_db, sample_sections):
        """Test that query section is not in results."""
        temp_db.save_sections(sample_sections, year=0)

        similar = temp_db.get_similar_sections(title=1, section="1.1", limit=10)

        sections = [(item["title"], item["section"]) for item in similar]
        assert (1, "1.1") not in sections

    def test_get_similar_sections_respects_limit(self, temp_db, sample_sections):
        """Test that limit is respected."""
        temp_db.save_sections(sample_sections, year=0)

        similar = temp_db.get_similar_sections(title=1, section="1.1", limit=2)

        assert len(similar) <= 2

    def test_get_similar_sections_min_similarity(self, temp_db, sample_sections):
        """Test filtering by minimum similarity."""
        temp_db.save_sections(sample_sections, year=0)

        similar = temp_db.get_similar_sections(
            title=1, section="1.1", min_similarity=0.5, limit=10
        )

        for item in similar:
            assert item["similarity"] >= 0.5

    def test_get_similar_sections_empty_for_missing(self, temp_db, sample_sections):
        """Test that missing section returns empty list."""
        temp_db.save_sections(sample_sections, year=0)

        similar = temp_db.get_similar_sections(title=99, section="999.999", limit=5)

        assert similar == []

    def test_get_similar_sections_sorted_by_similarity(self, temp_db, sample_sections):
        """Test that results are sorted by similarity descending."""
        temp_db.save_sections(sample_sections, year=0)

        similar = temp_db.get_similar_sections(title=1, section="1.1", limit=10)

        if len(similar) > 1:
            similarities = [item["similarity"] for item in similar]
            assert similarities == sorted(similarities, reverse=True)

    def test_get_similar_sections_historical_year(self, temp_db, sample_sections):
        """Test similarity works for historical years."""
        temp_db.save_sections(sample_sections, year=2020)

        similar = temp_db.get_similar_sections(title=1, section="1.1", year=2020, limit=5)

        assert isinstance(similar, list)
        assert len(similar) > 0

    def test_get_similar_sections_different_years_isolated(self, temp_db, sample_sections):
        """Test that sections from different years are isolated."""
        temp_db.save_sections(sample_sections, year=0)
        temp_db.save_sections(sample_sections, year=2020)

        similar_current = temp_db.get_similar_sections(title=1, section="1.1", year=0, limit=10)
        similar_2020 = temp_db.get_similar_sections(title=1, section="1.1", year=2020, limit=10)

        # Results should be similar but computed independently
        assert len(similar_current) > 0
        assert len(similar_2020) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
