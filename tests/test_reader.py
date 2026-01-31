"""Tests for ecfr/reader.py."""

import tempfile
from pathlib import Path

import pytest

from ecfr.database import ECFRDatabase
from ecfr.reader import ECFRReader


@pytest.fixture
def temp_db_with_data():
    """Create a temporary database with test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = ECFRDatabase(db_path)

        # Add sample sections
        sections = [
            {
                "title": 1,
                "chapter": "I",
                "part": "1",
                "section": "1.1",
                "heading": "Definitions",
                "text": "Federal regulations apply to all agencies and departments.",
                "word_count": 8,
            },
            {
                "title": 1,
                "chapter": "I",
                "part": "1",
                "section": "1.2",
                "heading": "Scope",
                "text": "Federal regulations apply to all federal workers.",
                "word_count": 7,
            },
            {
                "title": 1,
                "chapter": "II",
                "part": "2",
                "section": "2.1",
                "heading": "Purpose",
                "text": "This part establishes requirements for compliance.",
                "word_count": 6,
            },
            {
                "title": 2,
                "chapter": "I",
                "part": "200",
                "section": "200.1",
                "heading": "General",
                "text": "Grants and agreements requirements.",
                "word_count": 4,
            },
        ]
        db.save_sections(sections, year=0)
        db.compute_similarities(title=1, year=0)

        yield ECFRReader(db_path=str(db_path))


class TestECFRReaderInit:
    """Tests for reader initialization."""

    def test_default_path(self):
        """Default path is ecfr/ecfr_data/ecfr.db."""
        reader = ECFRReader()
        assert "ecfr.db" in str(reader._db.db_path)

    def test_custom_path(self):
        """Custom path is accepted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "custom.db"
            ECFRDatabase(db_path)  # Create the database

            reader = ECFRReader(db_path=str(db_path))
            assert reader._db.db_path == db_path


class TestECFRReaderListMethods:
    """Tests for list methods."""

    def test_list_years(self, temp_db_with_data):
        """List available years."""
        years = temp_db_with_data.list_years()
        assert 0 in years

    def test_list_titles(self, temp_db_with_data):
        """List available titles."""
        titles = temp_db_with_data.list_titles(year=0)
        assert 1 in titles
        assert 2 in titles


class TestECFRReaderNavigation:
    """Tests for navigation methods."""

    def test_navigate_to_section(self, temp_db_with_data):
        """Navigate to specific section."""
        result = temp_db_with_data.navigate(title=1, section="1.1")

        assert result is not None
        assert result["heading"] == "Definitions"

    def test_navigate_with_filters(self, temp_db_with_data):
        """Navigate with chapter and part filters."""
        result = temp_db_with_data.navigate(title=1, chapter="I", part="1")

        assert result is not None
        assert result["chapter"] == "I"
        assert result["part"] == "1"

    def test_navigate_not_found(self, temp_db_with_data):
        """Navigate returns None when not found."""
        result = temp_db_with_data.navigate(title=99, section="99.99")
        assert result is None


class TestECFRReaderSearch:
    """Tests for search functionality."""

    def test_search_finds_results(self, temp_db_with_data):
        """Search finds matching sections."""
        results = temp_db_with_data.search("agencies")

        assert len(results) >= 1
        assert any("agencies" in r["snippet"].lower() for r in results)

    def test_search_with_title_filter(self, temp_db_with_data):
        """Search within specific title."""
        results = temp_db_with_data.search("requirements", title=1)

        assert all(r["title"] == 1 for r in results)

    def test_search_no_results(self, temp_db_with_data):
        """Search returns empty list when no matches."""
        results = temp_db_with_data.search("xyznonexistent")
        assert results == []


class TestECFRReaderStructure:
    """Tests for structure methods."""

    def test_get_structure(self, temp_db_with_data):
        """Get title structure."""
        structure = temp_db_with_data.get_structure(title=1)

        assert structure["type"] == "title"
        assert structure["identifier"] == "1"
        assert len(structure["children"]) >= 1

    def test_get_structure_empty(self, temp_db_with_data):
        """Get structure for nonexistent title."""
        structure = temp_db_with_data.get_structure(title=99)
        assert structure == {}


class TestECFRReaderWordCounts:
    """Tests for word count methods."""

    def test_get_word_counts(self, temp_db_with_data):
        """Get word counts for sections."""
        counts = temp_db_with_data.get_word_counts(title=1)

        assert counts["total"] > 0
        assert "1.1" in counts["sections"]

    def test_get_word_counts_with_filter(self, temp_db_with_data):
        """Get word counts filtered by chapter."""
        counts = temp_db_with_data.get_word_counts(title=1, chapter="I")

        # Should only include chapter I sections
        assert "2.1" not in counts["sections"]

    def test_get_total_words(self, temp_db_with_data):
        """Get total words for title."""
        total = temp_db_with_data.get_total_words(title=1)
        assert total > 0


class TestECFRReaderSectionMethods:
    """Tests for section retrieval methods."""

    def test_get_section_heading(self, temp_db_with_data):
        """Get section heading."""
        heading = temp_db_with_data.get_section_heading(title=1, section="1.1")
        assert heading == "Definitions"

    def test_get_section_heading_not_found(self, temp_db_with_data):
        """Get heading for nonexistent section."""
        heading = temp_db_with_data.get_section_heading(title=99, section="99.99")
        assert heading is None

    def test_get_section_text(self, temp_db_with_data):
        """Get section text."""
        text = temp_db_with_data.get_section_text(title=1, section="1.1")
        assert "agencies" in text

    def test_get_section(self, temp_db_with_data):
        """Get full section data."""
        section = temp_db_with_data.get_section(title=1, section="1.1")

        assert section["heading"] == "Definitions"
        assert section["text"] is not None
        assert section["word_count"] > 0

    def test_get_sections(self, temp_db_with_data):
        """Get all sections for title."""
        sections = temp_db_with_data.get_sections(title=1)

        assert len(sections) == 3

    def test_get_sections_with_filter(self, temp_db_with_data):
        """Get sections filtered by part."""
        sections = temp_db_with_data.get_sections(title=1, part="1")

        assert len(sections) == 2
        assert all(s["part"] == "1" for s in sections)


class TestECFRReaderSimilarity:
    """Tests for similarity methods."""

    def test_get_similar_sections(self, temp_db_with_data):
        """Get similar sections."""
        similar = temp_db_with_data.get_similar_sections(title=1, section="1.1")

        assert isinstance(similar, list)
        for item in similar:
            assert "title" in item
            assert "section" in item
            assert "similarity" in item

    def test_get_similar_sections_with_limit(self, temp_db_with_data):
        """Limit number of similar sections."""
        similar = temp_db_with_data.get_similar_sections(title=1, section="1.1", limit=1)

        assert len(similar) <= 1

    def test_get_similar_sections_min_similarity(self, temp_db_with_data):
        """Filter by minimum similarity."""
        similar = temp_db_with_data.get_similar_sections(
            title=1, section="1.1", min_similarity=0.9
        )

        for item in similar:
            assert item["similarity"] >= 0.9

    def test_get_most_similar_pairs(self, temp_db_with_data):
        """Get most similar pairs."""
        pairs = temp_db_with_data.get_most_similar_pairs(min_similarity=0.1)

        assert isinstance(pairs, list)
        for pair in pairs:
            assert "title1" in pair
            assert "section1" in pair
            assert "title2" in pair
            assert "section2" in pair
            assert "similarity" in pair

    def test_get_most_similar_pairs_with_title(self, temp_db_with_data):
        """Filter pairs by title."""
        pairs = temp_db_with_data.get_most_similar_pairs(title=1, min_similarity=0.1)

        for pair in pairs:
            assert pair["title1"] == 1

    def test_find_duplicate_regulations(self, temp_db_with_data):
        """Find duplicate regulations."""
        dupes = temp_db_with_data.find_duplicate_regulations(min_similarity=0.1)

        assert isinstance(dupes, list)
        for dupe in dupes:
            assert "text1" in dupe
            assert "text2" in dupe

    def test_similarity_stats(self, temp_db_with_data):
        """Get similarity statistics."""
        stats = temp_db_with_data.similarity_stats()

        assert "total_pairs" in stats
        assert "titles_with_similarities" in stats
        assert "distribution" in stats
        assert "avg_similarity" in stats


class TestECFRReaderDelegation:
    """Tests to verify reader delegates to database."""

    def test_reader_uses_database(self, temp_db_with_data):
        """Reader methods call database methods."""
        # This is implicitly tested by all other tests,
        # but we can verify the internal structure
        assert hasattr(temp_db_with_data, '_db')
        assert isinstance(temp_db_with_data._db, ECFRDatabase)
