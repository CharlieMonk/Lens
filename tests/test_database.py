"""Tests for ecfr/database.py."""

import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from ecfr.database import ECFRDatabase


@pytest.fixture
def temp_db():
    """Create a temporary database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        yield ECFRDatabase(db_path)


@pytest.fixture
def sample_titles():
    """Sample titles data."""
    return [
        {
            "number": 1,
            "name": "General Provisions",
            "latest_amended_on": "2024-01-01",
            "latest_issue_date": "2024-01-15",
            "up_to_date_as_of": "2024-01-15",
            "reserved": False,
        },
        {
            "number": 2,
            "name": "Grants and Agreements",
            "latest_amended_on": "2024-01-02",
            "latest_issue_date": "2024-01-16",
            "up_to_date_as_of": "2024-01-16",
            "reserved": False,
        },
    ]


@pytest.fixture
def sample_agencies():
    """Sample agencies data."""
    return [
        {
            "slug": "test-agency",
            "name": "Test Agency",
            "short_name": "TA",
            "display_name": "Test Agency",
            "sortable_name": "Test Agency",
            "cfr_references": [
                {"title": 1, "chapter": "I"},
                {"title": 2, "chapter": "II"},
            ],
            "children": [
                {
                    "slug": "test-sub-agency",
                    "name": "Test Sub Agency",
                    "short_name": "TSA",
                    "display_name": "Test Sub Agency",
                    "sortable_name": "Test Sub Agency",
                    "cfr_references": [{"title": 1, "chapter": "III"}],
                }
            ],
        }
    ]


@pytest.fixture
def sample_sections():
    """Sample sections data."""
    return [
        {
            "title": 1,
            "chapter": "I",
            "part": "1",
            "section": "1.1",
            "heading": "Definitions",
            "text": "Terms used in this part have the following meanings.",
            "word_count": 9,
        },
        {
            "title": 1,
            "chapter": "I",
            "part": "1",
            "section": "1.2",
            "heading": "Scope",
            "text": "This part applies to all federal agencies.",
            "word_count": 7,
        },
        {
            "title": 1,
            "chapter": "II",
            "part": "2",
            "section": "2.1",
            "heading": "Purpose",
            "text": "The purpose of this part is to establish requirements.",
            "word_count": 9,
        },
    ]


class TestECFRDatabaseInit:
    """Tests for database initialization."""

    def test_creates_database_file(self):
        """Database file is created on init."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            ECFRDatabase(db_path)
            assert db_path.exists()

    def test_creates_parent_directories(self):
        """Parent directories are created if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "subdir" / "test.db"
            ECFRDatabase(db_path)
            assert db_path.exists()

    def test_creates_tables(self, temp_db):
        """All expected tables are created."""
        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()

        expected = {"titles", "agencies", "cfr_references", "agency_word_counts",
                    "sections", "section_embeddings"}
        assert expected.issubset(tables)

    def test_creates_indexes(self, temp_db):
        """Indexes are created."""
        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = {row[0] for row in cursor.fetchall()}
        conn.close()

        assert "idx_sections_year_title" in indexes
        assert "idx_embeddings_year_title" in indexes


class TestECFRDatabaseTitles:
    """Tests for titles operations."""

    def test_save_and_get_titles(self, temp_db, sample_titles):
        """Save and retrieve titles."""
        temp_db.save_titles(sample_titles)
        titles = temp_db.get_titles()

        assert len(titles) == 2
        assert 1 in titles
        assert titles[1]["name"] == "General Provisions"

    def test_has_titles_empty(self, temp_db):
        """has_titles returns False when empty."""
        assert not temp_db.has_titles()

    def test_has_titles_populated(self, temp_db, sample_titles):
        """has_titles returns True when populated."""
        temp_db.save_titles(sample_titles)
        assert temp_db.has_titles()

    def test_save_titles_replaces(self, temp_db, sample_titles):
        """Saving titles replaces existing data."""
        temp_db.save_titles(sample_titles)
        temp_db.save_titles([{"number": 3, "name": "New Title"}])

        titles = temp_db.get_titles()
        assert len(titles) == 1
        assert 3 in titles


class TestECFRDatabaseAgencies:
    """Tests for agencies operations."""

    def test_save_and_has_agencies(self, temp_db, sample_agencies):
        """Save agencies and check existence."""
        assert not temp_db.has_agencies()
        temp_db.save_agencies(sample_agencies)
        assert temp_db.has_agencies()

    def test_build_agency_lookup(self, temp_db, sample_agencies):
        """Build agency lookup table."""
        temp_db.save_agencies(sample_agencies)
        lookup = temp_db.build_agency_lookup()

        assert (1, "I") in lookup
        assert lookup[(1, "I")][0]["agency_slug"] == "test-agency"

    def test_agency_children_saved(self, temp_db, sample_agencies):
        """Child agencies are saved correctly."""
        temp_db.save_agencies(sample_agencies)
        lookup = temp_db.build_agency_lookup()

        assert (1, "III") in lookup
        assert lookup[(1, "III")][0]["agency_slug"] == "test-sub-agency"


class TestECFRDatabaseSections:
    """Tests for sections operations."""

    def test_save_sections(self, temp_db, sample_sections):
        """Save sections to database."""
        temp_db.save_sections(sample_sections, year=0)

        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sections WHERE year = 0")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 3

    def test_save_sections_with_year(self, temp_db, sample_sections):
        """Save sections for specific year."""
        temp_db.save_sections(sample_sections, year=2020)

        conn = sqlite3.connect(temp_db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sections WHERE year = 2020")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 3

    def test_has_year_data(self, temp_db, sample_sections):
        """Check if year has data."""
        assert not temp_db.has_year_data(0)
        temp_db.save_sections(sample_sections, year=0)
        assert temp_db.has_year_data(0)

    def test_save_empty_sections(self, temp_db):
        """Saving empty list does nothing."""
        temp_db.save_sections([], year=0)
        assert not temp_db.has_year_data(0)

    def test_get_section(self, temp_db, sample_sections):
        """Get a specific section."""
        temp_db.save_sections(sample_sections, year=0)
        section = temp_db.get_section(title=1, section="1.1", year=0)

        assert section is not None
        assert section["heading"] == "Definitions"

    def test_get_section_not_found(self, temp_db, sample_sections):
        """Get non-existent section returns None."""
        temp_db.save_sections(sample_sections, year=0)
        section = temp_db.get_section(title=1, section="99.99", year=0)

        assert section is None

    def test_get_sections(self, temp_db, sample_sections):
        """Get all sections for a title."""
        temp_db.save_sections(sample_sections, year=0)
        sections = temp_db.get_sections(title=1, year=0)

        assert len(sections) == 3

    def test_get_sections_with_filter(self, temp_db, sample_sections):
        """Get sections filtered by chapter."""
        temp_db.save_sections(sample_sections, year=0)
        sections = temp_db.get_sections(title=1, chapter="I", year=0)

        assert len(sections) == 2


class TestECFRDatabaseQueries:
    """Tests for query operations."""

    def test_list_years(self, temp_db, sample_sections):
        """List available years."""
        temp_db.save_sections(sample_sections, year=0)
        temp_db.save_sections(sample_sections, year=2020)

        years = temp_db.list_years()
        assert 0 in years
        assert 2020 in years

    def test_list_titles(self, temp_db, sample_sections):
        """List titles with sections."""
        temp_db.save_sections(sample_sections, year=0)
        titles = temp_db.list_titles(year=0)

        assert 1 in titles

    def test_navigate(self, temp_db, sample_sections):
        """Navigate to specific location."""
        temp_db.save_sections(sample_sections, year=0)
        result = temp_db.navigate(title=1, chapter="I", part="1", year=0)

        assert result is not None
        assert result["section"] == "1.1"

    def test_search(self, temp_db, sample_sections):
        """Search sections by text."""
        temp_db.save_sections(sample_sections, year=0)
        results = temp_db.search("agencies", title=1, year=0)

        assert len(results) >= 1
        assert any("agencies" in r["snippet"].lower() for r in results)

    def test_search_all_titles(self, temp_db, sample_sections):
        """Search across all titles."""
        temp_db.save_sections(sample_sections, year=0)
        results = temp_db.search("part", year=0)

        assert len(results) >= 1

    def test_get_structure(self, temp_db, sample_sections):
        """Get title structure."""
        temp_db.save_sections(sample_sections, year=0)
        structure = temp_db.get_structure(title=1, year=0)

        assert structure["type"] == "title"
        assert len(structure["children"]) >= 1

    def test_get_word_counts(self, temp_db, sample_sections):
        """Get word counts for sections."""
        temp_db.save_sections(sample_sections, year=0)
        counts = temp_db.get_word_counts(title=1, year=0)

        assert counts["total"] > 0
        assert "1.1" in counts["sections"]

    def test_get_section_heading(self, temp_db, sample_sections):
        """Get section heading."""
        temp_db.save_sections(sample_sections, year=0)
        heading = temp_db.get_section_heading(title=1, section="1.1", year=0)

        assert heading == "Definitions"

    def test_get_section_text(self, temp_db, sample_sections):
        """Get section text."""
        temp_db.save_sections(sample_sections, year=0)
        text = temp_db.get_section_text(title=1, section="1.1", year=0)

        assert "meanings" in text


class TestECFRDatabaseSimilarities:
    """Tests for similarity operations."""

    @pytest.fixture
    def sections_for_similarity(self):
        """Sections with enough text for similarity computation."""
        return [
            {
                "title": 1, "chapter": "I", "part": "1", "section": "1.1",
                "heading": "Definitions",
                "text": "Federal regulations apply to all agencies. "
                        "Requirements must be followed by federal employees.",
            },
            {
                "title": 1, "chapter": "I", "part": "1", "section": "1.2",
                "heading": "Scope",
                "text": "Federal regulations apply to all departments. "
                        "Requirements must be followed by federal workers.",
            },
            {
                "title": 1, "chapter": "I", "part": "1", "section": "1.3",
                "heading": "Purpose",
                "text": "This regulation establishes standards for compliance. "
                        "All entities must adhere to these provisions.",
            },
        ]

    def test_compute_similarities(self, temp_db, sections_for_similarity):
        """Compute similarities for a title."""
        temp_db.save_sections(sections_for_similarity, year=0)
        count = temp_db.compute_similarities(title=1, year=0)

        assert count > 0

    def test_compute_similarities_single_section(self, temp_db):
        """Single section still gets an embedding for cross-title similarity."""
        temp_db.save_sections([{
            "title": 1, "section": "1.1", "text": "Only one section."
        }], year=0)

        count = temp_db.compute_similarities(title=1, year=0)
        assert count == 1

    def test_get_similar_sections(self, temp_db, sections_for_similarity):
        """Get similar sections."""
        temp_db.save_sections(sections_for_similarity, year=0)
        temp_db.compute_similarities(title=1, year=0)

        similar = temp_db.get_similar_sections(title=1, section="1.1", year=0)
        assert len(similar) > 0


class TestECFRDatabaseUtils:
    """Tests for utility methods."""

    def test_is_fresh_new_db(self, temp_db):
        """New database is fresh."""
        assert temp_db.is_fresh()

    def test_clear(self, temp_db, sample_sections):
        """Clear deletes database."""
        temp_db.save_sections(sample_sections, year=0)
        assert temp_db.db_path.exists()

        temp_db.clear()
        assert not temp_db.db_path.exists()

    def test_update_word_counts(self, temp_db, sample_agencies):
        """Update agency word counts."""
        temp_db.save_agencies(sample_agencies)
        lookup = temp_db.build_agency_lookup()

        chapter_counts = {"I": 100, "II": 200}
        temp_db.update_word_counts(1, chapter_counts, lookup)

        counts = temp_db.get_agency_word_counts()
        assert "test-agency" in counts

    def test_get_agency_word_counts_with_parents(self, temp_db, sample_agencies):
        """Agency word counts include parent aggregates."""
        temp_db.save_agencies(sample_agencies)
        lookup = temp_db.build_agency_lookup()

        temp_db.update_word_counts(1, {"III": 50}, lookup)

        counts = temp_db.get_agency_word_counts()
        # Parent should have child's counts
        assert counts.get("test-agency", 0) >= counts.get("test-sub-agency", 0)
