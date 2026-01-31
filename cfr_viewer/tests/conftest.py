"""Pytest configuration for cfr_viewer tests."""

import pytest
import tempfile
import sqlite3
from pathlib import Path

# Add parent to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


@pytest.fixture
def test_db():
    """Create a temporary test database with minimal data."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables matching ECFRDatabase schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS titles (
            number INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            latest_amended_on TEXT,
            latest_issue_date TEXT,
            up_to_date_as_of TEXT,
            reserved INTEGER NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agencies (
            slug TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            short_name TEXT,
            display_name TEXT,
            sortable_name TEXT,
            parent_slug TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cfr_references (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agency_slug TEXT NOT NULL,
            title INTEGER NOT NULL,
            chapter TEXT,
            subtitle TEXT,
            subchapter TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agency_word_counts (
            agency_slug TEXT NOT NULL,
            title INTEGER NOT NULL,
            chapter TEXT NOT NULL,
            word_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (agency_slug, title, chapter)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sections (
            year INTEGER NOT NULL DEFAULT 0,
            title INTEGER NOT NULL,
            subtitle TEXT NOT NULL DEFAULT '',
            chapter TEXT NOT NULL DEFAULT '',
            subchapter TEXT NOT NULL DEFAULT '',
            part TEXT NOT NULL DEFAULT '',
            subpart TEXT NOT NULL DEFAULT '',
            section TEXT NOT NULL DEFAULT '',
            heading TEXT NOT NULL DEFAULT '',
            text TEXT NOT NULL DEFAULT '',
            word_count INTEGER NOT NULL,
            PRIMARY KEY (year, title, subtitle, chapter, subchapter, part, subpart, section)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS section_similarities (
            year INTEGER NOT NULL,
            title INTEGER NOT NULL,
            section TEXT NOT NULL,
            similar_title INTEGER NOT NULL,
            similar_section TEXT NOT NULL,
            similarity REAL NOT NULL,
            PRIMARY KEY (year, title, section, similar_title, similar_section)
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_year_title ON sections(year, title)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_year_title_section ON sections(year, title, section)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cfr_title_chapter ON cfr_references(title, chapter)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cfr_agency ON cfr_references(agency_slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_word_counts_agency ON agency_word_counts(agency_slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_similarities_source ON section_similarities(year, title, section)")

    # Insert test data
    cursor.execute("""
        INSERT INTO titles (number, name) VALUES (1, 'General Provisions')
    """)
    cursor.execute("""
        INSERT INTO titles (number, name) VALUES (2, 'Grants and Agreements')
    """)

    cursor.execute("""
        INSERT INTO sections (year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count)
        VALUES (0, 1, '', '', '', '1', '', '1.1', 'Purpose', 'This part establishes general provisions.', 6)
    """)
    cursor.execute("""
        INSERT INTO sections (year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count)
        VALUES (0, 1, '', '', '', '1', '', '1.2', 'Definitions', 'Terms used in this chapter.', 5)
    """)

    cursor.execute("""
        INSERT INTO agencies (slug, name, short_name) VALUES ('test-agency', 'Test Agency', 'TA')
    """)
    cursor.execute("""
        INSERT INTO agency_word_counts (agency_slug, title, chapter, word_count) VALUES ('test-agency', 1, 'I', 1000)
    """)

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def app(test_db):
    """Create test Flask application."""
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from cfr_viewer.app import create_app

    app = create_app(db_path=test_db)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()
