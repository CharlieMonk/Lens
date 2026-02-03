"""Pytest configuration for cfr_viewer tests."""

import pytest
import tempfile
import sqlite3
from pathlib import Path

# Add parent to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ecfr.database import ECFRDatabase


@pytest.fixture
def test_db():
    """Create a temporary test database with minimal data."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    # Use ECFRDatabase to create schema (ensures it stays in sync)
    db = ECFRDatabase(db_path)

    # Insert test data using database methods where available
    db.save_titles([
        {"number": 1, "name": "General Provisions"},
        {"number": 2, "name": "Grants and Agreements"},
    ])

    db.save_sections([
        {"title": 1, "subtitle": "", "chapter": "", "subchapter": "", "part": "1", "subpart": "", "section": "1.1", "heading": "Purpose", "text": "This part establishes general provisions for administrative procedures and regulatory compliance.", "word_count": 11},
        {"title": 1, "subtitle": "", "chapter": "", "subchapter": "", "part": "1", "subpart": "", "section": "1.2", "heading": "Definitions", "text": "Terms used in this part for administrative procedures and regulatory standards.", "word_count": 11},
    ], year=0)

    db.save_agencies([{"slug": "test-agency", "name": "Test Agency", "short_name": "TA"}])

    # Insert word counts directly (no dedicated save methods for these)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO agency_word_counts (year, agency_slug, title, chapter, word_count) VALUES (0, 'test-agency', 1, 'I', 1000)")
    cursor.execute("INSERT INTO title_word_counts (year, title, word_count) VALUES (0, 1, 1000)")
    conn.commit()
    conn.close()

    yield str(db_path)

    # Cleanup
    db_path.unlink(missing_ok=True)


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
