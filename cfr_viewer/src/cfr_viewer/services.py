"""Service layer wrapping ECFRDatabase."""

from pathlib import Path

from flask import current_app, g


def get_db_path() -> str:
    """Get database path from Flask config."""
    return current_app.config.get("ECFR_DB_PATH", str(Path(__file__).parent.parent.parent.parent / "ecfr" / "ecfr_data" / "ecfr.db"))


def get_database():
    """Get or create ECFRDatabase for the current request."""
    if "ecfr_database" not in g:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
        from ecfr.database import ECFRDatabase
        g.ecfr_database = ECFRDatabase(get_db_path())
    return g.ecfr_database


# Service functions wrapping ECFRDatabase methods

def list_years():
    """List available years (0 = current)."""
    return get_database().list_years()


def list_titles(year: int = 0):
    """List all titles for a given year."""
    db = get_database()

    # Get title numbers that have section data
    title_nums = db.list_titles(year)

    # Get title metadata
    titles_meta = db.get_titles()

    result = []
    for num in sorted(title_nums):
        meta = titles_meta.get(num, {})
        result.append({
            "number": num,
            "name": meta.get("name", f"Title {num}"),
            "word_count": db.get_total_words(num, year),
        })
    return result


def get_structure(title: int, year: int = 0):
    """Get hierarchical structure of a title."""
    return get_database().get_structure(title, year)


def get_section(title: int, section: str, year: int = 0):
    """Get full section data including text and word count."""
    return get_database().get_section(title, section, year)


def get_similar_sections(title: int, section: str, year: int = 0, limit: int = 10):
    """Get sections similar to the given one."""
    return get_database().get_similar_sections(title, section, year, limit)


def get_total_words(title: int, year: int = 0):
    """Get total word count for a title."""
    return get_database().get_total_words(title, year)


def get_agency_word_counts():
    """Get word counts aggregated by agency."""
    return get_database().get_agency_word_counts()


def get_agency_lookup():
    """Build (title, chapter) -> agency info mapping."""
    return get_database().build_agency_lookup()


def get_title_metadata():
    """Get metadata for all titles."""
    return get_database().get_titles()
