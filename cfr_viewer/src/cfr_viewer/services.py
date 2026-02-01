"""Service layer providing ECFRDatabase access for Flask views."""

from flask import current_app, g

from ecfr.database import ECFRDatabase


def get_database() -> ECFRDatabase:
    """Get or create ECFRDatabase for the current request."""
    if "ecfr_database" not in g:
        db_path = current_app.config.get("ECFR_DB_PATH", "ecfr/ecfr_data/ecfr.db")
        g.ecfr_database = ECFRDatabase(db_path)
    return g.ecfr_database


def list_titles_with_metadata(year: int = 0) -> list[dict]:
    """List all titles with metadata and word counts."""
    db = get_database()
    title_nums = db.list_titles(year)
    titles_meta = db.get_titles()

    return [
        {
            "number": num,
            "name": titles_meta.get(num, {}).get("name", f"Title {num}"),
            "word_count": db.get_total_words(num, year),
        }
        for num in sorted(title_nums)
    ]
