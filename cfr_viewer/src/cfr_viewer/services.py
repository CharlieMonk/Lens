"""Service layer providing ECFRDatabase access for Flask views."""

from flask import current_app

from ecfr.database import ECFRDatabase


def get_database() -> ECFRDatabase:
    """Get the app-level ECFRDatabase instance."""
    return current_app.ecfr_database


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
