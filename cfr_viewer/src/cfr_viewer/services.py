"""Service layer for Flask views."""
from flask import current_app
from ecfr.database import ECFRDatabase

def get_database() -> ECFRDatabase:
    return current_app.ecfr_database

def list_titles_with_metadata(year: int = 0) -> list[dict]:
    db = get_database()
    meta = db.get_titles()
    return [{"number": n, "name": meta.get(n, {}).get("name", f"Title {n}"), "word_count": db.get_total_words(n, year)} for n in sorted(db.list_titles(year))]
