"""Service layer for Flask views."""
from flask import current_app
from ecfr.database import ECFRDatabase

BASELINE_YEAR = 2010

def get_database() -> ECFRDatabase:
    return current_app.ecfr_database

def compute_change_pct(current: int, baseline: int | None) -> float | None:
    """Compute percentage change from baseline. Returns None if baseline is missing."""
    if baseline is None or baseline == 0:
        return None
    return ((current - baseline) / baseline) * 100

def list_titles_with_metadata(year: int = 0) -> list[dict]:
    db = get_database()
    meta = db.get_titles()
    word_counts = db.get_all_title_word_counts(year)
    baseline_counts = db.get_all_title_word_counts(BASELINE_YEAR)
    return [{"number": n, "name": meta.get(n, {}).get("name", f"Title {n}"), "word_count": word_counts.get(n, 0),
             "change_pct": compute_change_pct(word_counts.get(n, 0), baseline_counts.get(n))} for n in sorted(db.list_titles(year))]
