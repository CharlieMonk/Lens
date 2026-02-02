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
    results = []
    for n in sorted(db.list_titles(year)):
        wc = word_counts.get(n, 0)
        bc = baseline_counts.get(n)
        if year and year < BASELINE_YEAR:
            # "To BASELINE_YEAR": compute change from year to baseline (denominator is year's count)
            change_pct = compute_change_pct(bc, wc) if wc else None
        else:
            # "Since BASELINE_YEAR": compute change from baseline to year
            change_pct = compute_change_pct(wc, bc)
        results.append({"number": n, "name": meta.get(n, {}).get("name", f"Title {n}"), "word_count": wc, "change_pct": change_pct})
    return results
