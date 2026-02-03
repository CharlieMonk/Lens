"""Agency routes for word count statistics."""
from flask import Blueprint, render_template, request, redirect, url_for
from .services import get_database, compute_change_pct, BASELINE_YEAR

agencies_bp = Blueprint("agencies", __name__)


@agencies_bp.route("/")
def index():
    db = get_database()
    stats = db.get_statistics_data(BASELINE_YEAR)
    counts = stats["agency_counts"][0]
    baseline_counts = stats["agency_counts"].get(BASELINE_YEAR, {})
    details = stats["agency_details"]
    agencies_list = [{"slug": s, "name": details.get(s, {}).get("name", s), "abbreviation": details.get(s, {}).get("short_name") or "", "word_count": wc,
                      "change_pct": compute_change_pct(wc, baseline_counts.get(s))} for s, wc in counts.items()]
    return render_template("agencies/index.html", agencies=sorted(agencies_list, key=lambda x: x["word_count"], reverse=True))


@agencies_bp.route("/<slug>")
def detail(slug: str):
    db = get_database()
    agency = db.get_agency(slug)
    if not agency:
        return render_template("agencies/detail.html", agency=None, chapters=[], chapter_stats=[])

    chapters = db.get_agency_chapters(slug)
    counts_current = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, 0)}
    baseline_counts = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, BASELINE_YEAR)}

    chapter_stats = []
    for ch in chapters:
        key = (ch["title"], ch["chapter"])
        wc = counts_current.get(key, 0)
        chapter_stats.append({
            "title": ch["title"],
            "chapter": ch["chapter"],
            "title_name": ch.get("title_name", ""),
            "word_count": wc,
            "change_pct": compute_change_pct(wc, baseline_counts.get(key))
        })

    return render_template("agencies/detail.html", agency=agency, chapters=chapters, chapter_stats=sorted(chapter_stats, key=lambda x: x["word_count"], reverse=True))
