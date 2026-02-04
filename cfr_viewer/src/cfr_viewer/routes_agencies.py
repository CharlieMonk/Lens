"""Agency routes for word count statistics."""
from flask import Blueprint, render_template, request, redirect, url_for
from .services import get_database, get_validated_year, compute_change_vs_baseline, BASELINE_YEAR

agencies_bp = Blueprint("agencies", __name__)


@agencies_bp.route("/")
def index():
    db = get_database()
    year = get_validated_year()

    stats = db.get_statistics_data(BASELINE_YEAR, year)
    counts = stats["agency_counts"][year]
    baseline_counts = stats["agency_counts"].get(BASELINE_YEAR, {})
    details = stats["agency_details"]

    agencies_list = [
        {"slug": s, "name": details.get(s, {}).get("name", s), "abbreviation": details.get(s, {}).get("short_name") or "",
         "word_count": wc, "change_pct": compute_change_vs_baseline(wc, baseline_counts.get(s), year)}
        for s, wc in counts.items()
    ]

    return render_template("agencies/index.html", agencies=sorted(agencies_list, key=lambda x: x["word_count"], reverse=True), years=db.list_years(), year=year)


@agencies_bp.route("/<slug>")
def detail(slug: str):
    db = get_database()
    year = get_validated_year()
    years = db.list_years()

    agency = db.get_agency(slug)
    if not agency:
        return render_template("agencies/detail.html", agency=None, chapters=[], chapter_stats=[], years=years, year=year)

    chapters = db.get_agency_chapters(slug)
    counts_year = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, year)}
    baseline_counts = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, BASELINE_YEAR)}

    chapter_stats = [
        {"title": ch["title"], "chapter": ch["chapter"], "title_name": ch.get("title_name", ""),
         "word_count": counts_year.get((ch["title"], ch["chapter"]), 0),
         "change_pct": compute_change_vs_baseline(counts_year.get((ch["title"], ch["chapter"]), 0), baseline_counts.get((ch["title"], ch["chapter"])), year)}
        for ch in chapters
    ]

    return render_template("agencies/detail.html", agency=agency, chapters=chapters, chapter_stats=sorted(chapter_stats, key=lambda x: x["word_count"], reverse=True), years=years, year=year)
