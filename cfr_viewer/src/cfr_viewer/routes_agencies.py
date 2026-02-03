"""Agency routes for word count statistics."""
from flask import Blueprint, render_template, request, redirect, url_for
from .services import get_database, compute_change_pct, BASELINE_YEAR

agencies_bp = Blueprint("agencies", __name__)


@agencies_bp.route("/")
def index():
    db = get_database()
    years = db.list_years()
    year = request.args.get("year", 0, type=int)
    if year not in years:
        year = 0

    stats = db.get_statistics_data(BASELINE_YEAR, year)
    counts = stats["agency_counts"][year]
    baseline_counts = stats["agency_counts"].get(BASELINE_YEAR, {})
    details = stats["agency_details"]

    # Flip change direction if viewing year before baseline
    before_baseline = year and year < BASELINE_YEAR
    agencies_list = []
    for s, wc in counts.items():
        if before_baseline:
            change_pct = compute_change_pct(baseline_counts.get(s), wc) if wc else None
        else:
            change_pct = compute_change_pct(wc, baseline_counts.get(s))
        agencies_list.append({"slug": s, "name": details.get(s, {}).get("name", s), "abbreviation": details.get(s, {}).get("short_name") or "", "word_count": wc, "change_pct": change_pct})

    return render_template("agencies/index.html", agencies=sorted(agencies_list, key=lambda x: x["word_count"], reverse=True), years=years, year=year)


@agencies_bp.route("/<slug>")
def detail(slug: str):
    db = get_database()
    years = db.list_years()
    year = request.args.get("year", 0, type=int)
    if year not in years:
        year = 0

    agency = db.get_agency(slug)
    if not agency:
        return render_template("agencies/detail.html", agency=None, chapters=[], chapter_stats=[], years=years, year=year)

    chapters = db.get_agency_chapters(slug)
    counts_year = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, year)}
    baseline_counts = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, BASELINE_YEAR)}

    # Flip change direction if viewing year before baseline
    before_baseline = year and year < BASELINE_YEAR
    chapter_stats = []
    for ch in chapters:
        key = (ch["title"], ch["chapter"])
        wc = counts_year.get(key, 0)
        if before_baseline:
            change_pct = compute_change_pct(baseline_counts.get(key), wc) if wc else None
        else:
            change_pct = compute_change_pct(wc, baseline_counts.get(key))
        chapter_stats.append({
            "title": ch["title"],
            "chapter": ch["chapter"],
            "title_name": ch.get("title_name", ""),
            "word_count": wc,
            "change_pct": change_pct
        })

    return render_template("agencies/detail.html", agency=agency, chapters=chapters, chapter_stats=sorted(chapter_stats, key=lambda x: x["word_count"], reverse=True), years=years, year=year)
