"""Statistics routes for word count statistics."""
from flask import Blueprint, render_template, request
from .services import get_database

statistics_bp = Blueprint("statistics", __name__)

@statistics_bp.route("/")
def index():
    return render_template("statistics/index.html")

@statistics_bp.route("/agencies")
def agencies():
    db = get_database()
    stats = db.get_statistics_data()
    counts = stats["agency_counts"][0]
    counts_2010 = stats["agency_counts"].get(2010, {})
    details = stats["agency_details"]
    agencies_list = [{"slug": s, "name": details.get(s, {}).get("name", s), "abbreviation": details.get(s, {}).get("short_name") or "", "word_count": wc, "change_pct": ((wc - counts_2010[s]) / counts_2010[s]) * 100 if counts_2010.get(s) else None} for s, wc in counts.items()]
    return render_template("statistics/agencies.html", agencies=sorted(agencies_list, key=lambda x: x["word_count"], reverse=True))

@statistics_bp.route("/agencies/<slug>")
def agency_detail(slug: str):
    db = get_database()
    agency = db.get_agency(slug)
    if not agency:
        return render_template("statistics/agency_detail.html", agency=None, chapters=[], chapter_stats=[])

    chapters = db.get_agency_chapters(slug)
    counts_current = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, 0)}
    counts_2010 = {(c["title"], c["chapter"]): c["word_count"] for c in db.get_agency_chapter_word_counts(slug, 2010)}

    chapter_stats = []
    for ch in chapters:
        key = (ch["title"], ch["chapter"])
        wc = counts_current.get(key, 0)
        wc_2010 = counts_2010.get(key)
        change_pct = ((wc - wc_2010) / wc_2010 * 100) if wc_2010 else None
        chapter_stats.append({
            "title": ch["title"],
            "chapter": ch["chapter"],
            "title_name": ch.get("title_name", ""),
            "word_count": wc,
            "change_pct": change_pct
        })

    return render_template("statistics/agency_detail.html", agency=agency, chapters=chapters, chapter_stats=sorted(chapter_stats, key=lambda x: x["word_count"], reverse=True))

@statistics_bp.route("/titles")
def titles():
    db = get_database()
    year = request.args.get("year", 0, type=int)
    stats = db.get_statistics_data()
    title_counts = db.get_all_title_word_counts(year) if year and year != 0 else stats["title_counts"][0]
    counts_2010 = stats["title_counts"].get(2010, {})
    meta = stats["title_meta"]
    titles_list = [{"number": n, "name": meta.get(n, {}).get("name", f"Title {n}"), "word_count": wc, "change_pct": ((wc - counts_2010.get(n, 0)) / counts_2010[n]) * 100 if counts_2010.get(n) else None} for n, wc in title_counts.items()]
    return render_template("statistics/titles.html", titles=sorted(titles_list, key=lambda x: x["word_count"], reverse=True), year=year, years=db.list_years())
