"""Statistics routes for word count statistics."""
from flask import Blueprint, render_template, request
from .services import get_database, list_titles_with_metadata

statistics_bp = Blueprint("statistics", __name__)

@statistics_bp.route("/")
def index():
    return render_template("statistics/index.html")

@statistics_bp.route("/agencies")
def agencies():
    db = get_database()
    counts = db.get_agency_word_counts()
    # Get 2020 counts with parent aggregation
    direct_2020 = {r[0]: r[1] for r in db._query("SELECT r.agency_slug, SUM(s.word_count) FROM sections s JOIN cfr_references r ON s.title = r.title AND s.chapter = COALESCE(r.chapter, r.subtitle, r.subchapter) WHERE s.year = 2020 GROUP BY r.agency_slug")}
    parents = {r[0]: r[1] for r in db._query("SELECT slug, parent_slug FROM agencies WHERE parent_slug IS NOT NULL")}
    counts_2020 = dict(direct_2020)
    for child, parent in parents.items():
        if child in direct_2020:
            counts_2020[parent] = counts_2020.get(parent, 0) + direct_2020[child]
    details = {r[0]: {"name": r[1], "short_name": r[2]} for r in db._query("SELECT slug, name, short_name FROM agencies")}
    agencies_list = [{"slug": s, "name": details.get(s, {}).get("name", s), "abbreviation": details.get(s, {}).get("short_name") or "", "word_count": wc, "change_pct": ((wc - counts_2020[s]) / counts_2020[s]) * 100 if counts_2020.get(s) else None} for s, wc in counts.items()]
    return render_template("statistics/agencies.html", agencies=sorted(agencies_list, key=lambda x: x["word_count"], reverse=True))

@statistics_bp.route("/agencies/<slug>")
def agency_detail(slug: str):
    db = get_database()
    agency = db.get_agency(slug)
    return render_template("statistics/agency_detail.html", agency=agency, chapters=db.get_agency_chapters(slug) if agency else [])

@statistics_bp.route("/titles")
def titles():
    db = get_database()
    year = request.args.get("year", 0, type=int)
    titles_list = list_titles_with_metadata(year)
    counts_2020 = {n: db.get_total_words(n, 2020) for n in db.list_titles(2020)}
    for t in titles_list:
        base = counts_2020.get(t["number"])
        t["change_pct"] = ((t["word_count"] - base) / base) * 100 if base else None
    return render_template("statistics/titles.html", titles=sorted(titles_list, key=lambda x: x["word_count"], reverse=True), year=year, years=db.list_years())
