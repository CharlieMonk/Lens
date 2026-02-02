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
    counts_2020 = stats["agency_counts"][2020]
    details = stats["agency_details"]
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
    stats = db.get_statistics_data()
    title_counts = db.get_all_title_word_counts(year) if year and year != 0 else stats["title_counts"][0]
    counts_2020 = stats["title_counts"][2020]
    meta = stats["title_meta"]
    titles_list = [{"number": n, "name": meta.get(n, {}).get("name", f"Title {n}"), "word_count": wc, "change_pct": ((wc - counts_2020.get(n, 0)) / counts_2020[n]) * 100 if counts_2020.get(n) else None} for n, wc in title_counts.items()]
    return render_template("statistics/titles.html", titles=sorted(titles_list, key=lambda x: x["word_count"], reverse=True), year=year, years=db.list_years())
