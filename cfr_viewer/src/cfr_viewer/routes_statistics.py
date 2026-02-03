"""Statistics routes for word count statistics."""
from flask import Blueprint, render_template, request
from .services import get_database, compute_change_pct, BASELINE_YEAR

statistics_bp = Blueprint("statistics", __name__)

@statistics_bp.route("/")
def index():
    db = get_database()
    stats = db.get_statistics_data(BASELINE_YEAR)

    # Top 5 agencies by word count
    agency_counts = stats["agency_counts"][0]
    agency_details = stats["agency_details"]
    top_agencies = sorted(
        [{"slug": s, "name": agency_details.get(s, {}).get("name", s), "word_count": wc}
         for s, wc in agency_counts.items()],
        key=lambda x: x["word_count"], reverse=True
    )[:5]

    # Top 5 titles by word count
    title_counts = stats["title_counts"][0]
    title_meta = stats["title_meta"]
    top_titles = sorted(
        [{"number": n, "name": title_meta.get(n, {}).get("name", f"Title {n}"), "word_count": wc}
         for n, wc in title_counts.items()],
        key=lambda x: x["word_count"], reverse=True
    )[:5]

    return render_template("statistics/index.html", top_agencies=top_agencies, top_titles=top_titles)

@statistics_bp.route("/agencies")
def agencies():
    db = get_database()
    stats = db.get_statistics_data(BASELINE_YEAR)
    counts = stats["agency_counts"][0]
    baseline_counts = stats["agency_counts"].get(BASELINE_YEAR, {})
    details = stats["agency_details"]
    agencies_list = [{"slug": s, "name": details.get(s, {}).get("name", s), "abbreviation": details.get(s, {}).get("short_name") or "", "word_count": wc,
                      "change_pct": compute_change_pct(wc, baseline_counts.get(s))} for s, wc in counts.items()]
    return render_template("statistics/agencies.html", agencies=sorted(agencies_list, key=lambda x: x["word_count"], reverse=True))

@statistics_bp.route("/agencies/<slug>")
def agency_detail(slug: str):
    db = get_database()
    agency = db.get_agency(slug)
    if not agency:
        return render_template("statistics/agency_detail.html", agency=None, chapters=[], chapter_stats=[])

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

    return render_template("statistics/agency_detail.html", agency=agency, chapters=chapters, chapter_stats=sorted(chapter_stats, key=lambda x: x["word_count"], reverse=True))

@statistics_bp.route("/titles")
def titles():
    db = get_database()
    year = request.args.get("year", 0, type=int)
    stats = db.get_statistics_data(BASELINE_YEAR)
    title_counts = db.get_all_title_word_counts(year) if year and year != 0 else stats["title_counts"][0]
    baseline_counts = stats["title_counts"].get(BASELINE_YEAR, {})
    meta = stats["title_meta"]
    titles_list = [{"number": n, "name": meta.get(n, {}).get("name", f"Title {n}"), "word_count": wc,
                    "change_pct": compute_change_pct(wc, baseline_counts.get(n))} for n, wc in title_counts.items()]
    return render_template("statistics/titles.html", titles=sorted(titles_list, key=lambda x: x["word_count"], reverse=True), year=year, years=db.list_years(), BASELINE_YEAR=BASELINE_YEAR)
