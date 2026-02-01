"""Statistics routes for word count statistics."""

from flask import Blueprint, render_template, request

from .services import get_database, list_titles_with_metadata

statistics_bp = Blueprint("statistics", __name__)


@statistics_bp.route("/")
def index():
    """Statistics dashboard."""
    return render_template("statistics/index.html")


@statistics_bp.route("/agencies")
def agencies():
    """Agencies ranked by word count."""
    db = get_database()
    agency_counts = db.get_agency_word_counts()

    # Get 2020 word counts for % change calculation (with parent aggregation)
    direct_2020 = {
        row[0]: row[1]
        for row in db._query("""
            SELECT r.agency_slug, SUM(s.word_count)
            FROM sections s
            JOIN cfr_references r ON s.title = r.title
                AND s.chapter = COALESCE(r.chapter, r.subtitle, r.subchapter)
            WHERE s.year = 2020
            GROUP BY r.agency_slug
        """)
    }
    # Apply same parent aggregation as get_agency_word_counts()
    child_to_parent = {
        row[0]: row[1]
        for row in db._query("SELECT slug, parent_slug FROM agencies WHERE parent_slug IS NOT NULL")
    }
    counts_2020 = dict(direct_2020)
    for child_slug, parent_slug in child_to_parent.items():
        if child_slug in direct_2020:
            counts_2020[parent_slug] = counts_2020.get(parent_slug, 0) + direct_2020[child_slug]

    # Get agency details (name, short_name) keyed by slug
    agency_details = {
        row[0]: {"name": row[1], "short_name": row[2]}
        for row in db._query("SELECT slug, name, short_name FROM agencies")
    }

    # Build list with slug, name, abbreviation, word count, and % change
    agencies_list = []
    for slug, word_count in agency_counts.items():
        details = agency_details.get(slug, {})
        base = counts_2020.get(slug)
        change_pct = ((word_count - base) / base) * 100 if base else None
        agencies_list.append({
            "slug": slug,
            "name": details.get("name", slug),
            "abbreviation": details.get("short_name") or "",
            "word_count": word_count,
            "change_pct": change_pct,
        })

    # Sort by word count descending
    agencies_list.sort(key=lambda x: x["word_count"], reverse=True)

    return render_template("statistics/agencies.html", agencies=agencies_list)


@statistics_bp.route("/agencies/<slug>")
def agency_detail(slug: str):
    """Agency detail page showing CFR chapters."""
    db = get_database()
    agency = db.get_agency(slug)

    if not agency:
        return render_template("statistics/agency_detail.html", agency=None, chapters=[])

    chapters = db.get_agency_chapters(slug)

    return render_template("statistics/agency_detail.html", agency=agency, chapters=chapters)


@statistics_bp.route("/titles")
def titles():
    """Titles ranked by word count."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    years = db.list_years()
    titles_list = list_titles_with_metadata(year)

    # Get 2020 word counts for % change calculation
    counts_2020 = {num: db.get_total_words(num, 2020) for num in db.list_titles(2020)}

    for title in titles_list:
        base = counts_2020.get(title["number"])
        if base:
            title["change_pct"] = ((title["word_count"] - base) / base) * 100
        else:
            title["change_pct"] = None

    # Sort by word count descending
    sorted_titles = sorted(titles_list, key=lambda x: x["word_count"], reverse=True)

    return render_template("statistics/titles.html", titles=sorted_titles, year=year, years=years)
