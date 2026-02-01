"""Rankings routes for word count statistics."""

from flask import Blueprint, render_template, request

from .services import get_database, list_titles_with_metadata

rankings_bp = Blueprint("rankings", __name__)


@rankings_bp.route("/")
def index():
    """Rankings dashboard."""
    return render_template("rankings/index.html")


@rankings_bp.route("/agencies")
def agencies():
    """Agencies ranked by word count."""
    db = get_database()
    agency_counts = db.get_agency_word_counts()

    # Get agency details (name, short_name) keyed by slug
    agency_details = {
        row[0]: {"name": row[1], "short_name": row[2]}
        for row in db._query("SELECT slug, name, short_name FROM agencies")
    }

    # Build list with name, abbreviation, and word count
    agencies_list = []
    for slug, word_count in agency_counts.items():
        details = agency_details.get(slug, {})
        agencies_list.append({
            "name": details.get("name", slug),
            "abbreviation": details.get("short_name") or "",
            "word_count": word_count,
        })

    # Sort by word count descending
    agencies_list.sort(key=lambda x: x["word_count"], reverse=True)

    return render_template("rankings/agencies.html", agencies=agencies_list)


@rankings_bp.route("/titles")
def titles():
    """Titles ranked by word count."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    years = db.list_years()
    titles_list = list_titles_with_metadata(year)

    # Sort by word count descending
    sorted_titles = sorted(titles_list, key=lambda x: x["word_count"], reverse=True)

    return render_template("rankings/titles.html", titles=sorted_titles, year=year, years=years)
