"""Rankings routes for word count statistics."""

from flask import Blueprint, render_template, request

from . import services

rankings_bp = Blueprint("rankings", __name__)


@rankings_bp.route("/")
def index():
    """Rankings dashboard."""
    return render_template("rankings/index.html")


@rankings_bp.route("/agencies")
def agencies():
    """Agencies ranked by word count."""
    agency_counts = services.get_agency_word_counts()

    # Sort by word count descending
    sorted_agencies = sorted(
        agency_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    return render_template(
        "rankings/agencies.html",
        agencies=sorted_agencies,
    )


@rankings_bp.route("/titles")
def titles():
    """Titles ranked by word count."""
    year = request.args.get("year", 0, type=int)
    years = services.list_years()
    titles_list = services.list_titles(year)

    # Sort by word count descending
    sorted_titles = sorted(titles_list, key=lambda x: x["word_count"], reverse=True)

    return render_template(
        "rankings/titles.html",
        titles=sorted_titles,
        year=year,
        years=years,
    )
