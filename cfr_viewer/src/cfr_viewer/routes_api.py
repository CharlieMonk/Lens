"""API routes for HTMX partials."""

from flask import Blueprint, render_template, request

from .services import get_database

api_bp = Blueprint("api", __name__)


@api_bp.route("/similar/<int:title_num>/<path:section>")
def similar_sections(title_num: int, section: str):
    """HTMX partial - returns similar sections list."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    limit = request.args.get("limit", 10, type=int)

    similar = db.get_similar_sections(title_num, section, year, limit)

    return render_template(
        "components/similar_sections.html",
        similar=similar,
        source_title=title_num,
        source_section=section,
        year=year,
    )
