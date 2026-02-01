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

    similar, max_similarity = db.get_similar_sections(title_num, section, year, limit)
    distinctness = 1 - max_similarity if max_similarity is not None else None

    return render_template(
        "components/similar_sections.html",
        similar=similar,
        distinctness=distinctness,
        source_title=title_num,
        source_section=section,
        year=year,
    )


@api_bp.route("/section/<int:title_num>/<path:section>")
def section_content(title_num: int, section: str):
    """HTMX partial - returns section content for in-page navigation."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    years = db.list_years()
    section_data = db.get_section(title_num, section, year)
    title_meta = db.get_titles().get(title_num, {})
    prev_section, next_section = db.get_adjacent_sections(title_num, section, year)

    return render_template(
        "components/section_content.html",
        title_num=title_num,
        title_name=title_meta.get("name", f"Title {title_num}"),
        section=section_data,
        prev_section=prev_section,
        next_section=next_section,
        year=year,
        years=years,
    )
