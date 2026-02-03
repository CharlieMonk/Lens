"""API routes for HTMX partials."""
from flask import Blueprint, render_template, request
from .services import get_database

api_bp = Blueprint("api", __name__)

@api_bp.route("/similar/<int:title_num>/<path:section>")
def similar_sections(title_num: int, section: str):
    db = get_database()
    similar, max_sim = db.get_similar_sections(title_num, section, request.args.get("year", 0, type=int), request.args.get("limit", 10, type=int))
    return render_template("components/similar_sections.html", similar=similar, distinctness=1-max_sim if max_sim else None, source_title=title_num, source_section=section, year=request.args.get("year", 0, type=int))

@api_bp.route("/section/<int:title_num>/<path:section>")
def section_content(title_num: int, section: str):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    prev_sec, next_sec = db.get_adjacent_sections(title_num, section, year)
    return render_template("components/section_content.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           section=db.get_section(title_num, section, year), prev_section=prev_sec, next_section=next_sec, year=year, years=db.list_years())
