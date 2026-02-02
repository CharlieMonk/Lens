"""Browse routes for navigating CFR titles and sections."""
from flask import Blueprint, render_template, request
from .services import get_database, list_titles_with_metadata

browse_bp = Blueprint("browse", __name__)

@browse_bp.route("/")
def index():
    db = get_database()
    year = request.args.get("year", 0, type=int)
    return render_template("browse/titles.html", titles=list_titles_with_metadata(year), year=year, years=db.list_years())

@browse_bp.route("/title/<int:title_num>")
def title(title_num: int):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    return render_template("browse/title.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           structure=db.get_structure(title_num, year), word_count=db.get_total_words(title_num, year), year=year, years=db.list_years())

@browse_bp.route("/title/<int:title_num>/section/<path:section>")
def section(title_num: int, section: str):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    prev_sec, next_sec = db.get_adjacent_sections(title_num, section, year)
    return render_template("browse/section.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           section=db.get_section(title_num, section, year), prev_section=prev_sec, next_section=next_sec, year=year, years=db.list_years())
