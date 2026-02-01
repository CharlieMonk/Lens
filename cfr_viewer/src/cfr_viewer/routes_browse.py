"""Browse routes for navigating CFR titles and sections."""

from flask import Blueprint, render_template, request

from .services import get_database, list_titles_with_metadata

browse_bp = Blueprint("browse", __name__)


@browse_bp.route("/")
def index():
    """Home page - list all 50 titles."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    years = db.list_years()
    titles = list_titles_with_metadata(year)
    return render_template("browse/titles.html", titles=titles, year=year, years=years)


@browse_bp.route("/title/<int:title_num>")
def title(title_num: int):
    """Title structure page - show parts and sections."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    years = db.list_years()
    structure = db.get_structure(title_num, year)
    title_meta = db.get_titles().get(title_num, {})
    word_count = db.get_total_words(title_num, year)

    return render_template(
        "browse/title.html",
        title_num=title_num,
        title_name=title_meta.get("name", f"Title {title_num}"),
        structure=structure,
        word_count=word_count,
        year=year,
        years=years,
    )


@browse_bp.route("/title/<int:title_num>/section/<path:section>")
def section(title_num: int, section: str):
    """Section view with stats and similar sections (loaded via HTMX)."""
    db = get_database()
    year = request.args.get("year", 0, type=int)
    years = db.list_years()
    section_data = db.get_section(title_num, section, year)
    title_meta = db.get_titles().get(title_num, {})
    prev_section, next_section = db.get_adjacent_sections(title_num, section, year)

    return render_template(
        "browse/section.html",
        title_num=title_num,
        title_name=title_meta.get("name", f"Title {title_num}"),
        section=section_data,
        prev_section=prev_section,
        next_section=next_section,
        year=year,
        years=years,
    )
