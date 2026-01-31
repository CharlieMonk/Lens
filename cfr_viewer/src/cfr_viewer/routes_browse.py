"""Browse routes for navigating CFR titles and sections."""

from flask import Blueprint, render_template, request

from . import services

browse_bp = Blueprint("browse", __name__)


@browse_bp.route("/")
def index():
    """Home page - list all 50 titles."""
    year = request.args.get("year", 0, type=int)
    years = services.list_years()
    titles = services.list_titles(year)
    return render_template("browse/titles.html", titles=titles, year=year, years=years)


@browse_bp.route("/title/<int:title_num>")
def title(title_num: int):
    """Title structure page - show parts and sections."""
    year = request.args.get("year", 0, type=int)
    years = services.list_years()
    structure = services.get_structure(title_num, year)
    title_meta = services.get_title_metadata().get(title_num, {})
    word_count = services.get_total_words(title_num, year)

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
    """Section view with stats and similar sections."""
    year = request.args.get("year", 0, type=int)
    years = services.list_years()
    section_data = services.get_section(title_num, section, year)
    title_meta = services.get_title_metadata().get(title_num, {})

    # Get similar sections count (for the indicator)
    similar = services.get_similar_sections(title_num, section, year, limit=5)

    return render_template(
        "browse/section.html",
        title_num=title_num,
        title_name=title_meta.get("name", f"Title {title_num}"),
        section=section_data,
        similar_count=len(similar),
        year=year,
        years=years,
    )
