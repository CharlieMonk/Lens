"""Comparison routes for historical diff view."""

import difflib

from flask import Blueprint, render_template, request

from .services import get_database

compare_bp = Blueprint("compare", __name__)


@compare_bp.route("/title/<int:title_num>/section/<path:section>")
def diff(title_num: int, section: str):
    """Side-by-side comparison of a section across years."""
    db = get_database()
    year1 = request.args.get("year1", 0, type=int)
    year2 = request.args.get("year2", type=int)

    years = db.list_years()
    title_meta = db.get_titles().get(title_num, {})

    # Default year2 to the previous available year
    if year2 is None:
        for y in years:
            if y != year1 and y != 0:
                year2 = y
                break
        if year2 is None:
            year2 = year1

    section1 = db.get_section(title_num, section, year1)
    section2 = db.get_section(title_num, section, year2)

    # Generate diff if both sections exist
    diff_html = None
    if section1 and section2:
        text1 = section1.get("text", "").splitlines()
        text2 = section2.get("text", "").splitlines()

        differ = difflib.HtmlDiff(wrapcolumn=80)
        diff_html = differ.make_table(
            text2,  # Older version on left
            text1,  # Newer version on right
            fromdesc=f"Year {year2 or 'Current'}",
            todesc=f"Year {year1 or 'Current'}",
            context=True,
            numlines=3,
        )

    return render_template(
        "compare/diff.html",
        title_num=title_num,
        title_name=title_meta.get("name", f"Title {title_num}"),
        section_id=section,
        section1=section1,
        section2=section2,
        year1=year1,
        year2=year2,
        years=years,
        diff_html=diff_html,
    )
