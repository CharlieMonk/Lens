"""Comparison routes for historical diff view."""

import difflib

from flask import Blueprint, render_template, request
from markupsafe import Markup

from .services import get_database

compare_bp = Blueprint("compare", __name__)


def generate_unified_diff_html(text1: str, text2: str, label1: str, label2: str) -> str:
    """Generate HTML for unified diff with inline styling."""
    lines1 = text1.splitlines(keepends=True)
    lines2 = text2.splitlines(keepends=True)

    diff = difflib.unified_diff(lines1, lines2, fromfile=label1, tofile=label2, lineterm='')

    html_lines = []
    for line in diff:
        line = line.rstrip('\n')
        if line.startswith('+++') or line.startswith('---'):
            html_lines.append(f'<div class="diff-header">{Markup.escape(line)}</div>')
        elif line.startswith('@@'):
            html_lines.append(f'<div class="diff-hunk">{Markup.escape(line)}</div>')
        elif line.startswith('+'):
            html_lines.append(f'<div class="diff-add">{Markup.escape(line)}</div>')
        elif line.startswith('-'):
            html_lines.append(f'<div class="diff-sub">{Markup.escape(line)}</div>')
        else:
            html_lines.append(f'<div class="diff-context">{Markup.escape(line)}</div>')

    return '\n'.join(html_lines)


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
        text1 = section1.get("text", "")
        text2 = section2.get("text", "")

        if text1 != text2:
            label1 = f"Year {year1 or 'Current'}"
            label2 = f"Year {year2 or 'Current'}"
            diff_html = generate_unified_diff_html(text2, text1, label2, label1)

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
