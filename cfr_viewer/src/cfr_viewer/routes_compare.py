"""Comparison routes for historical diff view."""
import difflib
import re
from flask import Blueprint, redirect, render_template, request, url_for
from markupsafe import Markup
from .services import get_database

compare_bp = Blueprint("compare", __name__)

def diff_html(text1: str, text2: str, label1: str, label2: str) -> str:
    lines = []
    for line in difflib.unified_diff(text1.splitlines(keepends=True), text2.splitlines(keepends=True), fromfile=label1, tofile=label2, lineterm=''):
        line = line.rstrip('\n')
        cls = "diff-header" if line[:3] in ('+++', '---') else "diff-hunk" if line[:2] == '@@' else "diff-add" if line[0] == '+' else "diff-sub" if line[0] == '-' else "diff-context"
        lines.append(f'<div class="{cls}">{Markup.escape(line)}</div>')
    return '\n'.join(lines)

def parse_citation(citation: str) -> tuple[int | None, str | None]:
    """Parse CFR citation formats like '47 C.F.R. § 73.609', '29 CFR 1910.134a', etc."""
    if not citation:
        return None, None
    # Remove common prefixes, year suffixes, and normalize
    text = re.sub(r'^[^,]*,\s*', '', citation)  # Remove rule name prefix before comma
    text = re.sub(r'\(\d{4}\)\s*$', '', text)   # Remove year like (2019)
    text = re.sub(r'\([a-zA-Z0-9)(-]+\)\s*$', '', text)  # Remove subsections like (a)(1)
    # Match: title number, optional C.F.R./CFR, optional §, section number
    m = re.search(r'(\d+)\s*C\.?F\.?R\.?\s*§?\s*([\d.]+)', text, re.IGNORECASE)
    if m:
        title, section = int(m.group(1)), re.sub(r'[a-zA-Z]+$', '', m.group(2))  # Strip trailing letters
        return title, section
    # Try section-only format
    m = re.search(r'§?\s*([\d.]+)', text)
    if m:
        return None, re.sub(r'[a-zA-Z]+$', '', m.group(1))
    return None, None

@compare_bp.route("/title/<int:title_num>/section/<path:section>")
def diff(title_num: int, section: str):
    db = get_database()
    year1, year2 = request.args.get("year1", 0, type=int), request.args.get("year2", type=int)
    other = request.args.get("other", "").strip()
    if other:
        other_title, other_section = parse_citation(other)
        if other_section:
            return redirect(url_for("compare.diff", title_num=other_title or title_num, section=other_section, year1=year1, year2=year2))
    years = db.list_years()
    if year2 is None:
        year2 = next((y for y in years if y != year1 and y != 0), year1)
    s1, s2 = db.get_section(title_num, section, year1), db.get_section(title_num, section, year2)
    d = diff_html(s2.get("text", ""), s1.get("text", ""), f"Year {year2 or 'Current'}", f"Year {year1 or 'Current'}") if s1 and s2 and s1.get("text") != s2.get("text") else None
    return render_template("compare/diff.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"), section_id=section, section1=s1, section2=s2, year1=year1, year2=year2, years=years, diff_html=d)
