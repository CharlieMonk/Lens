"""Comparison routes for historical diff view."""
import difflib
import re
from flask import Blueprint, redirect, render_template, request, url_for
from markupsafe import Markup
from .services import get_database

compare_bp = Blueprint("compare", __name__)

@compare_bp.route("/")
def index():
    """Compare landing page."""
    return render_template("compare/index.html")

def side_by_side_diff(text1: str, text2: str) -> tuple[str, str]:
    """Generate side-by-side HTML with word-level highlighting.

    Returns (old_html, new_html) with deletions/additions highlighted inline.
    Ignores whitespace differences - only highlights actual word changes.
    """
    if not text1 and not text2:
        return "", ""
    if not text1:
        return "", f'<span class="diff-add">{Markup.escape(text2)}</span>'
    if not text2:
        return f'<span class="diff-del">{Markup.escape(text1)}</span>', ""

    # Split into words only (ignore whitespace for comparison)
    words1 = text1.split()
    words2 = text2.split()
    matcher = difflib.SequenceMatcher(None, words1, words2)

    old_parts, new_parts = [], []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        old_words = words1[i1:i2]
        new_words = words2[j1:j2]

        if tag == 'equal':
            old_parts.append(Markup.escape(' '.join(old_words)))
            new_parts.append(Markup.escape(' '.join(new_words)))
        elif tag == 'delete':
            old_parts.append(f'<span class="diff-del">{Markup.escape(" ".join(old_words))}</span>')
        elif tag == 'insert':
            new_parts.append(f'<span class="diff-add">{Markup.escape(" ".join(new_words))}</span>')
        elif tag == 'replace':
            old_parts.append(f'<span class="diff-del">{Markup.escape(" ".join(old_words))}</span>')
            new_parts.append(f'<span class="diff-add">{Markup.escape(" ".join(new_words))}</span>')

    return Markup(' '.join(old_parts)), Markup(' '.join(new_parts))

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
    prev_sec, next_sec = db.get_adjacent_sections(title_num, section, year1)

    # Generate side-by-side diff with inline highlighting (ignoring whitespace)
    old_html, new_html = None, None
    has_changes = s1 and s2 and s1.get("text", "").split() != s2.get("text", "").split()
    if has_changes:
        old_html, new_html = side_by_side_diff(s2.get("text", ""), s1.get("text", ""))

    return render_template("compare/diff.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"), section_id=section, section1=s1, section2=s2, year1=year1, year2=year2, years=years, old_html=old_html, new_html=new_html, has_changes=has_changes, prev_section=prev_sec, next_section=next_sec)
