"""Browse routes for navigating CFR titles and sections."""
from flask import Blueprint, render_template, request
from .services import get_database, list_titles_with_metadata

browse_bp = Blueprint("browse", __name__)


def _find_node(structure, path):
    """Navigate structure tree to find node at path (e.g., 'chapter/I/part/1')."""
    if not path or not structure:
        return structure
    parts = path.strip("/").split("/")
    node = structure
    breadcrumb = []
    for i in range(0, len(parts), 2):
        if i + 1 >= len(parts):
            break
        node_type, identifier = parts[i], parts[i + 1]
        for child in node.get("children", []):
            if child.get("type") == node_type and child.get("identifier") == identifier:
                label = _node_label(child)
                breadcrumb.append({"type": node_type, "identifier": identifier, "label": label, "path": "/".join(parts[:i + 2])})
                node = child
                break
        else:
            return None, []
    return node, breadcrumb


def _node_label(node):
    """Get display label for a structure node."""
    t, ident = node.get("type", ""), node.get("identifier", "")
    if t == "subtitle":
        return ident
    if t == "chapter":
        return f"Chapter {ident}"
    if t == "subchapter":
        return f"Subchapter {ident}" if len(ident) <= 3 else ident
    if t == "part":
        return f"Part {ident}"
    if t == "subpart":
        return f"Subpart {ident}"
    return ident


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


@browse_bp.route("/title/<int:title_num>/<path:path>")
def structure(title_num: int, path: str):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    full_structure = db.get_structure(title_num, year)
    node, breadcrumb = _find_node(full_structure, path)
    if not node:
        return render_template("browse/structure.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                               node=None, breadcrumb=[], year=year, years=db.list_years())
    return render_template("browse/structure.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           node=node, breadcrumb=breadcrumb, year=year, years=db.list_years())


@browse_bp.route("/title/<int:title_num>/section/<path:section>")
def section(title_num: int, section: str):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    prev_sec, next_sec = db.get_adjacent_sections(title_num, section, year)
    return render_template("browse/section.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           section=db.get_section(title_num, section, year), prev_section=prev_sec, next_section=next_sec, year=year, years=db.list_years())
