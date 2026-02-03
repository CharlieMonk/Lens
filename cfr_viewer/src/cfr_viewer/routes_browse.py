"""Browse routes for navigating CFR titles and sections."""
from flask import Blueprint, render_template, request, redirect, url_for
from .services import get_database, list_titles_with_metadata, get_structure_with_changes, BASELINE_YEAR

browse_bp = Blueprint("browse", __name__)


def _find_node(structure, path):
    """Navigate structure tree to find node at path (e.g., 'chapter/I/part/1').

    If exact identifier match fails, tries to find equivalent node by matching
    on child identifiers (parts/sections have stable IDs across years).
    """
    if not path or not structure:
        return structure, []
    parts = path.strip("/").split("/")
    node = structure
    breadcrumb = []

    for i in range(0, len(parts), 2):
        if i + 1 >= len(parts):
            break
        node_type, identifier = parts[i], parts[i + 1]

        # Try exact match first
        match = None
        for child in node.get("children", []):
            if child.get("type") == node_type and child.get("identifier") == identifier:
                match = child
                break

        # If no exact match, try to find equivalent node by matching children
        if not match and i + 2 < len(parts):
            next_type, next_id = parts[i + 2], parts[i + 3] if i + 3 < len(parts) else None
            if next_id:
                for child in node.get("children", []):
                    if child.get("type") == node_type:
                        # Check if this node contains the next path segment
                        for grandchild in child.get("children", []):
                            if grandchild.get("type") == next_type and grandchild.get("identifier") == next_id:
                                match = child
                                identifier = child.get("identifier")  # Use actual identifier for breadcrumb
                                break
                    if match:
                        break

        if not match:
            return None, []

        label = _node_label(match)
        actual_path = "/".join(parts[:i] + [node_type, match.get("identifier")]) if i > 0 else f"{node_type}/{match.get('identifier')}"
        breadcrumb.append({"type": node_type, "identifier": match.get("identifier"), "label": label, "path": actual_path})
        node = match

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
    return render_template("browse/titles.html", titles=list_titles_with_metadata(year), year=year, years=db.list_years(), BASELINE_YEAR=BASELINE_YEAR)


@browse_bp.route("/title/<int:title_num>")
def title(title_num: int):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    return render_template("browse/title.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           structure=get_structure_with_changes(title_num, year), word_count=db.get_total_words(title_num, year), year=year, years=db.list_years(), BASELINE_YEAR=BASELINE_YEAR)


@browse_bp.route("/title/<int:title_num>/section/<path:section>")
def section(title_num: int, section: str):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    prev_sec, next_sec = db.get_adjacent_sections(title_num, section, year)
    return render_template("browse/section.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           section=db.get_section(title_num, section, year), prev_section=prev_sec, next_section=next_sec, year=year, years=db.list_years())


@browse_bp.route("/title/<int:title_num>/<path:path>")
def structure(title_num: int, path: str):
    db = get_database()
    year = request.args.get("year", 0, type=int)
    full_structure = get_structure_with_changes(title_num, year)
    node, breadcrumb = _find_node(full_structure, path)
    if not node:
        # Path doesn't exist in this year (identifiers may differ), redirect to title page
        return redirect(url_for("browse.title", title_num=title_num, year=year))
    return render_template("browse/structure.html", title_num=title_num, title_name=db.get_titles().get(title_num, {}).get("name", f"Title {title_num}"),
                           node=node, breadcrumb=breadcrumb, year=year, years=db.list_years(), BASELINE_YEAR=BASELINE_YEAR)
