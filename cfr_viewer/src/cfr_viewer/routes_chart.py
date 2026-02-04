"""Chart routes for visualizing CFR word count trends over time."""
from datetime import datetime
from flask import Blueprint, render_template, jsonify, request
from .services import get_database, navigate_to_path, node_label

chart_bp = Blueprint("chart", __name__)

CURRENT_YEAR = datetime.now().year


@chart_bp.route("/")
def index():
    db = get_database()
    titles = db.get_titles()
    historical_years = sorted([y for y in db.list_years() if y > 0])
    return render_template("chart/index.html", titles=titles, historical_years=historical_years, current_year=CURRENT_YEAR)


@chart_bp.route("/data/total")
def data_total():
    """Return total CFR word count across all titles by year as JSON."""
    db = get_database()
    counts = db.get_total_word_counts_by_year()
    result = {}
    for y, c in counts.items():
        if y == 0:
            result[str(CURRENT_YEAR)] = c
        elif y > 0:
            result[str(y)] = c
    return jsonify(result)


@chart_bp.route("/data/<int:title_num>")
@chart_bp.route("/data/<int:title_num>/<path:path>")
def data(title_num: int, path: str = ""):
    """Return word count data for a node across all years as JSON."""
    db = get_database()
    counts = db.get_node_word_counts_by_year(title_num, path)
    result = {}
    for y, c in counts.items():
        if y == 0:
            result[str(CURRENT_YEAR)] = c
        elif y > 0:
            result[str(y)] = c
    return jsonify(result)


@chart_bp.route("/structure/<int:title_num>")
@chart_bp.route("/structure/<int:title_num>/<path:path>")
def structure(title_num: int, path: str = ""):
    """Return children of a node for building the selector."""
    db = get_database()
    # Use year 0 (current) for structure navigation
    full_structure = db.get_structure(title_num, 0)
    if not full_structure:
        return jsonify([])

    node = navigate_to_path(full_structure, path)
    if not node:
        return jsonify([])

    # Return children info
    children = [
        {"type": child.get("type"), "identifier": child.get("identifier"),
         "label": node_label(child, include_heading=True), "has_children": bool(child.get("children"))}
        for child in node.get("children", [])
    ]
    return jsonify(children)


@chart_bp.route("/section-path/<int:title_num>/<section>")
def section_path(title_num: int, section: str):
    """Return the path to a specific section in the structure tree."""
    db = get_database()
    full_structure = db.get_structure(title_num, 0)
    if not full_structure:
        return jsonify({"found": False})

    # Search for the section in the tree and build the path
    path = _find_section_path(full_structure, section, [])
    if path:
        return jsonify({"found": True, "path": path})
    return jsonify({"found": False})


def _find_section_path(node, target_section, current_path):
    """Recursively search for a section and return the path to it."""
    for child in node.get("children", []):
        child_type = child.get("type")
        child_ident = child.get("identifier")

        # Build the path segment for this child
        new_path = current_path + [{"type": child_type, "identifier": child_ident}]

        # If this is the target section, return the path
        if child_type == "section" and child_ident == target_section:
            return new_path

        # Otherwise, search children recursively
        result = _find_section_path(child, target_section, new_path)
        if result:
            return result

    return None
