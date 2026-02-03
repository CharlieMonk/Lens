"""Chart routes for visualizing CFR word count trends over time."""
from flask import Blueprint, render_template, jsonify, request
from .services import get_database

chart_bp = Blueprint("chart", __name__)


@chart_bp.route("/")
def index():
    db = get_database()
    titles = db.get_titles()
    years = [y for y in db.list_years() if y > 0]
    return render_template("chart/index.html", titles=titles, years=years)


@chart_bp.route("/data/total")
def data_total():
    """Return total CFR word count across all titles by year as JSON."""
    db = get_database()
    counts = db.get_total_word_counts_by_year()
    result = {str(y): c for y, c in counts.items() if y > 0}
    return jsonify(result)


@chart_bp.route("/data/<int:title_num>")
@chart_bp.route("/data/<int:title_num>/<path:path>")
def data(title_num: int, path: str = ""):
    """Return word count data for a node across all years as JSON."""
    db = get_database()
    counts = db.get_node_word_counts_by_year(title_num, path)
    # Convert year 0 to current year label, filter out year 0 for chart
    result = {str(y): c for y, c in counts.items() if y > 0}
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

    # Navigate to the requested path
    node = full_structure
    if path:
        parts = path.strip("/").split("/")
        for i in range(0, len(parts), 2):
            if i + 1 >= len(parts):
                break
            node_type, identifier = parts[i], parts[i + 1]
            found = None
            for child in node.get("children", []):
                if child.get("type") == node_type and child.get("identifier") == identifier:
                    found = child
                    break
            if not found:
                return jsonify([])
            node = found

    # Return children info
    children = []
    for child in node.get("children", []):
        children.append({
            "type": child.get("type"),
            "identifier": child.get("identifier"),
            "label": _node_label(child),
            "has_children": bool(child.get("children"))
        })
    return jsonify(children)


def _node_label(node):
    """Get display label for a structure node."""
    t, ident = node.get("type", ""), node.get("identifier", "")
    if t == "subtitle":
        return f"Subtitle {ident}"
    if t == "chapter":
        return f"Chapter {ident}"
    if t == "subchapter":
        return f"Subchapter {ident}" if len(ident) <= 3 else ident
    if t == "part":
        return f"Part {ident}"
    if t == "subpart":
        return f"Subpart {ident}"
    if t == "section":
        heading = node.get("heading", "")
        return f"§ {ident} - {heading}" if heading else f"§ {ident}"
    return ident
