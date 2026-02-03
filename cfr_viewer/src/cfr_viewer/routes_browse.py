"""Browse routes for navigating CFR titles and sections."""
from flask import Blueprint, render_template, request, redirect, url_for
from .services import get_database, list_titles_with_metadata, get_structure_with_changes, compute_change_pct, BASELINE_YEAR

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
    # Map type to prefix, skip if identifier already has it (case-insensitive)
    prefixes = {"subtitle": "Subtitle", "chapter": "Chapter", "subchapter": "Subchapter", "part": "Part", "subpart": "Subpart"}
    if t in prefixes:
        prefix = prefixes[t]
        return ident if ident.upper().startswith(prefix.upper()) else f"{prefix} {ident}"
    return ident


@browse_bp.route("/")
def index():
    """Dashboard homepage with aggregate stats and previews."""
    db = get_database()
    stats = db.get_statistics_data(BASELINE_YEAR)

    # Top 5 agencies
    agency_counts = stats["agency_counts"][0]
    agency_details = stats["agency_details"]
    top_agencies = sorted(
        [{"slug": s, "name": agency_details.get(s, {}).get("name", s), "word_count": wc}
         for s, wc in agency_counts.items()],
        key=lambda x: x["word_count"], reverse=True
    )[:5]

    # Top 5 titles
    title_counts = stats["title_counts"][0]
    baseline_title_counts = stats["title_counts"].get(BASELINE_YEAR, {})
    title_meta = stats["title_meta"]
    top_titles = sorted(
        [{"number": n, "name": title_meta.get(n, {}).get("name", f"Title {n}"), "word_count": wc}
         for n, wc in title_counts.items()],
        key=lambda x: x["word_count"], reverse=True
    )[:5]

    # Aggregate stats
    total_words = sum(title_counts.values())
    total_sections = db._query("SELECT COUNT(*) FROM sections WHERE year = 0 AND section != ''")[0][0]
    baseline_words = sum(baseline_title_counts.values()) if baseline_title_counts else 0

    aggregate = {
        "total_words": total_words,
        "total_sections": total_sections,
        "total_titles": len(title_counts),  # Titles with content (excludes reserved)
        "all_titles_count": len(title_meta),  # All titles including reserved
        "total_agencies": len(agency_counts),
        "baseline_year": BASELINE_YEAR,
        "change_pct": compute_change_pct(total_words, baseline_words) if baseline_words else None,
    }

    return render_template("browse/index.html", top_agencies=top_agencies, top_titles=top_titles, aggregate=aggregate)


@browse_bp.route("/titles")
def titles():
    """Full list of all CFR titles."""
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


# Backwards compatibility redirects for old /statistics/ URLs
@browse_bp.route("/statistics/")
def statistics_index():
    return redirect(url_for("browse.index"), code=301)


@browse_bp.route("/statistics/titles")
def statistics_titles():
    return redirect(url_for("browse.titles"), code=301)


@browse_bp.route("/statistics/agencies")
def statistics_agencies():
    return redirect(url_for("agencies.index"), code=301)


@browse_bp.route("/statistics/agencies/<slug>")
def statistics_agency_detail(slug: str):
    return redirect(url_for("agencies.detail", slug=slug), code=301)
