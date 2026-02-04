"""Service layer for Flask views."""
from flask import current_app, request
from ecfr.config import config
from ecfr.database import ECFRDatabase

BASELINE_YEAR = config.baseline_year
COMPARE_DEFAULT_YEAR = config.compare_default_year


def get_database() -> ECFRDatabase:
    return current_app.ecfr_database


def get_validated_year(param: str = "year", default: int = 0) -> int:
    """Extract and validate year from request query parameter."""
    year = request.args.get(param, default, type=int)
    years = get_database().list_years()
    return year if year in years else 0


def get_title_name(title_num: int) -> str:
    """Get title name with fallback to 'Title N'."""
    return get_database().get_titles().get(title_num, {}).get("name", f"Title {title_num}")


def compute_change_pct(current: int | None, baseline: int | None) -> float | None:
    """Compute percentage change from baseline. Returns None if either value is missing."""
    if current is None or baseline is None or baseline == 0:
        return None
    return ((current - baseline) / baseline) * 100


def compute_change_vs_baseline(current: int | None, baseline: int | None, year: int) -> float | None:
    """Compute change percentage, accounting for year direction relative to baseline.

    If year < BASELINE_YEAR, computes change TO baseline (baseline/current).
    Otherwise computes change SINCE baseline (current/baseline).
    """
    if year and year < BASELINE_YEAR:
        return compute_change_pct(baseline, current) if current else None
    return compute_change_pct(current, baseline)

def list_titles_with_metadata(year: int = 0) -> list[dict]:
    db = get_database()
    meta = db.get_titles()
    word_counts = db.get_all_title_word_counts(year)
    baseline_counts = db.get_all_title_word_counts(BASELINE_YEAR)
    results = []
    for n in sorted(meta.keys()):
        title_meta = meta.get(n, {})
        # Skip reserved titles with no content
        if title_meta.get("reserved") and n not in word_counts:
            continue
        wc = word_counts.get(n, 0)
        bc = baseline_counts.get(n)
        results.append({
            "number": n,
            "name": title_meta.get("name", f"Title {n}"),
            "word_count": wc,
            "change_pct": compute_change_vs_baseline(wc, bc, year),
        })
    return results


def _build_baseline_maps(node, path=""):
    """Build maps for baseline comparison: path -> word_count and section_id -> word_count."""
    path_map, section_map = {}, {}
    node_path = f"{path}/{node['type']}/{node['identifier']}" if path else f"{node['type']}/{node['identifier']}"
    if node.get("word_count"):
        path_map[node_path] = node["word_count"]
        if node.get("type") == "section":
            section_map[node["identifier"]] = node["word_count"]
    for child in node.get("children", []):
        child_path, child_section = _build_baseline_maps(child, node_path)
        path_map.update(child_path)
        section_map.update(child_section)
    return path_map, section_map


def _enrich_with_changes(node, baseline_map, section_map, path="", before_baseline=False):
    """Recursively add change_pct to each node by comparing against baseline."""
    node_path = f"{path}/{node['type']}/{node['identifier']}" if path else f"{node['type']}/{node['identifier']}"
    wc = node.get("word_count", 0)
    # For sections, use section_map (by ID) since paths may differ between years
    if node.get("type") == "section":
        bc = section_map.get(node["identifier"])
    else:
        bc = baseline_map.get(node_path)
    if before_baseline:
        node["change_pct"] = compute_change_pct(bc, wc) if wc else None
    else:
        node["change_pct"] = compute_change_pct(wc, bc)
    for child in node.get("children", []):
        _enrich_with_changes(child, baseline_map, section_map, node_path, before_baseline)


def get_structure_with_changes(title_num: int, year: int = 0) -> dict | None:
    """Get title structure enriched with change percentages vs baseline year."""
    db = get_database()
    structure = db.get_structure(title_num, year)
    if not structure:
        return None
    baseline_structure = db.get_structure(title_num, BASELINE_YEAR)
    baseline_map, section_map = _build_baseline_maps(baseline_structure) if baseline_structure else ({}, {})
    before_baseline = year and year < BASELINE_YEAR
    _enrich_with_changes(structure, baseline_map, section_map, before_baseline=before_baseline)
    return structure


def node_label(node: dict, include_heading: bool = False) -> str:
    """Get display label for a structure node.

    Args:
        node: Structure node with 'type' and 'identifier' keys
        include_heading: If True, include section heading in label (for sections)
    """
    t, ident = node.get("type", ""), node.get("identifier", "")

    # Section handling with optional heading
    if t == "section":
        if include_heading:
            heading = node.get("heading", "")
            return f"§ {ident} - {heading}" if heading else f"§ {ident}"
        return f"§ {ident}"

    # Map type to prefix, skip if identifier already has it (case-insensitive)
    prefixes = {"subtitle": "Subtitle", "chapter": "Chapter", "subchapter": "Subchapter", "part": "Part", "subpart": "Subpart"}
    if t in prefixes:
        prefix = prefixes[t]
        # For subchapters with long identifiers (full names), don't add prefix
        if t == "subchapter" and len(ident) > 3:
            return ident
        return ident if ident.upper().startswith(prefix.upper()) else f"{prefix} {ident}"
    return ident


def navigate_to_path(structure: dict | None, path: str) -> dict | None:
    """Navigate structure tree to find node at path (e.g., 'chapter/I/part/1').

    Returns the node at the path, or None if not found.
    """
    if not structure or not path:
        return structure

    node = structure
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
            return None
        node = found

    return node
