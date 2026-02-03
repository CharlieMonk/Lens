"""Service layer for Flask views."""
from flask import current_app
from ecfr.database import ECFRDatabase

BASELINE_YEAR = 2010
COMPARE_DEFAULT_YEAR = 2020

def get_database() -> ECFRDatabase:
    return current_app.ecfr_database

def compute_change_pct(current: int | None, baseline: int | None) -> float | None:
    """Compute percentage change from baseline. Returns None if either value is missing."""
    if current is None or baseline is None or baseline == 0:
        return None
    return ((current - baseline) / baseline) * 100

def list_titles_with_metadata(year: int = 0) -> list[dict]:
    db = get_database()
    meta = db.get_titles()
    word_counts = db.get_all_title_word_counts(year)
    baseline_counts = db.get_all_title_word_counts(BASELINE_YEAR)
    results = []
    for n in sorted(meta.keys()):
        title_meta = meta.get(n, {})
        wc = word_counts.get(n, 0)
        bc = baseline_counts.get(n)
        if year and year < BASELINE_YEAR:
            # "To BASELINE_YEAR": compute change from year to baseline (denominator is year's count)
            change_pct = compute_change_pct(bc, wc) if wc else None
        else:
            # "Since BASELINE_YEAR": compute change from baseline to year
            change_pct = compute_change_pct(wc, bc)
        results.append({
            "number": n,
            "name": title_meta.get("name", f"Title {n}"),
            "word_count": wc,
            "change_pct": change_pct,
            "reserved": title_meta.get("reserved", False),
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
