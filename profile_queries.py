#!/usr/bin/env python3
"""Profile query latencies for CFR viewer pages."""
import time
import statistics
from ecfr.database import ECFRDatabase

def time_query(func, iterations=10):
    """Time a query function and return statistics."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func()
        elapsed = (time.perf_counter() - start) * 1000  # ms
        times.append(elapsed)
    return {
        "min": min(times),
        "max": max(times),
        "avg": statistics.mean(times),
        "median": statistics.median(times),
        "p95": sorted(times)[int(len(times) * 0.95)] if len(times) >= 20 else max(times),
        "iterations": len(times),
    }

def format_stats(stats):
    """Format statistics for display."""
    return f"avg={stats['avg']:.1f}ms, median={stats['median']:.1f}ms, min={stats['min']:.1f}ms, max={stats['max']:.1f}ms"

def main():
    db = ECFRDatabase()

    # Clear caches for accurate measurements
    db._stats_cache = {}
    db._structure_cache = {}
    db._tfidf_cache = {}

    print("=" * 80)
    print("CFR Viewer Query Latency Profile")
    print("=" * 80)
    print(f"Database: {db.db_path} ({db.db_path.stat().st_size / 1e9:.2f} GB)")
    print()

    # Get some test data
    years = db.list_years()
    titles = db.get_titles()
    print(f"Years available: {years}")
    print(f"Titles count: {len(titles)}")
    print()

    results = {}

    # --- HOME PAGE QUERIES ---
    print("-" * 80)
    print("HOME PAGE (/) - First load (cold cache)")
    print("-" * 80)

    db._stats_cache = {}  # Clear cache
    stats = time_query(lambda: db.get_statistics_data(2010), iterations=5)
    results["get_statistics_data (cold)"] = stats
    print(f"  get_statistics_data(2010):    {format_stats(stats)}")

    stats = time_query(lambda: db._query("SELECT COUNT(*) FROM sections WHERE year=0 AND section != ''"), iterations=10)
    results["count_sections"] = stats
    print(f"  count_sections (year=0):      {format_stats(stats)}")

    print("\nHOME PAGE (/) - Cached")
    stats = time_query(lambda: db.get_statistics_data(2010), iterations=10)
    results["get_statistics_data (cached)"] = stats
    print(f"  get_statistics_data (cached): {format_stats(stats)}")

    # --- TITLES LIST PAGE ---
    print("\n" + "-" * 80)
    print("TITLES LIST (/titles)")
    print("-" * 80)

    stats = time_query(lambda: db.list_years(), iterations=10)
    results["list_years"] = stats
    print(f"  list_years():                 {format_stats(stats)}")

    stats = time_query(lambda: db.get_titles(), iterations=10)
    results["get_titles"] = stats
    print(f"  get_titles():                 {format_stats(stats)}")

    stats = time_query(lambda: db.get_all_title_word_counts(0), iterations=10)
    results["get_all_title_word_counts"] = stats
    print(f"  get_all_title_word_counts(0): {format_stats(stats)}")

    # --- TITLE STRUCTURE PAGE ---
    print("\n" + "-" * 80)
    print("TITLE STRUCTURE (/title/<n>) - Cold cache")
    print("-" * 80)

    for title_num in [1, 26, 42]:  # Small, medium, large titles
        db._structure_cache = {}
        stats = time_query(lambda t=title_num: db.get_structure(t, 0), iterations=3)
        results[f"get_structure(title={title_num}, cold)"] = stats
        print(f"  get_structure(title={title_num}, cold):  {format_stats(stats)}")

    print("\nTITLE STRUCTURE - Cached")
    for title_num in [1, 26, 42]:
        stats = time_query(lambda t=title_num: db.get_structure(t, 0), iterations=10)
        results[f"get_structure(title={title_num}, cached)"] = stats
        print(f"  get_structure(title={title_num}, cached):{format_stats(stats)}")

    stats = time_query(lambda: db.get_total_words(26, 0), iterations=10)
    results["get_total_words"] = stats
    print(f"  get_total_words(title=26):    {format_stats(stats)}")

    # --- SECTION DETAIL PAGE ---
    print("\n" + "-" * 80)
    print("SECTION DETAIL (/title/<n>/section/<s>)")
    print("-" * 80)

    stats = time_query(lambda: db.get_section(26, "1.1", 0), iterations=10)
    results["get_section"] = stats
    print(f"  get_section(26, '1.1'):       {format_stats(stats)}")

    stats = time_query(lambda: db.get_adjacent_sections(26, "1.1", 0), iterations=10)
    results["get_adjacent_sections"] = stats
    print(f"  get_adjacent_sections():      {format_stats(stats)}")

    # --- AGENCIES PAGE ---
    print("\n" + "-" * 80)
    print("AGENCIES (/agencies)")
    print("-" * 80)

    db._stats_cache = {}
    stats = time_query(lambda: db.get_statistics_data(2010), iterations=5)
    results["get_statistics_data (agencies, cold)"] = stats
    print(f"  get_statistics_data (cold):   {format_stats(stats)}")

    # --- AGENCY DETAIL PAGE ---
    print("\n" + "-" * 80)
    print("AGENCY DETAIL (/agencies/<slug>)")
    print("-" * 80)

    stats = time_query(lambda: db.get_agency("securities-and-exchange-commission"), iterations=10)
    results["get_agency"] = stats
    print(f"  get_agency(SEC):              {format_stats(stats)}")

    stats = time_query(lambda: db.get_agency_chapters("securities-and-exchange-commission"), iterations=10)
    results["get_agency_chapters"] = stats
    print(f"  get_agency_chapters(SEC):     {format_stats(stats)}")

    stats = time_query(lambda: db.get_agency_chapter_word_counts("securities-and-exchange-commission", 0), iterations=10)
    results["get_agency_chapter_word_counts"] = stats
    print(f"  get_agency_chapter_word_counts: {format_stats(stats)}")

    # --- COMPARE PAGE ---
    print("\n" + "-" * 80)
    print("COMPARE (/compare/title/<n>/section/<s>)")
    print("-" * 80)

    stats = time_query(lambda: db.get_section(26, "1.1", 0), iterations=10)
    print(f"  get_section (year1):          {format_stats(stats)}")
    stats = time_query(lambda: db.get_section(26, "1.1", 2010), iterations=10)
    print(f"  get_section (year2):          {format_stats(stats)}")

    # --- CHART DATA ---
    print("\n" + "-" * 80)
    print("CHART (/chart/data/...)")
    print("-" * 80)

    stats = time_query(lambda: db.get_total_word_counts_by_year(), iterations=10)
    results["get_total_word_counts_by_year"] = stats
    print(f"  get_total_word_counts_by_year: {format_stats(stats)}")

    stats = time_query(lambda: db.get_node_word_counts_by_year(26, ""), iterations=10)
    results["get_node_word_counts_by_year"] = stats
    print(f"  get_node_word_counts_by_year: {format_stats(stats)}")

    # --- SIMILAR SECTIONS (TF-IDF) ---
    print("\n" + "-" * 80)
    print("SIMILAR SECTIONS (/api/similar/...)")
    print("-" * 80)

    db._tfidf_cache = {}
    stats = time_query(lambda: db.get_similar_sections(26, "1.1", 0), iterations=3)
    results["get_similar_sections (cold)"] = stats
    print(f"  get_similar_sections (cold):  {format_stats(stats)}")

    stats = time_query(lambda: db.get_similar_sections(26, "1.1", 0), iterations=10)
    results["get_similar_sections (cached)"] = stats
    print(f"  get_similar_sections (cached):{format_stats(stats)}")

    # --- SUMMARY ---
    print("\n" + "=" * 80)
    print("SUMMARY: Page Load Estimates")
    print("=" * 80)

    page_estimates = {
        "Home (/)": ["get_statistics_data (cold)", "count_sections"],
        "Home (cached)": ["get_statistics_data (cached)", "count_sections"],
        "Titles (/titles)": ["list_years", "get_titles", "get_all_title_word_counts"],
        "Title Structure (cold)": ["get_structure(title=26, cold)", "get_total_words", "list_years"],
        "Title Structure (cached)": ["get_structure(title=26, cached)", "get_total_words", "list_years"],
        "Section Detail": ["get_section", "get_adjacent_sections", "get_titles", "list_years"],
        "Agency Detail": ["get_agency", "get_agency_chapters", "get_agency_chapter_word_counts"],
        "Similar Sections (cold)": ["get_similar_sections (cold)"],
        "Similar Sections (cached)": ["get_similar_sections (cached)"],
    }

    for page, queries in page_estimates.items():
        total = sum(results.get(q, {"avg": 0})["avg"] for q in queries)
        print(f"  {page:30s}: ~{total:.0f}ms")

    # --- ARCHITECTURE COMPARISON ---
    print("\n" + "=" * 80)
    print("ARCHITECTURE COMPARISON")
    print("=" * 80)

    print("""
Current Architecture: SQLite + Python (local disk)
┌─────────────────────────────┬─────────────┬─────────────────────────────────┐
│ Query Type                  │ Current     │ Notes                           │
├─────────────────────────────┼─────────────┼─────────────────────────────────┤
│ Simple lookups (by PK)      │ <1ms        │ Excellent                       │
│ Title metadata              │ <1ms        │ Small table, fast               │
│ Word count aggregations     │ 1-5ms       │ Pre-aggregated tables help      │
│ Structure tree (cold)       │ 100-500ms   │ Complex multi-level aggregation │
│ Structure tree (cached)     │ <1ms        │ In-memory cache works well      │
│ TF-IDF similarity (cold)    │ 500-2000ms  │ ML computation per chapter      │
│ TF-IDF similarity (cached)  │ 10-50ms     │ Matrix cached in memory         │
│ Section count (full scan)   │ 50-200ms    │ Large table scan                │
└─────────────────────────────┴─────────────┴─────────────────────────────────┘

Alternative Architectures:

1. PostgreSQL (local or remote):
   ├─ Pros: Better query planner, parallel queries, materialized views
   ├─ Cons: Network latency if remote, more complex setup
   └─ Expected: -20% on complex queries, +5-10ms network overhead if remote

2. PostgreSQL with Full-Text Search:
   ├─ Pros: Native GIN indexes, pg_trgm for similarity
   ├─ Cons: Different similarity algorithm than TF-IDF
   └─ Expected: TF-IDF queries 10-50ms vs current 500-2000ms cold

3. Elasticsearch/OpenSearch:
   ├─ Pros: Excellent full-text search, built-in similarity
   ├─ Cons: Separate service, more RAM, sync complexity
   └─ Expected: Search/similarity <50ms, aggregations ~same

4. Redis + SQLite hybrid:
   ├─ Pros: Sub-ms cached reads, works well with current caching pattern
   ├─ Cons: Cache invalidation complexity, extra service
   └─ Expected: Cached queries <1ms, cold same as current

5. Pre-computed JSON files:
   ├─ Pros: Zero query time for structure, simple deployment
   ├─ Cons: Disk space, update complexity
   └─ Expected: Structure reads <10ms (file I/O only)

Recommendation:
Current SQLite architecture is well-suited for this workload:
- 3.8GB DB is well within SQLite's capabilities
- Read-heavy workload matches SQLite strengths
- In-memory caching handles expensive queries effectively
- No network latency for local deployment

Potential improvements within current architecture:
1. Pre-warm structure cache on startup (already supported)
2. Add index on sections(year, title, chapter) for TF-IDF queries
3. Materialize section counts in title_word_counts table
4. Consider WAL mode for better concurrent read performance
""")

if __name__ == "__main__":
    main()
