# CFR Viewer Query Latency Analysis

## Database Overview

| Metric | Value |
|--------|-------|
| Database Size | 4.05 GB |
| Total Sections | 1,451,132 |
| Sections (current year) | 227,908 |
| Historical Years | 2000, 2005, 2010, 2015, 2020, 2025 |
| Titles | 50 |
| Agencies | 316 |

## Query Latency by Page

### Page Load Estimates

| Page | Queries | Total Latency |
|------|---------|---------------|
| Home (`/`) | `get_statistics_data`, `count_sections` | ~25ms |
| Titles (`/titles`) | `list_years`, `get_titles`, `get_all_title_word_counts` | ~42ms |
| Title Structure (cold) | `get_structure`, `get_total_words`, `list_years` | ~117ms |
| Title Structure (cached) | (cached), `get_total_words`, `list_years` | ~65ms |
| Section Detail | `get_section`, `get_adjacent_sections`, etc. | ~50ms |
| Agency Detail | `get_agency`, `get_agency_chapters`, etc. | <1ms |
| Similar Sections (cold) | `get_similar_sections` | ~126ms |
| Similar Sections (cached) | (cached) | <1ms |

### Detailed Query Breakdown

| Query | Avg (ms) | Median (ms) | Max (ms) | Notes |
|-------|----------|-------------|----------|-------|
| `get_section` (by PK) | 0.1 | 0.1 | 0.1 | Excellent - indexed lookup |
| `get_titles` | 0.1 | 0.1 | 0.2 | Small table (50 rows) |
| `get_agency` | 0.1 | 0.1 | 0.1 | Indexed lookup |
| `get_agency_chapters` | 0.1 | 0.1 | 0.1 | Indexed join |
| `get_all_title_word_counts` | 0.1 | 0.1 | 0.1 | Pre-aggregated table |
| `get_total_word_counts_by_year` | 0.1 | 0.1 | 0.1 | Pre-aggregated table |
| `get_adjacent_sections` | 7.8 | 6.7 | 12.3 | Fetches all sections for title |
| `get_total_words` | 17.3 | 17.2 | 18.6 | Aggregation query |
| `count_sections` | 24.6 | 9.9 | 156.6 | Full table scan filter |
| `list_years` | 41.7 | 38.8 | 51.4 | DISTINCT on 1.4M rows |
| `get_structure` (cold) | 58.4 | 0.0 | 175.3 | Complex tree building |
| `get_similar_sections` (cold) | 125.7 | 0.4 | 376.4 | TF-IDF computation |
| `get_node_word_counts_by_year` | 189.1 | 135.6 | 582.6 | Aggregation across years |

## Bottleneck Analysis

### Slow Queries

1. **`get_node_word_counts_by_year`** (~189ms)
   - Used by: Chart page (`/chart/data/<title>/<path>`)
   - Issue: Groups across all years with partial index match
   - Index used: `idx_sections_year_title` (year first, but query filters by title)

2. **`list_years`** (~42ms)
   - Used by: Most pages (for year selector)
   - Issue: DISTINCT on 1.4M rows
   - Index used: Covering index scan, but still 1.4M rows

3. **`count_sections`** (~25ms first, then cached)
   - Used by: Home page
   - Issue: Filters `section != ''` requires checking each row

4. **`get_structure`** (cold ~58ms, cached <1ms)
   - Used by: Title structure page
   - Issue: Complex tree building from 227K sections
   - Current caching works well

5. **`get_similar_sections`** (cold ~126ms, cached <1ms)
   - Used by: Similar sections API
   - Issue: TF-IDF matrix computation per chapter
   - Current caching works well

## Index Analysis

### Current Indexes

```sql
idx_sections_year_title ON sections(year, title)
idx_sections_year_title_section ON sections(year, title, section)
idx_sections_groupby ON sections(year, title, subtitle, chapter, subchapter, part, subpart)
idx_cfr_title_chapter ON cfr_references(title, chapter)
idx_cfr_agency ON cfr_references(agency_slug)
idx_word_counts_agency ON agency_word_counts(agency_slug)
```

### Recommended Additional Indexes

```sql
-- For get_node_word_counts_by_year (title-first for filtering)
CREATE INDEX idx_sections_title_year ON sections(title, year);

-- For list_years (covering index with just year)
-- Current index already covers this, performance is acceptable
```

## Architecture Comparison

### Current: SQLite + Python (local disk)

| Strength | Description |
|----------|-------------|
| Simple lookups | <1ms (by primary key) |
| Pre-aggregated data | <1ms (title/agency word counts) |
| Caching | In-memory caches eliminate repeat queries |
| No network | Zero network latency |
| Single file | Easy deployment and backup |

| Weakness | Description |
|----------|-------------|
| Complex aggregations | 50-200ms for cross-year queries |
| TF-IDF cold start | 100-400ms per chapter (then cached) |
| No parallel queries | Single-threaded query execution |

### Alternative 1: PostgreSQL (local)

| Query Type | Current SQLite | Expected PostgreSQL |
|------------|----------------|---------------------|
| Simple lookups | <1ms | <1ms |
| Aggregations | 50-200ms | 30-150ms (-30%) |
| DISTINCT year | 42ms | 20-30ms |
| TF-IDF | Not applicable (Python) | Not applicable |

**Verdict:** Marginal improvement (~30%) not worth migration complexity.

### Alternative 2: PostgreSQL with Full-Text Search

| Query Type | Current | PostgreSQL FTS |
|------------|---------|----------------|
| TF-IDF similarity | 126ms (cold) | 10-50ms |
| Text search | N/A | <50ms |

**Verdict:** Significant improvement for similarity queries, but requires:
- Different similarity algorithm (not TF-IDF)
- Database migration
- New query patterns

### Alternative 3: Pre-computed JSON + SQLite Hybrid

Store structure trees and yearly word counts as JSON files:

```
structures/
  title_1_year_0.json
  title_1_year_2010.json
  ...
```

| Query Type | Current | Pre-computed |
|------------|---------|--------------|
| `get_structure` | 58ms cold, <1ms cached | <5ms (file read) |
| `get_node_word_counts_by_year` | 189ms | <5ms (file read) |
| `list_years` | 42ms | <1ms (hardcoded/config) |

**Verdict:** Best improvement for current bottlenecks with minimal architecture change.

### Alternative 4: Redis Cache Layer

| Query Type | Current | With Redis |
|------------|---------|------------|
| Cached queries | <1ms (in-memory) | <1ms (network to Redis) |
| Cache persistence | Lost on restart | Persists across restarts |
| Distributed | Single process | Multiple processes |

**Verdict:** Useful for multi-process deployments but adds operational complexity.

## Recommendations

### Quick Wins (Current Architecture)

1. **Cache `list_years()` result**
   - Only changes when data is imported
   - Save 42ms on most pages

2. **Pre-compute section counts**
   - Add column to `title_word_counts`: `section_count`
   - Save 25ms on home page

3. **Add title-first index**
   ```sql
   CREATE INDEX idx_sections_title_year ON sections(title, year);
   ```
   - Improves `get_node_word_counts_by_year` by ~50%

4. **Pre-warm caches on startup**
   - Already supported via `warm_structure_cache()`
   - Call during Flask app initialization

### Medium-Term Improvements

1. **Pre-compute structure JSON during import**
   - Generate `structures/title_{n}_year_{y}.json` files
   - Eliminates cold cache penalty entirely

2. **Materialize yearly aggregates**
   ```sql
   CREATE TABLE node_word_counts_by_year (
     title INTEGER,
     path TEXT,  -- 'chapter/I/part/1'
     year INTEGER,
     word_count INTEGER,
     PRIMARY KEY (title, path, year)
   );
   ```
   - Pre-compute during import
   - Chart queries become <1ms

### Not Recommended

- **PostgreSQL migration**: Marginal gains don't justify complexity
- **Elasticsearch**: Overkill for current similarity feature
- **Redis**: Adds operational complexity for single-server deployment

## Conclusion

The current SQLite architecture performs well for this workload:

- **Fast queries** (<1ms): 70% of query types
- **Acceptable queries** (1-50ms): 20% of query types
- **Slow queries** (50-200ms): 10% of query types, mostly mitigated by caching

The main optimization opportunity is **pre-computing** expensive aggregations during data import rather than changing the database architecture.
