"""SQLite database operations for eCFR data."""
import hashlib
import json, re, sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from .config import config

# Columns for sections table (text_hash references texts table)
SECTION_COLS = ("year", "title", "subtitle", "chapter", "subchapter", "part", "subpart", "section", "heading", "text_hash", "word_count")
# Columns returned by queries (text resolved from texts table)
COLS = ("year", "title", "subtitle", "chapter", "subchapter", "part", "subpart", "section", "heading", "text", "word_count")
HISTORICAL_YEARS = sorted(config.historical_years)


def _hash_text(text: str) -> str:
    """Compute SHA-256 hash of text content."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def roman_to_int(s):
    vals = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    s = s.upper()
    if not all(c in vals for c in s): return None
    total, prev = 0, 0
    for c in reversed(s):
        v = vals[c]
        total += -v if v < prev else v
        prev = v
    return total

def sort_key(ident, use_roman=False):
    if not ident: return (2, 0, "")
    try: return (0, int(ident), "")
    except ValueError: pass
    if use_roman:
        r = roman_to_int(ident)
        if r: return (0, r, "")
    m = re.match(r'^(\d+)', ident)
    return (0, int(m.group(1)), ident) if m else (1, 0, ident)

def section_sort_key(s):
    ident = s if isinstance(s, str) else s.get("identifier", "")
    result = []
    for p in ident.split("."):
        try: result.append((0, int(p), ""))
        except ValueError: result.append((1, 0, p))
    return result

class ECFRDatabase:
    def __init__(self, db_path: str | Path | None = None):
        db_path = db_path or config.database_path
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._tfidf_cache = {}
        self._stats_cache = {}
        self._stats_cache_time = 0
        self._stats_cache_ttl = config.cache_stats_ttl
        self._structure_cache = {}
        self._structure_cache_time = 0
        self._ensure_schema()

    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self.db_path)
        try: yield conn
        finally: conn.close()

    def _query(self, sql, params=()):
        with self._connection() as c: return c.cursor().execute(sql, params).fetchall()
    def _query_one(self, sql, params=()):
        with self._connection() as c: return c.cursor().execute(sql, params).fetchone()
    def _execute(self, sql, params=()):
        with self._connection() as c: c.cursor().execute(sql, params); c.commit()

    def _ensure_schema(self):
        """Initialize schema only if database is new or missing tables."""
        if not self.db_path.exists() or self.db_path.stat().st_size == 0:
            self._init_schema()
            return
        # Check if key table exists
        with self._connection() as c:
            result = c.cursor().execute("SELECT name FROM sqlite_master WHERE type='table' AND name='titles'").fetchone()
        if not result:
            self._init_schema()

    def _init_schema(self):
        with self._connection() as conn:
            c = conn.cursor()
            # Core tables
            c.executescript("""
                CREATE TABLE IF NOT EXISTS titles (number INTEGER PRIMARY KEY, name TEXT NOT NULL, latest_amended_on TEXT, latest_issue_date TEXT, up_to_date_as_of TEXT, reserved INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS agencies (slug TEXT PRIMARY KEY, name TEXT NOT NULL, short_name TEXT, display_name TEXT, sortable_name TEXT, parent_slug TEXT);
                CREATE TABLE IF NOT EXISTS cfr_references (id INTEGER PRIMARY KEY AUTOINCREMENT, agency_slug TEXT NOT NULL, title INTEGER NOT NULL, chapter TEXT, subtitle TEXT, subchapter TEXT);
                CREATE TABLE IF NOT EXISTS agency_word_counts (year INTEGER NOT NULL DEFAULT 0, agency_slug TEXT NOT NULL, title INTEGER NOT NULL, chapter TEXT NOT NULL, word_count INTEGER DEFAULT 0, PRIMARY KEY (year, agency_slug, title, chapter));
                CREATE TABLE IF NOT EXISTS title_word_counts (year INTEGER NOT NULL, title INTEGER NOT NULL, word_count INTEGER DEFAULT 0, PRIMARY KEY (year, title));
                CREATE TABLE IF NOT EXISTS texts (hash TEXT PRIMARY KEY, content TEXT NOT NULL);
            """)
            # Migrate agency_word_counts if missing year column
            c.execute("PRAGMA table_info(agency_word_counts)")
            if "year" not in {r[1] for r in c.fetchall()}:
                c.execute("ALTER TABLE agency_word_counts RENAME TO agency_word_counts_old")
                c.execute("CREATE TABLE agency_word_counts (year INTEGER NOT NULL DEFAULT 0, agency_slug TEXT NOT NULL, title INTEGER NOT NULL, chapter TEXT NOT NULL, word_count INTEGER DEFAULT 0, PRIMARY KEY (year, agency_slug, title, chapter))")
                c.execute("INSERT INTO agency_word_counts (year, agency_slug, title, chapter, word_count) SELECT 0, agency_slug, title, chapter, word_count FROM agency_word_counts_old")
                c.execute("DROP TABLE agency_word_counts_old")
            # Check sections table schema
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sections'")
            if c.fetchone():
                c.execute("PRAGMA table_info(sections)")
                cols = {r[1] for r in c.fetchall()}
                needs_text_migration = "text" in cols and "text_hash" not in cols
                needs_year_migration = "year" not in cols

                # Check if texts table already has data (migration may have been interrupted)
                c.execute("SELECT COUNT(*) FROM texts")
                texts_populated = c.fetchone()[0] > 0

                if needs_text_migration and not texts_populated:
                    # Migrate from inline text to content-addressed storage
                    self._migrate_to_content_addressed(c, include_year=True)
                elif needs_text_migration and texts_populated:
                    # Texts already migrated, just need to update sections schema
                    print("Resuming interrupted migration...")
                    self._migrate_sections_schema(c, include_year=True)
                elif needs_year_migration:
                    # Old schema without year column
                    self._migrate_to_content_addressed(c, include_year=False)
            else:
                c.execute("""CREATE TABLE sections (
                    year INTEGER NOT NULL DEFAULT 0, title INTEGER NOT NULL,
                    subtitle TEXT DEFAULT '', chapter TEXT DEFAULT '', subchapter TEXT DEFAULT '',
                    part TEXT DEFAULT '', subpart TEXT DEFAULT '', section TEXT DEFAULT '',
                    heading TEXT DEFAULT '', text_hash TEXT DEFAULT '', word_count INTEGER NOT NULL,
                    PRIMARY KEY (year, title, subtitle, chapter, subchapter, part, subpart, section))""")
            # Indexes
            for n, cols in [
                ("idx_sections_year_title", "sections(year, title)"),
                ("idx_sections_year_title_section", "sections(year, title, section)"),
                ("idx_sections_groupby", "sections(year, title, subtitle, chapter, subchapter, part, subpart)"),
                ("idx_cfr_title_chapter", "cfr_references(title, chapter)"),
                ("idx_cfr_agency", "cfr_references(agency_slug)"),
                ("idx_word_counts_agency", "agency_word_counts(agency_slug)")
            ]:
                c.execute(f"CREATE INDEX IF NOT EXISTS {n} ON {cols}")
            # Drop unused tables
            c.execute("DROP TABLE IF EXISTS title_structures")
            c.execute("DROP TABLE IF EXISTS structure_nodes")
            # Populate title_word_counts if empty but sections has data
            c.execute("SELECT COUNT(*) FROM title_word_counts")
            if c.fetchone()[0] == 0:
                c.execute("SELECT COUNT(*) FROM sections")
                if c.fetchone()[0] > 0:
                    c.execute("INSERT INTO title_word_counts SELECT year, title, SUM(word_count) FROM sections GROUP BY year, title")
            conn.commit()

    def _migrate_to_content_addressed(self, cursor, include_year=True):
        """Migrate from inline text to content-addressed storage with progress reporting."""
        print("Migrating to content-addressed storage...")

        # Count total texts to migrate
        cursor.execute("SELECT COUNT(DISTINCT text) FROM sections WHERE text != ''")
        total_texts = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM sections")
        total_sections = cursor.fetchone()[0]
        print(f"  Found {total_texts:,} unique texts in {total_sections:,} sections")

        # Extract unique texts with progress
        print("  Extracting unique texts...")
        cursor.execute("SELECT DISTINCT text FROM sections WHERE text != ''")
        texts = cursor.fetchall()

        batch_size = 10000
        for i, (text,) in enumerate(texts):
            h = _hash_text(text)
            cursor.execute("INSERT OR IGNORE INTO texts (hash, content) VALUES (?, ?)", (h, text))
            if (i + 1) % batch_size == 0:
                print(f"    Processed {i + 1:,}/{total_texts:,} texts ({(i + 1) * 100 // total_texts}%)")
        print(f"  Extracted {total_texts:,} unique texts")

        # Migrate sections schema
        self._migrate_sections_schema(cursor, include_year)
        print("Migration complete.")

    def _migrate_sections_schema(self, cursor, include_year=True):
        """Update sections table to use text_hash instead of inline text."""
        print("  Updating sections table schema...")

        cursor.execute("ALTER TABLE sections RENAME TO sections_old")
        cursor.execute("""CREATE TABLE sections (
            year INTEGER NOT NULL DEFAULT 0, title INTEGER NOT NULL,
            subtitle TEXT DEFAULT '', chapter TEXT DEFAULT '', subchapter TEXT DEFAULT '',
            part TEXT DEFAULT '', subpart TEXT DEFAULT '', section TEXT DEFAULT '',
            heading TEXT DEFAULT '', text_hash TEXT DEFAULT '', word_count INTEGER NOT NULL,
            PRIMARY KEY (year, title, subtitle, chapter, subchapter, part, subpart, section))""")

        # Build hash lookup for efficient migration
        print("  Building text hash lookup...")
        cursor.execute("SELECT hash, content FROM texts")
        text_to_hash = {content: hash for hash, content in cursor.fetchall()}

        # Migrate sections in batches with progress
        cursor.execute("SELECT COUNT(*) FROM sections_old")
        total = cursor.fetchone()[0]
        print(f"  Migrating {total:,} sections...")

        year_select = "year" if include_year else "0"
        cursor.execute(f"SELECT {year_select}, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count FROM sections_old")

        batch = []
        batch_size = 10000
        processed = 0
        for row in cursor.fetchall():
            year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count = row
            text_hash = text_to_hash.get(text, "") if text else ""
            batch.append((year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text_hash, word_count))

            if len(batch) >= batch_size:
                cursor.executemany("INSERT INTO sections VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch)
                processed += len(batch)
                print(f"    Migrated {processed:,}/{total:,} sections ({processed * 100 // total}%)")
                batch = []

        if batch:
            cursor.executemany("INSERT INTO sections VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch)
            processed += len(batch)
            print(f"    Migrated {processed:,}/{total:,} sections (100%)")

        cursor.execute("DROP TABLE sections_old")
        print("  Schema migration complete.")

    def is_fresh(self): return self.db_path.exists() and self.db_path.stat().st_mtime >= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    def clear(self):
        if self.db_path.exists(): self.db_path.unlink()

    def get_titles(self): return {r[0]: {"name": r[1], "latest_amended_on": r[2], "latest_issue_date": r[3], "up_to_date_as_of": r[4], "reserved": bool(r[5])} for r in self._query("SELECT number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved FROM titles")}
    def has_titles(self): return self._query_one("SELECT COUNT(*) FROM titles")[0] > 0
    def save_titles(self, titles):
        with self._connection() as c:
            cur = c.cursor(); cur.execute("DELETE FROM titles")
            for t in titles: cur.execute("INSERT INTO titles (number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved) VALUES (?,?,?,?,?,?)", (t["number"], t.get("name"), t.get("latest_amended_on"), t.get("latest_issue_date"), t.get("up_to_date_as_of"), 1 if t.get("reserved") else 0))
            c.commit()

    def get_stale_titles(self, api_titles):
        stored = self.get_titles()
        return sorted([t["number"] for t in api_titles if 1 <= t["number"] <= 50 and t.get("latest_amended_on") and (t["number"] not in stored or stored[t["number"]].get("latest_amended_on") != t.get("latest_amended_on"))])

    def delete_title_sections(self, title, year=0):
        cnt = self._query_one("SELECT COUNT(*) FROM sections WHERE year=? AND title=?", (year, title))[0]
        self._execute("DELETE FROM sections WHERE year=? AND title=?", (year, title)); return cnt

    def has_agencies(self): return self._query_one("SELECT COUNT(*) FROM agencies")[0] > 0
    def save_agencies(self, agencies):
        with self._connection() as c:
            cur = c.cursor(); cur.execute("DELETE FROM cfr_references"); cur.execute("DELETE FROM agencies")
            for a in agencies:
                for ag, ps in [(a, None)] + [(ch, a["slug"]) for ch in a.get("children", [])]:
                    cur.execute("INSERT INTO agencies VALUES (?,?,?,?,?,?)", (ag["slug"], ag.get("name"), ag.get("short_name"), ag.get("display_name"), ag.get("sortable_name"), ps))
                    for r in ag.get("cfr_references", []): cur.execute("INSERT INTO cfr_references (agency_slug, title, chapter, subtitle, subchapter) VALUES (?,?,?,?,?)", (ag["slug"], r.get("title"), r.get("chapter"), r.get("subtitle"), r.get("subchapter")))
            c.commit()

    def build_agency_lookup(self):
        lookup = {}
        for t, ch, s, n, ps, pn in self._query("SELECT r.title, COALESCE(r.chapter, r.subtitle, r.subchapter), a.slug, a.name, a.parent_slug, p.name FROM cfr_references r JOIN agencies a ON r.agency_slug = a.slug LEFT JOIN agencies p ON a.parent_slug = p.slug WHERE COALESCE(r.chapter, r.subtitle, r.subchapter) IS NOT NULL"):
            if t and ch: lookup.setdefault((t, ch), []).append({"agency_slug": s, "agency_name": n, "parent_slug": ps, "parent_name": pn})
        return lookup

    def get_agency_word_counts(self, year=0):
        direct = {r[0]: r[1] for r in self._query("SELECT agency_slug, SUM(word_count) FROM agency_word_counts WHERE year=? GROUP BY agency_slug", (year,))}
        # Fall back to computing from sections if no cached data for this year
        if not direct and self.has_year_data(year):
            direct = {r[0]: r[1] for r in self._query("SELECT r.agency_slug, SUM(s.word_count) FROM sections s JOIN cfr_references r ON s.title = r.title AND s.chapter = COALESCE(r.chapter, r.subtitle, r.subchapter) WHERE s.year = ? GROUP BY r.agency_slug", (year,))}
        totals = dict(direct)
        for child, parent in {r[0]: r[1] for r in self._query("SELECT slug, parent_slug FROM agencies WHERE parent_slug IS NOT NULL")}.items():
            if child in direct: totals[parent] = totals.get(parent, 0) + direct[child]
        return totals

    def get_agency_chapters(self, slug): return [{"title": r[0], "chapter": r[1], "title_name": r[2]} for r in self._query("SELECT r.title, COALESCE(r.chapter, r.subtitle, r.subchapter), t.name FROM cfr_references r JOIN titles t ON r.title = t.number WHERE r.agency_slug = ? ORDER BY r.title", (slug,)) if r[1]]

    def get_agency_chapter_word_counts(self, slug, year=0):
        """Get word counts by title/chapter for a specific agency."""
        return [{"title": r[0], "chapter": r[1], "word_count": r[2]} for r in self._query("SELECT title, chapter, word_count FROM agency_word_counts WHERE agency_slug = ? AND year = ? ORDER BY title, chapter", (slug, year))]
    def get_agency(self, slug):
        r = self._query_one("SELECT slug, name, short_name FROM agencies WHERE slug = ?", (slug,)); return {"slug": r[0], "name": r[1], "short_name": r[2]} if r else None

    def has_year_data(self, year): return self._query_one("SELECT COUNT(*) FROM sections WHERE year=?", (year,))[0] > 0
    def save_sections(self, sections, year=0):
        if not sections: return
        with self._connection() as c:
            cur = c.cursor()
            for s in sections:
                text = s.get("text") or ""
                text_hash = _hash_text(text) if text else ""
                if text:
                    cur.execute("INSERT OR IGNORE INTO texts (hash, content) VALUES (?, ?)", (text_hash, text))
                cur.execute("INSERT OR REPLACE INTO sections VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (year, int(s.get("title", 0)), s.get("subtitle") or "", s.get("chapter") or "",
                     s.get("subchapter") or "", s.get("part") or "", s.get("subpart") or "",
                     s.get("section") or "", s.get("heading") or "", text_hash, s.get("word_count", 0)))
            c.commit()

    def update_word_counts(self, title_num, chapter_wc, agency_lookup, year=0):
        if not chapter_wc: return
        with self._connection() as c:
            cur = c.cursor()
            for ch, wc in chapter_wc.items():
                for info in agency_lookup.get((title_num, ch), []): cur.execute("INSERT OR REPLACE INTO agency_word_counts VALUES (?,?,?,?,?)", (year, info["agency_slug"], title_num, ch, wc))
            c.commit()

    def populate_title_word_counts(self):
        """Populate title_word_counts from existing section data (for migration)."""
        self._execute("DELETE FROM title_word_counts")
        self._execute("INSERT INTO title_word_counts SELECT year, title, SUM(word_count) FROM sections GROUP BY year, title")

    def get_title_word_counts_by_year(self):
        """Get all title word counts grouped by year (for charts). Returns {year: {title: count}}."""
        result = {}
        for year, title, wc in self._query("SELECT year, title, word_count FROM title_word_counts ORDER BY year, title"):
            result.setdefault(year, {})[title] = wc
        return result

    def get_total_word_counts_by_year(self):
        """Get total CFR word count by year (for charts). Returns {year: total}."""
        return {r[0]: r[1] for r in self._query("SELECT year, SUM(word_count) FROM title_word_counts GROUP BY year ORDER BY year")}

    def list_years(self):
        """Return available years from config. Year 0 represents current data."""
        return [0] + HISTORICAL_YEARS
    def list_titles(self, year=0): return [r[0] for r in self._query("SELECT DISTINCT title FROM sections WHERE year=? ORDER BY title", (year,))]
    list_section_titles = list_titles

    def _section_select(self):
        """SQL SELECT clause that joins sections with texts table."""
        return """SELECT s.year, s.title, s.subtitle, s.chapter, s.subchapter, s.part, s.subpart,
                         s.section, s.heading, COALESCE(t.content, '') as text, s.word_count
                  FROM sections s LEFT JOIN texts t ON s.text_hash = t.hash"""

    def get_section(self, title, section, year=0):
        r = self._query_one(f"{self._section_select()} WHERE s.year=? AND s.title=? AND s.section=?", (year, title, section))
        return dict(zip(COLS, r)) if r else None

    def get_adjacent_sections(self, title, section, year=0):
        rows = self._query("SELECT section FROM sections WHERE year=? AND title=?", (year, title))
        if not rows: return None, None
        secs = sorted([r[0] for r in rows], key=section_sort_key)
        try:
            i = secs.index(section); return (secs[i-1] if i > 0 else None, secs[i+1] if i < len(secs)-1 else None)
        except ValueError: return None, None

    def get_sections(self, title, chapter=None, part=None, year=0):
        q, p = f"{self._section_select()} WHERE s.year=? AND s.title=?", [year, title]
        if chapter: q, p = q+" AND s.chapter=?", p+[chapter]
        if part: q, p = q+" AND s.part=?", p+[part]
        return [dict(zip(COLS, r)) for r in self._query(q+" ORDER BY s.part, s.section", tuple(p))]

    def navigate(self, title, subtitle=None, chapter=None, subchapter=None, part=None, subpart=None, section=None, year=0):
        q, p = f"{self._section_select()} WHERE s.year=? AND s.title=?", [year, title]
        for c, v in [("section", section), ("subtitle", subtitle), ("chapter", chapter), ("subchapter", subchapter), ("part", part), ("subpart", subpart)]:
            if v: q, p = q+f" AND s.{c}=?", p+[v]
        r = self._query_one(q+" LIMIT 1", tuple(p)); return dict(zip(COLS, r)) if r else None

    def search(self, query, title=None, year=0):
        base = f"{self._section_select()} WHERE s.year=? AND t.content LIKE ?"
        if title:
            sql, params = base + " AND s.title=?", [year, f"%{query}%", title]
        else:
            sql, params = base, [year, f"%{query}%"]
        ql = query.lower()
        return [{"title": r[1], "section": r[7], "heading": r[8], "snippet": ("..." if (i:=r[9].lower().find(ql))-50>0 else "")+r[9][max(0,i-50):i+len(query)+50]+("..." if i+len(query)+50<len(r[9]) else "")} for r in self._query(sql, tuple(params))]

    def get_structure_word_counts(self, title, year=0):
        result = {"total": 0, "subtitles": {}}
        for sub, ch, subch, prt, subprt, wc in self._query("SELECT subtitle, chapter, subchapter, part, subpart, SUM(word_count) FROM sections WHERE year=? AND title=? GROUP BY subtitle, chapter, subchapter, part, subpart", (year, title)):
            result["total"] += wc
            keys = [sub or "", ch or "", subch or "", prt or "", subprt or ""]
            d = result["subtitles"].setdefault(keys[0], {"total": 0, "chapters": {}})
            d["total"] += wc
            d = d["chapters"].setdefault(keys[1], {"total": 0, "subchapters": {}})
            d["total"] += wc
            d = d["subchapters"].setdefault(keys[2], {"total": 0, "parts": {}})
            d["total"] += wc
            d = d["parts"].setdefault(keys[3], {"total": 0, "subparts": {}})
            d["total"] += wc
            d["subparts"].setdefault(keys[4], {"total": 0})["total"] += wc
        return result

    def get_word_counts(self, title, chapter=None, subchapter=None, part=None, subpart=None, year=0):
        q, p = "SELECT section, word_count FROM sections WHERE year=? AND title=?", [year, title]
        for c, v in [("chapter", chapter), ("subchapter", subchapter), ("part", part), ("subpart", subpart)]:
            if v: q, p = q+f" AND {c}=?", p+[v]
        rows = self._query(q, tuple(p)); return {"sections": {r[0]: r[1] for r in rows if r[0]}, "total": sum(r[1] for r in rows)}

    def get_total_words(self, title, year=0):
        """Get total word count for a title. Uses pre-aggregated table for speed."""
        r = self._query_one("SELECT word_count FROM title_word_counts WHERE year=? AND title=?", (year, title))
        if r:
            return r[0]
        return self.get_word_counts(title, year=year)["total"]

    def get_all_title_word_counts(self, year=0):
        """Get word counts for all titles in one query."""
        result = {r[0]: r[1] for r in self._query("SELECT title, word_count FROM title_word_counts WHERE year=?", (year,))}
        if not result and self.has_year_data(year):
            result = {r[0]: r[1] for r in self._query("SELECT title, SUM(word_count) FROM sections WHERE year=? GROUP BY title", (year,))}
        return result

    def _get_cached_stats(self, key):
        """Get cached statistics if still valid."""
        import time
        if time.time() - self._stats_cache_time > self._stats_cache_ttl:
            self._stats_cache = {}
            self._stats_cache_time = time.time()
        return self._stats_cache.get(key)

    def _set_cached_stats(self, key, value):
        """Cache statistics value."""
        import time
        self._stats_cache[key] = value
        self._stats_cache_time = time.time()
        return value

    def get_statistics_data(self, baseline_year: int = 2010, year: int = 0):
        """Get all statistics data in bulk with caching."""
        cache_key = f"statistics_{baseline_year}_{year}"
        cached = self._get_cached_stats(cache_key)
        if cached:
            return cached

        # Get all title word counts for selected year and baseline
        title_counts_year = self.get_all_title_word_counts(year)
        title_counts_baseline = self.get_all_title_word_counts(baseline_year)

        # Get all agency word counts for selected year and baseline
        agency_counts_year = self.get_agency_word_counts(year)
        agency_counts_baseline = self.get_agency_word_counts(baseline_year)

        # Get agency details
        agency_details = {r[0]: {"name": r[1], "short_name": r[2]} for r in self._query("SELECT slug, name, short_name FROM agencies")}

        # Get title metadata
        title_meta = self.get_titles()

        result = {
            "title_counts": {year: title_counts_year, baseline_year: title_counts_baseline},
            "agency_counts": {year: agency_counts_year, baseline_year: agency_counts_baseline},
            "agency_details": agency_details,
            "title_meta": title_meta,
        }
        return self._set_cached_stats(cache_key, result)

    def get_structure(self, title, year=0):
        import time
        cache_key = (title, year)
        # Check cache TTL
        if time.time() - self._structure_cache_time > config.cache_structure_ttl:
            self._structure_cache = {}
            self._structure_cache_time = time.time()
        if cache_key in self._structure_cache:
            return self._structure_cache[cache_key]

        wc = self.get_structure_word_counts(title, year)
        section_wc = {r[0]: r[1] for r in self._query("SELECT section, word_count FROM sections WHERE year=? AND title=?", (year, title))}
        def get_wc(path):
            try:
                d = wc["subtitles"].get(path.get("subtitle", ""), {})
                for k, l in [("chapter", "chapters"), ("subchapter", "subchapters"), ("part", "parts"), ("subpart", "subparts")]:
                    if path.get(k) is None: return d.get("total", 0)
                    d = d.get(l, {}).get(path[k], {})
                return d.get("total", 0)
            except: return 0

        # Build structure from sections table
        rows = self._query("SELECT subtitle, chapter, subchapter, part, subpart, section, heading FROM sections WHERE year=? AND title=?", (year, title))
        if not rows:
            self._structure_cache[cache_key] = {}
            return {}
        tree = {}
        for sub, ch, subch, prt, subprt, sec, hd in rows:
            keys = [sub or "", ch or "", subch or "", prt or "", subprt or ""]
            node = tree
            for i, k in enumerate(keys): node = node.setdefault(k, {} if i < 4 else {"sections": []})
            if sec: node["sections"].append({"type": "section", "identifier": sec, "heading": hd or "", "word_count": section_wc.get(sec, 0)})

        def build(data, lvl, path):
            levels = ["subtitle", "chapter", "subchapter", "part", "subpart"]
            if lvl >= len(levels):
                secs = sorted(data.get("sections", []), key=section_sort_key)
                return secs, len(secs)
            ch, tot = [], 0
            for k in sorted(data.keys(), key=lambda x: sort_key(x, use_roman=(lvl == 1))):
                if k == "sections": continue
                np = dict(path, **{levels[lvl]: k})
                sub_ch, cnt = build(data[k], lvl+1, np)
                w = get_wc(np)
                if k: ch.append({"type": levels[lvl], "identifier": k, "children": sub_ch, "section_count": cnt, "word_count": w})
                else: ch.extend(sub_ch)
                tot += cnt
            return ch, tot

        ch, tot = build(tree, 0, {})
        result = {"type": "title", "identifier": str(title), "children": ch, "section_count": tot, "word_count": wc.get("total", 0)}
        self._structure_cache[cache_key] = result
        return result

    def warm_structure_cache(self, years=None, titles=None):
        """Pre-load structure cache for all titles and years. ~544MB RAM for all data."""
        import time
        years = years or [0] + [y for y in self.list_years() if y != 0]
        titles = titles or [t for t in range(1, 51) if t != 35]
        self._structure_cache = {}
        self._structure_cache_time = time.time()
        count = 0
        for year in years:
            for title in titles:
                self.get_structure(title, year)
                count += 1
        return count

    def get_node_word_counts_by_year(self, title, path=""):
        """Get word counts for a node across all years. Path format: 'chapter/I/part/1'."""
        # Use pre-aggregated table when no path filter (title-level query)
        if not path:
            return {r[0]: r[1] for r in self._query(
                "SELECT year, word_count FROM title_word_counts WHERE title=? ORDER BY year",
                (title,)
            )}

        # Parse path filters
        filters = {}
        parts = path.strip("/").split("/")
        for i in range(0, len(parts), 2):
            if i + 1 < len(parts):
                filters[parts[i]] = parts[i + 1]

        q = "SELECT year, SUM(word_count) FROM sections WHERE title=?"
        p = [title]
        for col in ["subtitle", "chapter", "subchapter", "part", "subpart", "section"]:
            if col in filters:
                q += f" AND {col}=?"
                p.append(filters[col])
        q += " GROUP BY year ORDER BY year"

        return {r[0]: r[1] for r in self._query(q, tuple(p))}

    def get_similar_sections(self, title, section, year=0, limit=None, min_similarity=None):
        import numpy as np
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        limit = limit if limit is not None else config.similar_default_limit
        min_similarity = min_similarity if min_similarity is not None else config.similar_min_similarity
        r = self._query_one("SELECT chapter FROM sections WHERE year=0 AND title=? AND section=?", (title, section))
        if not r: return [], None
        ch, key = r[0], (title, r[0])
        if key not in self._tfidf_cache:
            rows = self._query("""SELECT s.section, s.heading, t.content
                FROM sections s JOIN texts t ON s.text_hash = t.hash
                WHERE s.year=0 AND s.title=? AND s.chapter=? AND s.text_hash != ''""", (title, ch))
            if len(rows) < 2: return [], None
            secs, heads, txts = [], {}, []
            for s, h, t in rows: secs.append(s); heads[s] = h; txts.append(t)
            vectorizer = TfidfVectorizer(stop_words='english', max_features=config.tfidf_max_features)
            matrix = vectorizer.fit_transform(txts)
            self._tfidf_cache[key] = {"matrix": matrix, "vectorizer": vectorizer, "sections": secs, "headings": heads}
        c = self._tfidf_cache[key]
        try: idx = c["sections"].index(section)
        except ValueError: return [], None
        sims = cosine_similarity(c["matrix"][idx:idx+1], c["matrix"])[0]
        feature_names = c["vectorizer"].get_feature_names_out()
        source_vec = c["matrix"][idx].toarray().flatten()

        def get_shared_keywords(target_idx, top_n=None):
            """Get top shared keywords between source and target sections."""
            top_n = top_n if top_n is not None else config.similar_keywords_count
            target_vec = c["matrix"][target_idx].toarray().flatten()
            # Find terms present in both (min of the two scores)
            shared = np.minimum(source_vec, target_vec)
            top_indices = shared.argsort()[-top_n:][::-1]
            return [feature_names[i] for i in top_indices if shared[i] > 0]

        res = []
        for i, (s, sim) in enumerate(zip(c["sections"], sims)):
            if s != section and sim >= min_similarity:
                res.append({"title": title, "section": s, "similarity": float(sim), "heading": c["headings"][s], "keywords": get_shared_keywords(i)})
        res.sort(key=lambda x: x["similarity"], reverse=True)
        return res[:limit], res[0]["similarity"] if res else None
