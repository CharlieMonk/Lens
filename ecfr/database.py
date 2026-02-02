"""SQLite database operations for eCFR data."""
import json, re, sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

COLS = ("year", "title", "subtitle", "chapter", "subchapter", "part", "subpart", "section", "heading", "text", "word_count")

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
    def __init__(self, db_path: str | Path = "ecfr/ecfr_data/ecfr.db"):
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._tfidf_cache = {}
        self._stats_cache = {}
        self._stats_cache_time = 0
        self._stats_cache_ttl = 300  # 5 minutes
        self._init_schema()

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

    def _init_schema(self):
        with self._connection() as conn:
            c = conn.cursor()
            c.executescript("""CREATE TABLE IF NOT EXISTS titles (number INTEGER PRIMARY KEY, name TEXT NOT NULL, latest_amended_on TEXT, latest_issue_date TEXT, up_to_date_as_of TEXT, reserved INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS agencies (slug TEXT PRIMARY KEY, name TEXT NOT NULL, short_name TEXT, display_name TEXT, sortable_name TEXT, parent_slug TEXT);
                CREATE TABLE IF NOT EXISTS cfr_references (id INTEGER PRIMARY KEY AUTOINCREMENT, agency_slug TEXT NOT NULL, title INTEGER NOT NULL, chapter TEXT, subtitle TEXT, subchapter TEXT);
                CREATE TABLE IF NOT EXISTS agency_word_counts (year INTEGER NOT NULL DEFAULT 0, agency_slug TEXT NOT NULL, title INTEGER NOT NULL, chapter TEXT NOT NULL, word_count INTEGER DEFAULT 0, PRIMARY KEY (year, agency_slug, title, chapter));
                CREATE TABLE IF NOT EXISTS title_structures (title INTEGER NOT NULL, year INTEGER NOT NULL, structure_json TEXT NOT NULL, PRIMARY KEY (title, year));""")
            # Migrate agency_word_counts if missing year column
            c.execute("PRAGMA table_info(agency_word_counts)")
            if "year" not in {r[1] for r in c.fetchall()}:
                c.execute("ALTER TABLE agency_word_counts RENAME TO agency_word_counts_old")
                c.execute("CREATE TABLE agency_word_counts (year INTEGER NOT NULL DEFAULT 0, agency_slug TEXT NOT NULL, title INTEGER NOT NULL, chapter TEXT NOT NULL, word_count INTEGER DEFAULT 0, PRIMARY KEY (year, agency_slug, title, chapter))")
                c.execute("INSERT INTO agency_word_counts (year, agency_slug, title, chapter, word_count) SELECT 0, agency_slug, title, chapter, word_count FROM agency_word_counts_old")
                c.execute("DROP TABLE agency_word_counts_old")
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sections'")
            if c.fetchone():
                c.execute("PRAGMA table_info(sections)")
                if "year" not in {r[1] for r in c.fetchall()}:
                    c.execute("ALTER TABLE sections RENAME TO sections_old")
                    c.execute("CREATE TABLE sections (year INTEGER NOT NULL DEFAULT 0, title INTEGER NOT NULL, subtitle TEXT DEFAULT '', chapter TEXT DEFAULT '', subchapter TEXT DEFAULT '', part TEXT DEFAULT '', subpart TEXT DEFAULT '', section TEXT DEFAULT '', heading TEXT DEFAULT '', text TEXT DEFAULT '', word_count INTEGER NOT NULL, PRIMARY KEY (year, title, subtitle, chapter, subchapter, part, subpart, section))")
                    c.execute("INSERT INTO sections (year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count) SELECT 0, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count FROM sections_old")
                    c.execute("DROP TABLE sections_old")
            else:
                c.execute("CREATE TABLE IF NOT EXISTS sections (year INTEGER NOT NULL DEFAULT 0, title INTEGER NOT NULL, subtitle TEXT DEFAULT '', chapter TEXT DEFAULT '', subchapter TEXT DEFAULT '', part TEXT DEFAULT '', subpart TEXT DEFAULT '', section TEXT DEFAULT '', heading TEXT DEFAULT '', text TEXT DEFAULT '', word_count INTEGER NOT NULL, PRIMARY KEY (year, title, subtitle, chapter, subchapter, part, subpart, section))")
            for n, cols in [("idx_sections_year_title", "sections(year, title)"), ("idx_sections_year_title_section", "sections(year, title, section)"), ("idx_cfr_title_chapter", "cfr_references(title, chapter)"), ("idx_cfr_agency", "cfr_references(agency_slug)"), ("idx_word_counts_agency", "agency_word_counts(agency_slug)")]:
                c.execute(f"CREATE INDEX IF NOT EXISTS {n} ON {cols}")
            conn.commit()

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

    def save_title_structure(self, title, structure, year=0): self._execute("INSERT OR REPLACE INTO title_structures VALUES (?,?,?)", (title, year, json.dumps(structure)))
    def get_title_structure_metadata(self, title, year=0):
        r = self._query_one("SELECT structure_json FROM title_structures WHERE title=? AND year=?", (title, year)); return json.loads(r[0]) if r else None
    def has_title_structure(self, title, year=0): return self._query_one("SELECT 1 FROM title_structures WHERE title=? AND year=?", (title, year)) is not None

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
    def get_agency(self, slug):
        r = self._query_one("SELECT slug, name, short_name FROM agencies WHERE slug = ?", (slug,)); return {"slug": r[0], "name": r[1], "short_name": r[2]} if r else None

    def has_year_data(self, year): return self._query_one("SELECT COUNT(*) FROM sections WHERE year=?", (year,))[0] > 0
    def save_sections(self, sections, year=0):
        if not sections: return
        with self._connection() as c:
            cur = c.cursor()
            for s in sections: cur.execute("INSERT OR REPLACE INTO sections VALUES (?,?,?,?,?,?,?,?,?,?,?)", (year, int(s.get("title", 0)), s.get("subtitle") or "", s.get("chapter") or "", s.get("subchapter") or "", s.get("part") or "", s.get("subpart") or "", s.get("section") or "", s.get("heading") or "", s.get("text") or "", s.get("word_count", 0)))
            c.commit()

    def update_word_counts(self, title_num, chapter_wc, agency_lookup, year=0):
        if not chapter_wc: return
        with self._connection() as c:
            cur = c.cursor()
            for ch, wc in chapter_wc.items():
                for info in agency_lookup.get((title_num, ch), []): cur.execute("INSERT OR REPLACE INTO agency_word_counts VALUES (?,?,?,?,?)", (year, info["agency_slug"], title_num, ch, wc))
            c.commit()

    def list_years(self): return [r[0] for r in self._query("SELECT DISTINCT year FROM sections ORDER BY year")]
    def list_titles(self, year=0): return [r[0] for r in self._query("SELECT DISTINCT title FROM sections WHERE year=? ORDER BY title", (year,))]
    list_section_titles = list_titles

    def get_section(self, title, section, year=0):
        r = self._query_one(f"SELECT {','.join(COLS)} FROM sections WHERE year=? AND title=? AND section=?", (year, title, section)); return dict(zip(COLS, r)) if r else None

    def get_adjacent_sections(self, title, section, year=0):
        rows = self._query("SELECT section FROM sections WHERE year=? AND title=?", (year, title))
        if not rows: return None, None
        secs = sorted([r[0] for r in rows], key=section_sort_key)
        try:
            i = secs.index(section); return (secs[i-1] if i > 0 else None, secs[i+1] if i < len(secs)-1 else None)
        except ValueError: return None, None

    def get_sections(self, title, chapter=None, part=None, year=0):
        q, p = f"SELECT {','.join(COLS)} FROM sections WHERE year=? AND title=?", [year, title]
        if chapter: q, p = q+" AND chapter=?", p+[chapter]
        if part: q, p = q+" AND part=?", p+[part]
        return [dict(zip(COLS, r)) for r in self._query(q+" ORDER BY part, section", tuple(p))]

    def navigate(self, title, subtitle=None, chapter=None, subchapter=None, part=None, subpart=None, section=None, year=0):
        q, p = f"SELECT {','.join(COLS)} FROM sections WHERE year=? AND title=?", [year, title]
        for c, v in [("section", section), ("subtitle", subtitle), ("chapter", chapter), ("subchapter", subchapter), ("part", part), ("subpart", subpart)]:
            if v: q, p = q+f" AND {c}=?", p+[v]
        r = self._query_one(q+" LIMIT 1", tuple(p)); return dict(zip(COLS, r)) if r else None

    def search(self, query, title=None, year=0):
        sql, params = ("SELECT title, section, heading, text FROM sections WHERE year=? AND title=? AND text LIKE ?", [year, title, f"%{query}%"]) if title else ("SELECT title, section, heading, text FROM sections WHERE year=? AND text LIKE ?", [year, f"%{query}%"])
        ql = query.lower()
        return [{"title": t, "section": s, "heading": h, "snippet": ("..." if (i:=txt.lower().find(ql))-50>0 else "")+txt[max(0,i-50):i+len(query)+50]+("..." if i+len(query)+50<len(txt) else "")} for t, s, h, txt in self._query(sql, tuple(params))]

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

    def get_total_words(self, title, year=0): return self.get_word_counts(title, year=year)["total"]

    def get_all_title_word_counts(self, year=0):
        """Get word counts for all titles in one query."""
        return {r[0]: r[1] for r in self._query("SELECT title, SUM(word_count) FROM sections WHERE year=? GROUP BY title", (year,))}

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

    def get_statistics_data(self):
        """Get all statistics data in bulk with caching."""
        cached = self._get_cached_stats("statistics")
        if cached:
            return cached

        # Get all title word counts for current year (0) and 2020
        title_counts_current = self.get_all_title_word_counts(0)
        title_counts_2020 = self.get_all_title_word_counts(2020)

        # Get all agency word counts for current year and 2020
        agency_counts_current = self.get_agency_word_counts(0)
        agency_counts_2020 = self.get_agency_word_counts(2020)

        # Get agency details
        agency_details = {r[0]: {"name": r[1], "short_name": r[2]} for r in self._query("SELECT slug, name, short_name FROM agencies")}

        # Get title metadata
        title_meta = self.get_titles()

        result = {
            "title_counts": {0: title_counts_current, 2020: title_counts_2020},
            "agency_counts": {0: agency_counts_current, 2020: agency_counts_2020},
            "agency_details": agency_details,
            "title_meta": title_meta,
        }
        return self._set_cached_stats("statistics", result)

    def get_structure(self, title, year=0):
        wc = self.get_structure_word_counts(title, year)
        def get_wc(path):
            try:
                d = wc["subtitles"].get(path.get("subtitle", ""), {})
                for k, l in [("chapter", "chapters"), ("subchapter", "subchapters"), ("part", "parts"), ("subpart", "subparts")]:
                    if path.get(k) is None: return d.get("total", 0)
                    d = d.get(l, {}).get(path[k], {})
                return d.get("total", 0)
            except: return 0

        meta = self.get_title_structure_metadata(title, year)
        if meta:
            def count(n): return 1 if n.get("type") == "section" else sum(count(c) for c in n.get("children", []))
            def convert(n, path=None):
                path = path or {}
                t, ident = n.get("type", ""), n.get("identifier", "")
                np = dict(path)
                if t in ["subtitle", "chapter", "subchapter", "part", "subpart"]: np[t] = ident or ""
                ch = [convert(c, np) for c in sorted(n.get("children", []), key=lambda x: sort_key(x.get("identifier", ""), use_roman=x.get("type") == "chapter"))]
                r = {"type": t, "identifier": ident, "children": ch, "section_count": count(n), "reserved": n.get("reserved", False), "word_count": get_wc(np) if t in ["subtitle", "chapter", "subchapter", "part", "subpart"] else 0}
                if t == "section" and n.get("label_description"): r["heading"] = n["label_description"]
                return r
            res = convert(meta); res["word_count"] = wc.get("total", 0); return res

        rows = self._query("SELECT subtitle, chapter, subchapter, part, subpart, section, heading FROM sections WHERE year=? AND title=?", (year, title))
        if not rows: return {}
        tree = {}
        for sub, ch, subch, prt, subprt, sec, hd in rows:
            keys = [sub or "", ch or "", subch or "", prt or "", subprt or ""]
            node = tree
            for i, k in enumerate(keys): node = node.setdefault(k, {} if i < 4 else {"sections": []})
            if sec: node["sections"].append({"type": "section", "identifier": sec, "heading": hd or ""})

        def build(data, lvl, path):
            levels = ["subtitle", "chapter", "subchapter", "part", "subpart"]
            if lvl >= len(levels): return sorted(data.get("sections", []), key=section_sort_key), len(data.get("sections", []))
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
        return {"type": "title", "identifier": str(title), "children": ch, "section_count": tot, "word_count": wc.get("total", 0)}

    def get_similar_sections(self, title, section, year=0, limit=10, min_similarity=0.1):
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        r = self._query_one("SELECT chapter FROM sections WHERE year=0 AND title=? AND section=?", (title, section))
        if not r: return [], None
        ch, key = r[0], (title, r[0])
        if key not in self._tfidf_cache:
            rows = self._query("SELECT section, heading, text FROM sections WHERE year=0 AND title=? AND chapter=? AND text != ''", (title, ch))
            if len(rows) < 2: return [], None
            secs, heads, txts = [], {}, []
            for s, h, t in rows: secs.append(s); heads[s] = h; txts.append(t)
            self._tfidf_cache[key] = {"matrix": TfidfVectorizer(stop_words='english', max_features=10000).fit_transform(txts), "sections": secs, "headings": heads}
        c = self._tfidf_cache[key]
        try: idx = c["sections"].index(section)
        except ValueError: return [], None
        sims = cosine_similarity(c["matrix"][idx:idx+1], c["matrix"])[0]
        res = sorted([{"title": title, "section": s, "similarity": float(sim), "heading": c["headings"][s]} for s, sim in zip(c["sections"], sims) if s != section and sim >= min_similarity], key=lambda x: x["similarity"], reverse=True)
        return res[:limit], res[0]["similarity"] if res else None
