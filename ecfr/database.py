"""SQLite database operations for eCFR data."""

import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

class ECFRDatabase:
    """Handles all SQLite database operations for eCFR data."""

    SECTION_COLUMNS = (
        "year", "title", "subtitle", "chapter", "subchapter",
        "part", "subpart", "section", "heading", "text", "word_count"
    )

    def __init__(self, db_path: str | Path = "ecfr/ecfr_data/ecfr.db"):
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._tfidf_cache = {}  # Cache for TF-IDF matrices: (title, chapter) -> {matrix, sections, headings}
        self._init_schema()

    @contextmanager
    def _connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _query(self, sql: str, params: tuple = ()) -> list:
        """Execute a query and return all results."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _query_one(self, sql: str, params: tuple = ()):
        """Execute a query and return first result."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchone()

    def _execute(self, sql: str, params: tuple = ()) -> None:
        """Execute a statement and commit."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()

    def _row_to_section(self, row: tuple) -> dict:
        """Convert a database row to a section dict."""
        return dict(zip(self.SECTION_COLUMNS, row))

    # Schema initialization

    def _init_schema(self) -> None:
        """Initialize the database schema."""
        with self._connection() as conn:
            cursor = conn.cursor()
            self._create_tables(cursor)
            self._migrate_sections_table(cursor, conn)
            self._create_indexes(cursor)
            conn.commit()

    def _create_tables(self, cursor: sqlite3.Cursor) -> None:
        """Create all tables."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS titles (
                number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                latest_amended_on TEXT,
                latest_issue_date TEXT,
                up_to_date_as_of TEXT,
                reserved INTEGER NOT NULL DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                slug TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                short_name TEXT,
                display_name TEXT,
                sortable_name TEXT,
                parent_slug TEXT,
                FOREIGN KEY (parent_slug) REFERENCES agencies(slug)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cfr_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency_slug TEXT NOT NULL,
                title INTEGER NOT NULL,
                chapter TEXT,
                subtitle TEXT,
                subchapter TEXT,
                FOREIGN KEY (agency_slug) REFERENCES agencies(slug)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agency_word_counts (
                agency_slug TEXT NOT NULL,
                title INTEGER NOT NULL,
                chapter TEXT NOT NULL,
                word_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (agency_slug, title, chapter),
                FOREIGN KEY (agency_slug) REFERENCES agencies(slug)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_structures (
                title INTEGER NOT NULL,
                year INTEGER NOT NULL,
                structure_json TEXT NOT NULL,
                PRIMARY KEY (title, year)
            )
        """)

    def _migrate_sections_table(self, cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
        """Handle sections table creation and migration."""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sections'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            cursor.execute("PRAGMA table_info(sections)")
            columns = {row[1] for row in cursor.fetchall()}
            if "year" not in columns:
                cursor.execute("ALTER TABLE sections RENAME TO sections_old")
                self._create_sections_table(cursor)
                cursor.execute("""
                    INSERT INTO sections (year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count)
                    SELECT 0, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count
                    FROM sections_old
                """)
                cursor.execute("DROP TABLE sections_old")
                conn.commit()
        else:
            self._create_sections_table(cursor)

    def _create_sections_table(self, cursor: sqlite3.Cursor) -> None:
        """Create the sections table."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sections (
                year INTEGER NOT NULL DEFAULT 0,
                title INTEGER NOT NULL,
                subtitle TEXT NOT NULL DEFAULT '',
                chapter TEXT NOT NULL DEFAULT '',
                subchapter TEXT NOT NULL DEFAULT '',
                part TEXT NOT NULL DEFAULT '',
                subpart TEXT NOT NULL DEFAULT '',
                section TEXT NOT NULL DEFAULT '',
                heading TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                word_count INTEGER NOT NULL,
                PRIMARY KEY (year, title, subtitle, chapter, subchapter, part, subpart, section)
            )
        """)

    def _create_indexes(self, cursor: sqlite3.Cursor) -> None:
        """Create all indexes."""
        indexes = [
            ("idx_sections_year_title", "sections(year, title)"),
            ("idx_sections_year_title_section", "sections(year, title, section)"),
            ("idx_cfr_title_chapter", "cfr_references(title, chapter)"),
            ("idx_cfr_agency", "cfr_references(agency_slug)"),
            ("idx_word_counts_agency", "agency_word_counts(agency_slug)"),
        ]
        for name, columns in indexes:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {columns}")

    # Utility methods

    def is_fresh(self) -> bool:
        """Check if the database was modified today."""
        if not self.db_path.exists():
            return False
        midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        return self.db_path.stat().st_mtime >= midnight

    def clear(self) -> None:
        """Delete the database file."""
        if self.db_path.exists():
            self.db_path.unlink()

    # Titles

    def get_titles(self) -> dict[int, dict]:
        """Get all titles from the database."""
        rows = self._query("""
            SELECT number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved
            FROM titles
        """)
        return {
            row[0]: {
                "name": row[1],
                "latest_amended_on": row[2],
                "latest_issue_date": row[3],
                "up_to_date_as_of": row[4],
                "reserved": bool(row[5]),
            }
            for row in rows
        }

    def has_titles(self) -> bool:
        """Check if titles table has data."""
        return self._query_one("SELECT COUNT(*) FROM titles")[0] > 0

    def save_titles(self, titles: list[dict]) -> None:
        """Save titles to the database, replacing existing data."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM titles")
            for t in titles:
                cursor.execute("""
                    INSERT INTO titles (number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    t["number"],
                    t.get("name"),
                    t.get("latest_amended_on"),
                    t.get("latest_issue_date"),
                    t.get("up_to_date_as_of"),
                    1 if t.get("reserved", False) else 0,
                ))
            conn.commit()

    def get_stale_titles(self, api_titles: list[dict]) -> list[int]:
        """Find titles that need updates based on API metadata.

        Compares latest_amended_on from API with stored values.
        Returns list of title numbers that are stale or missing.
        """
        stored = self.get_titles()
        stale = []

        for t in api_titles:
            num = t["number"]
            if num < 1 or num > 50:
                continue

            api_amended = t.get("latest_amended_on")
            if not api_amended:
                continue

            stored_meta = stored.get(num)
            if not stored_meta:
                stale.append(num)
            elif stored_meta.get("latest_amended_on") != api_amended:
                stale.append(num)

        return sorted(stale)

    def delete_title_sections(self, title: int, year: int = 0) -> int:
        """Delete all sections for a specific title and year.

        Returns number of sections deleted.
        """
        count = self._query_one(
            "SELECT COUNT(*) FROM sections WHERE year = ? AND title = ?",
            (year, title)
        )[0]

        self._execute(
            "DELETE FROM sections WHERE year = ? AND title = ?",
            (year, title)
        )

        return count

    # Title structure metadata (for reserved elements)

    def save_title_structure(self, title: int, structure: dict, year: int = 0) -> None:
        """Save the structure metadata for a title (from eCFR structure API)."""
        import json
        self._execute("""
            INSERT OR REPLACE INTO title_structures (title, year, structure_json)
            VALUES (?, ?, ?)
        """, (title, year, json.dumps(structure)))

    def get_title_structure_metadata(self, title: int, year: int = 0) -> dict | None:
        """Get the structure metadata for a title."""
        import json
        row = self._query_one(
            "SELECT structure_json FROM title_structures WHERE title = ? AND year = ?",
            (title, year)
        )
        return json.loads(row[0]) if row else None

    def has_title_structure(self, title: int, year: int = 0) -> bool:
        """Check if structure metadata exists for a title."""
        row = self._query_one(
            "SELECT 1 FROM title_structures WHERE title = ? AND year = ?",
            (title, year)
        )
        return row is not None

    # Agencies

    def has_agencies(self) -> bool:
        """Check if agencies table has data."""
        return self._query_one("SELECT COUNT(*) FROM agencies")[0] > 0

    def save_agencies(self, agencies: list[dict]) -> None:
        """Save agencies and their CFR references to the database."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cfr_references")
            cursor.execute("DELETE FROM agencies")

            for agency in agencies:
                self._save_agency(cursor, agency, parent_slug=None)
                for child in agency.get("children", []):
                    self._save_agency(cursor, child, parent_slug=agency["slug"])

            conn.commit()

    def _save_agency(self, cursor: sqlite3.Cursor, agency: dict, parent_slug: str | None) -> None:
        """Save a single agency and its CFR references."""
        slug = agency["slug"]
        cursor.execute("""
            INSERT INTO agencies (slug, name, short_name, display_name, sortable_name, parent_slug)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            slug,
            agency.get("name"),
            agency.get("short_name"),
            agency.get("display_name"),
            agency.get("sortable_name"),
            parent_slug,
        ))

        for ref in agency.get("cfr_references", []):
            cursor.execute("""
                INSERT INTO cfr_references (agency_slug, title, chapter, subtitle, subchapter)
                VALUES (?, ?, ?, ?, ?)
            """, (slug, ref.get("title"), ref.get("chapter"), ref.get("subtitle"), ref.get("subchapter")))

    def build_agency_lookup(self) -> dict:
        """Build a lookup table mapping CFR references to agency info."""
        lookup = defaultdict(list)
        rows = self._query("""
            SELECT
                r.title,
                COALESCE(r.chapter, r.subtitle, r.subchapter) as chapter,
                a.slug as agency_slug,
                a.name as agency_name,
                a.parent_slug,
                p.name as parent_name
            FROM cfr_references r
            JOIN agencies a ON r.agency_slug = a.slug
            LEFT JOIN agencies p ON a.parent_slug = p.slug
            WHERE COALESCE(r.chapter, r.subtitle, r.subchapter) IS NOT NULL
        """)

        for title, chapter, agency_slug, agency_name, parent_slug, parent_name in rows:
            if title and chapter:
                lookup[(title, chapter)].append({
                    "agency_slug": agency_slug,
                    "agency_name": agency_name,
                    "parent_slug": parent_slug,
                    "parent_name": parent_name,
                })

        return dict(lookup)

    def get_agency_word_counts(self) -> dict[str, int]:
        """Get total word counts for all agencies, including parent aggregates."""
        direct_counts = {
            row[0]: row[1]
            for row in self._query("SELECT agency_slug, SUM(word_count) FROM agency_word_counts GROUP BY agency_slug")
        }
        child_to_parent = {
            row[0]: row[1]
            for row in self._query("SELECT slug, parent_slug FROM agencies WHERE parent_slug IS NOT NULL")
        }

        totals = dict(direct_counts)
        for child_slug, parent_slug in child_to_parent.items():
            if child_slug in direct_counts:
                totals[parent_slug] = totals.get(parent_slug, 0) + direct_counts[child_slug]

        return totals

    def get_agency_chapters(self, agency_slug: str) -> list[dict]:
        """Get CFR chapters associated with an agency.

        Returns list of dicts with title, chapter, and title name.
        """
        rows = self._query("""
            SELECT r.title, COALESCE(r.chapter, r.subtitle, r.subchapter) as chapter, t.name
            FROM cfr_references r
            JOIN titles t ON r.title = t.number
            WHERE r.agency_slug = ?
            ORDER BY r.title, chapter
        """, (agency_slug,))

        return [
            {"title": row[0], "chapter": row[1], "title_name": row[2]}
            for row in rows if row[1]
        ]

    def get_agency(self, slug: str) -> dict | None:
        """Get agency details by slug."""
        row = self._query_one(
            "SELECT slug, name, short_name FROM agencies WHERE slug = ?",
            (slug,)
        )
        if row:
            return {"slug": row[0], "name": row[1], "short_name": row[2]}
        return None

    # Sections - Write

    def has_year_data(self, year: int) -> bool:
        """Check if sections table has data for a specific year."""
        return self._query_one("SELECT COUNT(*) FROM sections WHERE year = ?", (year,))[0] > 0

    def save_sections(self, sections: list[dict], year: int = 0) -> None:
        """Save section data to the database."""
        if not sections:
            return

        with self._connection() as conn:
            cursor = conn.cursor()
            for s in sections:
                cursor.execute("""
                    INSERT OR REPLACE INTO sections
                    (year, title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    year,
                    int(s.get("title", 0)),
                    s.get("subtitle") or "",
                    s.get("chapter") or "",
                    s.get("subchapter") or "",
                    s.get("part") or "",
                    s.get("subpart") or "",
                    s.get("section") or "",
                    s.get("heading") or "",
                    s.get("text") or "",
                    s.get("word_count", 0),
                ))
            conn.commit()

    def update_word_counts(self, title_num: int, chapter_word_counts: dict, agency_lookup: dict) -> None:
        """Update agency word counts in the database for a given title."""
        if not chapter_word_counts:
            return

        with self._connection() as conn:
            cursor = conn.cursor()
            for chapter, word_count in chapter_word_counts.items():
                for agency_info in agency_lookup.get((title_num, chapter), []):
                    cursor.execute("""
                        INSERT OR REPLACE INTO agency_word_counts (agency_slug, title, chapter, word_count)
                        VALUES (?, ?, ?, ?)
                    """, (agency_info["agency_slug"], title_num, chapter, word_count))
            conn.commit()

    # Sections - Read

    def list_years(self) -> list[int]:
        """List available years from database (0 = current)."""
        return [row[0] for row in self._query("SELECT DISTINCT year FROM sections ORDER BY year")]

    def list_titles(self, year: int = 0) -> list[int]:
        """List available title numbers that have sections."""
        return [row[0] for row in self._query(
            "SELECT DISTINCT title FROM sections WHERE year = ? ORDER BY title", (year,)
        )]

    # Alias for backwards compatibility
    list_section_titles = list_titles

    def get_section(self, title: int, section: str, year: int = 0) -> dict | None:
        """Get full section data."""
        row = self._query_one(f'''
            SELECT {", ".join(self.SECTION_COLUMNS)}
            FROM sections WHERE year = ? AND title = ? AND section = ?
        ''', (year, title, section))
        return self._row_to_section(row) if row else None

    def get_section_heading(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the heading text for a section."""
        s = self.get_section(title, section, year)
        return s["heading"] if s else None

    def get_section_text(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the full text content of a section."""
        s = self.get_section(title, section, year)
        return s["text"] if s else None

    def get_adjacent_sections(self, title: int, section: str, year: int = 0) -> tuple[str | None, str | None]:
        """Get previous and next section identifiers for navigation."""
        # Get all sections for this title, sorted
        rows = self._query(
            "SELECT section FROM sections WHERE year = ? AND title = ? ORDER BY section",
            (year, title)
        )
        if not rows:
            return None, None

        sections = [r[0] for r in rows]

        # Sort sections numerically (1.1, 1.2, 1.10, 2.1, etc.)
        def section_sort_key(s):
            result = []
            for p in s.split("."):
                try:
                    result.append((0, int(p), ""))
                except ValueError:
                    result.append((1, 0, p))
            return result

        sections.sort(key=section_sort_key)

        try:
            idx = sections.index(section)
            prev_section = sections[idx - 1] if idx > 0 else None
            next_section = sections[idx + 1] if idx < len(sections) - 1 else None
            return prev_section, next_section
        except ValueError:
            return None, None

    def get_sections(self, title: int, chapter: str = None, part: str = None, year: int = 0) -> list[dict]:
        """Get all sections for a title."""
        query = f"SELECT {', '.join(self.SECTION_COLUMNS)} FROM sections WHERE year = ? AND title = ?"
        params = [year, title]

        if chapter:
            query += " AND chapter = ?"
            params.append(chapter)
        if part:
            query += " AND part = ?"
            params.append(part)

        query += " ORDER BY part, section"
        return [self._row_to_section(row) for row in self._query(query, tuple(params))]

    def navigate(
        self,
        title: int,
        subtitle: str = None,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        section: str = None,
        year: int = 0,
    ) -> dict | None:
        """Navigate to a specific location in the CFR hierarchy."""
        query = f"SELECT {', '.join(self.SECTION_COLUMNS)} FROM sections WHERE year = ? AND title = ?"
        params = [year, title]

        filters = [
            ("section", section), ("subtitle", subtitle), ("chapter", chapter),
            ("subchapter", subchapter), ("part", part), ("subpart", subpart)
        ]
        for col, val in filters:
            if val:
                query += f" AND {col} = ?"
                params.append(val)

        query += " LIMIT 1"
        row = self._query_one(query, tuple(params))
        return self._row_to_section(row) if row else None

    def search(self, query: str, title: int = None, year: int = 0) -> list[dict]:
        """Full-text search across sections."""
        if title:
            sql = "SELECT title, section, heading, text FROM sections WHERE year = ? AND title = ? AND text LIKE ?"
            params = (year, title, f"%{query}%")
        else:
            sql = "SELECT title, section, heading, text FROM sections WHERE year = ? AND text LIKE ?"
            params = (year, f"%{query}%")

        results = []
        query_lower = query.lower()

        for t, section, heading, text in self._query(sql, params):
            idx = text.lower().find(query_lower)
            start, end = max(0, idx - 50), min(len(text), idx + len(query) + 50)
            snippet = ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")
            results.append({"title": t, "section": section, "heading": heading, "snippet": snippet})

        return results

    def _convert_api_structure(self, api_node: dict, title: int, year: int = 0) -> dict:
        """Convert eCFR API structure format to our display format."""
        import re

        # Get word counts from database
        wc_data = self.get_structure_word_counts(title, year)

        def roman_to_int(s):
            """Convert Roman numeral to integer."""
            roman_values = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
            s = s.upper()
            if not all(c in roman_values for c in s):
                return None
            total = 0
            prev = 0
            for c in reversed(s):
                val = roman_values[c]
                if val < prev:
                    total -= val
                else:
                    total += val
                prev = val
            return total

        def sort_key(node):
            """Sort key for structure nodes."""
            identifier = node.get("identifier", "")
            if not identifier:
                return (2, 0, "")
            try:
                return (0, int(identifier), "")
            except ValueError:
                pass
            roman_val = roman_to_int(identifier)
            if roman_val is not None:
                return (0, roman_val, "")
            match = re.match(r'^(\d+)', identifier)
            if match:
                return (0, int(match.group(1)), identifier)
            return (1, 0, identifier)

        def count_sections(node):
            """Count sections in a node recursively."""
            if node.get("type") == "section":
                return 1
            return sum(count_sections(child) for child in node.get("children", []))

        def get_wc(path):
            """Look up word count from pre-computed data based on path."""
            try:
                sub_key = path.get("subtitle", "")
                ch_key = path.get("chapter")
                subch_key = path.get("subchapter")
                part_key = path.get("part")
                subpart_key = path.get("subpart")

                data = wc_data["subtitles"].get(sub_key, {})
                if ch_key is None:
                    return data.get("total", 0)
                data = data.get("chapters", {}).get(ch_key, {})
                if subch_key is None:
                    return data.get("total", 0)
                data = data.get("subchapters", {}).get(subch_key, {})
                if part_key is None:
                    return data.get("total", 0)
                data = data.get("parts", {}).get(part_key, {})
                if subpart_key is None:
                    return data.get("total", 0)
                return data.get("subparts", {}).get(subpart_key, {}).get("total", 0)
            except (KeyError, TypeError):
                return 0

        def convert_node(node, path=None):
            """Recursively convert a node from API format to display format."""
            if path is None:
                path = {}

            node_type = node.get("type", "")
            identifier = node.get("identifier", "")
            reserved = node.get("reserved", False)
            children = node.get("children", [])
            label_desc = node.get("label_description", "")

            # Update path for word count lookup
            new_path = dict(path)
            if node_type in ["subtitle", "chapter", "subchapter", "part", "subpart"]:
                new_path[node_type] = identifier or ""

            # Convert children recursively
            converted_children = []
            for child in sorted(children, key=sort_key):
                converted_children.append(convert_node(child, new_path))

            result = {
                "type": node_type,
                "identifier": identifier,
                "children": converted_children,
                "section_count": count_sections(node),
                "reserved": reserved,
            }

            # Look up word count from database based on path
            if node_type in ["subtitle", "chapter", "subchapter", "part", "subpart"]:
                result["word_count"] = get_wc(new_path)
            else:
                result["word_count"] = 0

            # Add heading for sections
            if node_type == "section" and label_desc:
                result["heading"] = label_desc

            return result

        converted = convert_node(api_node)
        # Set total word count from database
        converted["word_count"] = wc_data.get("total", 0)
        return converted

    def get_structure(self, title: int, year: int = 0) -> dict:
        """Return full hierarchy tree for a title: Subtitle → Chapter → Subchapter → Part → Subpart → Section."""
        # Check if we have structure metadata (includes reserved elements)
        metadata = self.get_title_structure_metadata(title, year)
        if metadata:
            return self._convert_api_structure(metadata, title, year)

        rows = self._query(
            "SELECT subtitle, chapter, subchapter, part, subpart, section, heading FROM sections WHERE year = ? AND title = ?",
            (year, title)
        )
        if not rows:
            return {}

        import re

        def roman_to_int(s):
            """Convert Roman numeral to integer."""
            roman_values = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
            s = s.upper()
            if not all(c in roman_values for c in s):
                return None
            total = 0
            prev = 0
            for c in reversed(s):
                val = roman_values[c]
                if val < prev:
                    total -= val
                else:
                    total += val
                prev = val
            return total

        def sort_key(identifier):
            """Sort key that handles numeric, Roman numeral, and alpha identifiers."""
            if not identifier:
                return (2, 0, "")
            # Try as integer first
            try:
                return (0, int(identifier), "")
            except ValueError:
                pass
            # Try as Roman numeral
            roman_val = roman_to_int(identifier)
            if roman_val is not None:
                return (0, roman_val, "")
            # Handle cases like "15a", "16A" - extract leading number
            match = re.match(r'^(\d+)', identifier)
            if match:
                return (0, int(match.group(1)), identifier)
            # Alphabetic sorting for letters (A, B, C...)
            return (1, 0, identifier)

        def section_sort_key(s):
            """Sort sections like 1.1, 1.2, 1.10, 2.1."""
            result = []
            for p in s["identifier"].split("."):
                try:
                    result.append((0, int(p), ""))
                except ValueError:
                    result.append((1, 0, p))
            return result

        # Build nested structure: subtitles → chapters → subchapters → parts → subparts → sections
        subtitles = {}

        for subtitle, chapter, subchapter, part, subpart, section, heading in rows:
            sub_key = subtitle or ""
            ch_key = chapter or ""
            subch_key = subchapter or ""
            part_key = part or ""
            subpart_key = subpart or ""

            # Initialize subtitle
            if sub_key not in subtitles:
                subtitles[sub_key] = {
                    "type": "subtitle",
                    "identifier": subtitle or "",
                    "chapters": {}
                }

            # Initialize chapter within subtitle
            chapters = subtitles[sub_key]["chapters"]
            if ch_key not in chapters:
                chapters[ch_key] = {
                    "type": "chapter",
                    "identifier": chapter or "",
                    "subchapters": {}
                }

            # Initialize subchapter within chapter
            subchapters = chapters[ch_key]["subchapters"]
            if subch_key not in subchapters:
                subchapters[subch_key] = {
                    "type": "subchapter",
                    "identifier": subchapter or "",
                    "parts": {}
                }

            # Initialize part within subchapter
            parts = subchapters[subch_key]["parts"]
            if part_key not in parts:
                parts[part_key] = {
                    "type": "part",
                    "identifier": part or "",
                    "subparts": {}
                }

            # Initialize subpart within part
            subparts = parts[part_key]["subparts"]
            if subpart_key not in subparts:
                subparts[subpart_key] = {
                    "type": "subpart",
                    "identifier": subpart or "",
                    "sections": []
                }

            # Add section
            if section:
                subparts[subpart_key]["sections"].append({
                    "type": "section",
                    "identifier": section,
                    "heading": heading or "",
                })

        # Get word counts from database
        wc_data = self.get_structure_word_counts(title, year)

        def get_wc(sub_key, ch_key=None, subch_key=None, part_key=None, subpart_key=None):
            """Look up word count from pre-computed data."""
            try:
                data = wc_data["subtitles"].get(sub_key, {})
                if ch_key is None:
                    return data.get("total", 0)
                data = data.get("chapters", {}).get(ch_key, {})
                if subch_key is None:
                    return data.get("total", 0)
                data = data.get("subchapters", {}).get(subch_key, {})
                if part_key is None:
                    return data.get("total", 0)
                data = data.get("parts", {}).get(part_key, {})
                if subpart_key is None:
                    return data.get("total", 0)
                return data.get("subparts", {}).get(subpart_key, {}).get("total", 0)
            except (KeyError, TypeError):
                return 0

        def build_part_children(part, sub_key, ch_key, subch_key):
            """Build children list for a part (subparts and sections).

            Orphaned sections (no subpart) are interleaved with named subparts
            based on section numbers. Named subparts maintain alphabetical order.
            """
            part_section_count = 0
            part_word_count = 0

            # Collect named subparts and orphaned sections separately
            named_subparts = []
            orphaned_sections = []

            for subpart_key in sorted(part["subparts"].keys(), key=sort_key):
                subpart = part["subparts"][subpart_key]
                sections = sorted(subpart["sections"], key=section_sort_key)
                subpart_section_count = len(sections)
                part_section_count += subpart_section_count
                subpart_wc = get_wc(sub_key, ch_key, subch_key, part["identifier"] or "", subpart["identifier"] or "")
                part_word_count += subpart_wc

                if subpart["identifier"]:
                    named_subparts.append({
                        "type": "subpart",
                        "identifier": subpart["identifier"],
                        "children": sections,
                        "section_count": subpart_section_count,
                        "word_count": subpart_wc,
                        "_min_section_key": section_sort_key(sections[0]) if sections else None,
                    })
                else:
                    orphaned_sections.extend(sections)

            # Interleave orphaned sections with named subparts
            if not named_subparts:
                part_children = orphaned_sections
            elif not orphaned_sections:
                # Remove internal sort keys
                for sp in named_subparts:
                    sp.pop("_min_section_key", None)
                part_children = named_subparts
            else:
                # Place orphaned sections before the first subpart whose
                # min section number is greater than the orphan's section number
                part_children = []
                orphan_idx = 0

                for sp in named_subparts:
                    sp_min = sp.pop("_min_section_key", None)
                    # Add orphans that come before this subpart
                    while orphan_idx < len(orphaned_sections):
                        orphan_key = section_sort_key(orphaned_sections[orphan_idx])
                        if sp_min is not None and orphan_key < sp_min:
                            part_children.append(orphaned_sections[orphan_idx])
                            orphan_idx += 1
                        else:
                            break
                    part_children.append(sp)

                # Add remaining orphans at the end
                part_children.extend(orphaned_sections[orphan_idx:])

            return part_children, part_section_count, part_word_count

        def build_subchapter_children(subch, sub_key, ch_key):
            """Build children list for a subchapter (parts)."""
            subch_children = []
            subch_section_count = 0
            subch_word_count = 0
            for part_key in sorted(subch["parts"].keys(), key=sort_key):
                part = subch["parts"][part_key]
                part_children, part_section_count, part_wc = build_part_children(part, sub_key, ch_key, subch["identifier"] or "")
                if part["identifier"]:
                    subch_children.append({
                        "type": "part",
                        "identifier": part["identifier"],
                        "children": part_children,
                        "section_count": part_section_count,
                        "word_count": part_wc,
                    })
                    subch_section_count += part_section_count
                    subch_word_count += part_wc
            return subch_children, subch_section_count, subch_word_count

        def build_chapter_children(ch, sub_key):
            """Build children list for a chapter (subchapters and parts)."""
            ch_children = []
            ch_section_count = 0
            ch_word_count = 0
            for subch_key in sorted(ch["subchapters"].keys(), key=sort_key):
                subch = ch["subchapters"][subch_key]
                subch_children, subch_section_count, subch_wc = build_subchapter_children(subch, sub_key, ch["identifier"] or "")

                if subch["identifier"]:
                    ch_children.append({
                        "type": "subchapter",
                        "identifier": subch["identifier"],
                        "children": subch_children,
                        "section_count": subch_section_count,
                        "word_count": subch_wc,
                    })
                    ch_section_count += subch_section_count
                    ch_word_count += subch_wc
                else:
                    ch_children.extend(subch_children)
                    ch_section_count += subch_section_count
                    ch_word_count += subch_wc
            return ch_children, ch_section_count, ch_word_count

        def build_subtitle_children(sub):
            """Build children list for a subtitle (chapters)."""
            sub_children = []
            sub_section_count = 0
            sub_word_count = 0
            for ch_key in sorted(sub["chapters"].keys(), key=sort_key):
                ch = sub["chapters"][ch_key]
                ch_children, ch_section_count, ch_wc = build_chapter_children(ch, sub["identifier"] or "")

                if ch["identifier"]:
                    sub_children.append({
                        "type": "chapter",
                        "identifier": ch["identifier"],
                        "children": ch_children,
                        "section_count": ch_section_count,
                        "word_count": ch_wc,
                    })
                    sub_section_count += ch_section_count
                    sub_word_count += ch_wc
                else:
                    sub_children.extend(ch_children)
                    sub_section_count += ch_section_count
                    sub_word_count += ch_wc
            return sub_children, sub_section_count, sub_word_count

        # Build the final structure
        result_children = []
        total_sections = 0
        total_words = 0

        for sub_key in sorted(subtitles.keys(), key=sort_key):
            sub = subtitles[sub_key]
            sub_children, sub_section_count, sub_wc = build_subtitle_children(sub)

            if sub["identifier"]:
                result_children.append({
                    "type": "subtitle",
                    "identifier": sub["identifier"],
                    "children": sub_children,
                    "section_count": sub_section_count,
                    "word_count": sub_wc,
                })
                total_sections += sub_section_count
                total_words += sub_wc
            else:
                result_children.extend(sub_children)
                total_sections += sub_section_count
                total_words += sub_wc

        return {
            "type": "title",
            "identifier": str(title),
            "children": result_children,
            "section_count": total_sections,
            "word_count": total_words,
        }

    def get_word_counts(
        self,
        title: int,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        year: int = 0,
    ) -> dict:
        """Get word counts for sections."""
        query = "SELECT section, word_count FROM sections WHERE year = ? AND title = ?"
        params = [year, title]

        filters = [("chapter", chapter), ("subchapter", subchapter), ("part", part), ("subpart", subpart)]
        for col, val in filters:
            if val:
                query += f" AND {col} = ?"
                params.append(val)

        rows = self._query(query, tuple(params))
        return {
            "sections": {row[0]: row[1] for row in rows if row[0]},
            "total": sum(row[1] for row in rows)
        }

    def get_total_words(self, title: int, year: int = 0) -> int:
        """Get total word count for a title."""
        return self.get_word_counts(title, year=year)["total"]

    def get_structure_word_counts(self, title: int, year: int = 0) -> dict:
        """Get word counts aggregated by hierarchy level for structure display.

        Returns nested dict: subtitle -> chapter -> subchapter -> part -> subpart -> word_count
        """
        rows = self._query("""
            SELECT subtitle, chapter, subchapter, part, subpart, SUM(word_count) as total
            FROM sections
            WHERE year = ? AND title = ?
            GROUP BY subtitle, chapter, subchapter, part, subpart
        """, (year, title))

        # Build nested aggregations
        result = {"total": 0, "subtitles": {}}

        for subtitle, chapter, subchapter, part, subpart, word_count in rows:
            result["total"] += word_count

            sub_key = subtitle or ""
            ch_key = chapter or ""
            subch_key = subchapter or ""
            part_key = part or ""
            subpart_key = subpart or ""

            if sub_key not in result["subtitles"]:
                result["subtitles"][sub_key] = {"total": 0, "chapters": {}}
            result["subtitles"][sub_key]["total"] += word_count

            chapters = result["subtitles"][sub_key]["chapters"]
            if ch_key not in chapters:
                chapters[ch_key] = {"total": 0, "subchapters": {}}
            chapters[ch_key]["total"] += word_count

            subchapters = chapters[ch_key]["subchapters"]
            if subch_key not in subchapters:
                subchapters[subch_key] = {"total": 0, "parts": {}}
            subchapters[subch_key]["total"] += word_count

            parts = subchapters[subch_key]["parts"]
            if part_key not in parts:
                parts[part_key] = {"total": 0, "subparts": {}}
            parts[part_key]["total"] += word_count

            subparts = parts[part_key]["subparts"]
            if subpart_key not in subparts:
                subparts[subpart_key] = {"total": 0}
            subparts[subpart_key]["total"] += word_count

        return result

    # Similarity Search (TF-IDF based, computed on-demand per chapter)

    def get_similar_sections(
        self,
        title: int,
        section: str,
        year: int = 0,
        limit: int = 10,
        min_similarity: float = 0.1,
    ) -> tuple[list[dict], float | None]:
        """Find sections similar to a given section within the same chapter.

        Uses TF-IDF for fast similarity computation. Searches all sections in
        the same chapter (which may span multiple parts). TF-IDF matrices are
        cached per (title, chapter) for faster repeated lookups.

        For historical years, uses current year (year=0) data. Returns empty
        if the section doesn't exist in the current year.

        Returns:
            Tuple of (similar_sections_list, max_similarity).
            max_similarity is the highest similarity score, or None if no similar sections.
        """
        from sklearn.metrics.pairwise import cosine_similarity

        # Always use current year (year=0) for similarity
        query_year = 0

        # Get the query section's chapter from current year
        query_row = self._query_one("""
            SELECT chapter FROM sections
            WHERE year = ? AND title = ? AND section = ?
        """, (query_year, title, section))

        if not query_row:
            return [], None

        chapter = query_row[0]
        cache_key = (title, chapter)

        # Check cache or compute TF-IDF matrix
        if cache_key not in self._tfidf_cache:
            from sklearn.feature_extraction.text import TfidfVectorizer

            rows = self._query("""
                SELECT section, heading, text
                FROM sections
                WHERE year = ? AND title = ? AND chapter = ? AND text != ''
            """, (query_year, title, chapter))

            if len(rows) < 2:
                return [], None

            sections = []
            headings = {}
            texts = []

            for sec, heading, text in rows:
                sections.append(sec)
                headings[sec] = heading
                texts.append(text)

            vectorizer = TfidfVectorizer(stop_words='english', max_features=10000)
            tfidf_matrix = vectorizer.fit_transform(texts)

            self._tfidf_cache[cache_key] = {
                "matrix": tfidf_matrix,
                "sections": sections,
                "headings": headings,
            }

        cached = self._tfidf_cache[cache_key]
        sections = cached["sections"]
        headings = cached["headings"]
        tfidf_matrix = cached["matrix"]

        # Find query section index
        try:
            query_idx = sections.index(section)
        except ValueError:
            return [], None

        # Compute similarities
        similarities = cosine_similarity(tfidf_matrix[query_idx:query_idx+1], tfidf_matrix)[0]

        # Build results
        results = []
        for i, (sec, sim) in enumerate(zip(sections, similarities)):
            if sec == section:
                continue
            if sim >= min_similarity:
                results.append({
                    "title": title,
                    "section": sec,
                    "similarity": float(sim),
                    "heading": headings[sec],
                })

        results.sort(key=lambda x: x["similarity"], reverse=True)
        max_similarity = results[0]["similarity"] if results else None
        return results[:limit], max_similarity
