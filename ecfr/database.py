"""SQLite database operations for eCFR data."""

import hashlib
import sqlite3
import struct
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import numpy as np


class ECFRDatabase:
    """Handles all SQLite database operations for eCFR data."""

    SECTION_COLUMNS = (
        "year", "title", "subtitle", "chapter", "subchapter",
        "part", "subpart", "section", "heading", "text", "word_count"
    )

    def __init__(self, db_path: str | Path = "ecfr/ecfr_data/ecfr.db"):
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
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
            self._migrate_embeddings_table(cursor)
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
            CREATE TABLE IF NOT EXISTS section_embeddings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                title INTEGER NOT NULL,
                section TEXT NOT NULL,
                embedding BLOB NOT NULL,
                text_hash TEXT,
                UNIQUE(year, title, section)
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

    def _migrate_embeddings_table(self, cursor: sqlite3.Cursor) -> None:
        """Add text_hash column to section_embeddings if missing."""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='section_embeddings'")
        if cursor.fetchone() is None:
            return

        cursor.execute("PRAGMA table_info(section_embeddings)")
        columns = {row[1] for row in cursor.fetchall()}
        if "text_hash" not in columns:
            cursor.execute("ALTER TABLE section_embeddings ADD COLUMN text_hash TEXT")

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
            ("idx_embeddings_year_title", "section_embeddings(year, title)"),
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

        Also deletes associated embeddings.
        Returns number of sections deleted.
        """
        count = self._query_one(
            "SELECT COUNT(*) FROM sections WHERE year = ? AND title = ?",
            (year, title)
        )[0]

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM sections WHERE year = ? AND title = ?",
                (year, title)
            )
            cursor.execute(
                "DELETE FROM section_embeddings WHERE year = ? AND title = ?",
                (year, title)
            )
            conn.commit()

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
            """Build children list for a part (subparts and sections)."""
            part_children = []
            part_section_count = 0
            part_word_count = 0
            for subpart_key in sorted(part["subparts"].keys(), key=sort_key):
                subpart = part["subparts"][subpart_key]
                sections = sorted(subpart["sections"], key=section_sort_key)
                subpart_section_count = len(sections)
                part_section_count += subpart_section_count
                subpart_wc = get_wc(sub_key, ch_key, subch_key, part["identifier"] or "", subpart["identifier"] or "")
                part_word_count += subpart_wc

                if subpart["identifier"]:
                    part_children.append({
                        "type": "subpart",
                        "identifier": subpart["identifier"],
                        "children": sections,
                        "section_count": subpart_section_count,
                        "word_count": subpart_wc,
                    })
                else:
                    part_children.extend(sections)
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

    # Embeddings and Similarity

    def has_embeddings(self, title: int = None, year: int = 0) -> bool:
        """Check if embeddings exist for a given year and optionally title."""
        if title is not None:
            return self._query_one(
                "SELECT COUNT(*) FROM section_embeddings WHERE year = ? AND title = ?",
                (year, title)
            )[0] > 0
        return self._query_one(
            "SELECT COUNT(*) FROM section_embeddings WHERE year = ?",
            (year,)
        )[0] > 0

    def _get_embedding_model(self):
        """Lazily load the sentence transformer model."""
        if not hasattr(self, '_embedding_model'):
            from sentence_transformers import SentenceTransformer
            self._embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        return self._embedding_model

    def _embedding_to_blob(self, embedding: np.ndarray) -> bytes:
        """Convert numpy array to binary blob."""
        return struct.pack(f'{len(embedding)}f', *embedding.astype(np.float32))

    def _blob_to_embedding(self, blob: bytes) -> np.ndarray:
        """Convert binary blob back to numpy array."""
        n_floats = len(blob) // 4
        return np.array(struct.unpack(f'{n_floats}f', blob), dtype=np.float32)

    def _get_text_embedding_lookup(self) -> dict[str, np.ndarray]:
        """Build a lookup of truncated text -> embedding from all existing embeddings.

        Used to avoid recomputing embeddings for identical text across the database.
        """
        rows = self._query("""
            SELECT s.text, se.embedding
            FROM section_embeddings se
            JOIN sections s ON se.year = s.year AND se.title = s.title AND se.section = s.section
            WHERE s.text != ''
        """)

        lookup = {}
        for text, embedding_blob in rows:
            truncated = text[:10000]
            if truncated not in lookup:
                lookup[truncated] = self._blob_to_embedding(embedding_blob)
        return lookup

    @staticmethod
    def _compute_text_hash(text: str) -> str:
        """Compute a hash of the text content for change detection."""
        return hashlib.md5(text[:10000].encode('utf-8', errors='replace')).hexdigest()

    def _get_sections_needing_embeddings(self, title: int, year: int = 0) -> list[tuple[str, str]]:
        """Get sections that need embeddings computed.

        Includes sections that:
        - Don't have embeddings yet
        - Have embeddings but text has changed (text_hash mismatch)

        Returns list of (section, text) tuples.
        """
        # Get sections without embeddings
        missing = self._query("""
            SELECT s.section, s.text
            FROM sections s
            LEFT JOIN section_embeddings se
                ON s.year = se.year AND s.title = se.title AND s.section = se.section
            WHERE s.year = ? AND s.title = ? AND s.text != '' AND se.id IS NULL
        """, (year, title))

        # Get sections with stale embeddings (text changed since embedding was computed)
        stale = self._query("""
            SELECT s.section, s.text
            FROM sections s
            JOIN section_embeddings se
                ON s.year = se.year AND s.title = se.title AND s.section = se.section
            WHERE s.year = ? AND s.title = ? AND s.text != ''
                AND (se.text_hash IS NULL OR se.text_hash != ?)
        """, (year, title, ''))  # Placeholder - we'll filter in Python

        # Filter stale in Python since we need to compute hash
        stale_filtered = []
        for section, text in stale:
            current_hash = self._compute_text_hash(text)
            stored_hash = self._query_one("""
                SELECT text_hash FROM section_embeddings
                WHERE year = ? AND title = ? AND section = ?
            """, (year, title, section))
            if not stored_hash or stored_hash[0] != current_hash:
                stale_filtered.append((section, text))

        return list(missing) + stale_filtered

    def _get_sections_without_embeddings(self, title: int, year: int = 0) -> list[tuple[str, str]]:
        """Get sections that don't have embeddings yet (legacy method).

        Returns list of (section, text) tuples.
        """
        return self._get_sections_needing_embeddings(title, year)

    def compute_similarities(self, title: int, year: int = 0, top_n: int = 5) -> int:
        """Compute vector embeddings for sections missing them.

        Only computes embeddings for sections that don't already have one.
        Reuses existing embeddings for identical text values to avoid recomputation.

        Returns number of new embeddings stored.
        """
        # Get only sections that need embeddings
        rows = self._get_sections_without_embeddings(title, year)
        if not rows:
            return 0

        sections = [row[0] for row in rows]
        texts = [row[1][:10000] for row in rows]  # Truncate to avoid memory issues

        # Build lookup of existing embeddings by text content
        text_to_embedding = self._get_text_embedding_lookup()

        # Separate texts that need computation from those we can reuse
        texts_to_encode = []
        text_indices = []

        for i, text in enumerate(texts):
            if text not in text_to_embedding:
                texts_to_encode.append(text)
                text_indices.append(i)

        # Compute embeddings only for texts not in lookup
        embeddings = [None] * len(texts)

        if texts_to_encode:
            model = self._get_embedding_model()
            new_embeddings = model.encode(texts_to_encode, show_progress_bar=False, normalize_embeddings=True)

            # Place new embeddings in their positions and add to lookup
            for idx, embedding in zip(text_indices, new_embeddings):
                embeddings[idx] = embedding
                text_to_embedding[texts[idx]] = embedding

        # Fill in reused embeddings from lookup
        for i, text in enumerate(texts):
            if embeddings[i] is None:
                embeddings[i] = text_to_embedding[text]

        # Insert embeddings with text_hash for change tracking
        with self._connection() as conn:
            cursor = conn.cursor()
            for i, (section, embedding) in enumerate(zip(sections, embeddings)):
                text_hash = self._compute_text_hash(texts[i])
                cursor.execute(
                    "INSERT OR REPLACE INTO section_embeddings (year, title, section, embedding, text_hash) VALUES (?, ?, ?, ?, ?)",
                    (year, title, section, self._embedding_to_blob(embedding), text_hash)
                )
            conn.commit()

        return len(sections)

    def get_similar_sections(
        self,
        title: int,
        section: str,
        year: int = 0,
        limit: int = 10,
        min_similarity: float = 0.1,
    ) -> list[dict]:
        """Find sections similar to a given section using vector similarity search."""
        row = self._query_one(
            "SELECT embedding FROM section_embeddings WHERE year = ? AND title = ? AND section = ?",
            (year, title, section)
        )
        if not row:
            return []

        query_embedding = self._blob_to_embedding(row[0])

        rows = self._query("""
            SELECT se.title, se.section, se.embedding, s.heading
            FROM section_embeddings se
            LEFT JOIN sections s ON se.year = s.year AND se.title = s.title AND se.section = s.section
            WHERE se.year = ?
        """, (year,))

        results = []
        for sim_title, sim_section, embedding_blob, heading in rows:
            if sim_title == title and sim_section == section:
                continue

            similarity = float(np.dot(query_embedding, self._blob_to_embedding(embedding_blob)))
            if similarity >= min_similarity:
                results.append({
                    "title": sim_title,
                    "section": sim_section,
                    "similarity": similarity,
                    "heading": heading,
                })

        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:limit]
