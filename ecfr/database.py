"""SQLite database operations for eCFR data."""

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
                UNIQUE(year, title, section)
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

    def get_structure(self, title: int, year: int = 0) -> dict:
        """Return hierarchy tree for a title with section headings."""
        rows = self._query(
            "SELECT part, section, heading FROM sections WHERE year = ? AND title = ?",
            (year, title)
        )
        if not rows:
            return {}

        parts = {}
        for part, section, heading in rows:
            if not part:
                continue
            if part not in parts:
                parts[part] = {"type": "part", "identifier": part, "children": []}
            if section:
                parts[part]["children"].append({
                    "type": "section",
                    "identifier": section,
                    "heading": heading or "",
                })

        # Sort parts numerically (handle mixed numeric/string identifiers)
        def part_sort_key(p):
            try:
                return (0, int(p["identifier"]))
            except ValueError:
                return (1, p["identifier"])

        sorted_parts = sorted(parts.values(), key=part_sort_key)

        # Sort sections within each part (handle mixed numeric/alpha identifiers)
        def section_sort_key(s):
            result = []
            for p in s["identifier"].split("."):
                try:
                    result.append((0, int(p), ""))
                except ValueError:
                    result.append((1, 0, p))
            return result

        for part in sorted_parts:
            part["children"].sort(key=section_sort_key)
            part["section_count"] = len(part["children"])

        return {"type": "title", "identifier": str(title), "children": sorted_parts}

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

    # Embeddings and Similarity

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

    def compute_similarities(self, title: int, year: int = 0, top_n: int = 5) -> int:
        """Compute vector embeddings and store in database for similarity search.

        Returns number of embeddings stored.
        """
        rows = self._query(
            "SELECT section, text FROM sections WHERE year = ? AND title = ? AND text != ''",
            (year, title)
        )
        if len(rows) < 2:
            return 0

        sections = [row[0] for row in rows]
        texts = [row[1][:10000] for row in rows]  # Truncate to avoid memory issues

        model = self._get_embedding_model()
        embeddings = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM section_embeddings WHERE year = ? AND title = ?", (year, title))

            for section, embedding in zip(sections, embeddings):
                cursor.execute(
                    "INSERT OR REPLACE INTO section_embeddings (year, title, section, embedding) VALUES (?, ?, ?, ?)",
                    (year, title, section, self._embedding_to_blob(embedding))
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
