"""SQLite database operations for eCFR data."""

import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


class ECFRDatabase:
    """Handles all SQLite database operations for eCFR data."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
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
            CREATE TABLE IF NOT EXISTS section_similarities (
                year INTEGER NOT NULL,
                title INTEGER NOT NULL,
                section TEXT NOT NULL,
                similar_title INTEGER NOT NULL,
                similar_section TEXT NOT NULL,
                similarity REAL NOT NULL,
                PRIMARY KEY (year, title, section, similar_title, similar_section)
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
                # Migrate: add year column by recreating table
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
            ("idx_similarities_source", "section_similarities(year, title, section)"),
        ]
        for name, columns in indexes:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {columns}")

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

    # Titles methods

    def get_titles(self) -> dict[int, dict]:
        """Get all titles from the database."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
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
                for row in cursor.fetchall()
            }

    def has_titles(self) -> bool:
        """Check if titles table has data."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM titles")
            return cursor.fetchone()[0] > 0

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

    # Agencies methods

    def has_agencies(self) -> bool:
        """Check if agencies table has data."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM agencies")
            return cursor.fetchone()[0] > 0

    def has_year_data(self, year: int) -> bool:
        """Check if sections table has data for a specific year."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sections WHERE year = ?", (year,))
            return cursor.fetchone()[0] > 0

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
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
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

            for row in cursor.fetchall():
                title, chapter, agency_slug, agency_name, parent_slug, parent_name = row
                if title and chapter:
                    lookup[(title, chapter)].append({
                        "agency_slug": agency_slug,
                        "agency_name": agency_name,
                        "parent_slug": parent_slug,
                        "parent_name": parent_name,
                    })

        return dict(lookup)

    # Sections methods

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

    def get_agency_word_counts(self) -> dict[str, int]:
        """Get total word counts for all agencies, including parent aggregates."""
        with self._connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT agency_slug, SUM(word_count) as total
                FROM agency_word_counts
                GROUP BY agency_slug
            """)
            direct_counts = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("""
                SELECT slug, parent_slug FROM agencies WHERE parent_slug IS NOT NULL
            """)
            child_to_parent = {row[0]: row[1] for row in cursor.fetchall()}

        totals = dict(direct_counts)
        for child_slug, parent_slug in child_to_parent.items():
            if child_slug in direct_counts:
                if parent_slug not in totals:
                    totals[parent_slug] = 0
                totals[parent_slug] += direct_counts[child_slug]

        return totals

    # Similarity methods

    def compute_similarities(self, title: int, year: int = 0, top_n: int = 5, max_sections: int = 2000) -> int:
        """Compute TF-IDF cosine similarities for sections within a title.

        Returns number of similarity pairs stored, or -1 if skipped.
        """
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import linear_kernel
        import numpy as np

        with self._connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT section, text FROM sections
                WHERE year = ? AND title = ? AND text != ''
            """, (year, title))
            rows = cursor.fetchall()

            if len(rows) < 2:
                return 0

            if len(rows) > max_sections:
                return -1  # Signal skipped

            sections = [row[0] for row in rows]
            texts = [row[1] for row in rows]

            vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                min_df=2,
                max_df=0.95
            )

            try:
                tfidf_matrix = vectorizer.fit_transform(texts)
            except ValueError:
                return 0

            cursor.execute("""
                DELETE FROM section_similarities
                WHERE year = ? AND title = ?
            """, (year, title))

            count = 0
            batch_size = 500
            n_sections = len(sections)

            for batch_start in range(0, n_sections, batch_size):
                batch_end = min(batch_start + batch_size, n_sections)
                batch_vectors = tfidf_matrix[batch_start:batch_end]
                similarities = linear_kernel(batch_vectors, tfidf_matrix)

                for i, row_idx in enumerate(range(batch_start, batch_end)):
                    section = sections[row_idx]
                    sim_scores = similarities[i].copy()
                    sim_scores[row_idx] = -1  # Exclude self

                    top_indices = np.argsort(sim_scores)[-top_n:][::-1]

                    for j in top_indices:
                        if sim_scores[j] > 0.1:
                            cursor.execute("""
                                INSERT OR REPLACE INTO section_similarities
                                (year, title, section, similar_title, similar_section, similarity)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (year, title, section, title, sections[j], float(sim_scores[j])))
                            count += 1

                conn.commit()

        return count
