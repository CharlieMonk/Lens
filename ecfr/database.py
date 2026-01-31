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

    # Query methods (read operations)

    def _row_to_section_dict(self, row: tuple) -> dict:
        """Convert a database row to a section dict."""
        return {
            "year": row[0],
            "title": row[1],
            "subtitle": row[2],
            "chapter": row[3],
            "subchapter": row[4],
            "part": row[5],
            "subpart": row[6],
            "section": row[7],
            "heading": row[8],
            "text": row[9],
            "word_count": row[10],
        }

    def list_years(self) -> list[int]:
        """List available years from database (0 = current)."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT year FROM sections ORDER BY year")
            return [row[0] for row in cursor.fetchall()]

    def list_section_titles(self, year: int = 0) -> list[int]:
        """List available title numbers that have sections."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT title FROM sections WHERE year = ? ORDER BY title",
                (year,)
            )
            return [row[0] for row in cursor.fetchall()]

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
        with self._connection() as conn:
            cursor = conn.cursor()

            query = """SELECT year, title, subtitle, chapter, subchapter, part, subpart,
                              section, heading, text, word_count
                       FROM sections WHERE year = ? AND title = ?"""
            params = [year, title]

            if section:
                query += " AND section = ?"
                params.append(section)
            if subtitle:
                query += " AND subtitle = ?"
                params.append(subtitle)
            if chapter:
                query += " AND chapter = ?"
                params.append(chapter)
            if subchapter:
                query += " AND subchapter = ?"
                params.append(subchapter)
            if part:
                query += " AND part = ?"
                params.append(part)
            if subpart:
                query += " AND subpart = ?"
                params.append(subpart)

            query += " LIMIT 1"
            cursor.execute(query, params)
            row = cursor.fetchone()

            return self._row_to_section_dict(row) if row else None

    def search(self, query: str, title: int = None, year: int = 0) -> list[dict]:
        """Full-text search across sections."""
        with self._connection() as conn:
            cursor = conn.cursor()

            if title:
                cursor.execute(
                    "SELECT title, section, heading, text FROM sections WHERE year = ? AND title = ? AND text LIKE ?",
                    (year, title, f"%{query}%")
                )
            else:
                cursor.execute(
                    "SELECT title, section, heading, text FROM sections WHERE year = ? AND text LIKE ?",
                    (year, f"%{query}%")
                )

            results = []
            query_lower = query.lower()

            for row in cursor.fetchall():
                t, section, heading, text = row
                idx = text.lower().find(query_lower)
                start = max(0, idx - 50)
                end = min(len(text), idx + len(query) + 50)
                snippet = text[start:end]
                if start > 0:
                    snippet = "..." + snippet
                if end < len(text):
                    snippet = snippet + "..."

                results.append({
                    "title": t,
                    "section": section,
                    "heading": heading,
                    "snippet": snippet,
                })

            return results

    def get_structure(self, title: int, year: int = 0) -> dict:
        """Return hierarchy tree for a title."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT part, section FROM sections
                WHERE year = ? AND title = ? ORDER BY part, section
            ''', (year, title))
            rows = cursor.fetchall()

            if not rows:
                return {}

            result = {"type": "title", "identifier": str(title), "children": []}
            parts = {}

            for part, section in rows:
                if part and part not in parts:
                    parts[part] = {"type": "part", "identifier": part, "children": []}
                    result["children"].append(parts[part])
                if part and section:
                    parts[part]["children"].append({"type": "section", "identifier": section})

            return result

    def get_section_word_counts(
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

        if chapter:
            query += " AND chapter = ?"
            params.append(chapter)
        if subchapter:
            query += " AND subchapter = ?"
            params.append(subchapter)
        if part:
            query += " AND part = ?"
            params.append(part)
        if subpart:
            query += " AND subpart = ?"
            params.append(subpart)

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

        section_counts = {row[0]: row[1] for row in rows if row[0]}
        total = sum(row[1] for row in rows)

        return {"sections": section_counts, "total": total}

    def get_total_section_words(self, title: int, year: int = 0) -> int:
        """Get total word count for a title."""
        return self.get_section_word_counts(title, year=year)["total"]

    def get_section_heading(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the heading text for a section."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT heading FROM sections WHERE year = ? AND title = ? AND section = ?",
                (year, title, section)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def get_section_text(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the full text content of a section."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT text FROM sections WHERE year = ? AND title = ? AND section = ?",
                (year, title, section)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def get_section(self, title: int, section: str, year: int = 0) -> dict | None:
        """Get full section data."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT year, title, subtitle, chapter, subchapter, part, subpart,
                       section, heading, text, word_count
                FROM sections WHERE year = ? AND title = ? AND section = ?
            ''', (year, title, section))
            row = cursor.fetchone()
            return self._row_to_section_dict(row) if row else None

    def get_sections(
        self,
        title: int,
        chapter: str = None,
        part: str = None,
        year: int = 0,
    ) -> list[dict]:
        """Get all sections for a title."""
        query = """SELECT year, title, subtitle, chapter, subchapter, part, subpart,
                          section, heading, text, word_count
                   FROM sections WHERE year = ? AND title = ?"""
        params = [year, title]

        if chapter:
            query += " AND chapter = ?"
            params.append(chapter)
        if part:
            query += " AND part = ?"
            params.append(part)

        query += " ORDER BY part, section"

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [self._row_to_section_dict(row) for row in cursor.fetchall()]

    def get_similar_sections(
        self,
        title: int,
        section: str,
        year: int = 0,
        limit: int = 10,
        min_similarity: float = 0.1,
    ) -> list[dict]:
        """Find sections similar to a given section based on TF-IDF cosine similarity."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ss.similar_title, ss.similar_section, ss.similarity, s.heading
                FROM section_similarities ss
                LEFT JOIN sections s
                    ON ss.year = s.year
                    AND ss.similar_title = s.title
                    AND ss.similar_section = s.section
                WHERE ss.year = ? AND ss.title = ? AND ss.section = ?
                    AND ss.similarity >= ?
                ORDER BY ss.similarity DESC
                LIMIT ?
            ''', (year, title, section, min_similarity, limit))

            return [
                {
                    "title": row[0],
                    "section": row[1],
                    "similarity": row[2],
                    "heading": row[3],
                }
                for row in cursor.fetchall()
            ]

    def get_most_similar_pairs(
        self,
        year: int = 0,
        limit: int = 20,
        min_similarity: float = 0.5,
        title: int = None,
    ) -> list[dict]:
        """Get the most similar section pairs across all titles."""
        with self._connection() as conn:
            cursor = conn.cursor()

            query = '''
                SELECT ss.title, ss.section, s1.heading,
                       ss.similar_title, ss.similar_section, s2.heading,
                       ss.similarity
                FROM section_similarities ss
                LEFT JOIN sections s1
                    ON ss.year = s1.year AND ss.title = s1.title AND ss.section = s1.section
                LEFT JOIN sections s2
                    ON ss.year = s2.year AND ss.similar_title = s2.title AND ss.similar_section = s2.section
                WHERE ss.year = ? AND ss.similarity >= ?
                    AND ss.section < ss.similar_section
            '''
            params = [year, min_similarity]

            if title:
                query += " AND ss.title = ?"
                params.append(title)

            query += " ORDER BY ss.similarity DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)

            return [
                {
                    "title1": row[0],
                    "section1": row[1],
                    "heading1": row[2],
                    "title2": row[3],
                    "section2": row[4],
                    "heading2": row[5],
                    "similarity": row[6],
                }
                for row in cursor.fetchall()
            ]

    def find_duplicate_regulations(
        self,
        year: int = 0,
        min_similarity: float = 0.95,
        limit: int = 100,
    ) -> list[dict]:
        """Find potential duplicate regulations (sections with very high similarity)."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ss.title, ss.section, s1.heading, s1.text,
                       ss.similar_title, ss.similar_section, s2.heading, s2.text,
                       ss.similarity
                FROM section_similarities ss
                LEFT JOIN sections s1
                    ON ss.year = s1.year AND ss.title = s1.title AND ss.section = s1.section
                LEFT JOIN sections s2
                    ON ss.year = s2.year AND ss.similar_title = s2.title AND ss.similar_section = s2.section
                WHERE ss.year = ? AND ss.similarity >= ?
                    AND ss.section < ss.similar_section
                ORDER BY ss.similarity DESC
                LIMIT ?
            ''', (year, min_similarity, limit))

            return [
                {
                    "title1": row[0],
                    "section1": row[1],
                    "heading1": row[2],
                    "text1": row[3],
                    "title2": row[4],
                    "section2": row[5],
                    "heading2": row[6],
                    "text2": row[7],
                    "similarity": row[8],
                }
                for row in cursor.fetchall()
            ]

    def similarity_stats(self, year: int = 0) -> dict:
        """Get statistics about section similarities in the database."""
        with self._connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM section_similarities WHERE year = ?",
                (year,)
            )
            total_pairs = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(DISTINCT title) FROM section_similarities WHERE year = ?",
                (year,)
            )
            titles_with_similarities = cursor.fetchone()[0]

            cursor.execute('''
                SELECT
                    COUNT(CASE WHEN similarity >= 0.9 THEN 1 END) as very_high,
                    COUNT(CASE WHEN similarity >= 0.7 AND similarity < 0.9 THEN 1 END) as high,
                    COUNT(CASE WHEN similarity >= 0.5 AND similarity < 0.7 THEN 1 END) as medium,
                    COUNT(CASE WHEN similarity >= 0.3 AND similarity < 0.5 THEN 1 END) as low,
                    COUNT(CASE WHEN similarity < 0.3 THEN 1 END) as very_low,
                    AVG(similarity) as avg_similarity,
                    MAX(similarity) as max_similarity
                FROM section_similarities
                WHERE year = ?
            ''', (year,))
            row = cursor.fetchone()

            return {
                "total_pairs": total_pairs,
                "titles_with_similarities": titles_with_similarities,
                "distribution": {
                    "very_high_0.9+": row[0],
                    "high_0.7-0.9": row[1],
                    "medium_0.5-0.7": row[2],
                    "low_0.3-0.5": row[3],
                    "very_low_<0.3": row[4],
                },
                "avg_similarity": row[5],
                "max_similarity": row[6],
            }
