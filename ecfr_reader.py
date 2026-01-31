#!/usr/bin/env python3
"""Interface for querying CFR data from the database."""

import sqlite3
from pathlib import Path


class ECFRReader:
    """Interface for reading and navigating eCFR data from SQLite database."""

    def __init__(self, db_path: str = "data_cache/ecfr.db"):
        self.db_path = Path(db_path)
        self._db: sqlite3.Connection | None = None

    @property
    def db(self) -> sqlite3.Connection:
        """Lazy-loaded database connection."""
        if self._db is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"Database not found: {self.db_path}")
            self._db = sqlite3.connect(self.db_path)
        return self._db

    def list_years(self) -> list[int]:
        """List available years from database (0 = current)."""
        cursor = self.db.cursor()
        cursor.execute("SELECT DISTINCT year FROM sections ORDER BY year")
        return [row[0] for row in cursor.fetchall()]

    def list_titles(self, year: int = 0) -> list[int]:
        """List available title numbers from database."""
        cursor = self.db.cursor()
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
        """Navigate to a specific location in the CFR hierarchy.

        Args:
            title: CFR title number (1-50)
            subtitle: Subtitle letter (A, B, C...)
            chapter: Chapter number (Roman numerals: I, II, III...)
            subchapter: Subchapter letter (A, B, C...)
            part: Part number (Arabic numerals)
            subpart: Subpart letter (A, B, C...)
            section: Section number (e.g., "1.1", "21.15")
            year: Year for historical data (0 = current)

        Returns:
            The matching section dict or None if not found.
        """
        cursor = self.db.cursor()

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

        if not row:
            return None

        return self._row_to_dict(row)

    def search(self, query: str, title: int = None, year: int = 0) -> list[dict]:
        """Full-text search across sections.

        Args:
            query: Search string (case-insensitive)
            title: Optional title number to limit search
            year: Year for historical data (0 = current)

        Returns:
            List of matching results with title, section, heading, and snippet.
        """
        cursor = self.db.cursor()

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
        """Return hierarchy tree for a title.

        Args:
            title: CFR title number
            year: Year for historical data (0 = current)

        Returns:
            Dict with type, identifier, and children representing the title structure.
        """
        cursor = self.db.cursor()
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

    def get_word_counts(
        self,
        title: int,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        year: int = 0,
    ) -> dict:
        """Get word counts for sections.

        Args:
            title: CFR title number
            chapter: Optional chapter filter
            subchapter: Optional subchapter filter
            part: Optional part filter
            subpart: Optional subpart filter
            year: Year for historical data (0 = current)

        Returns:
            Dict with 'sections' (section -> count) and 'total'.
        """
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

        cursor = self.db.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()

        section_counts = {row[0]: row[1] for row in rows if row[0]}
        total = sum(row[1] for row in rows)

        return {"sections": section_counts, "total": total}

    def get_total_words(self, title: int, year: int = 0) -> int:
        """Get total word count for a title."""
        return self.get_word_counts(title, year=year)["total"]

    def get_section_heading(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the heading text for a section."""
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT heading FROM sections WHERE year = ? AND title = ? AND section = ?",
            (year, title, section)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def get_section_text(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the full text content of a section."""
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT text FROM sections WHERE year = ? AND title = ? AND section = ?",
            (year, title, section)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def get_section(self, title: int, section: str, year: int = 0) -> dict | None:
        """Get full section data.

        Args:
            title: CFR title number
            section: Section number
            year: Year for historical data (0 = current)

        Returns dict with keys: year, title, subtitle, chapter, subchapter,
        part, subpart, section, heading, text, word_count.
        """
        cursor = self.db.cursor()
        cursor.execute('''
            SELECT year, title, subtitle, chapter, subchapter, part, subpart,
                   section, heading, text, word_count
            FROM sections WHERE year = ? AND title = ? AND section = ?
        ''', (year, title, section))
        row = cursor.fetchone()
        return self._row_to_dict(row) if row else None

    def get_sections(
        self,
        title: int,
        chapter: str = None,
        part: str = None,
        year: int = 0,
    ) -> list[dict]:
        """Get all sections for a title.

        Args:
            title: CFR title number
            chapter: Optional chapter filter
            part: Optional part filter
            year: Year for historical data (0 = current)

        Returns:
            List of section dicts with all fields.
        """
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

        cursor = self.db.cursor()
        cursor.execute(query, params)

        return [self._row_to_dict(row) for row in cursor.fetchall()]

    def _row_to_dict(self, row: tuple) -> dict:
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
