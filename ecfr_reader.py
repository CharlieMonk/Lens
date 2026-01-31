#!/usr/bin/env python3
"""Interface for querying downloaded Markdown CFR data."""

import re
import sqlite3
from pathlib import Path


# Hierarchy levels in order (used to clear lower levels when higher level changes)
HIERARCHY_LEVELS = ['title', 'subtitle', 'chapter', 'subchapter', 'part', 'subpart', 'section']


class ECFRReader:
    """Interface for reading and navigating eCFR Markdown data."""

    def __init__(self, data_dir: str = "data_cache"):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / "ecfr.db"
        self._db: sqlite3.Connection | None = None
        self._cache: dict[int, list[dict]] = {}
        self._section_index: dict[int, dict[str, dict]] = {}

    @property
    def db(self) -> sqlite3.Connection | None:
        """Lazy-loaded database connection."""
        if self._db is None and self.db_path.exists():
            self._db = sqlite3.connect(self.db_path)
        return self._db

    def list_titles(self) -> list[int]:
        """List available title numbers."""
        titles = []
        for f in self.data_dir.glob("title_*.md"):
            match = re.match(r"title_(\d+)\.md", f.name)
            if match:
                titles.append(int(match.group(1)))
        return sorted(titles)

    def load_title(self, title: int) -> list[dict]:
        """Load and cache a title's Markdown data as parsed sections.

        Returns a list of section dicts with keys:
        - heading: the section heading text
        - section: the section number (e.g., "1.1")
        - text: the full section text
        - path: hierarchy path as list of (level, identifier) tuples
        """
        if title in self._cache:
            return self._cache[title]

        path = self.data_dir / f"title_{title}.md"
        if not path.exists():
            raise FileNotFoundError(f"Title {title} not found at {path}")

        sections = self._parse_markdown(path)
        self._cache[title] = sections
        return sections

    def _parse_markdown(self, path: Path) -> list[dict]:
        """Parse Markdown file into section list with hierarchy info."""
        sections = []
        context = {}
        current_section = None
        current_text = []

        # Patterns for hierarchy elements
        heading_pattern = re.compile(r'^(#{1,6})\s+(.+)$')
        section_pattern = re.compile(r'§\s*(\d+\.\d+[a-z]?(?:-\d+)?)')
        hierarchy_patterns = {
            'title': re.compile(r'Title\s+(\d+)', re.IGNORECASE),
            'subtitle': re.compile(r'Subtitle\s+([A-Z])', re.IGNORECASE),
            'chapter': re.compile(r'Chapter\s+([IVXLCDM]+)', re.IGNORECASE),
            'subchapter': re.compile(r'Subchapter\s+([A-Z])', re.IGNORECASE),
            'part': re.compile(r'Part\s+(\d+)', re.IGNORECASE),
            'subpart': re.compile(r'Subpart\s+([A-Z])', re.IGNORECASE),
        }

        def save_section():
            nonlocal current_section, current_text
            if current_section:
                current_section['text'] = '\n'.join(current_text).strip()
                sections.append(current_section)
            current_section = None
            current_text = []

        def clear_lower_levels(level: str):
            """Clear hierarchy levels below the given level."""
            idx = HIERARCHY_LEVELS.index(level)
            for k in HIERARCHY_LEVELS[idx + 1:]:
                context.pop(k, None)

        with open(path, 'r') as f:
            for line in f:
                line = line.rstrip()

                heading_match = heading_pattern.match(line)
                if heading_match:
                    heading_text = heading_match.group(2)

                    # Check if this is a section heading
                    section_match = section_pattern.search(heading_text)
                    if section_match:
                        save_section()
                        section_num = section_match.group(1)
                        context['section'] = section_num
                        current_section = {
                            'heading': heading_text,
                            'section': section_num,
                            'path': list(context.items()),
                            'text': ''
                        }
                        continue

                    # Update hierarchy context
                    for level, pattern in hierarchy_patterns.items():
                        if m := pattern.search(heading_text):
                            save_section()
                            if level == 'title':
                                context.clear()
                            context[level] = m.group(1)
                            clear_lower_levels(level)
                            break
                    continue

                # Accumulate section text
                if current_section is not None:
                    current_text.append(line)

        save_section()
        return sections

    def _build_index(self, title: int) -> dict[str, dict]:
        """Build section lookup index for a title."""
        if title in self._section_index:
            return self._section_index[title]

        sections = self.load_title(title)
        index = {s['section']: s for s in sections}
        self._section_index[title] = index
        return index

    def navigate(
        self,
        title: int,
        subtitle: str = None,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        section: str = None,
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

        Returns:
            The matching section dict or None if not found.
        """
        if self.db:
            return self._navigate_db(title, subtitle, chapter, subchapter, part, subpart, section)
        return self._navigate_markdown(title, subtitle, chapter, subchapter, part, subpart, section)

    def _navigate_db(
        self,
        title: int,
        subtitle: str = None,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        section: str = None,
    ) -> dict | None:
        """Navigate using database queries."""
        cursor = self.db.cursor()

        # Build query with filters
        query = """SELECT title, subtitle, chapter, subchapter, part, subpart,
                          section, heading, text, word_count
                   FROM sections WHERE title = ?"""
        params = [title]

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

        return {
            "title": row[0],
            "subtitle": row[1],
            "chapter": row[2],
            "subchapter": row[3],
            "part": row[4],
            "subpart": row[5],
            "section": row[6],
            "heading": row[7],
            "text": row[8],
            "word_count": row[9],
        }

    def _navigate_markdown(
        self,
        title: int,
        subtitle: str = None,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        section: str = None,
    ) -> dict | None:
        """Fallback navigation using markdown parsing."""
        # Fast path for section lookups
        if section:
            index = self._build_index(title)
            return index.get(section)

        # Filter by hierarchy
        sections = self.load_title(title)
        criteria = {}
        if subtitle:
            criteria['subtitle'] = subtitle
        if chapter:
            criteria['chapter'] = chapter
        if subchapter:
            criteria['subchapter'] = subchapter
        if part:
            criteria['part'] = part
        if subpart:
            criteria['subpart'] = subpart

        if not criteria:
            return sections[0] if sections else None

        for s in sections:
            path_dict = dict(s['path'])
            if all(path_dict.get(k) == v for k, v in criteria.items()):
                return s

        return None

    def search(self, query: str, title: int = None) -> list[dict]:
        """Full-text search across sections using database.

        Args:
            query: Search string (case-insensitive)
            title: Optional title number to limit search

        Returns:
            List of matching results with title, section, heading, and snippet.
        """
        if self.db:
            return self._search_db(query, title)
        return self._search_markdown(query, title)

    def _search_db(self, query: str, title: int = None) -> list[dict]:
        """Search sections using database LIKE query."""
        results = []
        cursor = self.db.cursor()

        if title:
            cursor.execute(
                "SELECT title, section, heading, text FROM sections WHERE title = ? AND text LIKE ?",
                (title, f"%{query}%")
            )
        else:
            cursor.execute(
                "SELECT title, section, heading, text FROM sections WHERE text LIKE ?",
                (f"%{query}%",)
            )

        query_lower = query.lower()
        for row in cursor.fetchall():
            t, section, heading, text = row
            # Extract snippet around match
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

    def _search_markdown(self, query: str, title: int = None) -> list[dict]:
        """Fallback search using markdown files."""
        results = []
        query_lower = query.lower()

        titles_to_search = [title] if title else self.list_titles()

        for t in titles_to_search:
            try:
                sections = self.load_title(t)
            except FileNotFoundError:
                continue

            for s in sections:
                text = s['text']
                if query_lower in text.lower():
                    # Extract snippet around match
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
                        "section": s['section'],
                        "heading": s.get('heading', ''),
                        "snippet": snippet,
                    })

        return results

    def get_structure(self, title: int) -> dict:
        """Return hierarchy tree for a title from the database."""
        if not self.db:
            return {}

        cursor = self.db.cursor()
        cursor.execute('''
            SELECT DISTINCT part, section FROM sections
            WHERE title = ? ORDER BY part, section
        ''', (title,))
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
    ) -> dict:
        """Get word counts for sections from the database.

        Args:
            title: CFR title number
            chapter: Optional chapter filter
            subchapter: Optional subchapter filter
            part: Optional part filter
            subpart: Optional subpart filter

        Returns:
            Dict with 'sections' (section -> count) and 'total'.
        """
        if not self.db:
            return {"sections": {}, "total": 0}

        # Build query with filters
        query = "SELECT section, word_count FROM sections WHERE title = ?"
        params = [title]

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

    def get_total_words(self, title: int) -> int:
        """Get total word count for a title."""
        return self.get_word_counts(title)["total"]

    def get_section_heading(self, title: int, section: str) -> str | None:
        """Get the heading text for a section from database."""
        if self.db:
            cursor = self.db.cursor()
            cursor.execute(
                "SELECT heading FROM sections WHERE title = ? AND section = ?",
                (title, section)
            )
            row = cursor.fetchone()
            if row:
                return row[0]
        # Fallback to markdown parsing
        s = self.navigate(title, section=section)
        return s['heading'] if s else None

    def get_section_text(self, title: int, section: str) -> str | None:
        """Get the full text content of a section from database."""
        if self.db:
            cursor = self.db.cursor()
            cursor.execute(
                "SELECT text FROM sections WHERE title = ? AND section = ?",
                (title, section)
            )
            row = cursor.fetchone()
            if row:
                return row[0]
        # Fallback to markdown parsing
        s = self.navigate(title, section=section)
        return s['text'] if s else None

    def get_section(self, title: int, section: str) -> dict | None:
        """Get full section data from database.

        Returns dict with keys: title, subtitle, chapter, subchapter,
        part, subpart, section, heading, text, word_count.
        """
        if not self.db:
            return None

        cursor = self.db.cursor()
        cursor.execute('''
            SELECT title, subtitle, chapter, subchapter, part, subpart,
                   section, heading, text, word_count
            FROM sections WHERE title = ? AND section = ?
        ''', (title, section))
        row = cursor.fetchone()
        if not row:
            return None

        return {
            "title": row[0],
            "subtitle": row[1],
            "chapter": row[2],
            "subchapter": row[3],
            "part": row[4],
            "subpart": row[5],
            "section": row[6],
            "heading": row[7],
            "text": row[8],
            "word_count": row[9],
        }

    def get_sections(
        self,
        title: int,
        chapter: str = None,
        part: str = None,
    ) -> list[dict]:
        """Get all sections for a title from database.

        Args:
            title: CFR title number
            chapter: Optional chapter filter
            part: Optional part filter

        Returns:
            List of section dicts with all fields.
        """
        if not self.db:
            return self.load_title(title)

        cursor = self.db.cursor()
        query = """SELECT title, subtitle, chapter, subchapter, part, subpart,
                          section, heading, text, word_count
                   FROM sections WHERE title = ?"""
        params = [title]

        if chapter:
            query += " AND chapter = ?"
            params.append(chapter)
        if part:
            query += " AND part = ?"
            params.append(part)

        query += " ORDER BY part, section"
        cursor.execute(query, params)

        return [
            {
                "title": row[0],
                "subtitle": row[1],
                "chapter": row[2],
                "subchapter": row[3],
                "part": row[4],
                "subpart": row[5],
                "section": row[6],
                "heading": row[7],
                "text": row[8],
                "word_count": row[9],
            }
            for row in cursor.fetchall()
        ]
