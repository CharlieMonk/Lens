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
        """Full-text search across sections.

        Args:
            query: Search string (case-insensitive)
            title: Optional title number to limit search

        Returns:
            List of matching results with title, section, path, and snippet.
        """
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
                        "path": s['path'],
                        "snippet": snippet,
                    })

        return results

    def get_structure(self, title: int) -> dict:
        """Return hierarchy tree for a title."""
        sections = self.load_title(title)
        if not sections:
            return {}

        # Build nested structure from flat sections
        result = {"type": "title", "identifier": str(title), "children": []}

        # Group sections by hierarchy
        for s in sections:
            path_dict = dict(s['path'])
            # Just collect parts and their sections for now
            part_id = path_dict.get('part', '')
            if part_id:
                # Find or create part entry
                part_entry = None
                for child in result.get('children', []):
                    if child.get('type') == 'part' and child.get('identifier') == part_id:
                        part_entry = child
                        break
                if not part_entry:
                    part_entry = {"type": "part", "identifier": part_id, "children": []}
                    result['children'].append(part_entry)

                # Add section
                part_entry['children'].append({
                    "type": "section",
                    "identifier": s['section']
                })

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
        query = "SELECT section, word_count FROM word_counts WHERE title = ?"
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
        """Get the heading text for a section."""
        s = self.navigate(title, section=section)
        return s['heading'] if s else None

    def get_section_text(self, title: int, section: str) -> str | None:
        """Get the full text content of a section."""
        s = self.navigate(title, section=section)
        return s['text'] if s else None
