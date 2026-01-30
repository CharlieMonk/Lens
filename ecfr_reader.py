#!/usr/bin/env python3
"""Interface for querying downloaded YAML CFR data."""

import re
from pathlib import Path
from typing import Generator

import yaml


# Maps DIV element names to CFR hierarchy levels
DIV_TO_LEVEL = {
    "DIV1": "title",
    "DIV2": "subtitle",
    "DIV3": "chapter",
    "DIV4": "subchapter",
    "DIV5": "part",
    "DIV6": "subpart",
    "DIV8": "section",
}

# Maps TYPE attributes to CFR hierarchy levels
TYPE_TO_LEVEL = {
    "TITLE": "title",
    "SUBTITLE": "subtitle",
    "CHAPTER": "chapter",
    "SUBCHAP": "subchapter",
    "PART": "part",
    "SUBPART": "subpart",
    "SECTION": "section",
}


class ECFRReader:
    """Interface for reading and navigating eCFR YAML data."""

    def __init__(self, data_dir: str = "xml_output"):
        self.data_dir = Path(data_dir)
        self._cache: dict[int, dict] = {}
        self._section_index: dict[int, dict[str, dict]] = {}

    def list_titles(self) -> list[int]:
        """List available title numbers."""
        titles = []
        for f in self.data_dir.glob("title_*.yaml"):
            match = re.match(r"title_(\d+)\.yaml", f.name)
            if match:
                titles.append(int(match.group(1)))
        return sorted(titles)

    def load_title(self, title: int) -> dict:
        """Load and cache a title's YAML data."""
        if title in self._cache:
            return self._cache[title]

        path = self.data_dir / f"title_{title}.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Title {title} not found at {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        self._cache[title] = data
        return data

    def _get_div1(self, title: int) -> dict | None:
        """Get the DIV1 (title) node from loaded data."""
        data = self.load_title(title)
        ecfr = data.get("ECFR", {})
        children = ecfr.get("children", [])

        if isinstance(children, list):
            # New format: list of dicts with @tag key
            for child in children:
                if isinstance(child, dict) and child.get("@tag") == "DIV1":
                    return child
            return None
        elif isinstance(children, dict):
            # Legacy format: dict keyed by tag name
            return children.get("DIV1")

    def _extract_text(self, node: dict) -> str:
        """Extract all text content from a node recursively.

        Handles 'text' (content before/inside element), 'tail' (content after
        a child element), and preserves document order of children.
        """
        if not isinstance(node, dict):
            return ""

        texts = []
        if "text" in node:
            texts.append(node["text"])

        # Children is now a list (preserves document order)
        children = node.get("children", [])
        if isinstance(children, list):
            for child in children:
                texts.append(self._extract_text(child))
        elif isinstance(children, dict):
            # Legacy format: dict keyed by tag name
            for key, value in children.items():
                if isinstance(value, list):
                    for item in value:
                        texts.append(self._extract_text(item))
                elif isinstance(value, dict):
                    texts.append(self._extract_text(value))

        # Capture tail text (text after this element)
        if "tail" in node:
            texts.append(node["tail"])

        return " ".join(t for t in texts if t)

    def _walk_tree(
        self, node: dict, path: list[tuple[str, str]] | None = None
    ) -> Generator[tuple[list[tuple[str, str]], dict], None, None]:
        """Generator to traverse YAML structure with hierarchy path.

        Yields tuples of (path, node) where path is a list of (level, identifier) tuples.
        """
        if path is None:
            path = []

        if not isinstance(node, dict):
            return

        yield path, node

        children = node.get("children", [])
        if isinstance(children, list):
            # New format: list of dicts with @tag key
            for item in children:
                if not isinstance(item, dict):
                    continue
                tag = item.get("@tag", "")
                level = DIV_TO_LEVEL.get(tag)

                new_path = path.copy()
                if level:
                    attrs = item.get("@attributes", {})
                    identifier = attrs.get("N", "")
                    new_path.append((level, identifier))

                yield from self._walk_tree(item, new_path)
        elif isinstance(children, dict):
            # Legacy format: dict keyed by tag name
            for key, value in children.items():
                level = DIV_TO_LEVEL.get(key)
                items = value if isinstance(value, list) else [value]

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    new_path = path.copy()
                    if level:
                        attrs = item.get("@attributes", {})
                        identifier = attrs.get("N", "")
                        new_path.append((level, identifier))

                    yield from self._walk_tree(item, new_path)

    def _build_index(self, title: int) -> dict[str, dict]:
        """Build section lookup index for a title."""
        if title in self._section_index:
            return self._section_index[title]

        index = {}
        div1 = self._get_div1(title)
        if not div1:
            return index

        for path, node in self._walk_tree(div1):
            attrs = node.get("@attributes", {})
            if attrs.get("TYPE") == "SECTION":
                section_num = attrs.get("N", "")
                if section_num:
                    index[section_num] = {
                        "path": path,
                        "node": node,
                    }

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
            The matching node or None if not found.
        """
        # Fast path for section lookups
        if section and not any([subtitle, chapter, subchapter, part, subpart]):
            index = self._build_index(title)
            entry = index.get(section)
            return entry["node"] if entry else None

        div1 = self._get_div1(title)
        if not div1:
            return None

        # Build target criteria
        criteria = []
        if subtitle:
            criteria.append(("subtitle", subtitle))
        if chapter:
            criteria.append(("chapter", chapter))
        if subchapter:
            criteria.append(("subchapter", subchapter))
        if part:
            criteria.append(("part", part))
        if subpart:
            criteria.append(("subpart", subpart))
        if section:
            criteria.append(("section", section))

        if not criteria:
            return div1

        # Walk tree and find matching node
        for path, node in self._walk_tree(div1):
            path_dict = dict(path)
            matches = all(path_dict.get(level) == value for level, value in criteria)
            if matches:
                return node

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
                div1 = self._get_div1(t)
            except FileNotFoundError:
                continue

            if not div1:
                continue

            for path, node in self._walk_tree(div1):
                attrs = node.get("@attributes", {})
                if attrs.get("TYPE") != "SECTION":
                    continue

                text = self._extract_text(node)
                if query_lower in text.lower():
                    section_num = attrs.get("N", "")
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
                        "section": section_num,
                        "path": path,
                        "snippet": snippet,
                    })

        return results

    def get_structure(self, title: int) -> dict:
        """Return hierarchy tree for a title.

        Returns a nested dict representing the CFR structure.
        """
        div1 = self._get_div1(title)
        if not div1:
            return {}

        def build_structure(node: dict) -> dict:
            result = {}
            attrs = node.get("@attributes", {})
            node_type = attrs.get("TYPE", "")

            if node_type in TYPE_TO_LEVEL:
                result["type"] = TYPE_TO_LEVEL[node_type]
                result["identifier"] = attrs.get("N", "")

            children = node.get("children", [])
            child_list = []

            if isinstance(children, list):
                # New format: list of dicts with @tag key
                for item in children:
                    if not isinstance(item, dict):
                        continue
                    tag = item.get("@tag", "")
                    if tag.startswith("DIV") and tag in DIV_TO_LEVEL:
                        child_list.append(build_structure(item))
            elif isinstance(children, dict):
                # Legacy format: dict keyed by tag name
                for key, value in children.items():
                    if key.startswith("DIV") and key in DIV_TO_LEVEL:
                        items = value if isinstance(value, list) else [value]
                        for item in items:
                            if isinstance(item, dict):
                                child_list.append(build_structure(item))

            if child_list:
                result["children"] = child_list

            return result

        return build_structure(div1)

    def get_word_counts(
        self,
        title: int,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
    ) -> dict:
        """Calculate word counts by traversing text nodes.

        Args:
            title: CFR title number
            chapter: Optional chapter filter
            subchapter: Optional subchapter filter
            part: Optional part filter
            subpart: Optional subpart filter

        Returns:
            Dict with 'sections' (section -> count) and 'total'.
        """
        div1 = self._get_div1(title)
        if not div1:
            return {"sections": {}, "total": 0}

        # Build filter criteria
        filters = {}
        if chapter:
            filters["chapter"] = chapter
        if subchapter:
            filters["subchapter"] = subchapter
        if part:
            filters["part"] = part
        if subpart:
            filters["subpart"] = subpart

        section_counts = {}
        total = 0

        for path, node in self._walk_tree(div1):
            attrs = node.get("@attributes", {})
            if attrs.get("TYPE") != "SECTION":
                continue

            # Check filters
            path_dict = dict(path)
            if filters:
                if not all(path_dict.get(k) == v for k, v in filters.items()):
                    continue

            section_num = attrs.get("N", "")
            text = self._extract_text(node)
            word_count = len(text.split())

            section_counts[section_num] = word_count
            total += word_count

        return {"sections": section_counts, "total": total}

    def get_total_words(self, title: int) -> int:
        """Get total word count for a title."""
        return self.get_word_counts(title)["total"]

    def get_section_heading(self, title: int, section: str) -> str | None:
        """Get the heading text for a section."""
        node = self.navigate(title, section=section)
        if not node:
            return None

        children = node.get("children", [])
        if isinstance(children, list):
            # New format: list of dicts with @tag key
            for child in children:
                if isinstance(child, dict) and child.get("@tag") == "HEAD":
                    return child.get("text")
            return None
        elif isinstance(children, dict):
            # Legacy format
            head = children.get("HEAD", {})
            return head.get("text")

    def get_section_text(self, title: int, section: str) -> str | None:
        """Get the full text content of a section."""
        node = self.navigate(title, section=section)
        if not node:
            return None

        return self._extract_text(node)
