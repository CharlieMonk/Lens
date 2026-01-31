"""XML to Markdown conversion for eCFR data."""

import re
from collections import defaultdict
from pathlib import Path

from lxml import etree

from .constants import TYPE_TO_HEADING, TYPE_TO_LEVEL


def get_element_text(elem) -> str:
    """Recursively get all text content from an XML element."""
    texts = []
    if elem.text:
        texts.append(elem.text)
    for child in elem:
        texts.append(get_element_text(child))
        if child.tail:
            texts.append(child.tail)
    return ''.join(texts)


class SectionBuilder:
    """Builds section data during XML processing."""

    def __init__(self):
        self.sections = []
        self._current = None

    def start_section(self, context: dict, section_num: str) -> None:
        """Start a new section, finalizing any previous one."""
        self.finalize()
        self._current = {
            "title": context.get("title") or "",
            "subtitle": context.get("subtitle") or "",
            "chapter": context.get("chapter") or "",
            "subchapter": context.get("subchapter") or "",
            "part": context.get("part") or "",
            "subpart": context.get("subpart") or "",
            "section": section_num,
            "heading": "",
            "_text_parts": [],
        }

    def add_text(self, text: str) -> None:
        """Add text to the current section."""
        if self._current and text:
            self._current["_text_parts"].append(text)

    def set_heading(self, heading: str) -> None:
        """Set the heading for the current section."""
        if self._current:
            self._current["heading"] = heading

    def finalize(self) -> None:
        """Finalize the current section and add it to the list."""
        if self._current:
            s = self._current
            s["text"] = "\n".join(s["_text_parts"]).strip()
            s["word_count"] = len(s["text"].split())
            del s["_text_parts"]
            self.sections.append(s)
            self._current = None

    def get_sections(self) -> list[dict]:
        """Get all finalized sections."""
        self.finalize()
        return self.sections


class MarkdownConverter:
    """Converts eCFR XML to Markdown format."""

    def __init__(self, agency_lookup: dict = None):
        self.agency_lookup = agency_lookup or {}

    def convert(self, xml_content: bytes, output_path: Path, title_num: int = None) -> tuple[int, list, dict]:
        """Convert XML content to Markdown and write to file.

        Returns:
            Tuple of (file_size, sections, chapter_word_counts).
        """
        root = etree.fromstring(xml_content)
        chapter_word_counts = defaultdict(int)
        lines = []
        section_builder = SectionBuilder()
        cfr_title = str(title_num) if title_num else None

        def process_element(elem, context):
            tag = elem.tag
            elem_type = elem.attrib.get("TYPE", "")
            elem_n = elem.attrib.get("N", "")

            new_context = context.copy()
            if elem_type in TYPE_TO_LEVEL:
                if elem_type == "TITLE" and cfr_title:
                    new_context["title"] = cfr_title
                else:
                    new_context[TYPE_TO_LEVEL[elem_type]] = elem_n

            if elem_type == "SECTION":
                section_num = elem_n.lstrip("§ ").strip()
                section_builder.start_section(new_context, section_num)

            if tag == "HEAD":
                text = get_element_text(elem).strip()
                if text:
                    parent = elem.getparent()
                    parent_type = parent.attrib.get("TYPE", "") if parent is not None else ""
                    parent_n = parent.attrib.get("N", "") if parent is not None else ""
                    heading_level = TYPE_TO_HEADING.get(parent_type, 5)
                    lines.append(f"\n{'#' * heading_level} {text}\n")

                    if parent_type == "SECTION":
                        section_builder.set_heading(text)

                    self._add_agency_metadata(lines, new_context, parent_type, parent_n)
                return

            if tag == "P":
                text = get_element_text(elem).strip()
                if text:
                    chapter = new_context.get("chapter") or new_context.get("subtitle")
                    if chapter:
                        chapter_word_counts[chapter] += len(text.split())
                    section_builder.add_text(text)
                    lines.append(f"\n{text}\n")
                return

            if tag == "CITA":
                text = get_element_text(elem).strip()
                if text:
                    lines.append(f"\n*{text}*\n")
                return

            if tag == "AUTH":
                lines.append("\n**Authority:**\n")
                for child in elem:
                    process_element(child, new_context)
                return

            if tag == "SOURCE":
                lines.append("\n**Source:**\n")
                for child in elem:
                    process_element(child, new_context)
                return

            if tag in ("FP", "NOTE", "EXTRACT", "GPOTABLE"):
                text = get_element_text(elem).strip()
                if text:
                    chapter = new_context.get("chapter") or new_context.get("subtitle")
                    if chapter:
                        chapter_word_counts[chapter] += len(text.split())
                    section_builder.add_text(text)
                    lines.append(f"\n{text}\n")
                return

            for child in elem:
                process_element(child, new_context)

        process_element(root, {})
        sections = section_builder.get_sections()

        content = ''.join(lines)
        content = re.sub(r'\n{3,}', '\n\n', content)

        with open(output_path, "w") as f:
            f.write(content)

        return output_path.stat().st_size, sections, dict(chapter_word_counts)

    def _add_agency_metadata(self, lines: list, context: dict, parent_type: str, parent_n: str) -> None:
        """Add agency metadata comment to markdown output."""
        if not self.agency_lookup or parent_type not in ("CHAPTER", "SUBTITLE", "SUBCHAP"):
            return

        title_num_ctx = context.get("title")
        if not title_num_ctx:
            return

        try:
            key = (int(title_num_ctx), parent_n)
            agency_list = self.agency_lookup.get(key, [])
            if agency_list:
                lines.append("\n<!-- Agency Metadata\n")
                for agency_info in agency_list:
                    if agency_info["parent_slug"]:
                        lines.append(f"parent_agency: {agency_info['parent_slug']}\n")
                        lines.append(f"child_agency: {agency_info['agency_slug']}\n")
                    else:
                        lines.append(f"agency: {agency_info['agency_slug']}\n")
                lines.append("-->\n")
        except (ValueError, TypeError):
            pass

    def convert_govinfo(self, xml_content: bytes, output_path: Path, title_num: int = None) -> tuple[int, list, dict]:
        """Convert govinfo CFR XML to Markdown and extract sections.

        Returns:
            Tuple of (file_size, sections, chapter_word_counts).
        """
        root = etree.fromstring(xml_content)
        sections = []
        chapter_word_counts = defaultdict(int)
        lines = []
        context = {"title": str(title_num) if title_num else ""}

        for elem in root.iter():
            tag = elem.tag

            if tag == "CHAPTER":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    match = re.search(r'CHAPTER\s+([IVXLCDM]+)', hd.text)
                    if match:
                        context["chapter"] = match.group(1)
                        lines.append(f"\n## {hd.text.strip()}\n")

            elif tag == "SUBCHAP":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    context["subchapter"] = hd.text.strip()
                    lines.append(f"\n### {hd.text.strip()}\n")

            elif tag == "PART":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    match = re.search(r'PART\s+(\d+)', hd.text)
                    if match:
                        context["part"] = match.group(1)
                    lines.append(f"\n### {hd.text.strip()}\n")

            elif tag == "SUBPART":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    context["subpart"] = hd.text.strip()
                    lines.append(f"\n#### {hd.text.strip()}\n")

            elif tag == "SECTION":
                section = self._parse_govinfo_section(elem, context, lines, chapter_word_counts)
                if section:
                    sections.append(section)

        content = ''.join(lines)
        content = re.sub(r'\n{3,}', '\n\n', content)

        with open(output_path, "w") as f:
            f.write(content)

        return output_path.stat().st_size, sections, dict(chapter_word_counts)

    def _parse_govinfo_section(self, elem, context: dict, lines: list, chapter_word_counts: dict) -> dict | None:
        """Parse a single SECTION element from govinfo XML."""
        sectno_elem = elem.find("SECTNO")
        subject_elem = elem.find("SUBJECT")

        if sectno_elem is None or not sectno_elem.text:
            return None

        section_num = sectno_elem.text.lstrip("§ ").strip()
        heading = subject_elem.text.strip() if subject_elem is not None and subject_elem.text else ""

        text_parts = []
        for p in elem.findall(".//P"):
            p_text = get_element_text(p).strip()
            if p_text:
                text_parts.append(p_text)
                lines.append(f"\n{p_text}\n")

        full_text = "\n".join(text_parts)
        word_count = len(full_text.split())

        chapter = context.get("chapter", "")
        if chapter:
            chapter_word_counts[chapter] += word_count

        lines.append(f"\n#### § {section_num} {heading}\n")

        return {
            "title": context.get("title", ""),
            "subtitle": context.get("subtitle", ""),
            "chapter": chapter,
            "subchapter": context.get("subchapter", ""),
            "part": context.get("part", ""),
            "subpart": context.get("subpart", ""),
            "section": section_num,
            "heading": heading,
            "text": full_text,
            "word_count": word_count,
        }

    def convert_govinfo_volumes(self, xml_volumes: list[bytes], output_path: Path, title_num: int = None) -> tuple[int, list, dict]:
        """Convert multiple govinfo volume XMLs to a single Markdown file.

        Returns:
            Tuple of (file_size, sections, chapter_word_counts).
        """
        all_sections = []
        all_chapter_counts = defaultdict(int)

        for xml_content in xml_volumes:
            _, sections, chapter_counts = self.convert_govinfo(xml_content, Path("/dev/null"), title_num)
            all_sections.extend(sections)
            for k, v in chapter_counts.items():
                all_chapter_counts[k] += v

        with open(output_path, "w") as f:
            for s in all_sections:
                f.write(f"\n#### § {s['section']} {s['heading']}\n")
                f.write(f"\n{s['text']}\n")

        return output_path.stat().st_size, all_sections, dict(all_chapter_counts)

    def convert_chunks(self, xml_chunks: list[bytes], output_path: Path, title_num: int = None) -> tuple[int, list, dict]:
        """Convert multiple XML chunks to a single Markdown file.

        Returns:
            Tuple of (file_size, sections, chapter_word_counts).
        """
        all_sections = []
        all_chapter_counts = defaultdict(int)
        cfr_title = str(title_num) if title_num else None

        with open(output_path, "w") as f:
            for xml_content in xml_chunks:
                root = etree.fromstring(xml_content)
                lines = []
                chapter_word_counts = defaultdict(int)
                section_builder = SectionBuilder()

                def process_element(elem, context):
                    tag = elem.tag
                    elem_type = elem.attrib.get("TYPE", "")
                    elem_n = elem.attrib.get("N", "")

                    new_context = context.copy()
                    if elem_type in TYPE_TO_LEVEL:
                        if elem_type == "TITLE" and cfr_title:
                            new_context["title"] = cfr_title
                        else:
                            new_context[TYPE_TO_LEVEL[elem_type]] = elem_n

                    if elem_type == "SECTION":
                        section_num = elem_n.lstrip("§ ").strip()
                        section_builder.start_section(new_context, section_num)

                    if tag == "HEAD":
                        text = get_element_text(elem).strip()
                        if text:
                            parent = elem.getparent()
                            parent_type = parent.attrib.get("TYPE", "") if parent is not None else ""
                            heading_level = TYPE_TO_HEADING.get(parent_type, 5)
                            lines.append(f"\n{'#' * heading_level} {text}\n")
                            if parent_type == "SECTION":
                                section_builder.set_heading(text)
                        return

                    if tag == "P":
                        text = get_element_text(elem).strip()
                        if text:
                            chapter = new_context.get("chapter") or new_context.get("subtitle")
                            if chapter:
                                chapter_word_counts[chapter] += len(text.split())
                            section_builder.add_text(text)
                            lines.append(f"\n{text}\n")
                        return

                    if tag == "CITA":
                        text = get_element_text(elem).strip()
                        if text:
                            lines.append(f"\n*{text}*\n")
                        return

                    if tag in ("AUTH", "SOURCE"):
                        label = "Authority" if tag == "AUTH" else "Source"
                        lines.append(f"\n**{label}:**\n")
                        for child in elem:
                            process_element(child, new_context)
                        return

                    if tag in ("FP", "NOTE", "EXTRACT", "GPOTABLE"):
                        text = get_element_text(elem).strip()
                        if text:
                            chapter = new_context.get("chapter") or new_context.get("subtitle")
                            if chapter:
                                chapter_word_counts[chapter] += len(text.split())
                            section_builder.add_text(text)
                            lines.append(f"\n{text}\n")
                        return

                    for child in elem:
                        process_element(child, new_context)

                process_element(root, {})
                all_sections.extend(section_builder.get_sections())

                content = ''.join(lines)
                content = re.sub(r'\n{3,}', '\n\n', content)
                f.write(content)

                for k, v in chapter_word_counts.items():
                    all_chapter_counts[k] += v

        return output_path.stat().st_size, all_sections, dict(all_chapter_counts)
