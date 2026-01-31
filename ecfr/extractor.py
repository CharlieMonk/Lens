"""XML data extraction for eCFR data."""

import re
from collections import defaultdict

from lxml import etree

from .constants import TYPE_TO_LEVEL


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


class XMLExtractor:
    """Extracts section data from eCFR XML."""

    def __init__(self, agency_lookup: dict = None):
        self.agency_lookup = agency_lookup or {}

    def extract(self, xml_content: bytes, title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from eCFR XML content.

        Returns:
            Tuple of (xml_size, sections, chapter_word_counts).
        """
        root = etree.fromstring(xml_content)
        chapter_word_counts = defaultdict(int)
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
                return

            if tag == "AUTH":
                for child in elem:
                    process_element(child, new_context)
                return

            if tag == "SOURCE":
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
                return

            for child in elem:
                process_element(child, new_context)

        process_element(root, {})
        sections = section_builder.get_sections()

        return len(xml_content), sections, dict(chapter_word_counts)

    def extract_govinfo(self, xml_content: bytes, title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from govinfo CFR XML.

        Returns:
            Tuple of (xml_size, sections, chapter_word_counts).
        """
        root = etree.fromstring(xml_content)
        sections = []
        chapter_word_counts = defaultdict(int)
        context = {"title": str(title_num) if title_num else ""}

        for elem in root.iter():
            tag = elem.tag

            if tag == "CHAPTER":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    match = re.search(r'CHAPTER\s+([IVXLCDM]+)', hd.text)
                    if match:
                        context["chapter"] = match.group(1)

            elif tag == "SUBCHAP":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    context["subchapter"] = hd.text.strip()

            elif tag == "PART":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    match = re.search(r'PART\s+(\d+)', hd.text)
                    if match:
                        context["part"] = match.group(1)

            elif tag == "SUBPART":
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    context["subpart"] = hd.text.strip()

            elif tag == "SECTION":
                section = self._parse_govinfo_section(elem, context, chapter_word_counts)
                if section:
                    sections.append(section)

        return len(xml_content), sections, dict(chapter_word_counts)

    def _parse_govinfo_section(self, elem, context: dict, chapter_word_counts: dict) -> dict | None:
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

        full_text = "\n".join(text_parts)
        word_count = len(full_text.split())

        chapter = context.get("chapter", "")
        if chapter:
            chapter_word_counts[chapter] += word_count

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

    def extract_govinfo_volumes(self, xml_volumes: list[bytes], title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from multiple govinfo volume XMLs.

        Returns:
            Tuple of (total_xml_size, sections, chapter_word_counts).
        """
        all_sections = []
        all_chapter_counts = defaultdict(int)
        total_size = 0

        for xml_content in xml_volumes:
            size, sections, chapter_counts = self.extract_govinfo(xml_content, title_num)
            total_size += size
            all_sections.extend(sections)
            for k, v in chapter_counts.items():
                all_chapter_counts[k] += v

        return total_size, all_sections, dict(all_chapter_counts)

    def extract_chunks(self, xml_chunks: list[bytes], title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from multiple XML chunks.

        Returns:
            Tuple of (total_xml_size, sections, chapter_word_counts).
        """
        all_sections = []
        all_chapter_counts = defaultdict(int)
        total_size = 0
        cfr_title = str(title_num) if title_num else None

        for xml_content in xml_chunks:
            root = etree.fromstring(xml_content)
            chapter_word_counts = defaultdict(int)
            section_builder = SectionBuilder()
            total_size += len(xml_content)

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
                    return

                if tag in ("AUTH", "SOURCE"):
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
                    return

                for child in elem:
                    process_element(child, new_context)

            process_element(root, {})
            all_sections.extend(section_builder.get_sections())

            for k, v in chapter_word_counts.items():
                all_chapter_counts[k] += v

        return total_size, all_sections, dict(all_chapter_counts)
