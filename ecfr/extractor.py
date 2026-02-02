"""XML data extraction for eCFR data."""

import re
from collections import defaultdict
from lxml import etree

TYPE_TO_LEVEL = {"TITLE": "title", "SUBTITLE": "subtitle", "CHAPTER": "chapter", "SUBCHAP": "subchapter", "PART": "part", "SUBPART": "subpart", "SECTION": "section"}


def get_element_text(elem) -> str:
    """Recursively get all text content from an XML element."""
    texts = [elem.text or ""]
    for child in elem:
        texts.append(get_element_text(child))
        if child.tail:
            texts.append(child.tail)
    return ''.join(texts)


class XMLExtractor:
    """Extracts section data from eCFR XML."""

    def __init__(self, agency_lookup: dict = None):
        self.agency_lookup = agency_lookup or {}

    def extract(self, xml_content: bytes, title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from eCFR XML content."""
        return self._extract_ecfr([xml_content], title_num)

    def extract_chunks(self, xml_chunks: list[bytes], title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from multiple XML chunks."""
        return self._extract_ecfr(xml_chunks, title_num)

    def _extract_ecfr(self, xml_contents: list[bytes], title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from eCFR-format XML (single or multiple chunks)."""
        all_sections, chapter_wc, total_size = [], defaultdict(int), 0
        cfr_title = str(title_num) if title_num else None

        for xml_content in xml_contents:
            total_size += len(xml_content)
            root = etree.fromstring(xml_content)
            current, text_parts = None, []

            def finalize():
                nonlocal current, text_parts
                if current:
                    current["text"] = "\n".join(text_parts).strip()
                    current["word_count"] = len(current["text"].split())
                    all_sections.append(current)
                    current, text_parts = None, []

            def process(elem, ctx):
                nonlocal current, text_parts
                tag, etype, en = elem.tag, elem.attrib.get("TYPE", ""), elem.attrib.get("N", "")
                new_ctx = dict(ctx)
                if etype in TYPE_TO_LEVEL:
                    new_ctx[TYPE_TO_LEVEL[etype]] = cfr_title if etype == "TITLE" and cfr_title else en

                if etype == "SECTION":
                    finalize()
                    current = {k: new_ctx.get(k, "") for k in ["title", "subtitle", "chapter", "subchapter", "part", "subpart"]}
                    current["section"], current["heading"] = en.lstrip("§ ").strip(), ""
                    text_parts = []

                if tag == "HEAD":
                    text = get_element_text(elem).strip()
                    if text and current and elem.getparent() is not None and elem.getparent().attrib.get("TYPE") == "SECTION":
                        current["heading"] = text
                    return

                if tag in ("P", "FP", "NOTE", "EXTRACT", "GPOTABLE"):
                    text = get_element_text(elem).strip()
                    if text:
                        ch = new_ctx.get("chapter") or new_ctx.get("subtitle")
                        if ch:
                            chapter_wc[ch] += len(text.split())
                        if current:
                            text_parts.append(text)
                    return

                if tag in ("AUTH", "SOURCE"):
                    for child in elem:
                        process(child, new_ctx)
                    return

                for child in elem:
                    process(child, new_ctx)

            process(root, {})
            finalize()

        return total_size, all_sections, dict(chapter_wc)

    def extract_govinfo(self, xml_content: bytes, title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from govinfo CFR XML."""
        root = etree.fromstring(xml_content)
        sections, chapter_wc = [], defaultdict(int)
        ctx = {"title": str(title_num) if title_num else ""}

        for elem in root.iter():
            tag = elem.tag
            if tag in ("CHAPTER", "SUBCHAP", "PART", "SUBPART", "SUBTITLE"):
                hd = elem.find(".//HD")
                if hd is not None and hd.text:
                    # Use ^ anchor to avoid matching "SUBCHAPTER X" when looking for "CHAPTER X"
                    patterns = {
                        "CHAPTER": (r'^CHAPTER\s+([IVXLCDM]+)', "chapter"),
                        "PART": (r'^PART\s+(\d+)', "part"),
                        "SUBTITLE": (r'^Subtitle\s+([A-Z])', "subtitle"),
                    }
                    if tag in patterns:
                        m = re.search(patterns[tag][0], hd.text)
                        if m:
                            ctx[patterns[tag][1]] = m.group(1)
                    else:
                        ctx[{"SUBCHAP": "subchapter", "SUBPART": "subpart"}[tag]] = hd.text.strip()

            elif tag == "SECTION":
                sectno = elem.find("SECTNO")
                if sectno is None or not sectno.text:
                    continue
                subject = elem.find("SUBJECT")
                text_parts = [get_element_text(p).strip() for p in elem.findall(".//P") if get_element_text(p).strip()]
                full_text = "\n".join(text_parts)
                wc = len(full_text.split())
                ch = ctx.get("chapter", "")
                if ch:
                    chapter_wc[ch] += wc
                sections.append({
                    "title": ctx.get("title", ""), "subtitle": ctx.get("subtitle", ""), "chapter": ch,
                    "subchapter": ctx.get("subchapter", ""), "part": ctx.get("part", ""), "subpart": ctx.get("subpart", ""),
                    "section": sectno.text.lstrip("§ ").strip(), "heading": subject.text.strip() if subject is not None and subject.text else "",
                    "text": full_text, "word_count": wc
                })

        return len(xml_content), sections, dict(chapter_wc)

    def extract_govinfo_volumes(self, xml_volumes: list[bytes], title_num: int = None) -> tuple[int, list, dict]:
        """Extract sections from multiple govinfo volume XMLs."""
        all_sections, all_wc, total = [], defaultdict(int), 0
        for xml in xml_volumes:
            size, sections, wc = self.extract_govinfo(xml, title_num)
            total += size
            all_sections.extend(sections)
            for k, v in wc.items():
                all_wc[k] += v
        return total, all_sections, dict(all_wc)
