from __future__ import annotations

import re
from dataclasses import dataclass

from relevance.domain_models import CitationType


@dataclass(frozen=True)
class ExtractedCitation:
    title_number: int
    part: str
    section: str | None
    raw_text: str
    normalized: str
    citation_type: CitationType
    match_start: int
    match_end: int
    context_snippet: str


class CitationExtractor:
    _section_pattern = re.compile(
        r"(?P<title>\d{1,3})\s*"
        r"C\.?F\.?R\.?\s*"
        r"(?:§+|Sec\.?|Section)?\s*"
        r"(?P<part>\d+[A-Za-z]?)\.(?P<section>[0-9A-Za-z\-]+(?:\([^\)]+\))*)",
        re.IGNORECASE,
    )
    _section_long_pattern = re.compile(
        r"(?:Title\s*)?(?P<title>\d{1,3})\s*,?\s*"
        r"(?:Code\s+of\s+Federal\s+Regulations)\s*,?\s*"
        r"(?:Part\s*)?(?P<part>\d+[A-Za-z]?)\.(?P<section>[0-9A-Za-z\-]+(?:\([^\)]+\))*)",
        re.IGNORECASE,
    )
    _part_pattern = re.compile(
        r"(?P<title>\d{1,3})\s*"
        r"C\.?F\.?R\.?\s*"
        r"Part\s*(?P<part>\d+[A-Za-z]?)",
        re.IGNORECASE,
    )
    _part_long_pattern = re.compile(
        r"(?:Title\s*)?(?P<title>\d{1,3})\s*,?\s*"
        r"(?:Code\s+of\s+Federal\s+Regulations)\s*,?\s*"
        r"Part\s*(?P<part>\d+[A-Za-z]?)",
        re.IGNORECASE,
    )

    def extract(self, text: str) -> list[ExtractedCitation]:
        matches: list[ExtractedCitation] = []
        for match in self._section_pattern.finditer(text):
            title = int(match.group("title"))
            part = match.group("part")
            section = match.group("section")
            normalized = f"{title} CFR {part}.{section}"
            matches.append(
                ExtractedCitation(
                    title_number=title,
                    part=part,
                    section=section,
                    raw_text=match.group(0),
                    normalized=normalized,
                    citation_type=CitationType.SECTION,
                    match_start=match.start(),
                    match_end=match.end(),
                    context_snippet=self._snippet(text, match.start(), match.end()),
                )
            )
        for match in self._section_long_pattern.finditer(text):
            title = int(match.group("title"))
            part = match.group("part")
            section = match.group("section")
            normalized = f"{title} CFR {part}.{section}"
            matches.append(
                ExtractedCitation(
                    title_number=title,
                    part=part,
                    section=section,
                    raw_text=match.group(0),
                    normalized=normalized,
                    citation_type=CitationType.SECTION,
                    match_start=match.start(),
                    match_end=match.end(),
                    context_snippet=self._snippet(text, match.start(), match.end()),
                )
            )
        for match in self._part_pattern.finditer(text):
            title = int(match.group("title"))
            part = match.group("part")
            normalized = f"{title} CFR Part {part}"
            matches.append(
                ExtractedCitation(
                    title_number=title,
                    part=part,
                    section=None,
                    raw_text=match.group(0),
                    normalized=normalized,
                    citation_type=CitationType.PART,
                    match_start=match.start(),
                    match_end=match.end(),
                    context_snippet=self._snippet(text, match.start(), match.end()),
                )
            )
        for match in self._part_long_pattern.finditer(text):
            title = int(match.group("title"))
            part = match.group("part")
            normalized = f"{title} CFR Part {part}"
            matches.append(
                ExtractedCitation(
                    title_number=title,
                    part=part,
                    section=None,
                    raw_text=match.group(0),
                    normalized=normalized,
                    citation_type=CitationType.PART,
                    match_start=match.start(),
                    match_end=match.end(),
                    context_snippet=self._snippet(text, match.start(), match.end()),
                )
            )
        return matches

    def _snippet(self, text: str, start: int, end: int, radius: int = 80) -> str:
        left = max(0, start - radius)
        right = min(len(text), end + radius)
        snippet = text[left:right]
        return " ".join(snippet.split())
