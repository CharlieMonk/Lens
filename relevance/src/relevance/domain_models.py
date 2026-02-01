from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class SourceType(str, Enum):
    ENFORCEMENT = "enforcement"
    LITIGATION = "litigation"
    PRESS = "press"


class CitationType(str, Enum):
    SECTION = "section"
    PART = "part"


@dataclass(frozen=True)
class Agency:
    id: Optional[int]
    name: str
    aliases: list[str]


@dataclass(frozen=True)
class Source:
    id: Optional[int]
    agency_id: int
    source_type: SourceType
    base_url: str
    config_json: dict


@dataclass(frozen=True)
class Document:
    id: Optional[int]
    agency_id: int
    title: str
    url: str
    published_at: datetime
    retrieved_at: datetime


@dataclass(frozen=True)
class Citation:
    id: Optional[int]
    title_number: int
    part: str
    section: Optional[str]
    raw_text: str
    normalized: str
    citation_type: CitationType


@dataclass(frozen=True)
class DocumentCitation:
    document_id: int
    citation_id: int
    context_snippet: str
    match_start: int
    match_end: int
    occurrence_count: int
