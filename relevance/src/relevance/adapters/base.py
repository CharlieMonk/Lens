from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from relevance.application.fetcher import Fetcher


@dataclass(frozen=True)
class ParsedDocument:
    title: str
    url: str
    published_at: datetime
    raw_html: str
    text: str


class AgencyAdapter(ABC):
    @property
    @abstractmethod
    def agency_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch_documents(self, fetcher: Fetcher, base_url: str) -> list[ParsedDocument]:
        raise NotImplementedError
