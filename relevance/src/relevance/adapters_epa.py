from __future__ import annotations

from bs4 import BeautifulSoup

from relevance.adapters_base import AgencyAdapter, ParsedDocument
from relevance.application_dates import parse_date
from relevance.application_fetcher import Fetcher


class EpaEnforcementAdapter(AgencyAdapter):
    @property
    def agency_name(self) -> str:
        return "Environmental Protection Agency"

    def fetch_documents(self, fetcher: Fetcher, base_url: str) -> list[ParsedDocument]:
        listing_html = fetcher.get(base_url)
        soup = BeautifulSoup(listing_html, "lxml")
        links = [a.get("href") for a in soup.select("a.doc-link") if a.get("href")]
        documents: list[ParsedDocument] = []
        for link in links:
            raw_html = fetcher.get(link)
            doc_soup = BeautifulSoup(raw_html, "lxml")
            title = self._text(doc_soup.select_one("h1"))
            date_text = self._text(doc_soup.select_one("time"))
            body = self._text(doc_soup.select_one("div.article-body"))
            documents.append(
                ParsedDocument(
                    title=title,
                    url=link,
                    published_at=parse_date(date_text),
                    raw_html=raw_html,
                    text=body,
                )
            )
        return documents

    def _text(self, node) -> str:
        if node is None:
            return ""
        return " ".join(node.get_text(" ").split())
