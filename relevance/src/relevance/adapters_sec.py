from __future__ import annotations

from bs4 import BeautifulSoup

from relevance.adapters_base import (
    AgencyAdapter,
    ParsedDocument,
    extract_body_text,
    extract_date_text,
    extract_title,
    extract_pdf_text,
    find_pdf_links,
    looks_like_rss,
    parse_rss_items,
)
from relevance.application_dates import parse_date
from relevance.application_fetcher import Fetcher


class SecEnforcementAdapter(AgencyAdapter):
    @property
    def agency_name(self) -> str:
        return "Securities and Exchange Commission"

    def fetch_documents(
        self, fetcher: Fetcher, base_url: str, config: dict | None = None
    ) -> list[ParsedDocument]:
        listing_html = fetcher.get(base_url)
        links: list[str] = []
        rss_dates: dict[str, str] = {}
        rss_summaries: dict[str, str] = {}
        if looks_like_rss(listing_html):
            items = parse_rss_items(listing_html)
            for item in items:
                link = item.get("link")
                if not link:
                    continue
                links.append(link)
                if item.get("pubDate"):
                    rss_dates[link] = item["pubDate"]
                if item.get("summary"):
                    rss_summaries[link] = item["summary"]
        else:
            soup = BeautifulSoup(listing_html, "lxml")
            links = [a.get("href") for a in soup.select("a.doc-link") if a.get("href")]
        max_links = (config or {}).get("max_links")
        if max_links:
            links = links[: int(max_links)]
        documents: list[ParsedDocument] = []
        for link in links:
            raw_html = fetcher.get(link)
            doc_soup = BeautifulSoup(raw_html, "lxml")
            title = extract_title(doc_soup)
            date_text = extract_date_text(doc_soup)
            if not date_text and link in rss_dates:
                date_text = rss_dates[link]
            body = extract_body_text(doc_soup)
            if (config or {}).get("use_pdf"):
                pdf_links = find_pdf_links(doc_soup, link, limit=1)
                if pdf_links:
                    pdf_texts = []
                    for pdf in pdf_links:
                        try:
                            pdf_texts.append(extract_pdf_text(fetcher, pdf))
                        except Exception:
                            continue
                    if pdf_texts:
                        body = (body + " " + " ".join(pdf_texts)).strip()
            if not body and link in rss_summaries:
                body = rss_summaries[link]
            try:
                published_at = parse_date(date_text)
            except Exception:
                published_at = parse_date(rss_dates.get(link, "")) if rss_dates.get(link) else None
            if published_at is None:
                from datetime import datetime, timezone

                published_at = datetime.now(timezone.utc)
            documents.append(
                ParsedDocument(
                    title=title,
                    url=link,
                    published_at=published_at,
                    text=body,
                )
            )
        return documents
