from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
import re

from io import BytesIO
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from pypdf import PdfReader

from relevance.application_fetcher import Fetcher


@dataclass(frozen=True)
class ParsedDocument:
    title: str
    url: str
    published_at: datetime
    text: str


class AgencyAdapter(ABC):
    @property
    @abstractmethod
    def agency_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch_documents(
        self, fetcher: Fetcher, base_url: str, config: dict | None = None
    ) -> list[ParsedDocument]:
        raise NotImplementedError


_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)


def looks_like_rss(text: str) -> bool:
    snippet = text.lstrip()[:200].lower()
    return snippet.startswith("<?xml") or "<rss" in snippet or "<feed" in snippet


def parse_rss_items(xml_text: str) -> list[dict]:
    soup = BeautifulSoup(xml_text, "xml")
    items = []
    if soup.find("feed") is not None:
        for entry in soup.find_all("entry"):
            title = (entry.title.get_text(" ") if entry.title else "").strip()
            link_tag = entry.find("link")
            link = ""
            if link_tag is not None:
                link = link_tag.get("href", "") or link_tag.get_text(" ").strip()
            pub = (entry.updated.get_text(" ") if entry.updated else "").strip()
            if not pub:
                pub = (entry.published.get_text(" ") if entry.published else "").strip()
            summary = (entry.summary.get_text(" ") if entry.summary else "").strip()
            items.append({"title": title, "link": link, "pubDate": pub, "summary": summary})
        return items
    for item in soup.find_all("item"):
        title = (item.title.get_text(" ") if item.title else "").strip()
        link = (item.link.get_text(" ") if item.link else "").strip()
        pub = (item.pubDate.get_text(" ") if item.pubDate else "").strip()
        summary = (item.description.get_text(" ") if item.description else "").strip()
        items.append({"title": title, "link": link, "pubDate": pub, "summary": summary})
    return items


def extract_title(soup: BeautifulSoup) -> str:
    for selector in ["h1", "article h1", "#main-content h1"]:
        node = soup.select_one(selector)
        if node and node.get_text(strip=True):
            return " ".join(node.get_text(" ").split())
    title = soup.title.get_text(" ") if soup.title else ""
    return " ".join(title.split())


def extract_date_text(soup: BeautifulSoup) -> str:
    time_tag = soup.find("time")
    if time_tag:
        if time_tag.has_attr("datetime"):
            return time_tag["datetime"]
        text = time_tag.get_text(" ").strip()
        if text:
            return text
    for meta in soup.find_all("meta"):
        if meta.get("property") in {"article:published_time", "article:modified_time"}:
            return meta.get("content", "")
        if meta.get("name") in {"pubdate", "date", "dcterms.date"}:
            return meta.get("content", "")
    text = " ".join(soup.get_text(" ").split())
    match = _DATE_RE.search(text)
    return match.group(0) if match else ""


def extract_body_text(soup: BeautifulSoup) -> str:
    selectors = [
        "div.article-body",
        "div.field--name-body",
        "div#main-content",
        "article",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node and node.get_text(strip=True):
            return " ".join(node.get_text(" ").split())
    return " ".join(soup.get_text(" ").split())


def find_pdf_links(soup: BeautifulSoup, page_url: str, limit: int = 1) -> list[str]:
    links: list[str] = []
    for anchor in soup.select("a[href]"):
        href = anchor.get("href")
        if not href:
            continue
        if ".pdf" in href.lower():
            links.append(urljoin(page_url, href))
        if len(links) >= limit:
            break
    return links


def extract_pdf_text(fetcher: Fetcher, pdf_url: str) -> str:
    data = fetcher.get_bytes(pdf_url)
    reader = PdfReader(BytesIO(data))
    chunks = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text:
            chunks.append(text)
    return " ".join(" ".join(chunks).split())
