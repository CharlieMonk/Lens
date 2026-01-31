from __future__ import annotations

from bs4 import BeautifulSoup

from relevance.adapters_base import (
    AgencyAdapter,
    ParsedDocument,
    extract_links_from_listing,
    extract_body_text,
    extract_date_text,
    extract_title,
    extract_pdf_text,
    find_pdf_links,
    find_pdf_links_in_html,
    resolve_js_redirect,
    looks_like_rss,
    parse_rss_items,
)
from relevance.application_dates import parse_date
from relevance.application_fetcher import Fetcher
from concurrent.futures import ThreadPoolExecutor, as_completed


class EpaEnforcementAdapter(AgencyAdapter):
    @property
    def agency_name(self) -> str:
        return "Environmental Protection Agency"

    def fetch_documents(
        self, fetcher: Fetcher, base_url: str, config: dict | None = None
    ) -> list[ParsedDocument]:
        links: list[str] = []
        rss_dates: dict[str, str] = {}
        rss_summaries: dict[str, str] = {}
        rss_titles: dict[str, str] = {}
        listing_urls = (config or {}).get("listing_urls") or [base_url]
        link_selector = (config or {}).get("link_selector")
        link_regex = (config or {}).get("link_regex")
        use_playwright = bool((config or {}).get("use_playwright_listing"))
        for listing_url in listing_urls:
            listing_html = fetcher.get(listing_url)
            if use_playwright and not listing_html.strip():
                listing_html = self._fetch_with_playwright(listing_url)
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
                    if item.get("title"):
                        rss_titles[link] = item["title"]
            else:
                links.extend(
                    extract_links_from_listing(
                        listing_html,
                        listing_url,
                        link_selector=link_selector,
                        href_regex=link_regex,
                    )
                )
        max_links = (config or {}).get("max_links")
        if max_links:
            links = links[: int(max_links)]
        max_workers = int((config or {}).get("max_workers") or 0)
        documents: list[ParsedDocument] = []

        def fetch_one(link: str) -> ParsedDocument | None:
            if link.lower().endswith(".pdf"):
                body = extract_pdf_text(fetcher, link)
                title = rss_titles.get(link, link)
                published_at = None
                if link in rss_dates:
                    published_at = parse_date(rss_dates[link])
                if published_at is None:
                    from datetime import datetime, timezone

                    published_at = datetime.now(timezone.utc)
                return ParsedDocument(
                    title=title,
                    url=link,
                    published_at=published_at,
                    text=body,
                )
            raw_html = fetcher.get(link)
            redirect = resolve_js_redirect(raw_html, link)
            if redirect:
                raw_html = fetcher.get(redirect)
            doc_soup = BeautifulSoup(raw_html, "lxml")
            title = extract_title(doc_soup) or rss_titles.get(link, link)
            date_text = extract_date_text(doc_soup)
            if not date_text and link in rss_dates:
                date_text = rss_dates[link]
            body = extract_body_text(doc_soup)
            if (config or {}).get("use_pdf"):
                pdf_links = find_pdf_links(doc_soup, link, limit=1)
                if not pdf_links:
                    pdf_links = find_pdf_links_in_html(raw_html, link, limit=1)
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
            return ParsedDocument(
                title=title,
                url=link,
                published_at=published_at,
                text=body,
            )

        if max_workers and max_workers > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fetch_one, link): link for link in links}
                for future in as_completed(futures):
                    try:
                        doc = future.result()
                        if doc:
                            documents.append(doc)
                    except Exception:
                        continue
        else:
            for link in links:
                doc = fetch_one(link)
                if doc:
                    documents.append(doc)
        return documents

    def _fetch_with_playwright(self, url: str) -> str:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return ""
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=20000)
            content = page.content()
            browser.close()
            return content
