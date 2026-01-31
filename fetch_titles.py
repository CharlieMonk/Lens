#!/usr/bin/env python3
"""Fetch CFR titles 1-50 from the eCFR API for the latest issue date."""

import asyncio
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import aiohttp
import requests
from lxml import etree


class ECFRDatabase:
    """Handles all SQLite database operations for eCFR data."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection."""
        return sqlite3.connect(self.db_path)

    def init_schema(self) -> None:
        """Initialize the database schema."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS titles (
                number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                latest_amended_on TEXT,
                latest_issue_date TEXT,
                up_to_date_as_of TEXT,
                reserved INTEGER NOT NULL DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                slug TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                short_name TEXT,
                display_name TEXT,
                sortable_name TEXT,
                parent_slug TEXT,
                FOREIGN KEY (parent_slug) REFERENCES agencies(slug)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cfr_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency_slug TEXT NOT NULL,
                title INTEGER NOT NULL,
                chapter TEXT,
                subtitle TEXT,
                subchapter TEXT,
                FOREIGN KEY (agency_slug) REFERENCES agencies(slug)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agency_word_counts (
                agency_slug TEXT NOT NULL,
                title INTEGER NOT NULL,
                chapter TEXT NOT NULL,
                word_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (agency_slug, title, chapter),
                FOREIGN KEY (agency_slug) REFERENCES agencies(slug)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sections (
                title INTEGER NOT NULL,
                subtitle TEXT NOT NULL DEFAULT '',
                chapter TEXT NOT NULL DEFAULT '',
                subchapter TEXT NOT NULL DEFAULT '',
                part TEXT NOT NULL DEFAULT '',
                subpart TEXT NOT NULL DEFAULT '',
                section TEXT NOT NULL DEFAULT '',
                heading TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                word_count INTEGER NOT NULL,
                PRIMARY KEY (title, subtitle, chapter, subchapter, part, subpart, section)
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sections_title
            ON sections(title)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sections_title_section
            ON sections(title, section)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cfr_title_chapter
            ON cfr_references(title, chapter)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cfr_agency
            ON cfr_references(agency_slug)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_word_counts_agency
            ON agency_word_counts(agency_slug)
        """)

        conn.commit()
        conn.close()

    def is_fresh(self) -> bool:
        """Check if the database was modified today."""
        if not self.db_path.exists():
            return False
        midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        return self.db_path.stat().st_mtime >= midnight

    def clear(self) -> None:
        """Delete the database file."""
        if self.db_path.exists():
            self.db_path.unlink()

    # Titles methods

    def get_titles(self) -> dict[int, dict]:
        """Get all titles from the database.

        Returns:
            Dict mapping title number to metadata dict.
        """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved
            FROM titles
        """)
        metadata = {
            row[0]: {
                "name": row[1],
                "latest_amended_on": row[2],
                "latest_issue_date": row[3],
                "up_to_date_as_of": row[4],
                "reserved": bool(row[5]),
            }
            for row in cursor.fetchall()
        }
        conn.close()
        return metadata

    def has_titles(self) -> bool:
        """Check if titles table has data."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM titles")
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0

    def save_titles(self, titles: list[dict]) -> None:
        """Save titles to the database, replacing existing data."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM titles")

        for t in titles:
            cursor.execute("""
                INSERT INTO titles (number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                t["number"],
                t.get("name"),
                t.get("latest_amended_on"),
                t.get("latest_issue_date"),
                t.get("up_to_date_as_of"),
                1 if t.get("reserved", False) else 0,
            ))

        conn.commit()
        conn.close()

    # Agencies methods

    def has_agencies(self) -> bool:
        """Check if agencies table has data."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM agencies")
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0

    def save_agencies(self, agencies: list[dict]) -> None:
        """Save agencies and their CFR references to the database."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM cfr_references")
        cursor.execute("DELETE FROM agencies")

        for agency in agencies:
            slug = agency["slug"]
            cursor.execute("""
                INSERT INTO agencies (slug, name, short_name, display_name, sortable_name, parent_slug)
                VALUES (?, ?, ?, ?, ?, NULL)
            """, (
                slug,
                agency.get("name"),
                agency.get("short_name"),
                agency.get("display_name"),
                agency.get("sortable_name"),
            ))

            for ref in agency.get("cfr_references", []):
                cursor.execute("""
                    INSERT INTO cfr_references (agency_slug, title, chapter, subtitle, subchapter)
                    VALUES (?, ?, ?, ?, ?)
                """, (slug, ref.get("title"), ref.get("chapter"), ref.get("subtitle"), ref.get("subchapter")))

            for child in agency.get("children", []):
                child_slug = child["slug"]
                cursor.execute("""
                    INSERT INTO agencies (slug, name, short_name, display_name, sortable_name, parent_slug)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    child_slug,
                    child.get("name"),
                    child.get("short_name"),
                    child.get("display_name"),
                    child.get("sortable_name"),
                    slug,
                ))

                for ref in child.get("cfr_references", []):
                    cursor.execute("""
                        INSERT INTO cfr_references (agency_slug, title, chapter, subtitle, subchapter)
                        VALUES (?, ?, ?, ?, ?)
                    """, (child_slug, ref.get("title"), ref.get("chapter"), ref.get("subtitle"), ref.get("subchapter")))

        conn.commit()
        conn.close()

    def build_agency_lookup(self) -> dict:
        """Build a lookup table mapping CFR references to agency info.

        Returns:
            Dict mapping (title, chapter/subtitle/subchapter) tuples to list of agency info dicts.
        """
        lookup = defaultdict(list)
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                r.title,
                COALESCE(r.chapter, r.subtitle, r.subchapter) as chapter,
                a.slug as agency_slug,
                a.name as agency_name,
                a.parent_slug,
                p.name as parent_name
            FROM cfr_references r
            JOIN agencies a ON r.agency_slug = a.slug
            LEFT JOIN agencies p ON a.parent_slug = p.slug
            WHERE COALESCE(r.chapter, r.subtitle, r.subchapter) IS NOT NULL
        """)

        for row in cursor.fetchall():
            title, chapter, agency_slug, agency_name, parent_slug, parent_name = row
            if title and chapter:
                lookup[(title, chapter)].append({
                    "agency_slug": agency_slug,
                    "agency_name": agency_name,
                    "parent_slug": parent_slug,
                    "parent_name": parent_name,
                })

        conn.close()
        return dict(lookup)

    # Word counts methods

    def update_word_counts(self, title_num: int, chapter_word_counts: dict, agency_lookup: dict) -> None:
        """Update agency word counts in the database for a given title."""
        if not chapter_word_counts:
            return

        conn = self._connect()
        cursor = conn.cursor()

        for chapter, word_count in chapter_word_counts.items():
            for agency_info in agency_lookup.get((title_num, chapter), []):
                cursor.execute("""
                    INSERT OR REPLACE INTO agency_word_counts (agency_slug, title, chapter, word_count)
                    VALUES (?, ?, ?, ?)
                """, (agency_info["agency_slug"], title_num, chapter, word_count))

        conn.commit()
        conn.close()

    def save_sections(self, sections: list[dict]) -> None:
        """Save section data to the database.

        Args:
            sections: List of dicts with keys: title, subtitle, chapter, subchapter,
                     part, subpart, section, heading, text, word_count.
        """
        if not sections:
            return

        conn = self._connect()
        cursor = conn.cursor()

        for s in sections:
            cursor.execute("""
                INSERT OR REPLACE INTO sections
                (title, subtitle, chapter, subchapter, part, subpart, section, heading, text, word_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(s.get("title", 0)),
                s.get("subtitle") or "",
                s.get("chapter") or "",
                s.get("subchapter") or "",
                s.get("part") or "",
                s.get("subpart") or "",
                s.get("section") or "",
                s.get("heading") or "",
                s.get("text") or "",
                s.get("word_count", 0),
            ))

        conn.commit()
        conn.close()

    def get_agency_word_counts(self) -> dict[str, int]:
        """Get total word counts for all agencies, including parent aggregates."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT agency_slug, SUM(word_count) as total
            FROM agency_word_counts
            GROUP BY agency_slug
        """)
        direct_counts = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT slug, parent_slug FROM agencies WHERE parent_slug IS NOT NULL
        """)
        child_to_parent = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        totals = dict(direct_counts)
        for child_slug, parent_slug in child_to_parent.items():
            if child_slug in direct_counts:
                if parent_slug not in totals:
                    totals[parent_slug] = 0
                totals[parent_slug] += direct_counts[child_slug]

        return totals


class ECFRClient:
    """Handles all API requests to ecfr.gov."""

    BASE_URL = "https://www.ecfr.gov/api"

    def __init__(self, max_retries: int = 7, retry_delay: int = 3):
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _request_with_retry(self, url: str, timeout: int = 30, retry_on_timeout: bool = True) -> requests.Response:
        """Make a request with exponential backoff retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if not retry_on_timeout:
                    raise
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                raise
            except requests.exceptions.RequestException:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                raise
        raise requests.exceptions.RequestException("Max retries exceeded")

    def fetch_titles(self) -> list[dict]:
        """Fetch titles metadata from the API."""
        url = f"{self.BASE_URL}/versioner/v1/titles.json"
        response = self._request_with_retry(url)
        return response.json()["titles"]

    def fetch_agencies(self) -> list[dict]:
        """Fetch agencies metadata from the API."""
        url = f"{self.BASE_URL}/admin/v1/agencies.json"
        response = self._request_with_retry(url)
        return response.json().get("agencies", [])

    def fetch_title_xml(self, title_num: int, date: str, timeout: int = 60) -> bytes:
        """Fetch full XML for a title on a specific date. Fails fast on timeout."""
        url = f"{self.BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml"
        response = self._request_with_retry(url, timeout=timeout, retry_on_timeout=False)
        return response.content

    def fetch_title_xml_bulk(self, title_num: int, timeout: int = 120) -> bytes:
        """Fetch full XML for a title from govinfo bulk endpoint (current data only)."""
        url = f"https://www.govinfo.gov/bulkdata/ECFR/title-{title_num}/ECFR-title{title_num}.xml"
        response = self._request_with_retry(url, timeout=timeout)
        return response.content

    async def fetch_title_parallel(self, title_num: int, date: str) -> tuple[str, bytes]:
        """Fetch title from both eCFR and govinfo in parallel, return first success.

        Returns tuple of (source, xml_content).
        """
        async def fetch_ecfr():
            url = f"{self.BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml"
            connector = aiohttp.TCPConnector()
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return "ecfr", await resp.read()
                    return None

        async def fetch_govinfo():
            url = f"https://www.govinfo.gov/bulkdata/ECFR/title-{title_num}/ECFR-title{title_num}.xml"
            connector = aiohttp.TCPConnector()
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return "govinfo", await resp.read()
                    return None

        # Race both fetches, return first successful result
        tasks = [asyncio.create_task(fetch_ecfr()), asyncio.create_task(fetch_govinfo())]
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                if result:
                    # Cancel remaining tasks
                    for t in tasks:
                        t.cancel()
                    return result
            except:
                continue

        raise aiohttp.ClientError(f"Both sources failed for title {title_num}")

    async def fetch_cfr_annual_async(self, title_num: int, year: int) -> list[bytes]:
        """Fetch CFR annual edition volumes from govinfo bulk (fast historical data).

        Returns list of XML content for all volumes of the title.
        """
        base = f"https://www.govinfo.gov/bulkdata/CFR/{year}/title-{title_num}"

        async def fetch_vol(session, vol):
            url = f"{base}/CFR-{year}-title{title_num}-vol{vol}.xml"
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    return None
            except:
                return None

        # Fetch volumes 1-50 (most titles have fewer, extras will 404)
        connector = aiohttp.TCPConnector(limit=30)
        timeout = aiohttp.ClientTimeout(total=120)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [fetch_vol(session, v) for v in range(1, 51)]
            results = await asyncio.gather(*tasks)

        # Filter out None (404s)
        return [r for r in results if r is not None]

    def fetch_title_structure(self, title_num: int, date: str) -> dict:
        """Fetch the structure/TOC for a title on a specific date."""
        url = f"{self.BASE_URL}/versioner/v1/structure/{date}/title-{title_num}.json"
        response = self._request_with_retry(url, timeout=120)
        return response.json()

    def fetch_title_xml_chunk(self, title_num: int, date: str, chunk_type: str, chunk_id: str) -> bytes:
        """Fetch partial XML for a title (by chapter or subchapter)."""
        url = f"{self.BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml?{chunk_type}={chunk_id}"
        response = self._request_with_retry(url, timeout=300)
        return response.content

    def get_title_chunks(self, title_num: int, date: str) -> list[tuple[str, str]]:
        """Get list of chunks (parts) to fetch for a title.

        Returns list of ('part', part_id) tuples.
        """
        structure = self.fetch_title_structure(title_num, date)
        chunks = []

        def find_parts(node):
            if node.get('type') == 'part':
                chunks.append(('part', node.get('identifier')))
            for child in node.get('children', []):
                find_parts(child)

        find_parts(structure)
        return chunks

    async def fetch_chunks_async(self, title_num: int, date: str, chunks: list[tuple[str, str]],
                                   max_concurrent: int = 2, delay: float = 0.2) -> list[bytes]:
        """Fetch multiple chunks with rate limiting to avoid 429 errors.

        The eCFR API is heavily rate-limited. With max_concurrent=2 and delay=0.2,
        we achieve ~0.4 req/s which avoids rate limiting but is slow for large titles.

        Returns list of XML content bytes in the same order as chunks.
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        completed_count = [0]
        total = len(chunks)

        async def fetch_one(session, idx, chunk_type, chunk_id):
            url = f"{self.BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml?{chunk_type}={chunk_id}"
            async with semaphore:
                await asyncio.sleep(delay)  # Rate limiting delay
                for attempt in range(5):
                    try:
                        async with session.get(url) as response:
                            if response.status == 429:
                                wait_time = 5 * (2 ** attempt)
                                await asyncio.sleep(wait_time)
                                continue
                            response.raise_for_status()
                            content = await response.read()
                            completed_count[0] += 1
                            if completed_count[0] % 50 == 0:
                                print(f"    {completed_count[0]}/{total} parts...", flush=True)
                            return idx, content
                    except aiohttp.ClientError:
                        if attempt == 4:
                            raise
                        await asyncio.sleep(2)
            raise aiohttp.ClientError(f"Failed after 5 attempts: {url}")

        connector = aiohttp.TCPConnector(limit=max_concurrent)
        timeout = aiohttp.ClientTimeout(total=1800)  # 30 min timeout for large titles

        results = [None] * len(chunks)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [fetch_one(session, i, ct, cid) for i, (ct, cid) in enumerate(chunks)]
            completed = await asyncio.gather(*tasks, return_exceptions=False)
            for idx, content in completed:
                results[idx] = content

        return results


class MarkdownConverter:
    """Converts eCFR XML to Markdown format."""

    TYPE_TO_LEVEL = {
        "TITLE": "title",
        "SUBTITLE": "subtitle",
        "CHAPTER": "chapter",
        "SUBCHAP": "subchapter",
        "PART": "part",
        "SUBPART": "subpart",
        "SECTION": "section",
    }

    TYPE_TO_HEADING = {
        "TITLE": 1,
        "SUBTITLE": 2,
        "CHAPTER": 2,
        "SUBCHAP": 3,
        "PART": 3,
        "SUBPART": 4,
        "SECTION": 4,
    }

    def __init__(self, agency_lookup: dict = None):
        self.agency_lookup = agency_lookup or {}

    def convert(self, xml_content: bytes, output_path: Path, title_num: int = None) -> tuple[int, list, dict]:
        """Convert XML content to Markdown and write to file.

        Args:
            xml_content: Raw XML bytes.
            output_path: Path to write Markdown output.
            title_num: CFR title number (used for section tracking).

        Returns:
            Tuple of (file_size, sections, chapter_word_counts).
        """
        root = etree.fromstring(xml_content)
        sections = []
        chapter_word_counts = defaultdict(int)
        lines = []
        cfr_title = str(title_num) if title_num else None
        current_section = [None]  # Use list to allow mutation in nested function

        def get_text(elem):
            """Get all text content from an element."""
            texts = []
            if elem.text:
                texts.append(elem.text)
            for child in elem:
                texts.append(get_text(child))
                if child.tail:
                    texts.append(child.tail)
            return ''.join(texts)

        def finalize_section():
            """Finalize and save the current section."""
            if current_section[0]:
                s = current_section[0]
                s["text"] = "\n".join(s["_text_parts"]).strip()
                s["word_count"] = len(s["text"].split())
                del s["_text_parts"]
                sections.append(s)
                current_section[0] = None

        def process_element(elem, context):
            """Recursively process XML elements and generate Markdown."""
            tag = elem.tag
            elem_type = elem.attrib.get("TYPE", "")
            elem_n = elem.attrib.get("N", "")

            new_context = context.copy()
            if elem_type in self.TYPE_TO_LEVEL:
                # Use passed-in title_num for TITLE type, XML N attr for others
                if elem_type == "TITLE" and cfr_title:
                    new_context["title"] = cfr_title
                else:
                    new_context[self.TYPE_TO_LEVEL[elem_type]] = elem_n

            # When entering a new section, finalize previous and start new
            if elem_type == "SECTION":
                finalize_section()
                current_section[0] = {
                    "title": new_context.get("title") or "",
                    "subtitle": new_context.get("subtitle") or "",
                    "chapter": new_context.get("chapter") or "",
                    "subchapter": new_context.get("subchapter") or "",
                    "part": new_context.get("part") or "",
                    "subpart": new_context.get("subpart") or "",
                    "section": elem_n,
                    "heading": "",
                    "_text_parts": [],
                }

            if tag == "HEAD":
                text = get_text(elem).strip()
                if text:
                    parent = elem.getparent()
                    parent_type = parent.attrib.get("TYPE", "") if parent is not None else ""
                    parent_n = parent.attrib.get("N", "") if parent is not None else ""
                    heading_level = self.TYPE_TO_HEADING.get(parent_type, 5)
                    lines.append(f"\n{'#' * heading_level} {text}\n")

                    # Capture section heading
                    if parent_type == "SECTION" and current_section[0]:
                        current_section[0]["heading"] = text

                    if self.agency_lookup and parent_type in ("CHAPTER", "SUBTITLE", "SUBCHAP"):
                        title_num_ctx = new_context.get("title")
                        if title_num_ctx:
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
                return

            if tag == "P":
                text = get_text(elem).strip()
                if text:
                    wc = len(text.split())
                    chapter = new_context.get("chapter") or new_context.get("subtitle")
                    if chapter:
                        chapter_word_counts[chapter] += wc
                    if current_section[0]:
                        current_section[0]["_text_parts"].append(text)
                    lines.append(f"\n{text}\n")
                return

            if tag == "CITA":
                text = get_text(elem).strip()
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
                text = get_text(elem).strip()
                if text:
                    wc = len(text.split())
                    chapter = new_context.get("chapter") or new_context.get("subtitle")
                    if chapter:
                        chapter_word_counts[chapter] += wc
                    if current_section[0]:
                        current_section[0]["_text_parts"].append(text)
                    lines.append(f"\n{text}\n")
                return

            # Process children for DIV, ECFR, and other elements
            for child in elem:
                process_element(child, new_context)

        process_element(root, {})
        finalize_section()  # Finalize last section

        content = ''.join(lines)
        content = re.sub(r'\n{3,}', '\n\n', content)

        with open(output_path, "w") as f:
            f.write(content)

        return output_path.stat().st_size, sections, dict(chapter_word_counts)

    def convert_chunks(self, xml_chunks: list[bytes], output_path: Path, title_num: int = None) -> tuple[int, list, dict]:
        """Convert multiple XML chunks to a single Markdown file.

        Args:
            xml_chunks: List of raw XML bytes (one per volume).
            output_path: Path to write Markdown output.
            title_num: CFR title number (used for section tracking).

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
                current_section = [None]

                def get_text(elem):
                    texts = []
                    if elem.text:
                        texts.append(elem.text)
                    for child in elem:
                        texts.append(get_text(child))
                        if child.tail:
                            texts.append(child.tail)
                    return ''.join(texts)

                def finalize_section():
                    if current_section[0]:
                        s = current_section[0]
                        s["text"] = "\n".join(s["_text_parts"]).strip()
                        s["word_count"] = len(s["text"].split())
                        del s["_text_parts"]
                        all_sections.append(s)
                        current_section[0] = None

                def process_element(elem, context):
                    tag = elem.tag
                    elem_type = elem.attrib.get("TYPE", "")
                    elem_n = elem.attrib.get("N", "")

                    new_context = context.copy()
                    if elem_type in self.TYPE_TO_LEVEL:
                        # Use passed-in title_num for TITLE type, XML N attr for others
                        if elem_type == "TITLE" and cfr_title:
                            new_context["title"] = cfr_title
                        else:
                            new_context[self.TYPE_TO_LEVEL[elem_type]] = elem_n

                    # When entering a new section, finalize previous and start new
                    if elem_type == "SECTION":
                        finalize_section()
                        current_section[0] = {
                            "title": new_context.get("title") or "",
                            "subtitle": new_context.get("subtitle") or "",
                            "chapter": new_context.get("chapter") or "",
                            "subchapter": new_context.get("subchapter") or "",
                            "part": new_context.get("part") or "",
                            "subpart": new_context.get("subpart") or "",
                            "section": elem_n,
                            "heading": "",
                            "_text_parts": [],
                        }

                    if tag == "HEAD":
                        text = get_text(elem).strip()
                        if text:
                            parent = elem.getparent()
                            parent_type = parent.attrib.get("TYPE", "") if parent is not None else ""
                            heading_level = self.TYPE_TO_HEADING.get(parent_type, 5)
                            lines.append(f"\n{'#' * heading_level} {text}\n")

                            # Capture section heading
                            if parent_type == "SECTION" and current_section[0]:
                                current_section[0]["heading"] = text
                        return

                    if tag == "P":
                        text = get_text(elem).strip()
                        if text:
                            wc = len(text.split())
                            chapter = new_context.get("chapter") or new_context.get("subtitle")
                            if chapter:
                                chapter_word_counts[chapter] += wc
                            if current_section[0]:
                                current_section[0]["_text_parts"].append(text)
                            lines.append(f"\n{text}\n")
                        return

                    if tag == "CITA":
                        text = get_text(elem).strip()
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
                        text = get_text(elem).strip()
                        if text:
                            wc = len(text.split())
                            chapter = new_context.get("chapter") or new_context.get("subtitle")
                            if chapter:
                                chapter_word_counts[chapter] += wc
                            if current_section[0]:
                                current_section[0]["_text_parts"].append(text)
                            lines.append(f"\n{text}\n")
                        return

                    for child in elem:
                        process_element(child, new_context)

                process_element(root, {})
                finalize_section()  # Finalize last section in this chunk

                content = ''.join(lines)
                content = re.sub(r'\n{3,}', '\n\n', content)
                f.write(content)

                for k, v in chapter_word_counts.items():
                    all_chapter_counts[k] += v

        return output_path.stat().st_size, all_sections, dict(all_chapter_counts)


class ECFRFetcher:
    """Main orchestrator for fetching and processing eCFR data."""

    def __init__(self, output_dir: Path = Path("data_cache"), max_workers: int = 5):
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.db = ECFRDatabase(output_dir / "ecfr.db")
        self.client = ECFRClient()

    def clear_cache(self) -> None:
        """Delete all cached files in the output directory."""
        if not self.output_dir.exists():
            return
        for f in self.output_dir.glob("*.md"):
            f.unlink()
        self.db.clear()

    def _is_file_fresh(self, path: Path) -> bool:
        """Check if a file was modified today."""
        if not path.exists():
            return False
        midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        return path.stat().st_mtime >= midnight

    def _load_titles_metadata(self) -> dict[int, dict]:
        """Load titles metadata, fetching from API if needed."""
        if self.db.has_titles() and self.db.is_fresh():
            return self.db.get_titles()

        titles = self.client.fetch_titles()
        self.db.save_titles(titles)

        return {
            t["number"]: {
                "name": t.get("name"),
                "latest_amended_on": t.get("latest_amended_on"),
                "latest_issue_date": t.get("latest_issue_date"),
                "up_to_date_as_of": t.get("up_to_date_as_of"),
                "reserved": t.get("reserved", False),
            }
            for t in titles
        }

    def _load_agency_lookup(self) -> dict:
        """Load agency lookup, fetching from API if needed."""
        if self.db.has_agencies() and self.db.is_fresh():
            return self.db.build_agency_lookup()

        agencies = self.client.fetch_agencies()
        self.db.save_agencies(agencies)
        return self.db.build_agency_lookup()

    async def fetch_title_async(self, session: aiohttp.ClientSession, title_num: int,
                                  date: str, agency_lookup: dict,
                                  output_subdir: Path = None) -> tuple[bool, str, dict]:
        """Async fetch a single CFR title from eCFR (and govinfo for current data).

        Args:
            session: aiohttp session.
            title_num: CFR title number.
            date: Date string (YYYY-MM-DD). Historical dates use eCFR only.
            agency_lookup: Dict mapping (title, chapter) to agency info.
            output_subdir: Optional subdirectory for output.

        Returns:
            Tuple of (success, message, sections).
        """
        output_dir = output_subdir or self.output_dir
        output_file = output_dir / f"title_{title_num}.md"

        if self._is_file_fresh(output_file):
            return (True, "cached", [])

        converter = MarkdownConverter(agency_lookup)

        async def fetch_ecfr():
            url = f"https://www.ecfr.gov/api/versioner/v1/full/{date}/title-{title_num}.xml"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status == 200:
                        return "ecfr", await resp.read()
            except:
                pass
            return None

        async def fetch_govinfo():
            url = f"https://www.govinfo.gov/bulkdata/ECFR/title-{title_num}/ECFR-title{title_num}.xml"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status == 200:
                        return "govinfo", await resp.read()
            except:
                pass
            return None

        # Race both fetches, return first success
        tasks = [asyncio.create_task(fetch_ecfr()), asyncio.create_task(fetch_govinfo())]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                source, xml_content = result
                for t in tasks:
                    t.cancel()
                try:
                    size, sections, chapter_word_counts = converter.convert(xml_content, output_file, title_num)
                    self.db.save_sections(sections)
                    if agency_lookup and chapter_word_counts:
                        self.db.update_word_counts(title_num, chapter_word_counts, agency_lookup)
                    return (True, f"{size:,} bytes ({source})", sections)
                except Exception as e:
                    return (False, f"Convert error: {e}", [])

        return (False, "Both sources failed", [])

    async def fetch_current_async(self, clear_cache: bool = False) -> int:
        """Async fetch all CFR titles 1-50 for the latest issue date.

        Returns:
            Exit code (0 for success, 1 for failure).
        """
        if clear_cache:
            print("Clearing cache...")
            self.clear_cache()

        db_cached = self.db.is_fresh()
        print(f"Loading titles metadata {'(cached)' if db_cached else '(fetching)'}...")
        try:
            titles_metadata = self._load_titles_metadata()
        except (requests.exceptions.RequestException, KeyError, TypeError) as e:
            print(f"Error: Failed to fetch titles metadata: {e}")
            return 1

        print(f"Loading agencies metadata {'(cached)' if db_cached else '(fetching)'}...")
        try:
            agency_lookup = self._load_agency_lookup()
            print(f"  {len(agency_lookup)} chapter/agency mappings loaded")
        except (requests.exceptions.RequestException, KeyError, TypeError) as e:
            print(f"Warning: Could not load agencies metadata: {e}")
            agency_lookup = {}

        self.output_dir.mkdir(parents=True, exist_ok=True)

        titles_to_fetch = [
            (num, meta["latest_issue_date"])
            for num, meta in titles_metadata.items()
            if 1 <= num <= 50 and meta.get("latest_issue_date")
        ]
        print(f"Processing {len(titles_to_fetch)} titles...")
        print(f"Output directory: {self.output_dir}")
        print("-" * 50)

        success_count = 0
        total_words = 0

        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                self.fetch_title_async(session, num, date, agency_lookup)
                for num, date in titles_to_fetch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (num, _), result in zip(titles_to_fetch, results):
                if isinstance(result, Exception):
                    print(f"x Title {num}: {result}")
                else:
                    success, msg, sections = result
                    symbol = "+" if success else "x"
                    print(f"{symbol} Title {num}: {msg}")
                    if success:
                        success_count += 1
                        total_words += sum(s.get("word_count", 0) for s in sections)

        print("-" * 50)
        print(f"Complete: {success_count}/{len(titles_to_fetch)} titles downloaded")

        if total_words:
            print(f"Total words: {total_words:,}")

        return 0 if success_count == len(titles_to_fetch) else 1

    def fetch_current(self, clear_cache: bool = False) -> int:
        """Sync wrapper for fetch_current_async."""
        return asyncio.run(self.fetch_current_async(clear_cache))

    async def fetch_historical_async(self, historical_years: list[int], title_nums: list[int] = None) -> int:
        """Async fetch titles for historical years using govinfo CFR annual bulk data.

        Args:
            historical_years: List of years (e.g., [2020, 2015]).
            title_nums: Optional list of title numbers to fetch (default: 1-50).

        Returns:
            Exit code (0 for success, 1 for any failure).
        """
        if title_nums is None:
            # Title 35 is reserved (has never had content)
            title_nums = [t for t in range(1, 51) if t != 35]

        converter = MarkdownConverter()

        async def fetch_year_title(session, year, title_num):
            """Fetch volumes for a title/year in parallel."""
            output_subdir = self.output_dir / str(year)
            output_subdir.mkdir(parents=True, exist_ok=True)
            output_file = output_subdir / f"title_{title_num}.md"

            if self._is_file_fresh(output_file):
                return (year, title_num, True, "cached", 0)

            base = f"https://www.govinfo.gov/bulkdata/CFR/{year}/title-{title_num}"

            async def fetch_vol(vol):
                url = f"{base}/CFR-{year}-title{title_num}-vol{vol}.xml"
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            return vol, await resp.read()
                except:
                    pass
                return vol, None

            # Fetch all volumes in parallel
            results = await asyncio.gather(*[fetch_vol(v) for v in range(1, 51)])
            # Sort by volume number and filter successful ones
            xml_chunks = [data for vol, data in sorted(results) if data is not None]

            if not xml_chunks:
                return (year, title_num, False, f"no data for {year}", 0)

            try:
                size, _, _ = converter.convert_chunks(xml_chunks, output_file, title_num)
                return (year, title_num, True, f"{size:,} bytes ({len(xml_chunks)} vols)", size)
            except Exception as e:
                return (year, title_num, False, str(e), 0)

        all_success = True
        connector = aiohttp.TCPConnector(limit=50)
        timeout = aiohttp.ClientTimeout(total=300)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for year in historical_years:
                output_subdir = self.output_dir / str(year)
                output_subdir.mkdir(parents=True, exist_ok=True)
                print(f"\n{'='*50}")
                print(f"Fetching CFR {year} edition")
                print(f"Output: {output_subdir}")
                print("-" * 50)

                # Fetch all titles for this year in parallel
                tasks = [fetch_year_title(session, year, t) for t in title_nums]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                success_count = 0
                for result in results:
                    if isinstance(result, Exception):
                        print(f"x Error: {result}")
                    else:
                        yr, title_num, success, msg, _ = result
                        symbol = "+" if success else "x"
                        print(f"{symbol} Title {title_num}: {msg}")
                        if success:
                            success_count += 1

                print(f"Complete: {success_count}/{len(title_nums)} titles")
                if success_count < len(title_nums):
                    all_success = False

        return 0 if all_success else 1

    def fetch_historical(self, historical_years: list[int], title_nums: list[int] = None) -> int:
        """Sync wrapper for fetch_historical_async."""
        return asyncio.run(self.fetch_historical_async(historical_years, title_nums))

    async def fetch_all_async(self, historical_years: list[int] = None) -> int:
        """Async fetch current and historical data in parallel.

        Args:
            historical_years: List of years for historical data (default: [2024, 2020, 2015]).

        Returns:
            Exit code (0 for success, 1 for any failure).
        """
        if historical_years is None:
            historical_years = [2024, 2020, 2015]

        # Run both fetches in parallel
        current_result, historical_result = await asyncio.gather(
            self.fetch_current_async(),
            self.fetch_historical_async(historical_years)
        )

        return 0 if current_result == 0 and historical_result == 0 else 1

    def fetch_all(self, historical_years: list[int] = None) -> int:
        """Sync wrapper for fetch_all_async."""
        return asyncio.run(self.fetch_all_async(historical_years))


if __name__ == "__main__":
    import sys
    time0 = time.time()

    fetcher = ECFRFetcher()

    if "--current" in sys.argv:
        # Fetch only current data
        exit_code = fetcher.fetch_current()
    elif "--historical" in sys.argv:
        # Fetch only historical data
        historical_years = [2024, 2020, 2015]
        title_nums = None
        if "--title" in sys.argv:
            idx = sys.argv.index("--title")
            if idx + 1 < len(sys.argv):
                title_nums = [int(sys.argv[idx + 1])]
        exit_code = fetcher.fetch_historical(historical_years, title_nums)
    else:
        # Fetch both current and historical in parallel
        exit_code = fetcher.fetch_all()

    print(f"\n{time.time() - time0:.1f}s")
    exit(exit_code)
