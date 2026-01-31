#!/usr/bin/env python3
"""Fetch CFR titles 1-50 from the eCFR API for the latest issue date."""

import re
import sqlite3
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

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

    def _request_with_retry(self, url: str, timeout: int = 30) -> requests.Response:
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

    def fetch_title_xml(self, title_num: int, date: str) -> bytes:
        """Fetch full XML for a title on a specific date."""
        url = f"{self.BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml"
        response = self._request_with_retry(url, timeout=300)
        return response.content


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

    def convert(self, xml_content: bytes, output_path: Path) -> tuple[int, dict, dict]:
        """Convert XML content to Markdown and write to file.

        Returns:
            Tuple of (file_size, word_counts, chapter_word_counts).
        """
        root = etree.fromstring(xml_content)
        word_counts = defaultdict(int)
        chapter_word_counts = defaultdict(int)
        lines = []

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

        def process_element(elem, context):
            """Recursively process XML elements and generate Markdown."""
            tag = elem.tag
            elem_type = elem.attrib.get("TYPE", "")
            elem_n = elem.attrib.get("N", "")

            new_context = context.copy()
            if elem_type in self.TYPE_TO_LEVEL:
                new_context[self.TYPE_TO_LEVEL[elem_type]] = elem_n

            if tag == "HEAD":
                text = get_text(elem).strip()
                if text:
                    parent = elem.getparent()
                    parent_type = parent.attrib.get("TYPE", "") if parent is not None else ""
                    parent_n = parent.attrib.get("N", "") if parent is not None else ""
                    heading_level = self.TYPE_TO_HEADING.get(parent_type, 5)
                    lines.append(f"\n{'#' * heading_level} {text}\n")

                    if self.agency_lookup and parent_type in ("CHAPTER", "SUBTITLE", "SUBCHAP"):
                        title_num = new_context.get("title")
                        if title_num:
                            try:
                                key = (int(title_num), parent_n)
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
                    if new_context:
                        key = tuple(sorted(new_context.items()))
                        word_counts[key] += wc
                        chapter = new_context.get("chapter") or new_context.get("subtitle")
                        if chapter:
                            chapter_word_counts[chapter] += wc
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
                    if new_context:
                        key = tuple(sorted(new_context.items()))
                        word_counts[key] += wc
                        chapter = new_context.get("chapter") or new_context.get("subtitle")
                        if chapter:
                            chapter_word_counts[chapter] += wc
                    lines.append(f"\n{text}\n")
                return

            # Process children for DIV, ECFR, and other elements
            for child in elem:
                process_element(child, new_context)

        process_element(root, {})

        content = ''.join(lines)
        content = re.sub(r'\n{3,}', '\n\n', content)

        with open(output_path, "w") as f:
            f.write(content)

        return output_path.stat().st_size, dict(word_counts), dict(chapter_word_counts)


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
        csv_file = self.output_dir / "word_counts.csv"
        if csv_file.exists():
            csv_file.unlink()

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

    def fetch_title(self, title_num: int, date: str, agency_lookup: dict) -> tuple[bool, str, dict]:
        """Fetch a single CFR title and save to disk.

        Returns:
            Tuple of (success, message, word_counts).
        """
        output_file = self.output_dir / f"title_{title_num}.md"

        if self._is_file_fresh(output_file):
            return (True, "cached", {})

        try:
            xml_content = self.client.fetch_title_xml(title_num, date)
            converter = MarkdownConverter(agency_lookup)
            size, word_counts, chapter_word_counts = converter.convert(xml_content, output_file)

            if agency_lookup and chapter_word_counts:
                self.db.update_word_counts(title_num, chapter_word_counts, agency_lookup)

            return (True, f"{size:,} bytes", word_counts)

        except requests.exceptions.HTTPError as e:
            return (False, f"HTTP {e.response.status_code}", {})
        except requests.exceptions.RequestException as e:
            return (False, str(e), {})

    def fetch_all(self, clear_cache: bool = False) -> int:
        """Fetch all CFR titles 1-50 for the latest issue date in parallel.

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
        all_word_counts = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.fetch_title, num, date, agency_lookup): num
                for num, date in titles_to_fetch
            }

            for future in as_completed(futures):
                title_num = futures[future]
                success, msg, word_counts = future.result()
                symbol = "+" if success else "x"
                print(f"{symbol} Title {title_num}: {msg}")
                if success:
                    success_count += 1
                    all_word_counts.update(word_counts)

        print("-" * 50)
        print(f"Complete: {success_count}/{len(titles_to_fetch)} titles downloaded")

        if all_word_counts:
            csv_file = self.output_dir / "word_counts.csv"
            with open(csv_file, "w") as f:
                f.write("title,chapter,subchapter,part,subpart,word_count\n")
                for key, count in sorted(all_word_counts.items()):
                    ctx = dict(key)
                    row = [
                        ctx.get("title", ""),
                        ctx.get("chapter", ""),
                        ctx.get("subchapter", ""),
                        ctx.get("part", ""),
                        ctx.get("subpart", ""),
                        str(count),
                    ]
                    f.write(",".join(row) + "\n")

            total_words = sum(all_word_counts.values())
            print(f"Total words: {total_words:,}")

        return 0 if success_count == len(titles_to_fetch) else 1


if __name__ == "__main__":
    time0 = time.time()

    fetcher = ECFRFetcher()
    exit_code = fetcher.fetch_all()

    print(f"{time.time() - time0:.1f}s")
    exit(exit_code)
