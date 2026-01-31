"""Main orchestrator for fetching and processing eCFR data."""

import asyncio
from datetime import datetime
from pathlib import Path

import aiohttp
import requests

from .client import ECFRClient
from .constants import HISTORICAL_YEARS
from .extractor import XMLExtractor
from .database import ECFRDatabase


class ECFRFetcher:
    """Main orchestrator for fetching and processing eCFR data."""

    def __init__(self, output_dir: Path = Path("ecfr/ecfr_data"), max_workers: int = 5):
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.db = ECFRDatabase(output_dir / "ecfr.db")
        self.client = ECFRClient()

    def clear_cache(self) -> None:
        """Delete all cached files in the output directory."""
        if not self.output_dir.exists():
            return
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

    async def fetch_title_async(
        self,
        session: aiohttp.ClientSession,
        title_num: int,
        date: str,
        agency_lookup: dict,
    ) -> tuple[bool, str, list]:
        """Async fetch a single CFR title.

        Returns:
            Tuple of (success, message, sections).
        """
        extractor = XMLExtractor(agency_lookup)

        try:
            source, xml_content = await self.client.fetch_title_racing(session, title_num, date)
            size, sections, chapter_word_counts = extractor.extract(xml_content, title_num)
            self.db.save_sections(sections)

            if agency_lookup and chapter_word_counts:
                self.db.update_word_counts(title_num, chapter_word_counts, agency_lookup)

            return (True, f"{size:,} bytes ({source})", sections)

        except Exception as e:
            return (False, f"Error: {e}", [])

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

        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            for num, date in titles_to_fetch:
                try:
                    success, msg, sections = await self.fetch_title_async(session, num, date, agency_lookup)
                    symbol = "+" if success else "x"
                    print(f"{symbol} Title {num}: {msg}")
                    if success:
                        success_count += 1
                        total_words += sum(s.get("word_count", 0) for s in sections)
                        del sections
                except Exception as e:
                    print(f"x Title {num}: {e}")

        print("-" * 50)
        print(f"Complete: {success_count}/{len(titles_to_fetch)} titles downloaded")

        if total_words:
            print(f"Total words: {total_words:,}")

        if success_count > 0:
            print("\nComputing TF-IDF similarities...")
            self.compute_all_similarities(year=0)

        return 0 if success_count == len(titles_to_fetch) else 1

    def fetch_current(self, clear_cache: bool = False) -> int:
        """Sync wrapper for fetch_current_async."""
        return asyncio.run(self.fetch_current_async(clear_cache))

    async def fetch_historical_async(self, historical_years: list[int], title_nums: list[int] = None) -> int:
        """Async fetch titles for historical years using govinfo bulk data.

        Returns:
            Exit code (0 for success, 1 for any failure).
        """
        if title_nums is None:
            title_nums = [t for t in range(1, 51) if t != 35]  # Title 35 is reserved

        extractor = XMLExtractor()

        async def fetch_year_title(session, year, title_num):
            volumes = await self.client.fetch_govinfo_volumes(session, year, title_num)
            if volumes:
                try:
                    size, sections, _ = extractor.extract_govinfo_volumes(volumes, title_num)
                    return (year, title_num, True, f"{size:,} bytes ({len(volumes)} vols)", sections)
                except Exception as e:
                    return (year, title_num, False, f"extract error: {e}", [])

            return (year, title_num, False, "not available", [])

        years_to_fetch = [y for y in historical_years if not self.db.has_year_data(y)]
        if not years_to_fetch:
            print("All historical years already in database, skipping fetch")
            return 0

        skipped = set(historical_years) - set(years_to_fetch)
        if skipped:
            print(f"Skipping years already in database: {sorted(skipped)}")

        all_success = True
        connector = aiohttp.TCPConnector(limit=10)
        session_timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(connector=connector, timeout=session_timeout) as session:
            for year in years_to_fetch:
                print(f"\n{'='*50}")
                print(f"Fetching CFR {year} edition from govinfo")
                print("-" * 50)

                success_count = 0
                for title_num in title_nums:
                    try:
                        yr, t_num, success, msg, sections = await fetch_year_title(session, year, title_num)
                        symbol = "+" if success else "x"
                        print(f"{symbol} Title {t_num}: {msg}")
                        if success:
                            success_count += 1
                            if sections:
                                self.db.save_sections(sections, year=yr)
                                del sections
                    except Exception as e:
                        print(f"x Title {title_num}: {e}")

                print(f"Complete: {success_count}/{len(title_nums)} titles")
                if success_count < len(title_nums):
                    all_success = False

                if success_count > 0:
                    print(f"\nComputing TF-IDF similarities for {year}...")
                    self.compute_all_similarities(year=year)

        return 0 if all_success else 1

    def fetch_historical(self, historical_years: list[int], title_nums: list[int] = None) -> int:
        """Sync wrapper for fetch_historical_async."""
        return asyncio.run(self.fetch_historical_async(historical_years, title_nums))

    async def fetch_all_async(self, historical_years: list[int] = None) -> int:
        """Async fetch current data first, then historical data sequentially.

        Returns:
            Exit code (0 for success, 1 for any failure).
        """
        if historical_years is None:
            historical_years = HISTORICAL_YEARS

        current_result = await self.fetch_current_async()
        historical_result = await self.fetch_historical_async(historical_years)

        return 0 if current_result == 0 and historical_result == 0 else 1

    def fetch_all(self, historical_years: list[int] = None) -> int:
        """Sync wrapper for fetch_all_async."""
        return asyncio.run(self.fetch_all_async(historical_years))

    def compute_all_similarities(self, year: int = 0) -> dict[int, int]:
        """Compute TF-IDF similarities for all titles in the database.

        Returns:
            Dict mapping title number to similarity count (-1 if skipped).
        """
        import sqlite3
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT title FROM sections WHERE year = ? ORDER BY title", (year,))
        titles = [row[0] for row in cursor.fetchall()]
        conn.close()

        results = {}
        for title in titles:
            print(f"Computing similarities for Title {title}...", end=" ", flush=True)
            count = self.db.compute_similarities(title, year=year)
            if count >= 0:
                print(f"{count} pairs")
            else:
                print("skipped (too large)")
            results[title] = count

        return results
