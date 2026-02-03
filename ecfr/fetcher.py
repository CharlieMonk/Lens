"""Main orchestrator for fetching and processing eCFR data."""

import asyncio
import gc
import os
from pathlib import Path
import aiohttp
import requests
import sys

from .client import ECFRClient
from .config import config
from .database import ECFRDatabase
from .extractor import XMLExtractor

HISTORICAL_YEARS = config.historical_years
MAX_WORKERS = config.max_workers


def _run_async(coro):
    """Run async coroutine."""
    return asyncio.run(coro)


class ECFRFetcher:
    """Main orchestrator for fetching and processing eCFR data."""

    def __init__(self, output_dir: Path | str | None = None, max_workers: int = None):
        output_dir = output_dir or config.output_dir
        self.output_dir = Path(output_dir) if isinstance(output_dir, str) else output_dir
        self.max_workers = max_workers or MAX_WORKERS
        self.db = ECFRDatabase(self.output_dir / "ecfr.db")
        self.client = ECFRClient()

    def clear_cache(self):
        if self.output_dir.exists():
            self.db.clear()

    def _load_titles_metadata(self) -> dict[int, dict]:
        if self.db.has_titles() and self.db.is_fresh():
            return self.db.get_titles()
        titles = self.client.fetch_titles()
        self.db.save_titles(titles)
        return {t["number"]: {k: t.get(k) for k in ["name", "latest_amended_on", "latest_issue_date", "up_to_date_as_of"]} | {"reserved": t.get("reserved", False)} for t in titles}

    def _load_agency_lookup(self) -> dict:
        if self.db.has_agencies() and self.db.is_fresh():
            return self.db.build_agency_lookup()
        self.db.save_agencies(self.client.fetch_agencies())
        return self.db.build_agency_lookup()

    async def fetch_title_async(self, session: aiohttp.ClientSession, title_num: int, date: str, agency_lookup: dict, year: int = 0) -> tuple[bool, str, int]:
        extractor = XMLExtractor(agency_lookup)
        try:
            # Fetch XML first, then structure (sequential to reduce peak memory)
            source, xml = await self.client.fetch_title_racing(session, title_num, date)
            size, sections, chapter_wc = extractor.extract(xml, title_num)
            del xml  # Free XML memory immediately after extraction

            self.db.save_sections(sections, year=year)
            word_count = sum(s.get("word_count", 0) for s in sections)
            del sections  # Free sections memory after saving
            if agency_lookup and chapter_wc:
                self.db.update_word_counts(title_num, chapter_wc, agency_lookup, year=year)
            return True, f"{size:,} bytes", word_count
        except Exception as e:
            return False, f"Error: {e}", 0

    async def fetch_current_async(self, clear_cache: bool = False) -> int:
        if clear_cache:
            print("Clearing cache...", flush=True)
            self.clear_cache()

        cached = self.db.is_fresh()
        print(f"Loading titles metadata {'(cached)' if cached else '(fetching)'}...", flush=True)
        try:
            titles_meta = self._load_titles_metadata()
        except (requests.exceptions.RequestException, KeyError, TypeError) as e:
            print(f"Error: Failed to fetch titles metadata: {e}", flush=True)
            return 1

        print(f"Loading agencies metadata {'(cached)' if cached else '(fetching)'}...", flush=True)
        try:
            agency_lookup = self._load_agency_lookup()
            print(f"  {len(agency_lookup)} chapter/agency mappings loaded", flush=True)
        except (requests.exceptions.RequestException, KeyError, TypeError) as e:
            print(f"Warning: Could not load agencies metadata: {e}", flush=True)
            agency_lookup = {}

        self.output_dir.mkdir(parents=True, exist_ok=True)
        titles_to_fetch = [(n, m["latest_issue_date"]) for n, m in titles_meta.items() if 1 <= n <= 50 and m.get("latest_issue_date")]
        print(f"Processing {len(titles_to_fetch)} titles...\nOutput directory: {self.output_dir}\n" + "-" * 50, flush=True)

        semaphore = asyncio.Semaphore(self.max_workers)

        async def fetch_one(session, num, date):
            async with semaphore:
                try:
                    return num, await self.fetch_title_async(session, num, date, agency_lookup)
                except Exception as e:
                    return num, (False, f"Error: {e}", 0)

        success_count, total_words = 0, 0
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.max_workers), timeout=aiohttp.ClientTimeout(total=config.timeout_fetcher_session)) as session:
            tasks = [asyncio.create_task(fetch_one(session, num, date)) for num, date in titles_to_fetch]
            # Process results as they complete to free memory early
            for coro in asyncio.as_completed(tasks):
                num, (ok, msg, word_count) = await coro
                print(f"{'+' if ok else 'x'} Title {num}: {msg}", flush=True)
                if ok:
                    success_count += 1
                    total_words += word_count
                gc.collect()  # Force garbage collection after each title

        print("-" * 50 + f"\nComplete: {success_count}/{len(titles_to_fetch)} titles downloaded", flush=True)
        if total_words:
            print(f"Total words: {total_words:,}", flush=True)
        return 0 if success_count == len(titles_to_fetch) else 1

    def fetch_current(self, clear_cache: bool = False) -> int:
        return _run_async(self.fetch_current_async(clear_cache))

    async def fetch_historical_async(self, historical_years: list[int], title_nums: list[int] = None) -> int:
        title_nums = title_nums or [t for t in range(1, 51) if t != 35]
        try:
            agency_lookup = self._load_agency_lookup()
        except Exception:
            agency_lookup = {}
        extractor = XMLExtractor(agency_lookup)

        years_to_fetch = [y for y in historical_years if not self.db.has_year_data(y)]
        if not years_to_fetch:
            print("All historical years already in database, skipping fetch", flush=True)
            return 0
        skipped = set(historical_years) - set(years_to_fetch)
        if skipped:
            print(f"Skipping years already in database: {sorted(skipped)}", flush=True)

        semaphore = asyncio.Semaphore(self.max_workers)

        async def fetch_title_year(session, year, title_num):
            async with semaphore:
                try:
                    # Try govinfo bulk first
                    volumes = await self.client.fetch_govinfo_volumes(session, year, title_num)
                    if volumes:
                        size, sections, chapter_wc = extractor.extract_govinfo_volumes(volumes, title_num)
                        if sections:
                            self.db.save_sections(sections, year=year)
                        if agency_lookup and chapter_wc:
                            self.db.update_word_counts(title_num, chapter_wc, agency_lookup, year=year)
                        del volumes, sections  # Free memory immediately
                        return title_num, True, f"{size:,} bytes"
                    # Fall back to eCFR for recent years
                    date = f"{year}-01-01"
                    source, xml = await self.client.fetch_title_racing(session, title_num, date)
                    size, sections, chapter_wc = extractor.extract(xml, title_num)
                    if sections:
                        self.db.save_sections(sections, year=year)
                    if agency_lookup and chapter_wc:
                        self.db.update_word_counts(title_num, chapter_wc, agency_lookup, year=year)
                    return title_num, True, f"{size:,} bytes ({source})"
                except Exception as e:
                    return title_num, False, str(e)

        all_success = True
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.max_workers), timeout=aiohttp.ClientTimeout(total=config.timeout_fetcher_session)) as session:
            for year in years_to_fetch:
                print(f"\n{'='*50}\nFetching CFR {year} edition\n" + "-" * 50, flush=True)
                tasks = [asyncio.create_task(fetch_title_year(session, year, title_num)) for title_num in title_nums]
                # Process results as they complete to free memory early
                success_count = 0
                for coro in asyncio.as_completed(tasks):
                    title_num, ok, msg = await coro
                    print(f"{'+' if ok else 'x'} Title {title_num}: {msg}", flush=True)
                    if ok:
                        success_count += 1
                    gc.collect()  # Force garbage collection after each title
                print(f"Complete: {success_count}/{len(title_nums)} titles", flush=True)
                if success_count < len(title_nums):
                    all_success = False
        return 0 if all_success else 1

    def fetch_historical(self, historical_years: list[int], title_nums: list[int] = None) -> int:
        return _run_async(self.fetch_historical_async(historical_years, title_nums))

    async def fetch_all_async(self, historical_years: list[int] = None) -> int:
        historical_years = historical_years or HISTORICAL_YEARS
        return 0 if await self.fetch_current_async() == 0 and await self.fetch_historical_async(historical_years) == 0 else 1

    def fetch_all(self, historical_years: list[int] = None) -> int:
        return _run_async(self.fetch_all_async(historical_years))

    async def update_stale_titles_async(self, stale_titles: list[int], agency_lookup: dict = None) -> dict[int, str]:
        if not stale_titles:
            return {}
        agency_lookup = agency_lookup or self._load_agency_lookup()
        titles_meta = self._load_titles_metadata()

        # Delete old sections first (sequential, fast)
        for title_num in stale_titles:
            deleted = self.db.delete_title_sections(title_num, year=0)
            if deleted:
                print(f"  Deleted {deleted} old sections for Title {title_num}")

        async def update_one(session, title_num):
            meta = titles_meta.get(title_num)
            if not meta or not meta.get("latest_issue_date"):
                return title_num, "no metadata"
            try:
                ok, msg, _ = await self.fetch_title_async(session, title_num, meta["latest_issue_date"], agency_lookup)
                return title_num, msg
            except Exception as e:
                return title_num, f"Error: {e}"

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.max_workers)) as session:
            tasks = [update_one(session, title_num) for title_num in stale_titles]
            fetch_results = await asyncio.gather(*tasks)

        results = {}
        for title_num, msg in sorted(fetch_results):
            results[title_num] = msg
            ok = not msg.startswith("Error") and msg != "no metadata"
            print(f"  {'+' if ok else 'x'} Title {title_num}: {msg}")
        return results

    def update_stale_titles(self, stale_titles: list[int], agency_lookup: dict = None) -> dict[int, str]:
        return _run_async(self.update_stale_titles_async(stale_titles, agency_lookup))

    def sync(self) -> dict:
        print("=" * 50 + "\neCFR Database Sync\n" + "=" * 50)
        results = {"stale_titles": [], "updated_titles": {}, "errors": []}

        print("\n1. Checking for updates...")
        try:
            api_titles = self.client.fetch_titles()
            self.db.save_titles(api_titles)
            print(f"   Fetched metadata for {len(api_titles)} titles")
        except Exception as e:
            results["errors"].append(f"Failed to fetch titles: {e}")
            print(f"   Error: {e}")
            return results

        stale = self.db.get_stale_titles(api_titles)
        results["stale_titles"] = stale
        print(f"   Found {len(stale)} titles needing updates: {stale}" if stale else "   All titles are up-to-date")

        if stale:
            print("\n2. Updating stale titles...")
            try:
                agency_lookup = self._load_agency_lookup()
            except Exception as e:
                print(f"   Warning: Could not load agencies: {e}")
                agency_lookup = {}
            results["updated_titles"] = self.update_stale_titles(stale, agency_lookup)

        print("\n3. Checking for missing data...")
        stored = set(self.db.list_titles(0))
        missing = set(range(1, 51)) - {35} - stored
        if missing:
            print(f"   Found {len(missing)} titles with no data: {sorted(missing)}")
            try:
                agency_lookup = self._load_agency_lookup()
            except Exception:
                agency_lookup = {}
            results["updated_titles"].update(self.update_stale_titles(sorted(missing), agency_lookup))
        else:
            print("   All titles have section data")

        # Refresh derived tables
        if stale or missing:
            print("\n4. Refreshing title word counts...")
            self.db.populate_title_word_counts()

        print("\n" + "=" * 50 + "\nSync complete\n" + "=" * 50)
        return results


def main(historical_years: list[int] = None, max_retries: int = 3) -> int:
    historical_years = historical_years or HISTORICAL_YEARS
    all_titles = set(range(1, 51)) - {35}  # All CFR titles except 35 (reserved)

    # Run at lowest CPU priority
    try:
        os.nice(19)
    except (OSError, AttributeError):
        pass  # nice() not available on all platforms

    fetcher = ECFRFetcher()
    db = fetcher.db
    print("=" * 50 + "\neCFR Database Population\n" + "=" * 50, flush=True)

    for name, check, action in [
        ("Titles metadata", lambda: db.has_titles() and db.is_fresh(), fetcher._load_titles_metadata),
        ("Agencies metadata", lambda: db.has_agencies() and db.is_fresh(), fetcher._load_agency_lookup),
    ]:
        if check():
            print(f"{name}: already cached", flush=True)
        else:
            print(f"{name}: fetching...", flush=True)
            try:
                action()
                print("  Done", flush=True)
            except Exception as e:
                print(f"  {'Error' if 'Titles' in name else 'Warning'}: {e}", flush=True)
                if "Titles" in name:
                    return 1

    # Fetch current sections with retry logic
    for attempt in range(max_retries):
        stored_titles = set(db.list_titles(0))
        missing_titles = all_titles - stored_titles
        if not missing_titles:
            print("Current sections (year=0): all titles present", flush=True)
            break
        print(f"Current sections: {len(missing_titles)} titles missing{f' (attempt {attempt + 1}/{max_retries})' if attempt > 0 else ''}", flush=True)
        fetcher.update_stale_titles(sorted(missing_titles))
    else:
        missing = all_titles - set(db.list_titles(0))
        if missing:
            print(f"  Warning: {len(missing)} titles still missing after {max_retries} attempts: {sorted(missing)}", flush=True)

    # Fetch historical years
    for year in historical_years:
        if db.has_year_data(year):
            print(f"Historical sections ({year}): already in database", flush=True)
        else:
            print(f"Historical sections ({year}): fetching...", flush=True)
            fetcher.fetch_historical([year])

    # Populate derived tables from sections data
    print("Populating title word counts...", flush=True)
    db.populate_title_word_counts()

    # Build similarity index for global search
    print("Building similarity index...", flush=True)
    try:
        result = db.build_similarity_index()
        print(f"  {result['sections_indexed']:,} sections indexed ({result['index_size_mb']:.1f} MB)", flush=True)
    except Exception as e:
        print(f"  Warning: Could not build similarity index: {e}", flush=True)

    print("\n" + "=" * 50 + "\nDatabase population complete\n" + "=" * 50, flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())