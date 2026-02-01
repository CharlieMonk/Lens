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

    def __init__(self, output_dir: Path | str = Path("ecfr/ecfr_data"), max_workers: int = 5):
        self.output_dir = Path(output_dir) if isinstance(output_dir, str) else output_dir
        self.max_workers = max_workers
        self.db = ECFRDatabase(self.output_dir / "ecfr.db")
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
            print("\nComputing embeddings...")
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
                    print(f"\nComputing embeddings for {year}...")
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
        """Compute vector embeddings for all titles in the database.

        Returns:
            Dict mapping title number to embedding count.
        """
        titles = self.db.list_section_titles(year)

        results = {}
        for title in titles:
            print(f"Computing embeddings for Title {title}...", end=" ", flush=True)
            count = self.db.compute_similarities(title, year=year)
            print(f"{count} embeddings")
            results[title] = count

        return results

    def ensure_embeddings(self, year: int = 0) -> dict[int, int]:
        """Ensure embeddings exist for all sections in the database for a given year.

        Only computes embeddings for sections that don't already have them.
        Reuses embeddings for identical text across sections.

        Returns:
            Dict mapping title number to count of new embeddings computed.
        """
        titles = self.db.list_section_titles(year)

        results = {}
        for title in titles:
            count = self.db.compute_similarities(title, year=year)
            if count > 0:
                print(f"  Title {title}: {count} new embeddings")
                results[title] = count

        return results

    async def update_stale_titles_async(self, stale_titles: list[int], agency_lookup: dict = None) -> dict[int, str]:
        """Update specific titles that have been identified as stale.

        Args:
            stale_titles: List of title numbers to update.
            agency_lookup: Agency lookup dict (fetched if not provided).

        Returns:
            Dict mapping title number to status message.
        """
        if not stale_titles:
            return {}

        if agency_lookup is None:
            agency_lookup = self._load_agency_lookup()

        titles_metadata = self._load_titles_metadata()
        results = {}

        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            for title_num in stale_titles:
                meta = titles_metadata.get(title_num)
                if not meta or not meta.get("latest_issue_date"):
                    results[title_num] = "no metadata"
                    continue

                # Delete old sections for this title
                deleted = self.db.delete_title_sections(title_num, year=0)
                if deleted:
                    print(f"  Deleted {deleted} old sections for Title {title_num}")

                # Fetch fresh data
                try:
                    success, msg, sections = await self.fetch_title_async(
                        session, title_num, meta["latest_issue_date"], agency_lookup
                    )
                    results[title_num] = msg
                    if success:
                        print(f"  + Title {title_num}: {msg}")
                    else:
                        print(f"  x Title {title_num}: {msg}")
                except Exception as e:
                    results[title_num] = f"Error: {e}"
                    print(f"  x Title {title_num}: {e}")

        return results

    def update_stale_titles(self, stale_titles: list[int], agency_lookup: dict = None) -> dict[int, str]:
        """Sync wrapper for update_stale_titles_async."""
        return asyncio.run(self.update_stale_titles_async(stale_titles, agency_lookup))

    def sync(self, compute_embeddings: bool = True) -> dict:
        """Synchronize database with latest eCFR data.

        This is the main entry point for keeping the database up-to-date.
        It will:
        1. Fetch latest titles metadata from API
        2. Identify titles that have been updated
        3. Re-fetch those titles
        4. Compute missing/stale embeddings

        Args:
            compute_embeddings: Whether to compute embeddings after syncing.

        Returns:
            Dict with sync results including updated titles and embedding counts.
        """
        print("=" * 50)
        print("eCFR Database Sync")
        print("=" * 50)

        results = {
            "stale_titles": [],
            "updated_titles": {},
            "embeddings_computed": {},
            "errors": [],
        }

        # Step 1: Fetch latest titles metadata
        print("\n1. Checking for updates...")
        try:
            api_titles = self.client.fetch_titles()
            self.db.save_titles(api_titles)
            print(f"   Fetched metadata for {len(api_titles)} titles")
        except Exception as e:
            results["errors"].append(f"Failed to fetch titles: {e}")
            print(f"   Error: {e}")
            return results

        # Step 2: Identify stale titles
        stale = self.db.get_stale_titles(api_titles)
        results["stale_titles"] = stale

        if stale:
            print(f"   Found {len(stale)} titles needing updates: {stale}")
        else:
            print("   All titles are up-to-date")

        # Step 3: Update stale titles
        if stale:
            print("\n2. Updating stale titles...")
            try:
                agency_lookup = self._load_agency_lookup()
            except Exception as e:
                print(f"   Warning: Could not load agencies: {e}")
                agency_lookup = {}

            updated = self.update_stale_titles(stale, agency_lookup)
            results["updated_titles"] = updated

        # Step 4: Check for missing data (titles with no sections)
        print("\n3. Checking for missing data...")
        stored_titles = set(self.db.list_titles(0))
        all_titles = set(range(1, 51)) - {35}  # Title 35 is reserved
        missing = all_titles - stored_titles

        if missing:
            print(f"   Found {len(missing)} titles with no data: {sorted(missing)}")
            # Fetch missing titles
            try:
                agency_lookup = self._load_agency_lookup()
            except Exception:
                agency_lookup = {}

            updated = self.update_stale_titles(sorted(missing), agency_lookup)
            results["updated_titles"].update(updated)
        else:
            print("   All titles have section data")

        # Step 5: Compute missing embeddings
        if compute_embeddings:
            print("\n4. Computing missing embeddings...")
            embedding_results = self.ensure_embeddings(year=0)
            results["embeddings_computed"] = embedding_results

            if not embedding_results:
                print("   All sections have embeddings")

        print("\n" + "=" * 50)
        print("Sync complete")
        print("=" * 50)

        return results


def main(historical_years: list[int] = None) -> int:
    """Populate the database with all necessary CFR data.

    Checks if data already exists before fetching/computing:
    - Titles metadata
    - Agencies metadata
    - Current sections (year=0)
    - Historical sections (for each year in historical_years)
    - Embeddings for all sections (only computes missing ones)

    Args:
        historical_years: List of historical years to include.
                         Defaults to HISTORICAL_YEARS constant.

    Returns:
        Exit code (0 for success, 1 for any failure).
    """
    if historical_years is None:
        historical_years = HISTORICAL_YEARS

    fetcher = ECFRFetcher()
    db = fetcher.db

    print("=" * 50)
    print("eCFR Database Population")
    print("=" * 50)

    # Check and load titles metadata
    if db.has_titles() and db.is_fresh():
        print("Titles metadata: already cached")
    else:
        print("Titles metadata: fetching...")
        try:
            fetcher._load_titles_metadata()
            print("  Done")
        except Exception as e:
            print(f"  Error: {e}")
            return 1

    # Check and load agencies metadata
    if db.has_agencies() and db.is_fresh():
        print("Agencies metadata: already cached")
    else:
        print("Agencies metadata: fetching...")
        try:
            fetcher._load_agency_lookup()
            print("  Done")
        except Exception as e:
            print(f"  Warning: {e}")

    # Check and fetch current sections
    if db.has_year_data(0):
        print("Current sections (year=0): already in database")
    else:
        print("Current sections: fetching...")
        result = fetcher.fetch_current()
        if result != 0:
            print("  Warning: some titles failed to fetch")

    # Check and fetch historical sections
    for year in historical_years:
        if db.has_year_data(year):
            print(f"Historical sections ({year}): already in database")
        else:
            print(f"Historical sections ({year}): fetching...")
            fetcher.fetch_historical([year])

    # Ensure embeddings for all sections (only computes missing ones)
    print("\n" + "=" * 50)
    print("Ensuring embeddings for all sections...")
    print("=" * 50)

    all_years = [0] + historical_years
    for year in all_years:
        year_label = "current" if year == 0 else str(year)
        print(f"Checking embeddings ({year_label})...")
        results = fetcher.ensure_embeddings(year=year)
        if not results:
            print(f"  All sections already have embeddings")

    print("\n" + "=" * 50)
    print("Database population complete")
    print("=" * 50)

    return 0
