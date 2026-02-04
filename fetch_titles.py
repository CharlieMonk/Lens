#!/usr/bin/env python3
"""CLI for fetching CFR titles from eCFR and govinfo APIs.

Usage:
    python fetch_titles.py              # Fetch current + historical data
    python fetch_titles.py --current    # Fetch only current data
    python fetch_titles.py --historical # Fetch only historical data
    python fetch_titles.py --build-index # Build FAISS similarity index
"""

import sys
import time

from ecfr import ECFRFetcher, HISTORICAL_YEARS

# Re-export classes for backwards compatibility
from ecfr import ECFRClient, ECFRDatabase, XMLExtractor


def main():
    time0 = time.time()

    if "--build-index" in sys.argv:
        from ecfr import ECFRDatabase
        print("Building FAISS similarity index...")
        db = ECFRDatabase()
        result = db.build_similarity_index()
        print(f"Index built: {result['sections_indexed']:,} sections, "
              f"{result['index_size_mb']:.1f} MB, {result['build_time_s']:.1f}s")
        return 0

    fetcher = ECFRFetcher()

    if "--current" in sys.argv:
        exit_code = fetcher.fetch_current()

    elif "--historical" in sys.argv:
        title_nums = None
        if "--title" in sys.argv:
            idx = sys.argv.index("--title")
            if idx + 1 < len(sys.argv):
                title_nums = [int(sys.argv[idx + 1])]
        exit_code = fetcher.fetch_historical(HISTORICAL_YEARS, title_nums)

    else:
        exit_code = fetcher.fetch_all()

    # Build similarity index after fetching data
    if exit_code == 0:
        print("Building similarity index...", flush=True)
        try:
            result = fetcher.db.build_similarity_index()
            print(f"  {result['sections_indexed']:,} sections indexed ({result['index_size_mb']:.1f} MB)")
        except Exception as e:
            print(f"  Warning: Could not build similarity index: {e}")

    print(f"\n{time.time() - time0:.1f}s")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
