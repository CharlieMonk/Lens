#!/usr/bin/env python3
"""CLI for fetching CFR titles from eCFR and govinfo APIs.

Usage:
    python fetch_titles.py              # Fetch current + historical data
    python fetch_titles.py --current    # Fetch only current data
    python fetch_titles.py --historical # Fetch only historical data
    python fetch_titles.py --similarities [--year YEAR]  # Compute TF-IDF similarities
"""

import sys
import time

from ecfr import ECFRFetcher, HISTORICAL_YEARS

# Re-export classes for backwards compatibility
from ecfr import ECFRClient, ECFRDatabase, XMLExtractor


def main():
    time0 = time.time()
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

    elif "--similarities" in sys.argv:
        year = 0
        if "--year" in sys.argv:
            idx = sys.argv.index("--year")
            if idx + 1 < len(sys.argv):
                year = int(sys.argv[idx + 1])
        results = fetcher.compute_all_similarities(year=year)
        total = sum(v for v in results.values() if v >= 0)
        skipped = sum(1 for v in results.values() if v < 0)
        print(f"\nTotal: {total:,} similarity pairs, {skipped} titles skipped")
        exit_code = 0

    else:
        exit_code = fetcher.fetch_all()

    print(f"\n{time.time() - time0:.1f}s")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
