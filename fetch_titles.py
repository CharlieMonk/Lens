#!/usr/bin/env python3
"""CLI for fetching CFR titles from eCFR and govinfo APIs.

Usage:
    python fetch_titles.py              # Fetch current + historical data
    python fetch_titles.py --current    # Fetch only current data
    python fetch_titles.py --historical # Fetch only historical data
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

    else:
        exit_code = fetcher.fetch_all()

    print(f"\n{time.time() - time0:.1f}s")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
