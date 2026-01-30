#!/usr/bin/env python3
"""Fetch CFR titles 1-50 from the eCFR API for the current date."""

import os
import sys
from datetime import date
from pathlib import Path

import requests

BASE_URL = "https://www.ecfr.gov/api/versioner/v1/full"


def fetch_title(title_num: int, fetch_date: str, output_dir: Path) -> bool:
    """Fetch a single CFR title and save to disk.

    Args:
        title_num: The CFR title number (1-50)
        fetch_date: Date in YYYY-MM-DD format
        output_dir: Directory to save XML files

    Returns:
        True if successful, False otherwise
    """
    url = f"{BASE_URL}/{fetch_date}/title-{title_num}.xml"

    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()

        output_file = output_dir / f"title-{title_num}.xml"
        output_file.write_bytes(response.content)
        print(f"✓ Title {title_num}: saved ({len(response.content):,} bytes)")
        return True

    except requests.exceptions.HTTPError as e:
        print(f"✗ Title {title_num}: HTTP {e.response.status_code}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Title {title_num}: {e}")
        return False


def main() -> int:
    """Fetch all CFR titles 1-50 for the current date."""
    fetch_date = date.today().strftime("%Y-%m-%d")
    output_dir = Path("xml_output") / fetch_date

    print(f"Fetching CFR titles 1-50 for {fetch_date}")
    print(f"Output directory: {output_dir}")
    print("-" * 50)

    output_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    for title_num in range(1, 51):
        if fetch_title(title_num, fetch_date, output_dir):
            success_count += 1

    print("-" * 50)
    print(f"Complete: {success_count}/50 titles downloaded")

    return 0 if success_count == 50 else 1


if __name__ == "__main__":
    sys.exit(main())
