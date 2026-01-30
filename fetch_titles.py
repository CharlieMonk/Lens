#!/usr/bin/env python3
"""Fetch CFR titles 1-50 from the eCFR API for the latest issue date."""

import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
import yaml
from lxml import etree

BASE_URL = "https://www.ecfr.gov/api/versioner/v1"
MAX_WORKERS = 5
MAX_RETRIES = 7
RETRY_DELAY = 3  # seconds, doubles each retry
CLEAR_CACHE = False
OUTPUT_DIR = Path("xml_output")
METADATA_CACHE = OUTPUT_DIR / "titles_metadata.yaml"
WORD_COUNTS_CACHE = OUTPUT_DIR / "word_counts_cache.yaml"

# Maps DIV TYPE attributes to hierarchy levels
TYPE_TO_LEVEL = {
    "TITLE": "title",
    "CHAPTER": "chapter",
    "SUBCHAP": "subchapter",
    "PART": "part",
    "SUBPART": "subpart",
}


def xml_to_yaml(xml_content: bytes, output_file: Path) -> tuple[int, dict]:
    """Convert XML content to YAML and write to file.

    Returns tuple of (bytes_written, word_counts).
    word_counts is a dict mapping hierarchy keys to word counts.
    """
    root = etree.fromstring(xml_content)
    word_counts = defaultdict(int)

    # Two-pass approach: first build structure, then count words with ancestry
    # Pass 1: Build data structure
    stack = [(root, {})]
    root_result = stack[0][1]

    while stack:
        elem, result = stack.pop()

        if elem.attrib:
            result["@attributes"] = dict(elem.attrib)

        children = list(elem)
        if children:
            child_dict = {}
            result["children"] = child_dict

            # Capture text before first child element
            if elem.text and elem.text.strip():
                result["text"] = elem.text.strip()

            for child in reversed(children):
                child_result = {}
                # Capture tail text (text after this child element)
                if child.tail and child.tail.strip():
                    child_result["tail"] = child.tail.strip()
                if child.tag in child_dict:
                    if not isinstance(child_dict[child.tag], list):
                        child_dict[child.tag] = [child_dict[child.tag]]
                    child_dict[child.tag].insert(0, child_result)
                else:
                    child_dict[child.tag] = child_result
                stack.append((child, child_result))
        else:
            text = (elem.text or "").strip()
            if text:
                result["text"] = text

    data = {root.tag: root_result}

    with open(output_file, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # Pass 2: Count words by walking tree with parent pointers
    for elem in root.iter():
        text = (elem.text or "").strip()
        if not text:
            continue

        # Walk up to find hierarchy context
        context = {}
        node = elem
        while node is not None:
            elem_type = node.attrib.get("TYPE", "")
            if elem_type in TYPE_TO_LEVEL:
                level = TYPE_TO_LEVEL[elem_type]
                if level not in context:
                    context[level] = node.attrib.get("N", "")
            node = node.getparent()

        if context:
            key = tuple(sorted(context.items()))
            word_counts[key] += len(text.split())

    return output_file.stat().st_size, dict(word_counts)


def is_cache_valid(cache_path: Path) -> bool:
    """Check if a cache file exists and was modified today."""
    if not cache_path.exists():
        return False
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    return cache_path.stat().st_mtime >= midnight


def clear_cache() -> None:
    """Delete all cached files in the output directory."""
    if not OUTPUT_DIR.exists():
        return
    for f in OUTPUT_DIR.glob("*.yaml"):
        f.unlink()
    csv_file = OUTPUT_DIR / "word_counts.csv"
    if csv_file.exists():
        csv_file.unlink()


def load_word_counts_cache() -> dict[int, dict]:
    """Load word counts cache from file. Returns dict mapping title_num to word_counts."""
    if not is_cache_valid(WORD_COUNTS_CACHE):
        return {}
    with open(WORD_COUNTS_CACHE) as f:
        cached = yaml.safe_load(f) or {}
    # Convert string keys back to tuples
    result = {}
    for title_num, counts in cached.items():
        word_counts = {}
        for key_str, count in counts.items():
            pairs = tuple(tuple(p.split("=", 1)) for p in key_str.split(","))
            word_counts[tuple(sorted(pairs))] = count
        result[title_num] = word_counts
    return result


def save_word_counts_cache(cache: dict[int, dict]) -> None:
    """Save word counts cache to file."""
    # Convert tuple keys to strings for YAML serialization
    serializable = {}
    for title_num, word_counts in cache.items():
        serializable[title_num] = {
            ",".join(f"{k}={v}" for k, v in key): count
            for key, count in word_counts.items()
        }
    with open(WORD_COUNTS_CACHE, "w") as f:
        yaml.dump(serializable, f)


def get_titles_metadata() -> dict[int, str]:
    """Fetch metadata for all titles, using cache if available and fresh.

    Returns:
        Dict mapping title number to latest_issue_date.

    Raises:
        RuntimeError: If unable to fetch or parse the titles metadata.
    """
    if is_cache_valid(METADATA_CACHE):
        with open(METADATA_CACHE) as f:
            return yaml.safe_load(f)

    url = f"{BASE_URL}/titles.json"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        metadata = {
            t["number"]: t["latest_issue_date"]
            for t in data["titles"]
            if t.get("latest_issue_date")
        }
        METADATA_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with open(METADATA_CACHE, "w") as f:
            yaml.dump(metadata, f)
        return metadata
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch titles metadata: {e}")
    except (KeyError, TypeError) as e:
        raise RuntimeError(f"Failed to parse titles metadata: {e}")


def fetch_title(title_num: int, fetch_date: str, output_dir: Path) -> tuple[int, bool, str, dict]:
    """Fetch a single CFR title and save to disk with retry logic.

    Args:
        title_num: The CFR title number (1-50)
        fetch_date: Date in YYYY-MM-DD format
        output_dir: Directory to save XML files

    Returns:
        Tuple of (title_num, success, message, word_counts)
    """
    output_file = output_dir / f"title_{title_num}.yaml"

    # Use cache if valid (word counts handled by caller)
    if is_cache_valid(output_file):
        return (title_num, True, "cached", {})

    url = f"{BASE_URL}/full/{fetch_date}/title-{title_num}.xml"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            size, word_counts = xml_to_yaml(response.content, output_file)
            return (title_num, True, f"{size:,} bytes", word_counts)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            return (title_num, False, f"HTTP {e.response.status_code}", {})
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            return (title_num, False, str(e), {})

    return (title_num, False, "Max retries exceeded", {})


def main() -> int:
    """Fetch all CFR titles 1-50 for the latest issue date in parallel."""
    if CLEAR_CACHE:
        print("Clearing cache...")
        clear_cache()

    cached = is_cache_valid(METADATA_CACHE)
    print(f"Loading titles metadata {'(cached)' if cached else '(fetching)'}...")
    try:
        titles_metadata = get_titles_metadata()
    except RuntimeError as e:
        print(f"Error: {e}")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load word counts cache
    word_counts_cache = load_word_counts_cache()

    titles_to_fetch = [(num, date) for num, date in titles_metadata.items() if 1 <= num <= 50]
    print(f"Fetching {len(titles_to_fetch)} titles in parallel...")
    print(f"Output directory: {OUTPUT_DIR}")
    print("-" * 50)

    success_count = 0
    all_word_counts = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_title, num, date, OUTPUT_DIR): num
            for num, date in titles_to_fetch
        }

        for future in as_completed(futures):
            title_num, success, msg, word_counts = future.result()
            symbol = "✓" if success else "✗"
            print(f"{symbol} Title {title_num}: {msg}")
            if success:
                success_count += 1
                if msg == "cached":
                    # Use word counts from cache
                    word_counts = word_counts_cache.get(title_num, {})
                else:
                    # Save fresh word counts to cache
                    word_counts_cache[title_num] = word_counts
                all_word_counts.update(word_counts)

    # Save updated word counts cache
    save_word_counts_cache(word_counts_cache)

    print("-" * 50)
    print(f"Complete: {success_count}/{len(titles_to_fetch)} titles downloaded")

    # Save word counts to CSV
    if all_word_counts:
        csv_file = OUTPUT_DIR / "word_counts.csv"
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
        print(f"Word counts saved to {csv_file}")

        # Print summary
        total_words = sum(all_word_counts.values())
        print(f"Total words: {total_words:,}")

    return 0 if success_count == len(titles_to_fetch) else 1


if __name__ == "__main__":
    time0 = time.time()

    main()
    
    print(time.time()-time0)
