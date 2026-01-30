#!/usr/bin/env python3
"""Fetch CFR titles 1-50 from the eCFR API for the latest issue date."""

import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yaml
from lxml import etree

BASE_URL = "https://www.ecfr.gov/api/versioner/v1"
MAX_WORKERS = 5
MAX_RETRIES = 7
RETRY_DELAY = 3  # seconds, doubles each retry

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

            for child in reversed(children):
                child_result = {}
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


def get_titles_metadata() -> dict[int, str]:
    """Fetch metadata for all titles.

    Returns:
        Dict mapping title number to latest_issue_date.

    Raises:
        RuntimeError: If unable to fetch or parse the titles metadata.
    """
    url = f"{BASE_URL}/titles.json"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return {
            t["number"]: t["latest_issue_date"]
            for t in data["titles"]
            if t.get("latest_issue_date")
        }
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
    url = f"{BASE_URL}/full/{fetch_date}/title-{title_num}.xml"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            output_file = output_dir / f"title-{title_num}.yaml"
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
    print("Fetching titles metadata...")
    try:
        titles_metadata = get_titles_metadata()
    except RuntimeError as e:
        print(f"Error: {e}")
        return 1

    output_dir = Path("xml_output")
    output_dir.mkdir(parents=True, exist_ok=True)

    titles_to_fetch = [(num, date) for num, date in titles_metadata.items() if 1 <= num <= 50]
    print(f"Fetching {len(titles_to_fetch)} titles in parallel...")
    print(f"Output directory: {output_dir}")
    print("-" * 50)

    success_count = 0
    all_word_counts = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_title, num, date, output_dir): num
            for num, date in titles_to_fetch
        }

        for future in as_completed(futures):
            title_num, success, msg, word_counts = future.result()
            symbol = "✓" if success else "✗"
            print(f"{symbol} Title {title_num}: {msg}")
            if success:
                success_count += 1
                all_word_counts.update(word_counts)

    print("-" * 50)
    print(f"Complete: {success_count}/{len(titles_to_fetch)} titles downloaded")

    # Save word counts to CSV
    if all_word_counts:
        csv_file = output_dir / "word_counts.csv"
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
