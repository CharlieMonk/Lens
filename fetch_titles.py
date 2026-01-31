#!/usr/bin/env python3
"""Fetch CFR titles 1-50 from the eCFR API for the latest issue date."""

import re
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
OUTPUT_DIR = Path("md_output")
METADATA_CACHE = OUTPUT_DIR / "titles_metadata.yaml"
WORD_COUNTS_CACHE = OUTPUT_DIR / "word_counts_cache.yaml"

# Maps DIV TYPE attributes to hierarchy levels and markdown heading depth
TYPE_TO_LEVEL = {
    "TITLE": "title",
    "SUBTITLE": "subtitle",
    "CHAPTER": "chapter",
    "SUBCHAP": "subchapter",
    "PART": "part",
    "SUBPART": "subpart",
    "SECTION": "section",
}

# Markdown heading levels for each hierarchy type
TYPE_TO_HEADING = {
    "TITLE": 1,
    "SUBTITLE": 2,
    "CHAPTER": 2,
    "SUBCHAP": 3,
    "PART": 3,
    "SUBPART": 4,
    "SECTION": 4,
}


def xml_to_markdown(xml_content: bytes, output_file: Path) -> tuple[int, dict]:
    """Convert XML content to Markdown and write to file.

    Returns tuple of (bytes_written, word_counts).
    word_counts is a dict mapping hierarchy keys to word counts.
    """
    root = etree.fromstring(xml_content)
    word_counts = defaultdict(int)
    lines = []

    def get_text(elem):
        """Get all text content from an element, including tail text of children."""
        texts = []
        if elem.text:
            texts.append(elem.text)
        for child in elem:
            texts.append(get_text(child))
            if child.tail:
                texts.append(child.tail)
        return ''.join(texts)

    def process_element(elem, context, depth=0):
        """Recursively process XML elements and generate Markdown."""
        tag = elem.tag
        elem_type = elem.attrib.get("TYPE", "")
        elem_n = elem.attrib.get("N", "")

        # Update hierarchy context
        new_context = context.copy()
        if elem_type in TYPE_TO_LEVEL:
            level = TYPE_TO_LEVEL[elem_type]
            new_context[level] = elem_n

        # Handle HEAD elements (titles/headings)
        if tag == "HEAD":
            text = get_text(elem).strip()
            if text:
                # Determine heading level from parent's TYPE
                parent = elem.getparent()
                parent_type = parent.attrib.get("TYPE", "") if parent is not None else ""
                heading_level = TYPE_TO_HEADING.get(parent_type, 5)
                lines.append(f"\n{'#' * heading_level} {text}\n")
            return

        # Handle paragraph elements
        if tag == "P":
            text = get_text(elem).strip()
            if text:
                # Count words
                if new_context:
                    key = tuple(sorted(new_context.items()))
                    word_counts[key] += len(text.split())
                lines.append(f"\n{text}\n")
            return

        # Handle CITA (citation) elements
        if tag == "CITA":
            text = get_text(elem).strip()
            if text:
                lines.append(f"\n*{text}*\n")
            return

        # Handle AUTH (authority) elements
        if tag == "AUTH":
            lines.append("\n**Authority:**\n")
            for child in elem:
                process_element(child, new_context, depth + 1)
            return

        # Handle SOURCE elements
        if tag == "SOURCE":
            lines.append("\n**Source:**\n")
            for child in elem:
                process_element(child, new_context, depth + 1)
            return

        # Handle FP (flush paragraph) and other text containers
        if tag in ("FP", "NOTE", "EXTRACT", "GPOTABLE"):
            text = get_text(elem).strip()
            if text:
                if new_context:
                    key = tuple(sorted(new_context.items()))
                    word_counts[key] += len(text.split())
                lines.append(f"\n{text}\n")
            return

        # Handle DIV elements (structural hierarchy)
        if tag.startswith("DIV"):
            for child in elem:
                process_element(child, new_context, depth + 1)
            return

        # Handle ECFR root
        if tag == "ECFR":
            for child in elem:
                process_element(child, new_context, depth + 1)
            return

        # Default: process children
        for child in elem:
            process_element(child, new_context, depth + 1)

    # Process the XML tree
    process_element(root, {})

    # Write Markdown file
    content = ''.join(lines)
    # Clean up excessive blank lines
    content = re.sub(r'\n{3,}', '\n\n', content)

    with open(output_file, "w") as f:
        f.write(content)

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
    for f in OUTPUT_DIR.glob("*.md"):
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


def count_words_from_markdown(md_path: Path) -> dict:
    """Count words from a Markdown file by parsing headings for hierarchy.

    Returns dict mapping hierarchy keys to word counts.
    """
    word_counts = defaultdict(int)
    context = {}

    # Heading patterns to extract hierarchy
    # # Title N - level 1
    # ## Subtitle/Chapter - level 2
    # ### Subchapter/Part - level 3
    # #### Subpart/Section - level 4
    heading_pattern = re.compile(r'^(#{1,6})\s+(.+)$')

    # Patterns to identify hierarchy elements from heading text
    title_pattern = re.compile(r'Title\s+(\d+)', re.IGNORECASE)
    subtitle_pattern = re.compile(r'Subtitle\s+([A-Z])', re.IGNORECASE)
    chapter_pattern = re.compile(r'Chapter\s+([IVXLCDM]+)', re.IGNORECASE)
    subchapter_pattern = re.compile(r'Subchapter\s+([A-Z])', re.IGNORECASE)
    part_pattern = re.compile(r'Part\s+(\d+)', re.IGNORECASE)
    subpart_pattern = re.compile(r'Subpart\s+([A-Z])', re.IGNORECASE)
    section_pattern = re.compile(r'§\s*(\d+\.\d+)', re.IGNORECASE)

    current_text = []

    def flush_text():
        """Count words in accumulated text and reset."""
        nonlocal current_text
        if current_text and context:
            text = ' '.join(current_text)
            key = tuple(sorted(context.items()))
            word_counts[key] += len(text.split())
        current_text = []

    with open(md_path, 'r') as f:
        for line in f:
            line = line.rstrip()

            # Check for heading
            heading_match = heading_pattern.match(line)
            if heading_match:
                flush_text()
                heading_text = heading_match.group(2)

                # Try to identify what kind of heading this is
                if m := title_pattern.search(heading_text):
                    context = {'title': m.group(1)}
                elif m := subtitle_pattern.search(heading_text):
                    context['subtitle'] = m.group(1)
                elif m := chapter_pattern.search(heading_text):
                    context['chapter'] = m.group(1)
                    # Clear lower levels
                    for k in ['subchapter', 'part', 'subpart', 'section']:
                        context.pop(k, None)
                elif m := subchapter_pattern.search(heading_text):
                    context['subchapter'] = m.group(1)
                    for k in ['part', 'subpart', 'section']:
                        context.pop(k, None)
                elif m := part_pattern.search(heading_text):
                    context['part'] = m.group(1)
                    for k in ['subpart', 'section']:
                        context.pop(k, None)
                elif m := subpart_pattern.search(heading_text):
                    context['subpart'] = m.group(1)
                    context.pop('section', None)
                elif m := section_pattern.search(heading_text):
                    context['section'] = m.group(1)
                continue

            # Skip empty lines and metadata markers
            if not line or line.startswith('**Authority:**') or line.startswith('**Source:**'):
                continue

            # Skip italic citations
            if line.startswith('*') and line.endswith('*'):
                continue

            # Accumulate text
            current_text.append(line)

    flush_text()
    return dict(word_counts)


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


def get_titles_metadata() -> dict[int, dict]:
    """Fetch metadata for all titles, using cache if available and fresh.

    Returns:
        Dict mapping title number to full metadata dict containing:
        - name: Title name (e.g., "General Provisions")
        - latest_amended_on: Date of last amendment
        - latest_issue_date: Date of latest issue
        - up_to_date_as_of: Date data is current as of
        - reserved: Whether title is reserved

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
            t["number"]: {
                "name": t.get("name"),
                "latest_amended_on": t.get("latest_amended_on"),
                "latest_issue_date": t.get("latest_issue_date"),
                "up_to_date_as_of": t.get("up_to_date_as_of"),
                "reserved": t.get("reserved", False),
            }
            for t in data["titles"]
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
    output_file = output_dir / f"title_{title_num}.md"

    # Use cache if valid (word counts handled by caller)
    if is_cache_valid(output_file):
        return (title_num, True, "cached", {})

    url = f"{BASE_URL}/full/{fetch_date}/title-{title_num}.xml"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            size, word_counts = xml_to_markdown(response.content, output_file)
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

    titles_to_fetch = [
        (num, meta["latest_issue_date"])
        for num, meta in titles_metadata.items()
        if 1 <= num <= 50 and meta.get("latest_issue_date")
    ]
    print(f"Processing {len(titles_to_fetch)} titles...")
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
                    # Use word counts from cache, or parse from Markdown if missing
                    word_counts = word_counts_cache.get(title_num)
                    if not word_counts:
                        md_path = OUTPUT_DIR / f"title_{title_num}.md"
                        print(f"  Counting words for title {title_num}...")
                        word_counts = count_words_from_markdown(md_path)
                        word_counts_cache[title_num] = word_counts
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
