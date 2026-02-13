"""
Lambda function to fetch a single CFR title for a given year.
Writes extracted sections as gzipped JSON to S3.

Supports dynamic timeouts based on title size and per-title retry with exponential backoff.
"""

import json
import os
import glob
import gzip
import hashlib
import shutil
import boto3
import urllib.request
import urllib.error
from xml.etree import ElementTree as ET
from typing import Optional
from dataclasses import dataclass, field
import time


def _hash_text(text: str) -> str:
    """Compute SHA-256 hash of text content."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

s3 = boto3.client('s3')
S3_BUCKET = os.environ.get('S3_BUCKET')

# API endpoints
ECFR_BASE_URL = "https://www.ecfr.gov/api"
GOVINFO_BULK_URL = "https://www.govinfo.gov/bulkdata/CFR"
GOVINFO_CONTENT_URL = "https://www.govinfo.gov/content/pkg"

# Title size categories (from historical XML sizes)
# Very large titles that need subchapter-level fetching to avoid timeouts
VERY_LARGE_TITLES = {40}  # EPA - 39 volumes, ~800MB total
# Large titles: Tax (26), Health (42, 45), Acquisition (48)
LARGE_TITLES = {26, 42, 45, 48}
# Medium titles: Agriculture (7), Banks (12), Aviation (14), Commodities (17),
#               Food/Drug (21), Labor (29), Transportation (49)
MEDIUM_TITLES = {7, 12, 14, 17, 21, 29, 49}
# Reserved title (no content)
RESERVED_TITLES = {35}

# Special title availability - some titles have limited historical data
# Title 3 (The President): Only available from 2010 on govinfo, 2015 on eCFR
TITLE_AVAILABILITY = {
    3: {"govinfo_min_year": 2010, "ecfr_min_date": "2015-03-17"},
    2: {"govinfo_min_year": 2001},  # Title 2 added later
    6: {"govinfo_min_year": 2003},  # Homeland Security created 2002
}

# Current year - govinfo bulk data not available for current/recent year
import datetime
CURRENT_YEAR = datetime.datetime.now().year

# Cache for titles metadata (reused across invocations in same container)
_titles_cache = {}


@dataclass
class FetchError:
    """Structured error for tracking fetch failures."""
    title: int
    year: int
    source: str  # "ecfr", "govinfo", or "both"
    error_type: str  # "timeout", "http_error", "parse_error", "not_found"
    message: str
    attempt: int


@dataclass
class FetchResult:
    """Structured result for tracking fetch outcomes."""
    title: int
    year: int
    success: bool
    source: str = ""
    sections_count: int = 0
    duration: float = 0.0
    errors: list = field(default_factory=list)
    s3_key: str = ""


def get_timeout_for_title(title_num: int) -> int:
    """Get timeout in seconds based on title size category."""
    if title_num in LARGE_TITLES:
        return 300
    if title_num in MEDIUM_TITLES:
        return 120
    return 60


def get_title_metadata(title: int) -> Optional[dict]:
    """Get metadata for a title from eCFR API (cached)."""
    global _titles_cache

    if not _titles_cache:
        try:
            url = f"{ECFR_BASE_URL}/versioner/v1/titles.json"
            content, _ = fetch_with_retry(url, timeout=30)
            if content:
                data = json.loads(content)
                _titles_cache = {t["number"]: t for t in data.get("titles", [])}
        except Exception as e:
            print(f"Warning: Could not fetch titles metadata: {e}")

    return _titles_cache.get(title)


def handler(event, context):
    """
    Lambda handler. Expects event with:
    - title: CFR title number (1-50)
    - year: Year to fetch (0 for current)

    Returns FetchResult as dict with success/failure info.
    """
    title = event.get('title')
    year = event.get('year', 0)

    if not title:
        raise ValueError("Missing required 'title' parameter")

    start_time = time.time()
    timeout = get_timeout_for_title(title)

    # Skip reserved titles
    if title in RESERVED_TITLES:
        print(f"Title {title} is reserved, skipping")
        return FetchResult(
            title=title, year=year, success=True,
            source="reserved", sections_count=0,
            duration=time.time() - start_time
        ).__dict__

    print(f"Fetching title {title}, year {year} (timeout={timeout}s)")

    # Fetch XML from eCFR or govinfo
    xml_content, source, errors = fetch_title_xml(title, year)

    if not xml_content:
        duration = time.time() - start_time
        print(f"No content for title {title}, year {year} after {duration:.1f}s")
        result = FetchResult(
            title=title, year=year, success=False,
            source="none", sections_count=0,
            duration=duration, errors=[e.__dict__ for e in errors]
        )
        return result.__dict__

    try:
        # Extract sections - check for tmp_dir marker from subchapter fetching
        if xml_content.startswith(b"__TMP_DIR__:"):
            tmp_dir = xml_content.decode().split(":", 1)[1]
            # For very large titles, stream directly to S3
            sections_count, s3_key = extract_and_upload_from_tmp_dir(
                tmp_dir, title, year, S3_BUCKET, s3
            )
            duration = time.time() - start_time
            print(f"Wrote {sections_count} sections to s3://{S3_BUCKET}/{s3_key} in {duration:.1f}s")
            result = FetchResult(
                title=title, year=year, success=True,
                source=source, sections_count=sections_count,
                duration=duration, s3_key=s3_key
            )
            return result.__dict__

        # Normal path for smaller titles
        sections = extract_sections(xml_content, title, year)

        # Write to S3 as gzipped JSON (80%+ smaller)
        s3_key = f"sections/{year}/title-{title}.json.gz"
        json_bytes = json.dumps(sections, ensure_ascii=False).encode('utf-8')
        compressed = gzip.compress(json_bytes, compresslevel=6)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=compressed,
            ContentType='application/gzip'
        )

        duration = time.time() - start_time
        print(f"Wrote {len(sections)} sections to s3://{S3_BUCKET}/{s3_key} in {duration:.1f}s")

        result = FetchResult(
            title=title, year=year, success=True,
            source=source, sections_count=len(sections),
            duration=duration, s3_key=s3_key
        )
        return result.__dict__

    except Exception as e:
        duration = time.time() - start_time
        print(f"Error extracting/saving title {title}, year {year}: {e}")
        errors.append(FetchError(
            title=title, year=year, source=source,
            error_type="parse_error", message=str(e), attempt=0
        ))
        result = FetchResult(
            title=title, year=year, success=False,
            source=source, sections_count=0,
            duration=duration, errors=[e.__dict__ for e in errors]
        )
        return result.__dict__


def fetch_title_xml(title: int, year: int) -> tuple[Optional[bytes], str, list[FetchError]]:
    """
    Fetch title XML from eCFR or govinfo.

    Strategy:
    - Current year (0): Use eCFR with latest_issue_date
    - Recent year (current-1): Use eCFR (govinfo bulk not available yet)
    - Historical years: Try govinfo first (faster/reliable), fall back to eCFR

    Returns:
        Tuple of (xml_content, source_name, errors)
    """
    all_errors = []

    # Check if this title has limited availability
    availability = TITLE_AVAILABILITY.get(title, {})

    # For current data (year=0), use eCFR with latest_issue_date
    if year == 0:
        metadata = get_title_metadata(title)
        if metadata and metadata.get("latest_issue_date"):
            date_str = metadata["latest_issue_date"]
            print(f"  Using latest_issue_date: {date_str}")
            xml, errors = fetch_from_ecfr(title, date_str=date_str)
            all_errors.extend(errors)
            if xml:
                return xml, "ecfr", all_errors

    # For recent years (last year), try govinfo content API first, then eCFR
    # Govinfo bulk data isn't available, but content API has individual volumes
    if year == CURRENT_YEAR - 1 and year > 0:
        print(f"  Year {year} is recent, trying govinfo content API...")
        xml, errors = fetch_from_govinfo(title, year, use_content_api=True)
        all_errors.extend(errors)
        if xml:
            return xml, "govinfo", all_errors

        # Fall back to eCFR
        print(f"  Falling back to eCFR for year {year}...")
        ecfr_min_date = availability.get("ecfr_min_date")
        if ecfr_min_date and f"{year}-01-01" < ecfr_min_date:
            print(f"  Title {title} not available on eCFR before {ecfr_min_date}")
            return None, "", all_errors

        xml, errors = fetch_from_ecfr(title, date_str=f"{year}-01-01")
        all_errors.extend(errors)
        if xml:
            return xml, "ecfr", all_errors
        return None, "", all_errors

    # For historical years, try govinfo first (faster and more reliable)
    if year > 0:
        # Check if title is available on govinfo for this year
        govinfo_min = availability.get("govinfo_min_year", 0)
        if year >= govinfo_min:
            xml, errors = fetch_from_govinfo(title, year)
            all_errors.extend(errors)
            if xml:
                return xml, "govinfo", all_errors
        else:
            print(f"  Title {title} not available on govinfo before {govinfo_min}")

    # Fall back to eCFR for historical
    if year > 0:
        ecfr_min_date = availability.get("ecfr_min_date")
        if ecfr_min_date and f"{year}-01-01" < ecfr_min_date:
            print(f"  Title {title} not available before {ecfr_min_date}")
            return None, "", all_errors

        xml, errors = fetch_from_ecfr(title, date_str=f"{year}-01-01")
        all_errors.extend(errors)
        if xml:
            return xml, "ecfr", all_errors

    return None, "", all_errors


def fetch_from_ecfr(title: int, date_str: str = None) -> tuple[Optional[bytes], list[FetchError]]:
    """Fetch from eCFR API using the specified date."""
    if not date_str:
        return None, []

    # For very large titles, fetch by subchapter to avoid timeouts
    if title in VERY_LARGE_TITLES:
        return fetch_from_ecfr_by_subchapter(title, date_str)

    url = f"{ECFR_BASE_URL}/versioner/v1/full/{date_str}/title-{title}.xml"
    timeout = get_timeout_for_title(title)
    print(f"  Fetching from eCFR (timeout={timeout}s)...")

    return fetch_with_retry(url, timeout=timeout)


def fetch_from_ecfr_by_subchapter(title: int, date_str: str) -> tuple[Optional[bytes], list[FetchError]]:
    """
    Fetch a very large title by fetching each subchapter separately.
    Uses /tmp storage to avoid memory issues with 800+ MB titles.
    Returns a special marker that tells the handler to use the temp files.
    """
    all_errors = []
    tmp_dir = f"/tmp/title_{title}_{date_str.replace('-', '')}"

    # Clean up any previous attempt
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    # First, get the title structure to find all subchapters
    structure_url = f"{ECFR_BASE_URL}/versioner/v1/structure/{date_str}/title-{title}.json"
    print(f"  Fetching title {title} structure...")

    try:
        req = urllib.request.Request(structure_url, headers={
            'User-Agent': 'eCFR-Lambda/1.0',
            'Accept': 'application/json'
        })
        with urllib.request.urlopen(req, timeout=30) as response:
            structure = json.loads(response.read())
    except Exception as e:
        all_errors.append(FetchError(
            title=title, year=0, source="ecfr",
            error_type="structure_error",
            message=f"Failed to get structure: {e}",
            attempt=0
        ))
        return None, all_errors

    # Extract all subchapters from the structure
    def get_subchapters(node):
        results = []
        for child in node.get('children', []):
            if child.get('type') == 'subchapter':
                results.append(child.get('identifier', ''))
            else:
                results.extend(get_subchapters(child))
        return results

    subchapters = get_subchapters(structure)
    if not subchapters:
        # No subchapters found, fall back to chapters
        for child in structure.get('children', []):
            if child.get('type') == 'chapter':
                subchapters.append(f"chapter_{child.get('identifier', '')}")

    print(f"  Found {len(subchapters)} subchapters, fetching to /tmp...")

    # Fetch each subchapter and save to /tmp
    successful_files = []
    timeout = 120  # 2 minutes per subchapter

    for i, subchapter in enumerate(subchapters):
        if subchapter.startswith('chapter_'):
            chapter_id = subchapter.replace('chapter_', '')
            url = f"{ECFR_BASE_URL}/versioner/v1/full/{date_str}/title-{title}.xml?chapter={chapter_id}"
            filename = f"chapter_{chapter_id}.xml"
        else:
            url = f"{ECFR_BASE_URL}/versioner/v1/full/{date_str}/title-{title}.xml?subchapter={subchapter}"
            filename = f"subchapter_{subchapter}.xml"

        filepath = os.path.join(tmp_dir, filename)
        content, errors = fetch_with_retry(url, retries=2, timeout=timeout)
        all_errors.extend(errors)

        if content:
            # Write to /tmp file immediately, free memory
            with open(filepath, 'wb') as f:
                f.write(content)
            size_mb = len(content) / 1024 / 1024
            successful_files.append(filepath)
            print(f"    [{i+1}/{len(subchapters)}] {subchapter}: {size_mb:.1f} MB -> {filename}")
            del content  # Free memory
        else:
            print(f"    [{i+1}/{len(subchapters)}] {subchapter}: FAILED")

    if not successful_files:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, all_errors

    # Return marker with tmp_dir path - handler will process files
    print(f"  Saved {len(successful_files)} subchapters to {tmp_dir}")
    return f"__TMP_DIR__:{tmp_dir}".encode(), all_errors


def extract_and_upload_from_tmp_dir(tmp_dir: str, title: int, year: int, bucket: str, s3_client) -> tuple:
    """
    Extract sections from XML files and upload directly to S3.
    Streams sections to a gzipped file to minimize memory usage.
    Returns (sections_count, s3_key).
    """
    xml_files = sorted(glob.glob(os.path.join(tmp_dir, "*.xml")))
    print(f"  Extracting sections from {len(xml_files)} files...")

    # Write sections to a gzipped JSON file
    output_path = os.path.join(tmp_dir, "sections.json.gz")
    total_sections = 0

    with gzip.open(output_path, 'wt', encoding='utf-8') as gz:
        gz.write('[\n')  # Start JSON array
        first = True

        for i, filepath in enumerate(xml_files):
            # Read and process one XML file at a time
            with open(filepath, 'rb') as f:
                xml_content = f.read()

            sections = extract_sections(xml_content, title, year)

            # Write sections to gzipped file
            for section in sections:
                if not first:
                    gz.write(',\n')
                first = False
                gz.write(json.dumps(section, ensure_ascii=False))
                total_sections += 1

            # Free memory and delete XML file
            del xml_content
            del sections
            os.remove(filepath)

            if (i + 1) % 5 == 0:
                print(f"    Processed {i+1}/{len(xml_files)} files ({total_sections} sections)")

        gz.write('\n]')  # End JSON array

    # Upload to S3
    s3_key = f"sections/{year}/title-{title}.json.gz"
    print(f"  Uploading {os.path.getsize(output_path)/1024/1024:.1f} MB to s3://{bucket}/{s3_key}")

    with open(output_path, 'rb') as f:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=f,
            ContentType='application/gzip'
        )

    # Clean up
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return total_sections, s3_key


def extract_sections_from_tmp_dir(tmp_dir: str, title: int, year: int) -> list:
    """
    Extract sections from XML files in a temp directory, one at a time.
    Writes sections to JSON files incrementally to minimize memory usage.
    """
    xml_files = sorted(glob.glob(os.path.join(tmp_dir, "*.xml")))
    print(f"  Extracting sections from {len(xml_files)} files...")

    sections_dir = os.path.join(tmp_dir, "sections")
    os.makedirs(sections_dir, exist_ok=True)

    total_sections = 0

    for i, filepath in enumerate(xml_files):
        # Read and process one XML file at a time
        with open(filepath, 'rb') as f:
            xml_content = f.read()

        sections = extract_sections(xml_content, title, year)
        total_sections += len(sections)

        # Write sections to JSON file immediately
        if sections:
            json_path = os.path.join(sections_dir, f"part_{i:03d}.json")
            with open(json_path, 'w') as f:
                json.dump(sections, f)

        # Free memory and delete XML file
        del xml_content
        del sections
        os.remove(filepath)

        if (i + 1) % 5 == 0:
            print(f"    Processed {i+1}/{len(xml_files)} files ({total_sections} sections)")

    # Now combine all section JSON files
    print(f"  Combining {total_sections} sections from temp files...")
    all_sections = []
    json_files = sorted(glob.glob(os.path.join(sections_dir, "*.json")))

    for json_path in json_files:
        with open(json_path, 'r') as f:
            sections = json.load(f)
            all_sections.extend(sections)
        os.remove(json_path)

    # Clean up tmp directory
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return all_sections


def fetch_from_govinfo(title: int, year: int, use_content_api: bool = False) -> tuple[Optional[bytes], list[FetchError]]:
    """
    Fetch from govinfo for historical years.

    Args:
        title: CFR title number
        year: Year to fetch
        use_content_api: If True, use content API (for recent years like 2025).
                        If False, use bulk data API (for older historical years).

    For multi-volume titles, saves each volume to /tmp and returns a marker
    so the handler can process them separately (avoids invalid XML from concatenation).
    """
    if year <= 0:
        return None, []  # govinfo doesn't work for current data

    timeout = get_timeout_for_title(title)
    api_type = "content" if use_content_api else "bulk"
    print(f"  Fetching from govinfo {api_type} API (timeout={timeout}s)...")

    # Govinfo has multiple volumes per title, try to find them
    all_errors = []
    tmp_dir = f"/tmp/title_{title}_{year}_govinfo"

    # Clean up any previous attempt
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    volumes_found = 0

    for vol in range(1, 40):  # Some titles have many volumes (Title 40 has 37+)
        if use_content_api:
            # Content API URL format: /content/pkg/CFR-{year}-title{N}-vol{V}/xml/CFR-{year}-title{N}-vol{V}.xml
            pkg_name = f"CFR-{year}-title{title}-vol{vol}"
            url = f"{GOVINFO_CONTENT_URL}/{pkg_name}/xml/{pkg_name}.xml"
        else:
            # Bulk data URL format: /bulkdata/CFR/{year}/title-{N}/CFR-{year}-title{N}-vol{V}.xml
            url = f"{GOVINFO_BULK_URL}/{year}/title-{title}/CFR-{year}-title{title}-vol{vol}.xml"

        content, errors = fetch_with_retry(url, retries=1, timeout=timeout)
        all_errors.extend(errors)

        if content:
            # Save to tmp for multi-volume processing
            os.makedirs(tmp_dir, exist_ok=True)
            filepath = os.path.join(tmp_dir, f"vol_{vol:02d}.xml")
            with open(filepath, 'wb') as f:
                f.write(content)
            volumes_found += 1
            del content  # Free memory
        else:
            # Check if it's a 404 (no more volumes) vs error
            if errors and errors[-1].error_type == "not_found":
                break  # No more volumes
            elif vol == 1:
                # First volume failed with non-404 error
                break

    if volumes_found == 0:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        return None, all_errors

    print(f"  Found {volumes_found} volume(s)")

    # Single volume: return content directly
    if volumes_found == 1:
        filepath = os.path.join(tmp_dir, "vol_01.xml")
        with open(filepath, 'rb') as f:
            content = f.read()
        shutil.rmtree(tmp_dir)
        return content, all_errors

    # Multiple volumes: return tmp_dir marker for separate processing
    return f"__TMP_DIR__:{tmp_dir}".encode(), all_errors


def fetch_with_retry(url: str, retries: int = 3, timeout: int = 60) -> tuple[Optional[bytes], list[FetchError]]:
    """
    Fetch URL with exponential backoff retry.

    Returns:
        Tuple of (content bytes or None, list of errors encountered)
    """
    errors = []
    base_delay = 5

    for attempt in range(retries):
        if attempt > 0:
            delay = base_delay * (2 ** (attempt - 1))  # 5s, 10s
            print(f"  Retry {attempt + 1}/{retries} in {delay}s...")
            time.sleep(delay)

        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'eCFR-Fetcher/1.0',
                'Accept': 'application/xml'
            })

            with urllib.request.urlopen(req, timeout=timeout) as response:
                # Check for redirects to error pages (govinfo returns 302 for missing content)
                if response.url != url and 'error' in response.url.lower():
                    errors.append(FetchError(
                        title=0, year=0, source="http",
                        error_type="not_found",
                        message=f"Redirected to error page: {response.url}",
                        attempt=attempt
                    ))
                    return None, errors

                if response.status == 200:
                    content = response.read()
                    # Verify we got XML, not an HTML error page (only for .xml URLs)
                    if url.endswith('.xml') and content and not content.strip().startswith(b'<'):
                        errors.append(FetchError(
                            title=0, year=0, source="http",
                            error_type="invalid_content",
                            message="Response is not XML",
                            attempt=attempt
                        ))
                        return None, errors
                    return content, errors

        except urllib.error.HTTPError as e:
            if e.code in (404, 302):
                errors.append(FetchError(
                    title=0, year=0, source="http",
                    error_type="not_found",
                    message=f"HTTP 404: {url}",
                    attempt=attempt
                ))
                return None, errors  # Not found, don't retry
            print(f"HTTP error {e.code} for {url}, attempt {attempt + 1}")
            errors.append(FetchError(
                title=0, year=0, source="http",
                error_type="http_error",
                message=f"HTTP {e.code}: {url}",
                attempt=attempt
            ))

        except urllib.error.URLError as e:
            if "timed out" in str(e).lower() or isinstance(e.reason, TimeoutError):
                print(f"Timeout for {url}, attempt {attempt + 1}")
                errors.append(FetchError(
                    title=0, year=0, source="http",
                    error_type="timeout",
                    message=f"Timeout after {timeout}s: {url}",
                    attempt=attempt
                ))
            else:
                print(f"URL error for {url}: {e}, attempt {attempt + 1}")
                errors.append(FetchError(
                    title=0, year=0, source="http",
                    error_type="url_error",
                    message=f"URL error: {e}",
                    attempt=attempt
                ))

        except TimeoutError:
            print(f"Timeout for {url}, attempt {attempt + 1}")
            errors.append(FetchError(
                title=0, year=0, source="http",
                error_type="timeout",
                message=f"Timeout after {timeout}s: {url}",
                attempt=attempt
            ))

        except Exception as e:
            print(f"Error fetching {url}: {e}, attempt {attempt + 1}")
            errors.append(FetchError(
                title=0, year=0, source="http",
                error_type="error",
                message=str(e),
                attempt=attempt
            ))

    return None, errors


def extract_sections(xml_content: bytes, title: int, year: int) -> list:
    """Extract sections from XML content."""
    sections = []

    try:
        # Try parsing as XML
        root = ET.fromstring(xml_content)

        # Find all section elements
        for section in root.iter():
            if section.tag.endswith('SECTION') or section.tag == 'SECTION':
                section_data = parse_section(section, title, year)
                if section_data:
                    sections.append(section_data)

        # Also try DIV8 elements (eCFR format)
        for div in root.iter():
            if div.tag == 'DIV8' and div.get('TYPE') == 'SECTION':
                section_data = parse_div8_section(div, title, year)
                if section_data:
                    sections.append(section_data)

    except ET.ParseError as e:
        print(f"XML parse error: {e}")
        # Try to extract what we can
        sections = extract_sections_regex(xml_content, title, year)

    return sections


def parse_section(element: ET.Element, title: int, year: int) -> Optional[dict]:
    """Parse a SECTION element."""
    # Get section number
    sectno = element.find('.//SECTNO')
    if sectno is None or not sectno.text:
        return None

    section_num = sectno.text.strip()

    # Get section subject/heading
    subject = element.find('.//SUBJECT')
    heading = subject.text.strip() if subject is not None and subject.text else ""

    # Get full text content
    text_parts = []
    for p in element.iter():
        if p.text:
            text_parts.append(p.text.strip())
        if p.tail:
            text_parts.append(p.tail.strip())

    text = ' '.join(text_parts)
    word_count = len(text.split())

    return {
        'title': title,
        'year': year,
        'section': section_num,
        'heading': heading,
        'text': text,
        'text_hash': _hash_text(text) if text else '',
        'word_count': word_count
    }


def parse_div8_section(element: ET.Element, title: int, year: int) -> Optional[dict]:
    """Parse a DIV8 SECTION element (eCFR format)."""
    # Get section number from HEAD
    head = element.find('HEAD')
    if head is None or not head.text:
        return None

    section_num = head.text.strip()

    # Get full text
    text_parts = []
    for child in element:
        if child.tag != 'HEAD':
            text = ET.tostring(child, encoding='unicode', method='text')
            text_parts.append(text.strip())

    text = ' '.join(text_parts)
    word_count = len(text.split())

    return {
        'title': title,
        'year': year,
        'section': section_num,
        'heading': section_num,
        'text': text,
        'text_hash': _hash_text(text) if text else '',
        'word_count': word_count
    }


def extract_sections_regex(xml_content: bytes, title: int, year: int) -> list:
    """Fallback regex-based section extraction."""
    import re

    sections = []
    content = xml_content.decode('utf-8', errors='ignore')

    # Simple pattern to find section numbers
    pattern = r'§\s*(\d+\.\d+)'

    for match in re.finditer(pattern, content):
        sections.append({
            'title': title,
            'year': year,
            'section': f"§ {match.group(1)}",
            'heading': '',
            'text': '',
            'text_hash': '',
            'word_count': 0
        })

    return sections


def print_fetch_summary(results: list[dict]) -> None:
    """Print final summary of fetch results."""
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]
    total_sections = sum(r.get('sections_count', 0) for r in successful)
    total_duration = sum(r.get('duration', 0) for r in results)

    print(f"\n{'='*60}")
    print(f"FETCH SUMMARY")
    print(f"{'='*60}")
    print(f"Titles: {len(successful)}/{len(results)} succeeded")
    print(f"Sections: {total_sections:,}")
    print(f"Duration: {total_duration:.1f}s")

    if failed:
        print(f"\nFailed titles:")
        for r in failed:
            errors = r.get('errors', [])
            last_error = errors[-1] if errors else None
            msg = last_error.get('message', 'Unknown') if last_error else "Unknown"
            print(f"  Title {r['title']}: {msg}")

    print(f"{'='*60}\n")


def batch_handler(event, context):
    """
    Batch handler for fetching multiple titles with optimal parallelism.

    Uses a worker pool that matches the Lambda concurrency limit (default 10).
    Prioritizes large titles first so they start early and smaller titles
    fill in as workers become available.

    Expects event with:
    - titles: List of title numbers (default: 1-50)
    - year: Year to fetch (0 for current)
    - max_retries: Max retries per title (default: 3)
    - max_workers: Concurrent fetches (default: 10, matches typical Lambda limit)

    Returns summary of all fetch results.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles = event.get('titles', list(range(1, 51)))
    year = event.get('year', 0)
    max_retries = event.get('max_retries', 3)
    max_workers = event.get('max_workers', 10)  # Match Lambda concurrency limit

    # Sort titles by expected duration (large first, then medium, then small)
    # This ensures slow titles start immediately and fast titles fill gaps
    def title_priority(t):
        if t in RESERVED_TITLES:
            return 3  # Process last (instant)
        if t in LARGE_TITLES:
            return 0  # Process first (slowest)
        if t in MEDIUM_TITLES:
            return 1  # Process second
        return 2  # Small titles last (fastest)

    sorted_titles = sorted(titles, key=title_priority)

    results = []
    start_time = time.time()
    completed_count = 0

    print(f"Starting optimized batch fetch of {len(titles)} titles for year {year}")
    print(f"Worker pool size: {max_workers} (matches Lambda concurrency limit)")
    print(f"Processing order: large titles first, then medium, then small")
    print(f"Large titles: {sorted(LARGE_TITLES)}")
    print(f"Medium titles: {sorted(MEDIUM_TITLES)}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_title = {
            executor.submit(fetch_title_with_retry, title, year, max_retries): title
            for title in sorted_titles
        }

        for future in as_completed(future_to_title):
            title = future_to_title[future]
            completed_count += 1
            try:
                result = future.result()
                results.append(result)
                # Progress indicator
                elapsed = time.time() - start_time
                print(f"  [{completed_count}/{len(titles)}] Title {title} done "
                      f"({result.get('sections_count', 0)} sections, {elapsed:.1f}s elapsed)")
            except Exception as e:
                print(f"  [{completed_count}/{len(titles)}] Title {title} FAILED: {e}")
                results.append(FetchResult(
                    title=title, year=year, success=False,
                    source="exception", errors=[{"message": str(e)}]
                ).__dict__)

    # Sort results by title number for consistent output
    results.sort(key=lambda r: r['title'])

    total_duration = time.time() - start_time

    print_fetch_summary(results)

    return {
        'year': year,
        'total_titles': len(results),
        'successful': len([r for r in results if r.get('success')]),
        'failed': len([r for r in results if not r.get('success')]),
        'total_sections': sum(r.get('sections_count', 0) for r in results if r.get('success')),
        'total_duration': total_duration,
        'results': results
    }


def fetch_title_with_retry(title: int, year: int, max_attempts: int = 3) -> dict:
    """
    Fetch a single title with retry logic.

    This wraps the handler function with additional retry attempts
    at the title level (not just HTTP level).
    """
    errors = []
    base_delay = 10
    start_time = time.time()

    # Skip reserved titles
    if title in RESERVED_TITLES:
        print(f"Title {title} is reserved, skipping")
        return FetchResult(
            title=title, year=year, success=True,
            source="reserved", sections_count=0,
            duration=time.time() - start_time
        ).__dict__

    for attempt in range(max_attempts):
        if attempt > 0:
            delay = base_delay * (2 ** (attempt - 1))  # 10s, 20s
            print(f"  Retry {attempt + 1}/{max_attempts} for title {title} in {delay}s...")
            time.sleep(delay)

        try:
            result = handler({'title': title, 'year': year}, None)

            # If successful, return immediately
            if result.get('success'):
                return result

            # Collect errors from failed attempt
            errors.extend(result.get('errors', []))

        except Exception as e:
            print(f"  Exception for title {title}: {e}")
            errors.append(FetchError(
                title=title, year=year, source="handler",
                error_type="exception", message=str(e), attempt=attempt
            ).__dict__)

    # All retries exhausted
    duration = time.time() - start_time
    print(f"  Failed title {title} after {max_attempts} attempts ({duration:.1f}s)")

    return FetchResult(
        title=title, year=year, success=False,
        source="none", sections_count=0,
        duration=duration, errors=errors
    ).__dict__


if __name__ == "__main__":
    """Local testing entry point."""
    import sys

    # Parse command line args
    year = 0
    titles = list(range(1, 51))

    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--year" and i + 2 < len(sys.argv):
            year = int(sys.argv[i + 2])
        elif arg == "--title" and i + 2 < len(sys.argv):
            titles = [int(sys.argv[i + 2])]
        elif arg == "--titles" and i + 2 < len(sys.argv):
            titles = [int(t) for t in sys.argv[i + 2].split(",")]

    # Run batch fetch
    result = batch_handler({'titles': titles, 'year': year}, None)

    # Exit with error code if any failed
    if result['failed'] > 0:
        sys.exit(1)
