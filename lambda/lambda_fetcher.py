"""
Lambda function to fetch a single CFR title for a given year.
Writes extracted sections as JSON to S3.
"""

import json
import os
import boto3
import urllib.request
import urllib.error
from xml.etree import ElementTree as ET
from typing import Optional
import time

s3 = boto3.client('s3')
S3_BUCKET = os.environ.get('S3_BUCKET')

# API endpoints
ECFR_BASE_URL = "https://www.ecfr.gov/api"
GOVINFO_CFR_URL = "https://www.govinfo.gov/bulkdata/CFR"

# Cache for titles metadata (reused across invocations in same container)
_titles_cache = {}


def get_title_metadata(title: int) -> Optional[dict]:
    """Get metadata for a title from eCFR API (cached)."""
    global _titles_cache

    if not _titles_cache:
        try:
            url = f"{ECFR_BASE_URL}/versioner/v1/titles.json"
            content = fetch_with_retry(url)
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
    """
    title = event.get('title')
    year = event.get('year', 0)

    if not title:
        raise ValueError("Missing required 'title' parameter")

    print(f"Fetching title {title}, year {year}")

    try:
        # Fetch XML from eCFR or govinfo
        xml_content = fetch_title_xml(title, year)

        if not xml_content:
            print(f"No content for title {title}, year {year}")
            return {
                'title': title,
                'year': year,
                'status': 'empty',
                'sections': 0
            }

        # Extract sections
        sections = extract_sections(xml_content, title, year)

        # Write to S3
        s3_key = f"sections/{year}/title-{title}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(sections, ensure_ascii=False),
            ContentType='application/json'
        )

        print(f"Wrote {len(sections)} sections to s3://{S3_BUCKET}/{s3_key}")

        return {
            'title': title,
            'year': year,
            'status': 'success',
            'sections': len(sections),
            's3_key': s3_key
        }

    except Exception as e:
        print(f"Error fetching title {title}, year {year}: {e}")
        raise


def fetch_title_xml(title: int, year: int) -> Optional[bytes]:
    """Fetch title XML from eCFR or govinfo."""

    # For current data (year=0), get the latest_issue_date from metadata
    if year == 0:
        metadata = get_title_metadata(title)
        if metadata and metadata.get("latest_issue_date"):
            date_str = metadata["latest_issue_date"]
            print(f"  Using latest_issue_date: {date_str}")
            xml = fetch_from_ecfr(title, date_str=date_str)
            if xml:
                return xml

    # Try govinfo for historical data
    if year > 0:
        xml = fetch_from_govinfo(title, year)
        if xml:
            return xml

    # Try eCFR as fallback for historical
    if year > 0:
        xml = fetch_from_ecfr(title, date_str=f"{year}-01-01")
        if xml:
            return xml

    return None


def fetch_from_ecfr(title: int, date_str: str = None) -> Optional[bytes]:
    """Fetch from eCFR API using the specified date."""
    if not date_str:
        return None

    url = f"{ECFR_BASE_URL}/versioner/v1/full/{date_str}/title-{title}.xml"

    return fetch_with_retry(url)


def fetch_from_govinfo(title: int, year: int) -> Optional[bytes]:
    """Fetch from govinfo bulk data for historical years."""
    if year <= 0:
        return None  # govinfo doesn't work for current data

    # Govinfo has multiple volumes per title, try to find them
    volumes_content = []

    for vol in range(1, 20):  # Most titles have fewer than 20 volumes
        url = f"{GOVINFO_CFR_URL}/{year}/title-{title}/CFR-{year}-title{title}-vol{vol}.xml"
        content = fetch_with_retry(url, retries=1)

        if content:
            volumes_content.append(content)
        else:
            break  # No more volumes

    if not volumes_content:
        return None

    # For simplicity, return first volume (consolidator will merge)
    return volumes_content[0] if len(volumes_content) == 1 else merge_volumes(volumes_content)


def merge_volumes(volumes: list) -> bytes:
    """Merge multiple volume XMLs into one."""
    # Simple approach: return all volumes concatenated
    # The section extractor will handle duplicates
    return b'\n'.join(volumes)


def fetch_with_retry(url: str, retries: int = 3) -> Optional[bytes]:
    """Fetch URL with exponential backoff retry."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'eCFR-Fetcher/1.0',
                'Accept': 'application/xml'
            })

            with urllib.request.urlopen(req, timeout=120) as response:
                if response.status == 200:
                    return response.read()

        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # Not found, don't retry
            print(f"HTTP error {e.code} for {url}, attempt {attempt + 1}")

        except urllib.error.URLError as e:
            print(f"URL error for {url}: {e}, attempt {attempt + 1}")

        except Exception as e:
            print(f"Error fetching {url}: {e}, attempt {attempt + 1}")

        if attempt < retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff

    return None


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
            'word_count': 0
        })

    return sections
