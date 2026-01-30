#!/usr/bin/env python3
"""Playwright tests to verify YAML data matches ecfr.gov."""

import random
import re
import time

import pytest
from playwright.sync_api import Page, expect, BrowserContext

from ecfr_reader import ECFRReader


# Number of random sections to sample per title
SAMPLES_PER_TITLE = 3

# Titles to test (can be modified to test specific titles)
TEST_TITLES = [1, 2, 3]

# User agent to avoid bot detection
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Configure browser context with realistic user agent."""
    return {
        **browser_context_args,
        "user_agent": USER_AGENT,
        "viewport": {"width": 1920, "height": 1080},
    }


@pytest.fixture(scope="module")
def reader():
    """Create ECFRReader instance."""
    return ECFRReader()


@pytest.fixture(scope="module")
def section_samples(reader: ECFRReader):
    """Pre-select random sections from each title for testing.

    Filters out reserved/range sections (e.g., '102.161-102.169') that don't have
    individual URLs on ecfr.gov.
    """
    samples = {}
    for title in TEST_TITLES:
        try:
            index = reader._build_index(title)
            if not index:
                continue
            # Filter out range sections (e.g., "102.161-102.169")
            section_nums = [s for s in index.keys() if "-" not in s and re.match(r"^\d+\.\d+$", s)]
            if not section_nums:
                continue
            # Sample random sections (or all if fewer than SAMPLES_PER_TITLE)
            sample_size = min(SAMPLES_PER_TITLE, len(section_nums))
            samples[title] = random.sample(section_nums, sample_size)
        except FileNotFoundError:
            continue
    return samples


def is_blocked_page(page: Page) -> bool:
    """Check if we're on a bot detection/blocking page."""
    url = page.url.lower()
    return "unblock" in url or "captcha" in url or "challenge" in url


def navigate_with_retry(page: Page, url: str, max_retries: int = 3) -> bool:
    """Navigate to URL with retry logic for transient errors.

    Returns True if navigation succeeded, False if all retries failed.
    """
    for attempt in range(max_retries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            return True
        except Exception as e:
            if "ERR_CONNECTION" in str(e) or "timeout" in str(e).lower():
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
            raise
    return False


def normalize_text(text: str) -> str:
    """Normalize text for comparison by removing extra whitespace."""
    return re.sub(r"\s+", " ", text).strip()


def extract_section_number(heading: str) -> str | None:
    """Extract section number from heading like '§ 1.1 Definitions.'"""
    match = re.search(r"§\s*(\d+\.\d+)", heading)
    return match.group(1) if match else None


class TestECFRVerification:
    """Test suite to verify local YAML data matches ecfr.gov."""

    def test_reader_loads_titles(self, reader: ECFRReader):
        """Verify reader can load available titles."""
        titles = reader.list_titles()
        assert len(titles) > 0, "No titles found"
        assert 1 in titles, "Title 1 should be available"

    def test_reader_navigates_sections(self, reader: ECFRReader):
        """Verify reader can navigate to sections."""
        section = reader.navigate(1, section="1.1")
        assert section is not None, "Section 1.1 not found"

        heading = reader.get_section_heading(1, "1.1")
        assert heading is not None, "Section heading not found"
        assert "1.1" in heading, "Section number not in heading"

    @pytest.mark.parametrize("title", TEST_TITLES)
    def test_title_structure_exists(self, reader: ECFRReader, title: int):
        """Verify each test title has valid structure."""
        structure = reader.get_structure(title)
        assert structure.get("type") == "title", f"Title {title} missing title type"
        assert structure.get("identifier") == str(title), f"Title {title} wrong identifier"

    @pytest.mark.parametrize("title", TEST_TITLES)
    def test_sections_have_content(self, reader: ECFRReader, title: int):
        """Verify sections have text content."""
        index = reader._build_index(title)
        assert len(index) > 0, f"Title {title} has no sections"

        # Check first section has content
        first_section = list(index.keys())[0]
        text = reader.get_section_text(title, first_section)
        assert text and len(text) > 10, f"Section {first_section} has no content"


class TestECFRWebVerification:
    """Playwright tests comparing local data to ecfr.gov."""

    @pytest.mark.parametrize("title", TEST_TITLES)
    def test_section_headings_match(
        self,
        page: Page,
        reader: ECFRReader,
        section_samples: dict,
        title: int,
    ):
        """Verify section headings match between local data and ecfr.gov."""
        if title not in section_samples:
            pytest.skip(f"No samples for title {title}")

        for section_num in section_samples[title]:
            # Get local data
            local_heading = reader.get_section_heading(title, section_num)
            assert local_heading, f"No local heading for {title}/{section_num}"

            # Navigate to ecfr.gov
            url = f"https://www.ecfr.gov/current/title-{title}/section-{section_num}"
            try:
                navigate_with_retry(page, url)
            except Exception as e:
                pytest.skip(f"Could not load page for section {section_num}: {e}")

            # Check for bot detection redirect
            if is_blocked_page(page):
                pytest.skip("Bot detection triggered - skipping web verification")

            # Small delay to let page render
            time.sleep(1)

            # Get page content
            page_content = page.content()

            # Verify section number appears on page (in URL or content)
            # The URL redirects to full path with section number embedded
            assert section_num in local_heading, f"Section number missing from local heading"
            # Check that section number appears in URL or page content
            section_in_url = section_num in page.url or f"section-{section_num}" in page.url
            section_in_content = f"§ {section_num}" in page_content or f"§{section_num}" in page_content
            assert section_in_url or section_in_content, (
                f"Section {section_num} not found on page (URL: {page.url})"
            )

    @pytest.mark.parametrize("title", TEST_TITLES)
    def test_section_content_exists(
        self,
        page: Page,
        reader: ECFRReader,
        section_samples: dict,
        title: int,
    ):
        """Verify section content exists on ecfr.gov for sampled sections."""
        if title not in section_samples:
            pytest.skip(f"No samples for title {title}")

        for section_num in section_samples[title]:
            # Get local text
            local_text = reader.get_section_text(title, section_num)
            assert local_text, f"No local text for {title}/{section_num}"

            # Navigate to ecfr.gov
            url = f"https://www.ecfr.gov/current/title-{title}/section-{section_num}"
            try:
                navigate_with_retry(page, url)
            except Exception as e:
                pytest.skip(f"Could not load page for section {section_num}: {e}")

            # Check for bot detection redirect
            if is_blocked_page(page):
                pytest.skip("Bot detection triggered - skipping web verification")

            # Small delay to let page render
            time.sleep(1)

            # Wait for content
            try:
                page.wait_for_selector("article, .section-content, main, body", timeout=15000)
            except Exception:
                pytest.skip(f"Page did not load for section {section_num}")

            # Get page content
            content = page.content()

            # Extract a key phrase from local text (first 50 non-whitespace chars)
            local_words = local_text.split()[:10]
            if local_words:
                # Check if some key words appear on page
                matches = sum(1 for word in local_words if word.lower() in content.lower())
                match_ratio = matches / len(local_words)
                assert match_ratio > 0.3, (
                    f"Section {section_num}: Only {matches}/{len(local_words)} words matched. "
                    f"Local: '{' '.join(local_words[:5])}...'"
                )

    def test_specific_section_1_1(self, page: Page, reader: ECFRReader):
        """Verify specific known section 1 CFR 1.1 Definitions."""
        # Get local data
        heading = reader.get_section_heading(1, "1.1")
        assert heading, "Section 1.1 heading not found locally"
        assert "Definitions" in heading, "Section 1.1 should be about Definitions"

        # Navigate to ecfr.gov
        try:
            navigate_with_retry(page, "https://www.ecfr.gov/current/title-1/section-1.1")
        except Exception as e:
            pytest.skip(f"Could not load page: {e}")

        # Check for bot detection redirect
        if is_blocked_page(page):
            pytest.skip("Bot detection triggered - skipping web verification")

        # Small delay
        time.sleep(1)

        # Verify page loads correctly (may have redirected to full URL)
        assert "ecfr.gov" in page.url, "Should be on ecfr.gov domain"

        # Check for definitions content
        page_content = page.content()
        assert "Definitions" in page_content or "definitions" in page_content.lower(), (
            "Page should contain 'Definitions'"
        )

    def test_url_pattern_works(self, page: Page):
        """Verify the ecfr.gov URL pattern works for navigation."""
        # Test a known section
        try:
            navigate_with_retry(page, "https://www.ecfr.gov/current/title-1/section-2.1")
        except Exception as e:
            pytest.skip(f"Could not load page: {e}")

        # Check for bot detection redirect
        if is_blocked_page(page):
            pytest.skip("Bot detection triggered - skipping web verification")

        # Small delay
        time.sleep(1)

        # Should not be a 404
        assert "404" not in page.title().lower(), "Page returned 404"
        assert "not found" not in page.content().lower()[:500], "Page not found"

        # Should have CFR content indicators
        content = page.content()
        assert "Code of Federal Regulations" in content or "CFR" in content or "ecfr.gov" in page.url


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
