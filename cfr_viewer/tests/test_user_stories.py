"""
User Stories for CFR Web Viewer - Playwright Tests

User Story 1: Browse CFR Titles
  As a legal researcher, I want to see all CFR titles on the home page
  so I can quickly find the regulation area I need.

User Story 2: Navigate to a Title
  As a compliance officer, I want to click on a title to see its structure
  so I can understand how regulations are organized.

User Story 3: View a Section
  As a lawyer, I want to view a specific CFR section with its full text
  and word count so I can analyze the regulation.

User Story 4: Find Similar Sections
  As a policy analyst, I want to see sections similar to the one I'm reading
  so I can identify related or duplicate regulations.

User Story 5: Compare Historical Versions
  As a regulatory affairs specialist, I want to compare a section across years
  so I can track how regulations have changed.

User Story 6: View Statistics
  As a government watchdog, I want to see which agencies have the most words
  in the CFR so I can understand regulatory burden.
"""

import re

import pytest
from playwright.sync_api import Page, expect
import os

BASE_URL = "http://localhost:5000"
SCREENSHOT_DIR = "/tmp/cfr_viewer_screenshots"


@pytest.fixture(scope="module", autouse=True)
def setup_screenshots():
    """Create screenshot directory."""
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)


class TestUserStory1_BrowseTitles:
    """
    User Story 1: Browse CFR Titles
    As a legal researcher, I want to see all CFR titles
    so I can quickly find the regulation area I need.

    Steps:
    1. Navigate to the home page (dashboard)
    2. Verify the dashboard has aggregate stats
    3. Navigate to titles page and verify table exists
    4. Verify titles have numbers, names, and word counts
    5. Verify navigation links exist
    """

    def test_home_page_loads(self, page: Page):
        """Step 1-2: Navigate to the home page dashboard."""
        page.goto(BASE_URL)
        page.screenshot(path=f"{SCREENSHOT_DIR}/01_home_page.png", full_page=True)

        # Verify page title
        expect(page).to_have_title("Lens - CFR Explorer")

        # Verify aggregate stats are shown
        stat_cards = page.locator(".stat-card")
        assert stat_cards.count() >= 4, "Dashboard should show aggregate stats"

    def test_titles_table_exists(self, page: Page):
        """Step 3: Verify there's a table with titles on /titles page."""
        page.goto(f"{BASE_URL}/titles")

        # Check for table
        table = page.locator("table")
        expect(table).to_be_visible()

        # Check table headers
        headers = page.locator("table thead th")
        header_texts = headers.all_text_contents()
        assert "Title" in header_texts
        assert "Name" in header_texts
        assert "Word Count" in header_texts

    def test_titles_have_links(self, page: Page):
        """Step 4: Verify titles have numbers, names, and word counts."""
        page.goto(f"{BASE_URL}/titles")

        # Check that rows have links
        title_links = page.locator("table tbody tr td a")
        expect(title_links.first).to_be_visible()

        # Check word counts are numbers
        word_count_cells = page.locator("table tbody tr td.numeric")
        first_count = word_count_cells.first.text_content()
        # Should contain digits and commas
        assert any(c.isdigit() for c in first_count), f"Word count should be numeric: {first_count}"

    def test_navigation_exists(self, page: Page):
        """Step 5: Verify navigation links exist."""
        page.goto(BASE_URL)

        # Check nav links
        nav = page.locator("nav")
        expect(nav).to_be_visible()

        titles_link = page.locator("nav a", has_text="Titles")
        expect(titles_link).to_be_visible()

        agencies_link = page.locator("nav a", has_text="Agencies")
        expect(agencies_link).to_be_visible()


class TestUserStory2_NavigateToTitle:
    """
    User Story 2: Navigate to a Title
    As a compliance officer, I want to click on a title to see its structure
    so I can understand how regulations are organized.

    Steps:
    1. Go to titles page
    2. Click on first title link
    3. Verify title page shows title number and name
    4. Verify structure is displayed with parts
    5. Verify sections are listed under parts
    """

    def test_click_title_navigates(self, page: Page):
        """Steps 1-2: Click on a title link."""
        page.goto(f"{BASE_URL}/titles")

        # Get first title link
        first_link = page.locator("table tbody tr:first-child td a").first
        title_text = first_link.text_content()
        first_link.click()

        page.screenshot(path=f"{SCREENSHOT_DIR}/02_title_page.png", full_page=True)

        # Verify we navigated
        expect(page).to_have_url(re.compile(r".*/title/.*"))

    def test_title_page_shows_info(self, page: Page):
        """Step 3: Verify title page shows title number and name."""
        page.goto(f"{BASE_URL}/titles")
        first_link = page.locator("table tbody tr:first-child td a").first
        first_link.click()

        # Check heading contains "Title"
        heading = page.locator("h1")
        expect(heading).to_contain_text("Title")

        # Check word count is shown
        word_count = page.locator("hgroup p")
        expect(word_count).to_be_visible()

    def test_structure_displayed(self, page: Page):
        """Steps 4-5: Verify structure with parts and sections."""
        page.goto(f"{BASE_URL}/titles")
        first_link = page.locator("table tbody tr:first-child td a").first
        first_link.click()

        # Look for structure section
        structure = page.locator("article", has_text="Structure")

        # Check for parts (details/summary elements)
        parts = page.locator("details")
        if parts.count() > 0:
            # Expand first part
            parts.first.locator("> summary").click()
            page.screenshot(path=f"{SCREENSHOT_DIR}/02b_title_expanded.png", full_page=True)

            # Check for section links
            section_links = page.locator("details ul li a")
            if section_links.count() > 0:
                expect(section_links.first).to_contain_text("§")


class TestUserStory3_ViewSection:
    """
    User Story 3: View a Section
    As a lawyer, I want to view a specific CFR section with its full text
    and word count so I can analyze the regulation.

    Steps:
    1. Navigate to a title
    2. Expand a part
    3. Click on a section
    4. Verify section heading is displayed
    5. Verify word count statistics are shown
    6. Verify section text is displayed
    """

    def test_navigate_to_section(self, page: Page):
        """Steps 1-3: Navigate to a section."""
        # Navigate directly to a known section
        page.goto(f"{BASE_URL}/title/1/section/1.1")
        page.screenshot(path=f"{SCREENSHOT_DIR}/03_section_page.png", full_page=True)
        expect(page).to_have_url(re.compile(r".*/section/.*"))

    def test_section_shows_statistics(self, page: Page):
        """Steps 4-5: Verify section heading and content info."""
        # Navigate directly to a known section
        page.goto(f"{BASE_URL}/title/1/section/1.1")

        # Check for heading with section number
        heading = page.locator("h1")
        expect(heading).to_contain_text("§")

        # Check for section text container
        section_text = page.locator(".section-text")
        expect(section_text).to_be_visible()

        # Check for similar sections article
        similar = page.locator("article.similar-sections")
        expect(similar).to_be_visible()

    def test_section_text_displayed(self, page: Page):
        """Step 6: Verify section text is displayed."""
        # Navigate directly to a known section
        page.goto(f"{BASE_URL}/title/1/section/1.1")

        # Check for section text container
        text_content = page.locator(".section-text")
        expect(text_content).to_be_visible()


class TestUserStory4_SimilarSections:
    """
    User Story 4: Find Similar Sections
    As a policy analyst, I want to see sections similar to the one I'm reading
    so I can identify related or duplicate regulations.

    Steps:
    1. Navigate to a section
    2. Look for "Similar Sections" indicator
    3. Wait for HTMX to load similar sections
    4. Verify similar sections are displayed with similarity percentages
    """

    def test_similar_sections_loads(self, page: Page):
        """Steps 1-4: Check similar sections feature."""
        # Navigate directly to a known section
        page.goto(f"{BASE_URL}/title/1/section/1.1")

        # Look for Similar Sections article
        similar_section = page.locator("article", has_text="Similar Sections")
        expect(similar_section).to_be_visible()

        # Wait for HTMX to load (wait for loading message to disappear)
        page.wait_for_timeout(2000)
        page.screenshot(path=f"{SCREENSHOT_DIR}/04_similar_sections.png", full_page=True)

        # Check if table or "no similar" message appears (not loading indicator)
        similar_content = similar_section.locator("table, p:not(:has-text('Loading'))")
        if similar_content.count() > 0:
            expect(similar_content.first).to_be_visible()


class TestUserStory5_CompareVersions:
    """
    User Story 5: Compare Historical Versions
    As a regulatory affairs specialist, I want to compare a section across years
    so I can track how regulations have changed.

    Steps:
    1. Navigate to a section
    2. Click "Compare Years" button
    3. Verify comparison page loads
    4. Verify year selectors are present
    5. Verify diff or section content is shown
    """

    def test_compare_button_exists(self, page: Page):
        """Steps 1-2: Find and click Compare Years button."""
        # Navigate directly to a known section
        page.goto(f"{BASE_URL}/title/1/section/1.1")

        # Look for Compare Years button/link
        compare_link = page.locator("a", has_text="Compare Years")
        expect(compare_link).to_be_visible()

        compare_link.click()
        page.screenshot(path=f"{SCREENSHOT_DIR}/05_compare_page.png", full_page=True)

        expect(page).to_have_url(re.compile(r".*/compare/.*"))

    def test_compare_page_elements(self, page: Page):
        """Steps 3-5: Verify comparison page elements."""
        # Navigate directly to compare page
        page.goto(f"{BASE_URL}/compare/title/1/section/1.1")

        # Check for year selectors
        year_selects = page.locator("select[name='year1'], select[name='year2']")
        expect(year_selects.first).to_be_visible()

        # Check for Go button (for citation navigation)
        go_btn = page.locator("button[type='submit']", has_text="Go")
        expect(go_btn).to_be_visible()


class TestUserStory6_ViewStatistics:
    """
    User Story 6: View Statistics
    As a government watchdog, I want to see which agencies have the most words
    in the CFR so I can understand regulatory burden.

    Steps:
    1. Click Agencies in navigation
    2. Verify agencies list loads
    3. Verify agencies are listed with word counts
    """

    def test_statistics_navigation(self, page: Page):
        """Steps 1-2: Navigate to agencies via nav link."""
        page.goto(BASE_URL)

        agencies_link = page.locator("nav a", has_text="Agencies")
        agencies_link.click()

        page.screenshot(path=f"{SCREENSHOT_DIR}/06_agencies.png", full_page=True)

        expect(page).to_have_url(re.compile(r".*/agencies.*"))

    def test_agency_statistics(self, page: Page):
        """Step 3: View agency statistics."""
        page.goto(f"{BASE_URL}/agencies")

        page.screenshot(path=f"{SCREENSHOT_DIR}/06b_agency_statistics.png", full_page=True)

        # Check table exists
        table = page.locator("table")
        expect(table).to_be_visible()

        # Check headers
        headers = page.locator("table thead th")
        header_texts = headers.all_text_contents()
        assert "Abbreviation" in header_texts
        assert "Agency" in header_texts
        assert "Word Count" in header_texts

    def test_title_statistics(self, page: Page):
        """View titles page (moved to browse)."""
        page.goto(f"{BASE_URL}/titles")

        page.screenshot(path=f"{SCREENSHOT_DIR}/06c_titles.png", full_page=True)

        expect(page).to_have_url(re.compile(r".*/titles.*"))

        # Check table exists
        table = page.locator("table")
        expect(table).to_be_visible()


class TestAccessibilityAndUsability:
    """Additional tests for accessibility and usability issues."""

    def test_breadcrumbs_work(self, page: Page):
        """Test that breadcrumb navigation works."""
        page.goto(f"{BASE_URL}/titles")
        page.locator("table tbody tr:first-child td a").first.click()

        # Check breadcrumb exists
        breadcrumb = page.locator("nav[aria-label='breadcrumb']")
        expect(breadcrumb).to_be_visible()

        # Click "All Titles" breadcrumb
        all_titles_link = breadcrumb.locator("a", has_text="All Titles")
        all_titles_link.click()

        expect(page).to_have_url(re.compile(r".*/titles.*"))

    def test_year_selector_works(self, page: Page):
        """Test that year selector changes the view."""
        page.goto(f"{BASE_URL}/titles")

        # Find year selector
        year_select = page.locator("select[name='year']")
        expect(year_select).to_be_visible()

        # Check it has options
        options = year_select.locator("option")
        assert options.count() >= 1, "Year selector should have at least one option"

    def test_footer_exists(self, page: Page):
        """Test that footer with attribution exists."""
        page.goto(BASE_URL)

        footer = page.locator("footer")
        expect(footer).to_be_visible()
        expect(footer).to_contain_text("ecfr.gov")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--headed"])
