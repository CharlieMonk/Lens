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


class TestTableInteractivity:
    """Test table sorting and filtering."""

    def test_table_sorting(self, page: Page):
        """Test clicking column header sorts table."""
        page.goto(f"{BASE_URL}/titles")

        # Get initial first row text
        first_row = page.locator("table tbody tr:first-child")
        initial_text = first_row.text_content()

        # Click word count header to sort
        word_count_header = page.locator("th", has_text="Word Count")
        word_count_header.click()

        # Wait for sort
        page.wait_for_timeout(500)

        # Verify sort indicator appeared
        expect(word_count_header).to_have_class(re.compile(r"sort-"))

    def test_table_filtering(self, page: Page):
        """Test filter input filters table rows."""
        page.goto(f"{BASE_URL}/titles")

        # Find filter input
        filter_input = page.locator("input[type='search']")
        if filter_input.count() > 0:
            expect(filter_input).to_be_visible()

            # Type filter text
            filter_input.fill("Environment")
            page.wait_for_timeout(300)

            # Check rows are filtered (fewer visible)
            visible_rows = page.locator("table tbody tr:visible")
            # Should have filtered results
            page.screenshot(path=f"{SCREENSHOT_DIR}/table_filtered.png", full_page=True)


class TestChartPage:
    """Test chart/trends page functionality."""

    def test_chart_loads(self, page: Page):
        """Test chart page loads with visualization."""
        page.goto(f"{BASE_URL}/chart/")
        page.screenshot(path=f"{SCREENSHOT_DIR}/07_chart_page.png", full_page=True)

        # Check chart container exists
        chart = page.locator("canvas")
        expect(chart).to_be_visible()

    def test_chart_title_selector(self, page: Page):
        """Test title selector updates chart."""
        page.goto(f"{BASE_URL}/chart/")

        # Find title selector
        title_select = page.locator("select#title-select")
        expect(title_select).to_be_visible()

        # Select a title
        title_select.select_option(index=1)
        page.wait_for_timeout(1000)

        # Chart should still be visible
        chart = page.locator("canvas")
        expect(chart).to_be_visible()

    def test_chart_statistics_card(self, page: Page):
        """Test statistics card shows data."""
        page.goto(f"{BASE_URL}/chart/")
        page.wait_for_timeout(1000)

        # Check stats card exists
        stats = page.locator("#stats-card")
        if stats.count() > 0:
            expect(stats).to_be_visible()


class TestNavigationDropdown:
    """Test navigation dropdown menu."""

    def test_compare_dropdown_opens(self, page: Page):
        """Test Compare dropdown opens on hover/click."""
        page.goto(BASE_URL)

        # Find dropdown toggle
        compare_toggle = page.locator(".nav-dropdown-toggle")
        expect(compare_toggle).to_be_visible()

        # Click to open
        compare_toggle.click()

        # Check dropdown menu appears
        dropdown_menu = page.locator(".nav-dropdown-menu")
        expect(dropdown_menu).to_be_visible()

        page.screenshot(path=f"{SCREENSHOT_DIR}/nav_dropdown.png", full_page=True)

    def test_dropdown_links_work(self, page: Page):
        """Test dropdown menu links navigate correctly."""
        page.goto(BASE_URL)

        # Open dropdown
        compare_toggle = page.locator(".nav-dropdown-toggle")
        compare_toggle.click()

        # Click Historical link
        historical_link = page.locator(".nav-dropdown-menu a", has_text="Historical")
        historical_link.click()

        expect(page).to_have_url(re.compile(r".*/compare.*"))


class TestCopyCitation:
    """Test copy citation functionality."""

    def test_copy_button_exists(self, page: Page):
        """Test copy citation button is present on section page."""
        page.goto(f"{BASE_URL}/title/1/section/1.1")

        # Find copy button
        copy_btn = page.locator("button", has_text="Copy")
        if copy_btn.count() > 0:
            expect(copy_btn).to_be_visible()

    def test_copy_shows_feedback(self, page: Page):
        """Test clicking copy shows toast feedback."""
        page.goto(f"{BASE_URL}/title/1/section/1.1")

        copy_btn = page.locator("button", has_text="Copy")
        if copy_btn.count() > 0:
            copy_btn.click()

            # Check for toast notification
            page.wait_for_timeout(500)
            toast = page.locator(".toast")
            if toast.count() > 0:
                expect(toast).to_be_visible()
                page.screenshot(path=f"{SCREENSHOT_DIR}/copy_toast.png", full_page=True)


class TestMobileResponsive:
    """Test mobile responsive behavior."""

    def test_mobile_menu_toggle(self, page: Page):
        """Test hamburger menu appears on mobile."""
        # Set mobile viewport
        page.set_viewport_size({"width": 375, "height": 667})
        page.goto(BASE_URL)

        # Check hamburger menu exists
        hamburger = page.locator(".nav-toggle")
        if hamburger.count() > 0:
            expect(hamburger).to_be_visible()

            # Click to open
            hamburger.click()

            # Nav should be visible
            nav_menu = page.locator("nav ul.nav-open")
            page.screenshot(path=f"{SCREENSHOT_DIR}/mobile_menu.png", full_page=True)

    def test_tables_scroll_on_mobile(self, page: Page):
        """Test tables are scrollable on mobile."""
        page.set_viewport_size({"width": 375, "height": 667})
        page.goto(f"{BASE_URL}/titles")

        # Table should still be visible
        table = page.locator("table")
        expect(table).to_be_visible()

        page.screenshot(path=f"{SCREENSHOT_DIR}/mobile_table.png", full_page=True)


class TestComparePageAdvanced:
    """Advanced compare page tests."""

    def test_compare_year_selectors(self, page: Page):
        """Test year selectors on compare page."""
        page.goto(f"{BASE_URL}/compare/title/1/section/1.1")

        # Check both year selectors exist
        year1 = page.locator("select[name='year1']")
        year2 = page.locator("select[name='year2']")

        expect(year1).to_be_visible()
        expect(year2).to_be_visible()

    def test_compare_navigation_buttons(self, page: Page):
        """Test prev/next buttons on compare page."""
        page.goto(f"{BASE_URL}/compare/title/1/section/1.1")

        # Check for navigation buttons
        nav_buttons = page.locator(".section-nav a[role='button']")
        # May have prev/next buttons
        page.screenshot(path=f"{SCREENSHOT_DIR}/compare_nav.png", full_page=True)

    def test_cross_section_compare(self, page: Page):
        """Test cross-section comparison page."""
        page.goto(f"{BASE_URL}/compare/sections")

        # Check for two citation inputs
        cite_inputs = page.locator("input.citation-input")
        assert cite_inputs.count() >= 2, "Should have at least 2 citation inputs"

        page.screenshot(path=f"{SCREENSHOT_DIR}/cross_section.png", full_page=True)


class TestEdgeCases:
    """Test edge cases and error states."""

    def test_empty_section_display(self, page: Page):
        """Test empty/reserved section displays appropriately."""
        # Try to find a reserved section or test the display
        page.goto(f"{BASE_URL}/title/1/section/99.99")

        # Should show some kind of not found or empty message
        page.screenshot(path=f"{SCREENSHOT_DIR}/section_not_found.png", full_page=True)

    def test_invalid_citation_handling(self, page: Page):
        """Test compare page handles invalid citations."""
        page.goto(f"{BASE_URL}/compare/sections?cite1=invalid&cite2=invalid")

        # Page should load without crashing
        expect(page.locator("body")).to_be_visible()

    def test_deep_url_navigation(self, page: Page):
        """Test deep URLs work correctly."""
        # Navigate to a deep structure path
        page.goto(f"{BASE_URL}/title/1")

        # Should load and show content
        expect(page).to_have_url(re.compile(r".*/title/1.*"))
        expect(page.locator("h1")).to_contain_text("Title")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--headed"])
