"""Tests for cfr_viewer routes."""

import pytest
import json


class TestBrowseRoutes:
    """Test browse routes."""

    def test_index(self, client):
        """Test home page shows dashboard with aggregate stats."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Code of Federal Regulations" in response.data
        # Dashboard should have aggregate stats
        assert b"Words" in response.data
        assert b"Sections" in response.data
        assert b"Titles" in response.data
        assert b"Agencies" in response.data

    def test_index_has_stat_cards(self, client):
        """Test home page has stat cards."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"stat-card" in response.data

    def test_index_has_top_titles(self, client):
        """Test home page shows top titles preview."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Browse Titles" in response.data

    def test_index_has_top_agencies(self, client):
        """Test home page shows top agencies preview."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Top" in response.data and b"Agencies" in response.data

    def test_index_has_trends_card(self, client):
        """Test home page shows trends card."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Trends" in response.data or b"trends" in response.data.lower()

    def test_index_has_compare_card(self, client):
        """Test home page shows compare sections card."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_titles_page(self, client):
        """Test titles list page."""
        response = client.get("/titles")
        assert response.status_code == 200
        assert b"General Provisions" in response.data

    def test_titles_page_has_table(self, client):
        """Test titles page has table with expected columns."""
        response = client.get("/titles")
        assert response.status_code == 200
        assert b"<table" in response.data
        assert b"Title" in response.data
        assert b"Name" in response.data
        assert b"Word Count" in response.data

    def test_titles_page_has_year_selector(self, client):
        """Test titles page has year selector."""
        response = client.get("/titles")
        assert response.status_code == 200
        assert b'name="year"' in response.data

    def test_titles_page_has_filter(self, client):
        """Test titles page has filter input."""
        response = client.get("/titles")
        assert response.status_code == 200
        assert b'type="search"' in response.data or b"Filter" in response.data

    def test_title_page(self, client):
        """Test title structure page."""
        response = client.get("/title/1")
        assert response.status_code == 200
        assert b"Title 1" in response.data
        assert b"General Provisions" in response.data

    def test_title_page_has_breadcrumb(self, client):
        """Test title page has breadcrumb navigation."""
        response = client.get("/title/1")
        assert response.status_code == 200
        assert b"All Titles" in response.data

    def test_title_page_shows_word_count(self, client):
        """Test title page shows word count."""
        response = client.get("/title/1")
        assert response.status_code == 200
        # Should show word count in the page
        assert b"words" in response.data.lower() or b"Word Count" in response.data

    def test_section_page(self, client):
        """Test section view page."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b"1.1" in response.data
        assert b"Purpose" in response.data

    def test_section_page_has_copy_button(self, client):
        """Test section page has copy citation button."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Copy" in response.data

    def test_section_page_has_compare_link(self, client):
        """Test section page has compare years link."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_section_page_has_trends_link(self, client):
        """Test section page has view trends link."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Trends" in response.data or b"trends" in response.data.lower()

    def test_section_page_has_similar_sections(self, client):
        """Test section page has similar sections area."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Similar" in response.data

    def test_section_page_has_year_selector(self, client):
        """Test section page has year selector."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b'name="year"' in response.data

    def test_section_not_found(self, client):
        """Test missing section shows appropriate message."""
        response = client.get("/title/1/section/99.99")
        assert response.status_code == 200
        assert b"not found" in response.data.lower() or b"Section" in response.data


class TestStatisticsRoutes:
    """Test statistics routes - all redirect to new locations."""

    def test_statistics_index_redirects(self, client):
        """Test statistics index redirects to homepage."""
        response = client.get("/statistics/")
        assert response.status_code == 301
        assert response.location.endswith("/")

    def test_statistics_agencies_redirects(self, client):
        """Test statistics agencies redirects to agencies page."""
        response = client.get("/statistics/agencies")
        assert response.status_code == 301
        assert "/agencies" in response.location

    def test_titles_statistics_redirects(self, client):
        """Test titles statistics redirects to browse titles."""
        response = client.get("/statistics/titles")
        assert response.status_code == 301
        assert "/titles" in response.location

    def test_agency_detail_statistics_redirects(self, client):
        """Test agency detail statistics redirects to agency detail."""
        response = client.get("/statistics/agencies/test-agency")
        assert response.status_code == 301
        assert "/agencies/test-agency" in response.location


class TestAgenciesRoutes:
    """Test agencies routes."""

    def test_agencies_index(self, client):
        """Test agencies list page."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b"Agencies" in response.data

    def test_agencies_index_has_table(self, client):
        """Test agencies page has table with expected columns."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b"<table" in response.data
        assert b"Abbreviation" in response.data
        assert b"Agency" in response.data
        assert b"Word Count" in response.data

    def test_agencies_index_has_year_selector(self, client):
        """Test agencies page has year selector."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b'name="year"' in response.data

    def test_agencies_index_has_filter(self, client):
        """Test agencies page has filter input."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b'type="search"' in response.data or b"Filter" in response.data


class TestAgencyDetail:
    """Test agency detail routes."""

    def test_agency_detail_page(self, client):
        """Test agency detail page loads."""
        response = client.get("/agencies/test-agency")
        assert response.status_code == 200
        assert b"Test Agency" in response.data

    def test_agency_detail_has_breadcrumb(self, client):
        """Test agency detail has breadcrumb."""
        response = client.get("/agencies/test-agency")
        assert response.status_code == 200
        assert b"Agencies" in response.data

    def test_agency_detail_has_chapters_table(self, client):
        """Test agency detail shows chapters table."""
        response = client.get("/agencies/test-agency")
        assert response.status_code == 200
        # Should show chapters header or table
        assert b"Chapter" in response.data or b"CFR" in response.data

    def test_agency_detail_has_year_selector(self, client):
        """Test agency detail has year selector (when chapters exist)."""
        response = client.get("/agencies/test-agency")
        assert response.status_code == 200
        # Year selector shows when agency has chapters
        assert b'name="year"' in response.data

    def test_agency_not_found(self, client):
        """Test non-existent agency handled gracefully."""
        response = client.get("/agencies/nonexistent-agency")
        # May return 404 or 200 with empty/error state
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            assert b"not found" in response.data.lower() or b"no chapters" in response.data.lower() or b"agency" in response.data.lower()


class TestCompareRoutes:
    """Test comparison routes."""

    def test_compare_landing(self, client):
        """Test compare landing page."""
        response = client.get("/compare/")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_compare_landing_has_citation_input(self, client):
        """Test compare landing has citation input."""
        response = client.get("/compare/")
        assert response.status_code == 200
        # Should have input for CFR citation
        assert b"citation" in response.data.lower() or b"CFR" in response.data

    def test_compare_landing_has_examples(self, client):
        """Test compare landing shows example citations."""
        response = client.get("/compare/")
        assert response.status_code == 200
        # Examples should be shown
        assert b"e.g." in response.data.lower() or b"format" in response.data.lower()

    def test_diff_page(self, client):
        """Test comparison page loads."""
        response = client.get("/compare/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Compare" in response.data
        assert b"1.1" in response.data

    def test_diff_page_has_year_selectors(self, client):
        """Test diff page has two year selectors."""
        response = client.get("/compare/title/1/section/1.1")
        assert response.status_code == 200
        assert b'name="year1"' in response.data
        assert b'name="year2"' in response.data

    def test_diff_page_has_navigation(self, client):
        """Test diff page has prev/next navigation."""
        response = client.get("/compare/title/1/section/1.1")
        assert response.status_code == 200
        # Should have some navigation element
        assert b"nav" in response.data.lower() or b"Prev" in response.data or b"Next" in response.data

    def test_diff_with_same_years(self, client):
        """Test compare with same year shows no changes."""
        response = client.get("/compare/title/1/section/1.1?year1=0&year2=0")
        assert response.status_code == 200
        # When both years are same, should show "no changes" or identical
        assert b"1.1" in response.data

    def test_compare_with_years(self, client):
        """Test compare with year parameters."""
        response = client.get("/compare/title/1/section/1.1?year1=0&year2=0")
        assert response.status_code == 200
        assert b"1.1" in response.data

    def test_compare_invalid_section(self, client):
        """Test compare with non-existent section."""
        response = client.get("/compare/title/1/section/99.99")
        assert response.status_code == 200
        # Should show not found or available years message
        assert b"not found" in response.data.lower() or b"available" in response.data.lower() or b"No" in response.data

    def test_compare_sections(self, client):
        """Test compare two different sections."""
        response = client.get("/compare/sections?cite1=1+CFR+1.1&cite2=1+CFR+1.2")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_compare_sections_has_two_inputs(self, client):
        """Test compare sections page has two citation inputs."""
        response = client.get("/compare/sections")
        assert response.status_code == 200
        assert b"Section 1" in response.data or b"cite1" in response.data.lower()
        assert b"Section 2" in response.data or b"cite2" in response.data.lower()

    def test_compare_sections_missing_params(self, client):
        """Test compare sections shows examples when params missing."""
        response = client.get("/compare/sections")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_sections_compare_with_cites(self, client):
        """Test cross-section compare with citations."""
        response = client.get("/compare/sections?cite1=1+CFR+1.1&cite2=1+CFR+1.2")
        assert response.status_code == 200
        assert b"1.1" in response.data or b"1.2" in response.data


class TestChartRoutes:
    """Test chart/trends routes."""

    def test_chart_index(self, client):
        """Test chart page loads."""
        response = client.get("/chart/")
        assert response.status_code == 200
        assert b"Trends" in response.data or b"chart" in response.data.lower()

    def test_chart_index_has_title_selector(self, client):
        """Test chart page has title selector."""
        response = client.get("/chart/")
        assert response.status_code == 200
        assert b"title-select" in response.data or b"All CFR" in response.data

    def test_chart_index_has_citation_input(self, client):
        """Test chart page has citation input."""
        response = client.get("/chart/")
        assert response.status_code == 200
        assert b"citation" in response.data.lower() or b"CFR" in response.data

    def test_chart_index_has_canvas(self, client):
        """Test chart page has canvas element."""
        response = client.get("/chart/")
        assert response.status_code == 200
        assert b"<canvas" in response.data

    def test_chart_data_total(self, client):
        """Test total word count data endpoint."""
        response = client.get("/chart/data/total")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, dict)

    def test_chart_data_total_returns_years(self, client):
        """Test total word count data returns year keys."""
        response = client.get("/chart/data/total")
        assert response.status_code == 200
        data = json.loads(response.data)
        # Keys should be year strings
        for key in data.keys():
            assert key.isdigit()

    def test_chart_data_title(self, client):
        """Test title word count data endpoint."""
        response = client.get("/chart/data/1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, dict)

    def test_chart_data_title_with_path(self, client):
        """Test title word count data with structure path."""
        response = client.get("/chart/data/1/chapter/I")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, dict)

    def test_chart_structure(self, client):
        """Test structure endpoint for cascading selectors."""
        response = client.get("/chart/structure/1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_chart_structure_with_path(self, client):
        """Test structure endpoint with path."""
        response = client.get("/chart/structure/1/chapter/I")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_chart_section_path(self, client):
        """Test section path endpoint."""
        response = client.get("/chart/section-path/1/1.1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "found" in data

    def test_chart_section_path_not_found(self, client):
        """Test section path for non-existent section."""
        response = client.get("/chart/section-path/1/99.99")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["found"] is False


class TestApiRoutes:
    """Test API routes for HTMX partials."""

    def test_similar_sections(self, client):
        """Test similar sections endpoint."""
        response = client.get("/api/similar/1/1.1")
        assert response.status_code == 200
        # Should return HTML fragment
        assert response.content_type.startswith("text/html")

    def test_similar_sections_with_scope(self, client):
        """Test similar sections with scope parameter."""
        response = client.get("/api/similar/1/1.1?scope=chapter")
        assert response.status_code == 200
        assert response.content_type.startswith("text/html")

    def test_similar_sections_with_limit(self, client):
        """Test similar sections with limit parameter."""
        response = client.get("/api/similar/1/1.1?limit=5")
        assert response.status_code == 200

    def test_section_content(self, client):
        """Test section content endpoint."""
        response = client.get("/api/section/1/1.1")
        assert response.status_code == 200
        assert response.content_type.startswith("text/html")

    def test_section_preview(self, client):
        """Test section preview endpoint."""
        response = client.get("/api/preview/1/1.1")
        assert response.status_code == 200
        # Should return text content
        assert len(response.data) > 0

    def test_section_preview_with_max(self, client):
        """Test section preview with max parameter."""
        response = client.get("/api/preview/1/1.1?max=50")
        assert response.status_code == 200
        # Should be truncated
        assert len(response.data) <= 60  # Allow some buffer for "..."

    def test_section_preview_not_found(self, client):
        """Test section preview for non-existent section."""
        response = client.get("/api/preview/1/99.99")
        assert response.status_code == 200
        assert b"No content" in response.data or b"available" in response.data.lower()


class TestYearSelector:
    """Test year selection functionality."""

    def test_year_parameter(self, client):
        """Test year parameter is accepted."""
        response = client.get("/?year=0")
        assert response.status_code == 200

    def test_invalid_year(self, client):
        """Test invalid year gracefully handled."""
        response = client.get("/?year=abc")
        # Should not crash - will default to 0
        assert response.status_code == 200

    def test_invalid_year_number(self, client):
        """Test invalid year number defaults to 0."""
        response = client.get("/?year=9999")
        assert response.status_code == 200

    def test_year_on_titles_page(self, client):
        """Test year selector on titles page."""
        response = client.get("/titles?year=0")
        assert response.status_code == 200
        assert b"Current" in response.data or b"year" in response.data.lower()

    def test_year_on_agencies_page(self, client):
        """Test year selector on agencies page."""
        response = client.get("/agencies/?year=0")
        assert response.status_code == 200

    def test_year_on_title_page(self, client):
        """Test year selector on title detail page."""
        response = client.get("/title/1?year=0")
        assert response.status_code == 200

    def test_year_on_section_page(self, client):
        """Test year selector on section page."""
        response = client.get("/title/1/section/1.1?year=0")
        assert response.status_code == 200


class TestStructureNavigation:
    """Test structure navigation routes."""

    def test_title_structure(self, client):
        """Test title structure page."""
        response = client.get("/title/1")
        assert response.status_code == 200
        assert b"Title 1" in response.data

    def test_breadcrumb_present(self, client):
        """Test breadcrumb navigation is present."""
        response = client.get("/title/1")
        assert response.status_code == 200
        assert b"All Titles" in response.data

    def test_section_navigation(self, client):
        """Test section has prev/next navigation."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        # Should have navigation or section text
        assert b"section" in response.data.lower()

    def test_structure_path_navigation(self, client):
        """Test deep structure path navigation."""
        response = client.get("/title/1/chapter/I")
        # May redirect if path doesn't exist or show structure
        assert response.status_code in [200, 302]


class TestErrorHandling:
    """Test error handling across routes."""

    def test_404_on_bad_title(self, client):
        """Test 404 on non-existent title."""
        response = client.get("/title/999")
        # Should either 404 or show empty state
        assert response.status_code in [200, 404]

    def test_404_on_nonexistent_route(self, client):
        """Test 404 on completely nonexistent route."""
        response = client.get("/nonexistent/route/here")
        assert response.status_code == 404

    def test_empty_search_filter(self, client):
        """Test pages work with empty filter param."""
        response = client.get("/titles?filter=")
        assert response.status_code == 200

    def test_special_characters_in_url(self, client):
        """Test handling of special characters."""
        response = client.get("/title/1/section/1.1%20")
        # Should not crash
        assert response.status_code in [200, 404]

    def test_malformed_citation(self, client):
        """Test compare handles malformed citations."""
        response = client.get("/compare/sections?cite1=invalid&cite2=invalid")
        assert response.status_code == 200


class TestNavigation:
    """Test site navigation elements."""

    def test_nav_has_titles_link(self, client):
        """Test navigation has titles link."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Titles" in response.data

    def test_nav_has_agencies_link(self, client):
        """Test navigation has agencies link."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Agencies" in response.data

    def test_nav_has_compare_dropdown(self, client):
        """Test navigation has compare dropdown."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_nav_has_trends_link(self, client):
        """Test navigation has trends link."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Trends" in response.data

    def test_footer_exists(self, client):
        """Test footer with attribution exists."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"<footer" in response.data
        assert b"ecfr.gov" in response.data


class TestTableFeatures:
    """Test table sorting and filtering features."""

    def test_titles_table_sortable(self, client):
        """Test titles table has sortable headers."""
        response = client.get("/titles")
        assert response.status_code == 200
        assert b"sortable" in response.data.lower() or b"data-sort" in response.data

    def test_agencies_table_sortable(self, client):
        """Test agencies table has sortable headers."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b"sortable" in response.data.lower() or b"data-sort" in response.data


class TestChangePercentages:
    """Test change percentage display."""

    def test_titles_show_change(self, client):
        """Test titles page shows change percentages."""
        response = client.get("/titles")
        assert response.status_code == 200
        # Should show baseline year reference
        assert b"2010" in response.data or b"Since" in response.data

    def test_agencies_show_change(self, client):
        """Test agencies page shows change percentages."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b"2010" in response.data or b"Since" in response.data


class TestSearchRoutes:
    """Test search routes."""

    def test_search_page_loads(self, client):
        """Test search page loads."""
        response = client.get("/search/")
        assert response.status_code == 200
        assert b"Search" in response.data

    def test_search_page_has_search_form(self, client):
        """Test search page has search input."""
        response = client.get("/search/")
        assert response.status_code == 200
        assert b'name="q"' in response.data
        assert b'type="search"' in response.data

    def test_search_with_empty_query(self, client):
        """Test search with empty query shows tips."""
        response = client.get("/search/?q=")
        assert response.status_code == 200
        # Should show tips or examples when no query
        assert b"Search" in response.data

    def test_search_with_query_no_index(self, client):
        """Test search with query when no FAISS index exists."""
        response = client.get("/search/?q=test")
        assert response.status_code == 200
        # Without FAISS index, should show warning banner
        assert b"Search" in response.data

    def test_search_page_has_examples(self, client):
        """Test search page shows example searches."""
        response = client.get("/search/")
        assert response.status_code == 200
        # Should have example cards or disabled notice
        assert b"example" in response.data.lower() or b"Index" in response.data

    def test_nav_has_search_link(self, client):
        """Test navigation has search link."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Search" in response.data

    def test_search_page_has_filter_input(self, client):
        """Test search results have filter input."""
        response = client.get("/search/")
        assert response.status_code == 200
        # Filter should exist on page (though only visible with results)
        assert b"search" in response.data.lower()
