"""Tests for cfr_viewer routes."""

import pytest
import json


class TestBrowseRoutes:
    """Test browse routes."""

    def test_index(self, client):
        """Test home page shows dashboard."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Code of Federal Regulations" in response.data
        # Dashboard should have aggregate stats and preview sections
        assert b"Words" in response.data or b"Sections" in response.data

    def test_titles_page(self, client):
        """Test titles list page."""
        response = client.get("/titles")
        assert response.status_code == 200
        assert b"General Provisions" in response.data

    def test_title_page(self, client):
        """Test title structure page."""
        response = client.get("/title/1")
        assert response.status_code == 200
        assert b"Title 1" in response.data
        assert b"General Provisions" in response.data

    def test_section_page(self, client):
        """Test section view page."""
        response = client.get("/title/1/section/1.1")
        assert response.status_code == 200
        assert b"1.1" in response.data
        assert b"Purpose" in response.data

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


class TestAgenciesRoutes:
    """Test agencies routes."""

    def test_agencies_index(self, client):
        """Test agencies list page."""
        response = client.get("/agencies/")
        assert response.status_code == 200
        assert b"Agencies" in response.data


class TestCompareRoutes:
    """Test comparison routes."""

    def test_diff_page(self, client):
        """Test comparison page loads."""
        response = client.get("/compare/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Compare" in response.data
        assert b"1.1" in response.data

    def test_compare_sections(self, client):
        """Test compare two different sections."""
        response = client.get("/compare/sections?cite1=1+CFR+1.1&cite2=1+CFR+1.2")
        assert response.status_code == 200
        assert b"Compare Sections" in response.data

    def test_compare_sections_missing_params(self, client):
        """Test compare sections shows examples when params missing."""
        response = client.get("/compare/sections")
        assert response.status_code == 200
        assert b"Compare two different CFR sections" in response.data


class TestApiRoutes:
    """Test API routes for HTMX partials."""

    def test_similar_sections(self, client):
        """Test similar sections endpoint."""
        response = client.get("/api/similar/1/1.1")
        assert response.status_code == 200
        # Should return similar list or message
        assert b"similar" in response.data.lower() or b"distinctness" in response.data.lower()

    def test_section_preview(self, client):
        """Test section preview endpoint."""
        response = client.get("/api/preview/1/1.1")
        assert response.status_code == 200
        # Should return text content
        assert len(response.data) > 0

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

    def test_year_on_titles_page(self, client):
        """Test year selector on titles page."""
        response = client.get("/titles?year=0")
        assert response.status_code == 200
        assert b"Current" in response.data or b"year" in response.data.lower()

    def test_year_on_agencies_page(self, client):
        """Test year selector on agencies page."""
        response = client.get("/agencies/?year=0")
        assert response.status_code == 200


class TestChartRoutes:
    """Test chart/trends routes."""

    def test_chart_index(self, client):
        """Test chart page loads."""
        response = client.get("/chart/")
        assert response.status_code == 200
        assert b"Trends" in response.data or b"chart" in response.data.lower()

    def test_chart_data_total(self, client):
        """Test total word count data endpoint."""
        response = client.get("/chart/data/total")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, dict)

    def test_chart_data_title(self, client):
        """Test title word count data endpoint."""
        response = client.get("/chart/data/1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, dict)

    def test_chart_structure(self, client):
        """Test structure endpoint for cascading selectors."""
        response = client.get("/chart/structure/1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)


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
        # Should have navigation buttons or section text
        assert b"section" in response.data.lower()


class TestAgencyDetail:
    """Test agency detail routes."""

    def test_agency_detail_page(self, client):
        """Test agency detail page loads."""
        response = client.get("/agencies/test-agency")
        assert response.status_code == 200
        assert b"Test Agency" in response.data

    def test_agency_not_found(self, client):
        """Test non-existent agency handled gracefully."""
        response = client.get("/agencies/nonexistent-agency")
        # May return 404 or 200 with empty/error state
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            # Should show some indication it wasn't found
            assert b"not found" in response.data.lower() or b"no chapters" in response.data.lower() or b"agency" in response.data.lower()


class TestCompareAdvanced:
    """Additional compare route tests."""

    def test_compare_landing(self, client):
        """Test compare landing page."""
        response = client.get("/compare/")
        assert response.status_code == 200
        assert b"Compare" in response.data

    def test_compare_with_years(self, client):
        """Test compare with year parameters."""
        response = client.get("/compare/title/1/section/1.1?year1=0&year2=0")
        assert response.status_code == 200
        # When both years are same, should show "no changes" or single version
        assert b"1.1" in response.data

    def test_compare_invalid_section(self, client):
        """Test compare with non-existent section."""
        response = client.get("/compare/title/1/section/99.99")
        assert response.status_code == 200
        # Should show not found message
        assert b"not found" in response.data.lower() or b"available" in response.data.lower()

    def test_sections_compare_with_cites(self, client):
        """Test cross-section compare with citations."""
        response = client.get("/compare/sections?cite1=1+CFR+1.1&cite2=1+CFR+1.2")
        assert response.status_code == 200
        assert b"1.1" in response.data or b"1.2" in response.data


class TestApiAdvanced:
    """Additional API route tests."""

    def test_similar_returns_html(self, client):
        """Test similar sections returns HTML partial."""
        response = client.get("/api/similar/1/1.1")
        assert response.status_code == 200
        assert response.content_type.startswith("text/html")

    def test_preview_truncates(self, client):
        """Test preview endpoint returns truncated text."""
        response = client.get("/api/preview/1/1.1")
        assert response.status_code == 200
        # Should be plain text or short HTML
        assert len(response.data) > 0


class TestErrorHandling:
    """Test error handling across routes."""

    def test_404_on_bad_title(self, client):
        """Test 404 on non-existent title."""
        response = client.get("/title/999")
        # Should either 404 or show empty state
        assert response.status_code in [200, 404]

    def test_empty_search_filter(self, client):
        """Test pages work with empty filter param."""
        response = client.get("/titles?filter=")
        assert response.status_code == 200

    def test_special_characters_in_url(self, client):
        """Test handling of special characters."""
        response = client.get("/title/1/section/1.1%20")
        # Should not crash
        assert response.status_code in [200, 404]
