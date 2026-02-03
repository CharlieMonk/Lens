"""Tests for cfr_viewer routes."""

import pytest


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
        response = client.get("/compare/sections?title1=1&section1=1.1&title2=1&section2=1.2")
        assert response.status_code == 200
        assert b"Compare Sections" in response.data

    def test_compare_sections_missing_params(self, client):
        """Test compare sections redirects with missing params."""
        response = client.get("/compare/sections")
        assert response.status_code == 302  # Redirect to compare index


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
