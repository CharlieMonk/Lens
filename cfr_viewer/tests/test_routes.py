"""Tests for cfr_viewer routes."""

import pytest


class TestBrowseRoutes:
    """Test browse routes."""

    def test_index(self, client):
        """Test home page lists titles."""
        response = client.get("/")
        assert response.status_code == 200
        # Only Title 1 has section data in the test DB
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


class TestRankingsRoutes:
    """Test rankings routes."""

    def test_rankings_index(self, client):
        """Test rankings dashboard."""
        response = client.get("/rankings/")
        assert response.status_code == 200
        assert b"Rankings" in response.data
        assert b"Agency" in response.data
        assert b"Title" in response.data

    def test_agencies_ranking(self, client):
        """Test agencies by word count page."""
        response = client.get("/rankings/agencies")
        assert response.status_code == 200
        assert b"Agencies" in response.data
        assert b"Word Count" in response.data

    def test_titles_ranking(self, client):
        """Test titles by word count page."""
        response = client.get("/rankings/titles")
        assert response.status_code == 200
        assert b"Titles" in response.data


class TestCompareRoutes:
    """Test comparison routes."""

    def test_diff_page(self, client):
        """Test comparison page loads."""
        response = client.get("/compare/title/1/section/1.1")
        assert response.status_code == 200
        assert b"Compare" in response.data
        assert b"1.1" in response.data


class TestApiRoutes:
    """Test API routes for HTMX partials."""

    def test_similar_sections(self, client):
        """Test similar sections endpoint."""
        response = client.get("/api/similar/1/1.1")
        assert response.status_code == 200
        # Should return table or "No similar sections" message
        assert b"table" in response.data.lower() or b"similar" in response.data.lower()


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
