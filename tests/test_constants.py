"""Tests for ecfr/constants.py."""

import pytest

from ecfr.constants import HISTORICAL_YEARS, TYPE_TO_LEVEL, TYPE_TO_HEADING


class TestConstants:
    """Test constant values."""

    def test_historical_years_is_list(self):
        """HISTORICAL_YEARS should be a list."""
        assert isinstance(HISTORICAL_YEARS, list)

    def test_historical_years_are_integers(self):
        """All historical years should be integers."""
        assert all(isinstance(y, int) for y in HISTORICAL_YEARS)

    def test_historical_years_are_valid(self):
        """Historical years should be reasonable (2000-2030)."""
        assert all(2000 <= y <= 2030 for y in HISTORICAL_YEARS)

    def test_historical_years_descending(self):
        """Historical years should be in descending order."""
        assert HISTORICAL_YEARS == sorted(HISTORICAL_YEARS, reverse=True)

    def test_type_to_level_mapping(self):
        """TYPE_TO_LEVEL should map XML types to hierarchy levels."""
        assert TYPE_TO_LEVEL["TITLE"] == "title"
        assert TYPE_TO_LEVEL["CHAPTER"] == "chapter"
        assert TYPE_TO_LEVEL["PART"] == "part"
        assert TYPE_TO_LEVEL["SECTION"] == "section"

    def test_type_to_level_complete(self):
        """TYPE_TO_LEVEL should have all expected keys."""
        expected_keys = {"TITLE", "SUBTITLE", "CHAPTER", "SUBCHAP", "PART", "SUBPART", "SECTION"}
        assert set(TYPE_TO_LEVEL.keys()) == expected_keys

    def test_type_to_heading_mapping(self):
        """TYPE_TO_HEADING should map XML types to heading levels."""
        assert TYPE_TO_HEADING["TITLE"] == 1
        assert TYPE_TO_HEADING["CHAPTER"] == 2
        assert TYPE_TO_HEADING["PART"] == 3
        assert TYPE_TO_HEADING["SECTION"] == 4

    def test_type_to_heading_values_valid(self):
        """Heading levels should be between 1 and 6."""
        assert all(1 <= v <= 6 for v in TYPE_TO_HEADING.values())
