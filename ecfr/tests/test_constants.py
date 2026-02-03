"""Tests for constants and configuration."""

from ecfr import HISTORICAL_YEARS


class TestConstants:
    def test_historical_years_is_list(self):
        assert isinstance(HISTORICAL_YEARS, list)

    def test_historical_years_are_valid(self):
        assert all(isinstance(y, int) and 2000 <= y <= 2030 for y in HISTORICAL_YEARS)

    def test_historical_years_descending(self):
        assert HISTORICAL_YEARS == sorted(HISTORICAL_YEARS, reverse=True)
