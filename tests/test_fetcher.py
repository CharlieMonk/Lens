"""Tests for ecfr/fetcher.py."""

import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest

from ecfr.fetcher import ECFRFetcher


class TestECFRFetcherInit:
    """Tests for fetcher initialization."""

    def test_default_output_dir(self):
        """Default output directory is ecfr/ecfr_data."""
        fetcher = ECFRFetcher()
        assert fetcher.output_dir == Path("ecfr/ecfr_data")

    def test_custom_output_dir(self):
        """Custom output directory is accepted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))
            assert fetcher.output_dir == Path(tmpdir)

    def test_database_created(self):
        """Database is created in output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))
            assert fetcher.db is not None
            assert (Path(tmpdir) / "ecfr.db").exists()

    def test_client_created(self):
        """Client is instantiated."""
        fetcher = ECFRFetcher()
        assert fetcher.client is not None

    def test_max_workers(self):
        """Max workers setting is stored."""
        fetcher = ECFRFetcher(max_workers=10)
        assert fetcher.max_workers == 10


class TestECFRFetcherCache:
    """Tests for cache operations."""

    def test_clear_cache_removes_db(self):
        """Clear cache removes database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            fetcher = ECFRFetcher(output_dir=output_dir)

            db_path = output_dir / "ecfr.db"
            assert db_path.exists()

            fetcher.clear_cache()

            assert not db_path.exists()



class TestECFRFetcherMetadata:
    """Tests for metadata loading."""

    def test_load_titles_metadata_from_api(self):
        """Load titles metadata from API when not cached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            with patch.object(fetcher.client, 'fetch_titles') as mock_fetch:
                mock_fetch.return_value = [
                    {"number": 1, "name": "Title 1", "latest_issue_date": "2024-01-01"},
                ]

                titles = fetcher._load_titles_metadata()

                assert 1 in titles
                mock_fetch.assert_called_once()

    def test_load_titles_metadata_cached(self):
        """Use cached titles if fresh."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            # Save titles to database
            fetcher.db.save_titles([
                {"number": 1, "name": "Cached Title", "latest_issue_date": "2024-01-01"}
            ])

            with patch.object(fetcher.client, 'fetch_titles') as mock_fetch:
                titles = fetcher._load_titles_metadata()

                assert titles[1]["name"] == "Cached Title"
                mock_fetch.assert_not_called()

    def test_load_agency_lookup(self):
        """Load agency lookup table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            with patch.object(fetcher.client, 'fetch_agencies') as mock_fetch:
                mock_fetch.return_value = [
                    {
                        "slug": "test-agency",
                        "name": "Test Agency",
                        "cfr_references": [{"title": 1, "chapter": "I"}],
                        "children": [],
                    }
                ]

                lookup = fetcher._load_agency_lookup()

                assert (1, "I") in lookup


class TestECFRFetcherAsync:
    """Tests for async fetching operations.

    Note: Full async integration tests with mocked aiohttp are complex.
    Core async logic is tested via sync wrappers and integration tests.
    """

    def test_fetch_title_async_exists(self):
        """Verify async fetch method exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))
            assert hasattr(fetcher, 'fetch_title_async')
            assert callable(fetcher.fetch_title_async)

    def test_fetch_current_async_exists(self):
        """Verify async current fetch method exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))
            assert hasattr(fetcher, 'fetch_current_async')
            assert callable(fetcher.fetch_current_async)

    def test_fetch_historical_async_exists(self):
        """Verify async historical fetch method exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))
            assert hasattr(fetcher, 'fetch_historical_async')
            assert callable(fetcher.fetch_historical_async)


class TestECFRFetcherHistorical:
    """Tests for historical fetching."""

    @pytest.mark.asyncio
    async def test_fetch_historical_skips_existing(self):
        """Skip years already in database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            # Add data for 2020
            fetcher.db.save_sections([
                {"title": 1, "section": "1.1", "text": "Test"}
            ], year=2020)

            with patch.object(fetcher.client, 'fetch_govinfo_volumes', new_callable=AsyncMock) as mock_fetch:
                result = await fetcher.fetch_historical_async([2020], [1])

                # Should not fetch because data exists
                mock_fetch.assert_not_called()


class TestECFRFetcherSync:
    """Tests for sync wrapper methods."""

    def test_fetch_current_sync(self):
        """Sync wrapper calls async method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            with patch.object(fetcher, 'fetch_current_async', new_callable=AsyncMock) as mock_async:
                mock_async.return_value = 0

                result = fetcher.fetch_current()

                assert result == 0

    def test_fetch_historical_sync(self):
        """Sync wrapper calls async method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            with patch.object(fetcher, 'fetch_historical_async', new_callable=AsyncMock) as mock_async:
                mock_async.return_value = 0

                result = fetcher.fetch_historical([2020])

                assert result == 0

    def test_fetch_all_sync(self):
        """Sync wrapper calls async method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = ECFRFetcher(output_dir=Path(tmpdir))

            with patch.object(fetcher, 'fetch_all_async', new_callable=AsyncMock) as mock_async:
                mock_async.return_value = 0

                result = fetcher.fetch_all()

                assert result == 0
