from relevance.adapters_dol import DolEnforcementAdapter
from relevance.adapters_epa import EpaEnforcementAdapter
from relevance.adapters_sec import SecEnforcementAdapter
from relevance.infrastructure_fixture_fetcher import FixtureFetcher, FixtureRegistry


def test_sec_adapter_parses_fixtures(fixtures_path):
    fetcher = FixtureFetcher(FixtureRegistry(fixtures_path))
    adapter = SecEnforcementAdapter()
    docs = adapter.fetch_documents(fetcher, "fixture://sec/index")
    assert len(docs) == 3
    assert "SEC Charges" in docs[0].title


def test_epa_adapter_parses_fixtures(fixtures_path):
    fetcher = FixtureFetcher(FixtureRegistry(fixtures_path))
    adapter = EpaEnforcementAdapter()
    docs = adapter.fetch_documents(fetcher, "fixture://epa/index")
    assert len(docs) == 3
    assert "Clean Air Act" in docs[0].title


def test_dol_adapter_parses_fixtures(fixtures_path):
    fetcher = FixtureFetcher(FixtureRegistry(fixtures_path))
    adapter = DolEnforcementAdapter()
    docs = adapter.fetch_documents(fetcher, "fixture://dol/index")
    assert len(docs) == 3
    assert "DOL Cites" in docs[0].title
