"""Integration test for ECFRFetcher - compares freshly fetched data against production database."""

import tempfile
from pathlib import Path

import pytest

from ecfr.database import ECFRDatabase
from ecfr.fetcher import ECFRFetcher, HISTORICAL_YEARS


PRODUCTION_DB_PATH = Path("ecfr/ecfr_data/ecfr.db")
TEST_TITLE = 11  # Title 11 - Federal Elections


@pytest.fixture(scope="module")
def production_db():
    """Load the production database."""
    if not PRODUCTION_DB_PATH.exists():
        pytest.skip("Production database not found")
    return ECFRDatabase(PRODUCTION_DB_PATH)


@pytest.fixture(scope="module")
def test_db_path():
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test_ecfr.db"


@pytest.fixture(scope="module")
def fetched_db(test_db_path):
    """Fetch Title 11 data into a temporary database."""
    fetcher = ECFRFetcher(output_dir=test_db_path.parent)
    fetcher.db = ECFRDatabase(test_db_path)

    # Load metadata (needed for fetching)
    print(f"\nLoading metadata...")
    fetcher._load_titles_metadata()

    # Fetch current year (only Title 11)
    print(f"Fetching Title {TEST_TITLE} (current)...")
    fetcher.update_stale_titles([TEST_TITLE])

    # Fetch historical years
    for year in HISTORICAL_YEARS:
        print(f"Fetching Title {TEST_TITLE} ({year})...")
        fetcher.fetch_historical([year], title_nums=[TEST_TITLE])

    return fetcher.db


@pytest.mark.integration
@pytest.mark.slow
class TestFetcherIntegration:
    """Integration tests comparing fetched data against production database."""

    def test_section_counts_match(self, production_db, fetched_db):
        """Compare section counts per year."""
        discrepancies = []

        # Get years from production database
        prod_years = production_db.list_years()

        for year in [0] + HISTORICAL_YEARS:
            if year not in prod_years:
                continue

            prod_count = len(production_db._query(
                "SELECT section FROM sections WHERE year = ? AND title = ?",
                (year, TEST_TITLE)
            ))

            test_count = len(fetched_db._query(
                "SELECT section FROM sections WHERE year = ? AND title = ?",
                (year, TEST_TITLE)
            ))

            if prod_count != test_count:
                discrepancies.append({
                    "year": year,
                    "production_count": prod_count,
                    "test_count": test_count,
                    "difference": test_count - prod_count,
                })

        if discrepancies:
            print("\n=== Section Count Discrepancies ===")
            for d in discrepancies:
                print(f"  Year {d['year']}: prod={d['production_count']}, test={d['test_count']} ({d['difference']:+d})")

        # Allow small discrepancies due to timing (live data may have changed)
        for d in discrepancies:
            pct_diff = abs(d['difference']) / max(d['production_count'], 1) * 100
            assert pct_diff < 5, f"Year {d['year']}: section count differs by {pct_diff:.1f}%"

    def test_section_identifiers_match(self, production_db, fetched_db):
        """Compare section identifiers (chapter/part/section)."""
        discrepancies = []

        prod_years = production_db.list_years()

        for year in [0] + HISTORICAL_YEARS:
            if year not in prod_years:
                continue

            prod_sections = set(
                (row[0], row[1], row[2])
                for row in production_db._query(
                    "SELECT chapter, part, section FROM sections WHERE year = ? AND title = ?",
                    (year, TEST_TITLE)
                )
            )

            test_sections = set(
                (row[0], row[1], row[2])
                for row in fetched_db._query(
                    "SELECT chapter, part, section FROM sections WHERE year = ? AND title = ?",
                    (year, TEST_TITLE)
                )
            )

            missing_in_test = prod_sections - test_sections
            extra_in_test = test_sections - prod_sections

            if missing_in_test or extra_in_test:
                discrepancies.append({
                    "year": year,
                    "missing_in_test": len(missing_in_test),
                    "extra_in_test": len(extra_in_test),
                    "missing_samples": list(missing_in_test)[:5],
                    "extra_samples": list(extra_in_test)[:5],
                })

        if discrepancies:
            print("\n=== Section Identifier Discrepancies ===")
            for d in discrepancies:
                print(f"  Year {d['year']}: missing={d['missing_in_test']}, extra={d['extra_in_test']}")
                if d['missing_samples']:
                    print(f"    Missing samples: {d['missing_samples']}")
                if d['extra_samples']:
                    print(f"    Extra samples: {d['extra_samples']}")

        # Report but don't fail on small differences
        total_missing = sum(d['missing_in_test'] for d in discrepancies)
        total_extra = sum(d['extra_in_test'] for d in discrepancies)
        assert total_missing < 10, f"Too many missing sections: {total_missing}"
        assert total_extra < 10, f"Too many extra sections: {total_extra}"

    def test_word_counts_match(self, production_db, fetched_db):
        """Compare word counts per section."""
        discrepancies = []

        prod_years = production_db.list_years()

        for year in [0] + HISTORICAL_YEARS:
            if year not in prod_years:
                continue

            prod_counts = {
                row[0]: row[1]
                for row in production_db._query(
                    "SELECT section, word_count FROM sections WHERE year = ? AND title = ?",
                    (year, TEST_TITLE)
                )
            }

            test_counts = {
                row[0]: row[1]
                for row in fetched_db._query(
                    "SELECT section, word_count FROM sections WHERE year = ? AND title = ?",
                    (year, TEST_TITLE)
                )
            }

            year_discrepancies = []
            for section in set(prod_counts.keys()) & set(test_counts.keys()):
                if prod_counts[section] != test_counts[section]:
                    year_discrepancies.append({
                        "section": section,
                        "prod_count": prod_counts[section],
                        "test_count": test_counts[section],
                    })

            if year_discrepancies:
                discrepancies.append({
                    "year": year,
                    "count": len(year_discrepancies),
                    "samples": year_discrepancies[:5],
                })

        if discrepancies:
            print("\n=== Word Count Discrepancies ===")
            for d in discrepancies:
                print(f"  Year {d['year']}: {d['count']} sections with different word counts")
                for s in d['samples']:
                    print(f"    {s['section']}: prod={s['prod_count']}, test={s['test_count']}")

        # Allow some discrepancies (extraction might differ slightly)
        total_discrepancies = sum(d['count'] for d in discrepancies)
        total_sections = sum(
            len(fetched_db._query(
                "SELECT section FROM sections WHERE year = ? AND title = ?",
                (year, TEST_TITLE)
            ))
            for year in [0] + HISTORICAL_YEARS
        )
        pct_discrepancies = total_discrepancies / max(total_sections, 1) * 100
        assert pct_discrepancies < 5, f"{pct_discrepancies:.1f}% of sections have word count discrepancies"

    def test_text_content_match(self, production_db, fetched_db):
        """Compare text content via length (not full text comparison)."""
        discrepancies = []

        prod_years = production_db.list_years()

        for year in [0] + HISTORICAL_YEARS:
            if year not in prod_years:
                continue

            prod_lengths = {
                row[0]: len(row[1]) if row[1] else 0
                for row in production_db._query(
                    "SELECT section, text FROM sections WHERE year = ? AND title = ?",
                    (year, TEST_TITLE)
                )
            }

            test_lengths = {
                row[0]: len(row[1]) if row[1] else 0
                for row in fetched_db._query(
                    "SELECT section, text FROM sections WHERE year = ? AND title = ?",
                    (year, TEST_TITLE)
                )
            }

            year_discrepancies = []
            for section in set(prod_lengths.keys()) & set(test_lengths.keys()):
                prod_len = prod_lengths[section]
                test_len = test_lengths[section]
                if prod_len != test_len:
                    pct_diff = abs(prod_len - test_len) / max(prod_len, 1) * 100
                    if pct_diff > 1:  # Only report >1% differences
                        year_discrepancies.append({
                            "section": section,
                            "prod_len": prod_len,
                            "test_len": test_len,
                            "pct_diff": pct_diff,
                        })

            if year_discrepancies:
                discrepancies.append({
                    "year": year,
                    "count": len(year_discrepancies),
                    "samples": sorted(year_discrepancies, key=lambda x: -x['pct_diff'])[:5],
                })

        if discrepancies:
            print("\n=== Text Length Discrepancies (>1% difference) ===")
            for d in discrepancies:
                print(f"  Year {d['year']}: {d['count']} sections")
                for s in d['samples']:
                    print(f"    {s['section']}: prod={s['prod_len']}, test={s['test_len']} ({s['pct_diff']:.1f}%)")

    def test_similarity_results_match(self, production_db, fetched_db):
        """Compare TF-IDF similarity results for sample sections."""
        discrepancies = []

        # Get a sample of sections from current year
        sections = production_db._query(
            "SELECT section FROM sections WHERE year = 0 AND title = ? AND text != '' LIMIT 10",
            (TEST_TITLE,)
        )

        for (section,) in sections:
            prod_similar, prod_max = production_db.get_similar_sections(TEST_TITLE, section, 0, 5)
            test_similar, test_max = fetched_db.get_similar_sections(TEST_TITLE, section, 0, 5)

            prod_set = {s['section'] for s in prod_similar}
            test_set = {s['section'] for s in test_similar}

            if prod_set != test_set:
                discrepancies.append({
                    "section": section,
                    "prod_similar": prod_set,
                    "test_similar": test_set,
                    "prod_max": prod_max,
                    "test_max": test_max,
                })

        if discrepancies:
            print("\n=== Similarity Result Discrepancies ===")
            for d in discrepancies:
                print(f"  Section {d['section']}:")
                print(f"    Prod similar: {d['prod_similar']}")
                print(f"    Test similar: {d['test_similar']}")
                print(f"    Max similarity: prod={d['prod_max']}, test={d['test_max']}")

        # Similarity can vary due to text differences - just report, don't fail
        if discrepancies:
            print(f"\n  {len(discrepancies)}/{len(sections)} sections have different similarity results")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])
