from relevance.application.citation_extractor import CitationExtractor
from relevance.domain.models import CitationType


def test_citation_extractor_section_and_part():
    text = (
        "Violations of 40 C.F.R. § 52.21 and 40 CFR 60.42 were noted. "
        "Also 29 C.F.R. Part 1910 applies."
    )
    extractor = CitationExtractor()
    results = extractor.extract(text)
    normalized = {r.normalized for r in results}
    assert "40 CFR 52.21" in normalized
    assert "40 CFR 60.42" in normalized
    assert "29 CFR Part 1910" in normalized
    assert any(r.citation_type == CitationType.PART for r in results)


def test_citation_extractor_subsections():
    text = "See 8 CFR § 214.2(a)(1) and 17 C.F.R. 240.10b-5."
    extractor = CitationExtractor()
    results = extractor.extract(text)
    normalized = {r.normalized for r in results}
    assert "8 CFR 214.2(a)(1)" in normalized
    assert "17 CFR 240.10b-5" in normalized
