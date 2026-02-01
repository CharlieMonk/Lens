"""Shared constants for eCFR processing."""

# Historical years to fetch (in addition to current data)
HISTORICAL_YEARS = [2025, 2020, 2015, 2010, 2005, 2000]

# XML element type mappings
TYPE_TO_LEVEL = {
    "TITLE": "title",
    "SUBTITLE": "subtitle",
    "CHAPTER": "chapter",
    "SUBCHAP": "subchapter",
    "PART": "part",
    "SUBPART": "subpart",
    "SECTION": "section",
}

TYPE_TO_HEADING = {
    "TITLE": 1,
    "SUBTITLE": 2,
    "CHAPTER": 2,
    "SUBCHAP": 3,
    "PART": 3,
    "SUBPART": 4,
    "SECTION": 4,
}
