"""eCFR data fetching and processing package."""

from .client import ECFRClient
from .constants import HISTORICAL_YEARS
from .extractor import XMLExtractor
from .database import ECFRDatabase
from .fetcher import ECFRFetcher, main

# Backwards compatibility alias
ECFRReader = ECFRDatabase

__all__ = [
    "ECFRClient",
    "ECFRDatabase",
    "ECFRFetcher",
    "ECFRReader",
    "HISTORICAL_YEARS",
    "XMLExtractor",
    "main",
]
