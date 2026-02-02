"""eCFR data fetching and processing package."""
from .client import ECFRClient
from .extractor import XMLExtractor
from .database import ECFRDatabase
from .fetcher import ECFRFetcher, HISTORICAL_YEARS, MAX_WORKERS, main

ECFRReader = ECFRDatabase  # Backwards compatibility
__all__ = ["ECFRClient", "ECFRDatabase", "ECFRFetcher", "ECFRReader", "HISTORICAL_YEARS", "MAX_WORKERS", "XMLExtractor", "main"]
