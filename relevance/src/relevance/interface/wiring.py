from __future__ import annotations

import logging

from relevance.adapters.registry import AdapterRegistry
from relevance.application.aggregation import AggregationService
from relevance.application.citation_extractor import CitationExtractor
from relevance.application.ingestion import IngestionService
from relevance.infrastructure.config import AppSettings
from relevance.infrastructure.db import Database, DatabaseConfig
from relevance.infrastructure.logging import configure_logging
from relevance.infrastructure.migrations import MigrationRunner


def build_app(settings: AppSettings | None = None):
    settings = settings or AppSettings()
    configure_logging(getattr(logging, settings.log_level.upper(), logging.INFO))
    db = Database(DatabaseConfig(url=settings.database_url))
    MigrationRunner(db.engine).run()
    adapters = AdapterRegistry()
    ingestion = IngestionService(adapters, CitationExtractor())
    aggregation = AggregationService()
    return db, ingestion, aggregation
