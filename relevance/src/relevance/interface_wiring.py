from __future__ import annotations

import logging

from relevance.adapters_registry import AdapterRegistry
from relevance.application_aggregation import AggregationService
from relevance.application_citation_extractor import CitationExtractor
from relevance.application_ingestion import IngestionService
from relevance.infrastructure_config import AppSettings
from relevance.infrastructure_db import Database, DatabaseConfig
from relevance.infrastructure_logging import configure_logging
from relevance.infrastructure_migrations import MigrationRunner


def build_app(settings: AppSettings | None = None):
    settings = settings or AppSettings()
    configure_logging(getattr(logging, settings.log_level.upper(), logging.INFO))
    db = Database(DatabaseConfig(url=settings.database_url))
    MigrationRunner(db.engine).run()
    adapters = AdapterRegistry()
    ingestion = IngestionService(adapters, CitationExtractor())
    aggregation = AggregationService()
    return db, ingestion, aggregation
