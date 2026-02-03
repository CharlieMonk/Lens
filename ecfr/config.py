"""Central configuration management for eCFR.

Loads settings from config.yaml with environment variable overrides.
Environment variables use ECFR_ prefix, e.g., ECFR_FLASK_PORT=8080
"""
import os
from pathlib import Path
import yaml

def _find_config_path():
    """Find config.yaml in project root."""
    # Try relative to this file first
    path = Path(__file__).parent.parent / "config.yaml"
    if path.exists():
        return path
    # Try current directory
    path = Path("config.yaml")
    if path.exists():
        return path
    return None

def _load_yaml_config():
    """Load configuration from config.yaml."""
    config_path = _find_config_path()
    if config_path:
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}

def _get_env(key, default=None, type_fn=str):
    """Get environment variable with ECFR_ prefix."""
    env_key = f"ECFR_{key.upper()}"
    val = os.environ.get(env_key)
    if val is None:
        return default
    if type_fn == bool:
        return val.lower() in ("1", "true", "yes")
    return type_fn(val)

class Config:
    """Configuration container with attribute access."""

    def __init__(self):
        self._config = _load_yaml_config()

    def _get(self, *keys, default=None, env_key=None, type_fn=str):
        """Get nested config value with optional env override."""
        # Check environment variable first
        if env_key:
            env_val = _get_env(env_key, type_fn=type_fn)
            if env_val is not None:
                return env_val

        # Navigate nested config
        val = self._config
        for key in keys:
            if isinstance(val, dict):
                val = val.get(key)
            else:
                val = None
            if val is None:
                return default
        return val if val is not None else default

    # Database
    @property
    def database_path(self):
        return self._get("database", "path", default="ecfr/ecfr_data/ecfr.db", env_key="DATABASE_PATH")

    @property
    def output_dir(self):
        return self._get("database", "output_dir", default="ecfr/ecfr_data", env_key="OUTPUT_DIR")

    # API Endpoints
    @property
    def ecfr_base_url(self):
        return self._get("api", "ecfr_base_url", default="https://www.ecfr.gov/api", env_key="ECFR_BASE_URL")

    @property
    def govinfo_ecfr_url(self):
        return self._get("api", "govinfo_ecfr_url", default="https://www.govinfo.gov/bulkdata/ECFR", env_key="GOVINFO_ECFR_URL")

    @property
    def govinfo_cfr_url(self):
        return self._get("api", "govinfo_cfr_url", default="https://www.govinfo.gov/bulkdata/CFR", env_key="GOVINFO_CFR_URL")

    # Flask
    @property
    def flask_host(self):
        return self._get("flask", "host", default="0.0.0.0", env_key="FLASK_HOST")

    @property
    def flask_port(self):
        return self._get("flask", "port", default=5000, env_key="FLASK_PORT", type_fn=int)

    @property
    def flask_warm_cache(self):
        return self._get("flask", "warm_cache", default=False, env_key="WARM_CACHE")

    # Fetcher
    @property
    def max_workers(self):
        return self._get("fetcher", "max_workers", default=3, env_key="MAX_WORKERS", type_fn=int)

    @property
    def historical_years(self):
        return self._get("fetcher", "historical_years", default=[2025, 2020, 2015, 2010, 2005, 2000])

    @property
    def title_min(self):
        return self._get("fetcher", "title_range", "min", default=1, type_fn=int)

    @property
    def title_max(self):
        return self._get("fetcher", "title_range", "max", default=50, type_fn=int)

    @property
    def excluded_titles(self):
        return self._get("fetcher", "title_range", "excluded", default=[35])

    # Timeouts
    @property
    def timeout_default(self):
        return self._get("timeouts", "default", default=30, env_key="TIMEOUT_DEFAULT", type_fn=int)

    @property
    def timeout_title_xml(self):
        return self._get("timeouts", "title_xml", default=60, type_fn=int)

    @property
    def timeout_structure_api(self):
        return self._get("timeouts", "structure_api", default=120, type_fn=int)

    @property
    def timeout_race_fetch(self):
        return self._get("timeouts", "race_fetch", default=120, type_fn=int)

    @property
    def timeout_govinfo_volume(self):
        return self._get("timeouts", "govinfo_volume", default=60, type_fn=int)

    @property
    def timeout_chunk_fetch(self):
        return self._get("timeouts", "chunk_fetch", default=1800, type_fn=int)

    @property
    def timeout_fetcher_session(self):
        return self._get("timeouts", "fetcher_session", default=120, type_fn=int)

    # Retry
    @property
    def max_retries(self):
        return self._get("retry", "max_retries", default=7, env_key="MAX_RETRIES", type_fn=int)

    @property
    def retry_base_delay(self):
        return self._get("retry", "base_delay", default=3, type_fn=int)

    @property
    def rate_limit_delay(self):
        return self._get("retry", "rate_limit_delay", default=0.2, type_fn=float)

    @property
    def error_delay(self):
        return self._get("retry", "error_delay", default=2, type_fn=int)

    @property
    def chunk_backoff_base(self):
        return self._get("retry", "chunk_backoff_base", default=5, type_fn=int)

    # Concurrency
    @property
    def max_govinfo_volumes(self):
        return self._get("concurrency", "max_govinfo_volumes", default=20, type_fn=int)

    @property
    def max_concurrent_chunks(self):
        return self._get("concurrency", "max_concurrent_chunks", default=2, type_fn=int)

    @property
    def progress_report_interval(self):
        return self._get("concurrency", "progress_report_interval", default=50, type_fn=int)

    # Cache
    @property
    def cache_stats_ttl(self):
        return self._get("cache", "stats_ttl", default=300, env_key="CACHE_STATS_TTL", type_fn=int)

    @property
    def cache_structure_ttl(self):
        return self._get("cache", "structure_ttl", default=300, env_key="CACHE_STRUCTURE_TTL", type_fn=int)

    # Similar Sections
    @property
    def tfidf_max_features(self):
        return self._get("similar_sections", "max_tfidf_features", default=10000, type_fn=int)

    @property
    def similar_default_limit(self):
        return self._get("similar_sections", "default_limit", default=10, type_fn=int)

    @property
    def similar_min_similarity(self):
        return self._get("similar_sections", "min_similarity", default=0.1, type_fn=float)

    @property
    def similar_keywords_count(self):
        return self._get("similar_sections", "keywords_per_section", default=5, type_fn=int)

    # Viewer
    @property
    def baseline_year(self):
        return self._get("viewer", "baseline_year", default=2010, env_key="BASELINE_YEAR", type_fn=int)

    @property
    def compare_default_year(self):
        return self._get("viewer", "compare_default_year", default=2020, env_key="COMPARE_DEFAULT_YEAR", type_fn=int)

    @property
    def preview_max_chars(self):
        return self._get("viewer", "preview_max_chars", default=500, type_fn=int)


# Singleton instance
config = Config()
