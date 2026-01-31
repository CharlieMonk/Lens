from __future__ import annotations

from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    database_url: str = "sqlite:///data/relevance.sqlite"
    log_level: str = "INFO"

    class Config:
        env_prefix = "RELEVANCE_"
