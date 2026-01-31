from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from relevance.application_fetcher import Fetcher


@dataclass(frozen=True)
class FixtureRegistry:
    base_dir: Path

    def resolve(self, url: str) -> Path:
        if not url.startswith("fixture://"):
            raise ValueError(f"Not a fixture URL: {url}")
        path = url.removeprefix("fixture://")
        parts = path.split("/")
        if len(parts) != 2:
            raise ValueError(f"Fixture URL must be fixture://<agency>/<name>: {url}")
        agency, name = parts
        return self.base_dir / agency / f"{name}.html"


class FixtureFetcher(Fetcher):
    def __init__(self, registry: FixtureRegistry) -> None:
        self._registry = registry

    def get(self, url: str) -> str:
        path = self._registry.resolve(url)
        return path.read_text(encoding="utf-8")
