from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from relevance.application_fetcher import Fetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpFetcherConfig:
    timeout_seconds: float = 20.0
    user_agent: str = "relevance-bot/0.1"
    rate_limit_per_domain: float = 1.0
    respect_robots: bool = True


class HttpFetcher(Fetcher):
    def __init__(self, config: HttpFetcherConfig) -> None:
        self._config = config
        self._client = httpx.Client(timeout=config.timeout_seconds, headers={"User-Agent": config.user_agent})
        self._robots_cache: dict[str, RobotFileParser] = {}
        self._last_request: dict[str, float] = {}

    def close(self) -> None:
        self._client.close()

    def _rate_limit(self, domain: str) -> None:
        last = self._last_request.get(domain)
        if last is None:
            return
        elapsed = time.time() - last
        if elapsed < self._config.rate_limit_per_domain:
            time.sleep(self._config.rate_limit_per_domain - elapsed)

    def _robots_allowed(self, url: str) -> bool:
        if not self._config.respect_robots:
            return True
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        parser = self._robots_cache.get(base)
        if parser is None:
            robots_url = f"{base}/robots.txt"
            parser = RobotFileParser(robots_url)
            try:
                parser.read()
            except Exception:
                logger.warning("robots fetch failed", extra={"extra": {"url": robots_url}})
            self._robots_cache[base] = parser
        return parser.can_fetch(self._config.user_agent, url)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get(self, url: str) -> str:
        if not self._robots_allowed(url):
            raise RuntimeError(f"Blocked by robots.txt: {url}")
        domain = urlparse(url).netloc
        self._rate_limit(domain)
        logger.info("fetching", extra={"extra": {"url": url}})
        resp = self._client.get(url)
        resp.raise_for_status()
        self._last_request[domain] = time.time()
        return resp.text

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get_bytes(self, url: str) -> bytes:
        if not self._robots_allowed(url):
            raise RuntimeError(f"Blocked by robots.txt: {url}")
        domain = urlparse(url).netloc
        self._rate_limit(domain)
        logger.info("fetching-bytes", extra={"extra": {"url": url}})
        resp = self._client.get(url)
        resp.raise_for_status()
        self._last_request[domain] = time.time()
        return resp.content
