"""HTTP client for eCFR and govinfo APIs."""

import asyncio
import time
import aiohttp
import requests

from .config import config


class ECFRClient:
    """Handles all API requests to ecfr.gov and govinfo.gov."""

    def __init__(self, max_retries: int = None, retry_delay: int = None):
        self.max_retries = max_retries if max_retries is not None else config.max_retries
        self.retry_delay = retry_delay if retry_delay is not None else config.retry_base_delay
        self._ecfr_base = config.ecfr_base_url
        self._govinfo_ecfr = config.govinfo_ecfr_url
        self._govinfo_cfr = config.govinfo_cfr_url

    def _request_with_retry(self, url: str, timeout: int = None, retry_on_timeout: bool = True) -> requests.Response:
        timeout = timeout if timeout is not None else config.timeout_default
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if not retry_on_timeout or attempt >= self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (2 ** attempt))
            except requests.exceptions.RequestException:
                if attempt >= self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (2 ** attempt))
        raise requests.exceptions.RequestException("Max retries exceeded")

    def fetch_titles(self) -> list[dict]:
        return self._request_with_retry(f"{self._ecfr_base}/versioner/v1/titles.json").json()["titles"]

    def fetch_agencies(self) -> list[dict]:
        return self._request_with_retry(f"{self._ecfr_base}/admin/v1/agencies.json").json().get("agencies", [])

    def fetch_title_xml(self, title_num: int, date: str, timeout: int = None) -> bytes:
        timeout = timeout if timeout is not None else config.timeout_title_xml
        return self._request_with_retry(f"{self._ecfr_base}/versioner/v1/full/{date}/title-{title_num}.xml", timeout=timeout, retry_on_timeout=False).content

    def fetch_title_structure(self, title_num: int, date: str) -> dict:
        return self._request_with_retry(f"{self._ecfr_base}/versioner/v1/structure/{date}/title-{title_num}.json", timeout=config.timeout_structure_api).json()

    async def fetch_title_racing(self, session: aiohttp.ClientSession, title_num: int, date: str, timeout: int = None) -> tuple[str, bytes]:
        """Fetch title from both eCFR and govinfo in parallel, return first success."""
        timeout = timeout if timeout is not None else config.timeout_race_fetch

        async def fetch(url, source):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return source, await resp.read()
            except Exception:
                pass
            return None

        tasks = [
            asyncio.create_task(fetch(f"{self._ecfr_base}/versioner/v1/full/{date}/title-{title_num}.xml", "ecfr")),
            asyncio.create_task(fetch(f"{self._govinfo_ecfr}/title-{title_num}/ECFR-title{title_num}.xml", "govinfo"))
        ]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return result
        raise aiohttp.ClientError(f"Both sources failed for title {title_num}")

    async def fetch_govinfo_volumes(self, session: aiohttp.ClientSession, year: int, title_num: int, max_volumes: int = None) -> list[bytes]:
        """Fetch all CFR volumes for a title from govinfo bulk data."""
        max_volumes = max_volumes if max_volumes is not None else config.max_govinfo_volumes
        volumes = []
        for vol in range(1, max_volumes + 1):
            url = f"{self._govinfo_cfr}/{year}/title-{title_num}/CFR-{year}-title{title_num}-vol{vol}.xml"
            for attempt in range(3):
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=config.timeout_govinfo_volume)) as resp:
                        if resp.status == 200:
                            volumes.append(await resp.read())
                            break
                        elif resp.status == 404:
                            return volumes  # No more volumes exist
                        elif resp.status == 429:
                            await asyncio.sleep(2 ** attempt)  # Rate limited, backoff
                        else:
                            break  # Other error, skip this volume
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)  # Timeout, retry after delay
                except Exception:
                    break  # Other exception, skip this volume
        return volumes

    async def fetch_chunks_async(self, title_num: int, date: str, chunks: list[tuple[str, str]], max_concurrent: int = None, delay: float = None) -> list[bytes]:
        """Fetch multiple XML chunks with rate limiting."""
        max_concurrent = max_concurrent if max_concurrent is not None else config.max_concurrent_chunks
        delay = delay if delay is not None else config.rate_limit_delay
        semaphore = asyncio.Semaphore(max_concurrent)
        completed, total = [0], len(chunks)
        progress_interval = config.progress_report_interval

        async def fetch_one(session, idx, chunk_type, chunk_id):
            url = f"{self._ecfr_base}/versioner/v1/full/{date}/title-{title_num}.xml?{chunk_type}={chunk_id}"
            async with semaphore:
                await asyncio.sleep(delay)
                for attempt in range(5):
                    try:
                        async with session.get(url) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(config.chunk_backoff_base * (2 ** attempt))
                                continue
                            resp.raise_for_status()
                            completed[0] += 1
                            if completed[0] % progress_interval == 0:
                                print(f"    {completed[0]}/{total} parts...", flush=True)
                            return idx, await resp.read()
                    except aiohttp.ClientError:
                        if attempt == 4:
                            raise
                        await asyncio.sleep(config.error_delay)
                raise aiohttp.ClientError(f"Failed after 5 attempts: {url}")

        results = [None] * len(chunks)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=max_concurrent), timeout=aiohttp.ClientTimeout(total=config.timeout_chunk_fetch)) as session:
            for idx, content in await asyncio.gather(*[fetch_one(session, i, ct, cid) for i, (ct, cid) in enumerate(chunks)]):
                results[idx] = content
        return results

    def get_title_chunks(self, title_num: int, date: str) -> list[tuple[str, str]]:
        """Get list of chunks (parts) to fetch for a title."""
        chunks = []
        def find_parts(node):
            if node.get('type') == 'part':
                chunks.append(('part', node.get('identifier')))
            for child in node.get('children', []):
                find_parts(child)
        find_parts(self.fetch_title_structure(title_num, date))
        return chunks
