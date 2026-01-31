"""HTTP client for eCFR and govinfo APIs."""

import asyncio
import time

import aiohttp
import requests


class ECFRClient:
    """Handles all API requests to ecfr.gov and govinfo.gov."""

    ECFR_BASE_URL = "https://www.ecfr.gov/api"
    GOVINFO_ECFR_URL = "https://www.govinfo.gov/bulkdata/ECFR"
    GOVINFO_CFR_URL = "https://www.govinfo.gov/bulkdata/CFR"

    def __init__(self, max_retries: int = 7, retry_delay: int = 3):
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _request_with_retry(self, url: str, timeout: int = 30, retry_on_timeout: bool = True) -> requests.Response:
        """Make a request with exponential backoff retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if not retry_on_timeout or attempt >= self.max_retries - 1:
                    raise
                delay = self.retry_delay * (2 ** attempt)
                time.sleep(delay)
            except requests.exceptions.RequestException:
                if attempt >= self.max_retries - 1:
                    raise
                delay = self.retry_delay * (2 ** attempt)
                time.sleep(delay)

        raise requests.exceptions.RequestException("Max retries exceeded")

    # Synchronous API methods

    def fetch_titles(self) -> list[dict]:
        """Fetch titles metadata from the API."""
        url = f"{self.ECFR_BASE_URL}/versioner/v1/titles.json"
        response = self._request_with_retry(url)
        return response.json()["titles"]

    def fetch_agencies(self) -> list[dict]:
        """Fetch agencies metadata from the API."""
        url = f"{self.ECFR_BASE_URL}/admin/v1/agencies.json"
        response = self._request_with_retry(url)
        return response.json().get("agencies", [])

    def fetch_title_xml(self, title_num: int, date: str, timeout: int = 60) -> bytes:
        """Fetch full XML for a title on a specific date."""
        url = f"{self.ECFR_BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml"
        response = self._request_with_retry(url, timeout=timeout, retry_on_timeout=False)
        return response.content

    def fetch_title_structure(self, title_num: int, date: str) -> dict:
        """Fetch the structure/TOC for a title on a specific date."""
        url = f"{self.ECFR_BASE_URL}/versioner/v1/structure/{date}/title-{title_num}.json"
        response = self._request_with_retry(url, timeout=120)
        return response.json()

    # Async fetch methods

    async def fetch_title_racing(
        self,
        session: aiohttp.ClientSession,
        title_num: int,
        date: str,
        timeout: int = 120
    ) -> tuple[str, bytes]:
        """Fetch title from both eCFR and govinfo in parallel, return first success.

        Returns tuple of (source_name, xml_content).
        """
        async def fetch_ecfr():
            url = f"{self.ECFR_BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return "ecfr", await resp.read()
            except Exception:
                pass
            return None

        async def fetch_govinfo():
            url = f"{self.GOVINFO_ECFR_URL}/title-{title_num}/ECFR-title{title_num}.xml"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return "govinfo", await resp.read()
            except Exception:
                pass
            return None

        tasks = [asyncio.create_task(fetch_ecfr()), asyncio.create_task(fetch_govinfo())]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                for t in tasks:
                    t.cancel()
                return result

        raise aiohttp.ClientError(f"Both sources failed for title {title_num}")

    async def fetch_govinfo_volumes(
        self,
        session: aiohttp.ClientSession,
        year: int,
        title_num: int,
        max_volumes: int = 20
    ) -> list[bytes]:
        """Fetch all CFR volumes for a title from govinfo bulk data.

        Returns list of XML content for each volume.
        """
        volumes = []
        for vol in range(1, max_volumes + 1):
            url = f"{self.GOVINFO_CFR_URL}/{year}/title-{title_num}/CFR-{year}-title{title_num}-vol{vol}.xml"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        volumes.append(await resp.read())
                    elif resp.status == 404:
                        break  # No more volumes
            except Exception:
                break
        return volumes

    async def fetch_chunks_async(
        self,
        title_num: int,
        date: str,
        chunks: list[tuple[str, str]],
        max_concurrent: int = 2,
        delay: float = 0.2
    ) -> list[bytes]:
        """Fetch multiple XML chunks with rate limiting.

        Args:
            title_num: CFR title number.
            date: Date string (YYYY-MM-DD).
            chunks: List of (chunk_type, chunk_id) tuples.
            max_concurrent: Maximum concurrent requests.
            delay: Delay between requests in seconds.

        Returns:
            List of XML content bytes in the same order as chunks.
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        completed_count = [0]
        total = len(chunks)

        async def fetch_one(session, idx, chunk_type, chunk_id):
            url = f"{self.ECFR_BASE_URL}/versioner/v1/full/{date}/title-{title_num}.xml?{chunk_type}={chunk_id}"
            async with semaphore:
                await asyncio.sleep(delay)
                for attempt in range(5):
                    try:
                        async with session.get(url) as response:
                            if response.status == 429:
                                wait_time = 5 * (2 ** attempt)
                                await asyncio.sleep(wait_time)
                                continue
                            response.raise_for_status()
                            content = await response.read()
                            completed_count[0] += 1
                            if completed_count[0] % 50 == 0:
                                print(f"    {completed_count[0]}/{total} parts...", flush=True)
                            return idx, content
                    except aiohttp.ClientError:
                        if attempt == 4:
                            raise
                        await asyncio.sleep(2)
                raise aiohttp.ClientError(f"Failed after 5 attempts: {url}")

        connector = aiohttp.TCPConnector(limit=max_concurrent)
        timeout = aiohttp.ClientTimeout(total=1800)

        results = [None] * len(chunks)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [fetch_one(session, i, ct, cid) for i, (ct, cid) in enumerate(chunks)]
            completed = await asyncio.gather(*tasks, return_exceptions=False)
            for idx, content in completed:
                results[idx] = content

        return results

    def get_title_chunks(self, title_num: int, date: str) -> list[tuple[str, str]]:
        """Get list of chunks (parts) to fetch for a title.

        Returns list of ('part', part_id) tuples.
        """
        structure = self.fetch_title_structure(title_num, date)
        chunks = []

        def find_parts(node):
            if node.get('type') == 'part':
                chunks.append(('part', node.get('identifier')))
            for child in node.get('children', []):
                find_parts(child)

        find_parts(structure)
        return chunks
