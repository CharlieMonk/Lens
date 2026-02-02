"""HTTP client for eCFR and govinfo APIs."""

import asyncio
import time
import aiohttp
import requests


class ECFRClient:
    """Handles all API requests to ecfr.gov and govinfo.gov."""

    ECFR_BASE = "https://www.ecfr.gov/api"
    GOVINFO_ECFR = "https://www.govinfo.gov/bulkdata/ECFR"
    GOVINFO_CFR = "https://www.govinfo.gov/bulkdata/CFR"

    def __init__(self, max_retries: int = 7, retry_delay: int = 3):
        self.max_retries, self.retry_delay = max_retries, retry_delay

    def _request_with_retry(self, url: str, timeout: int = 30, retry_on_timeout: bool = True) -> requests.Response:
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
        return self._request_with_retry(f"{self.ECFR_BASE}/versioner/v1/titles.json").json()["titles"]

    def fetch_agencies(self) -> list[dict]:
        return self._request_with_retry(f"{self.ECFR_BASE}/admin/v1/agencies.json").json().get("agencies", [])

    def fetch_title_xml(self, title_num: int, date: str, timeout: int = 60) -> bytes:
        return self._request_with_retry(f"{self.ECFR_BASE}/versioner/v1/full/{date}/title-{title_num}.xml", timeout=timeout, retry_on_timeout=False).content

    def fetch_title_structure(self, title_num: int, date: str) -> dict:
        return self._request_with_retry(f"{self.ECFR_BASE}/versioner/v1/structure/{date}/title-{title_num}.json", timeout=120).json()

    async def fetch_title_structure_async(self, session: aiohttp.ClientSession, title_num: int, date: str, timeout: int = 60) -> list[tuple]:
        """Fetch structure and flatten to list of (node_type, identifier, parent_type, parent_identifier, label, reserved)."""
        import json
        url = f"{self.ECFR_BASE}/versioner/v1/structure/{date}/title-{title_num}.json"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    return []
                # Use content_type=None to handle server returning wrong mimetype
                data = json.loads(await resp.read())
                return self._flatten_structure(data)
        except Exception:
            return []

    def _flatten_structure(self, node, parent_type=None, parent_identifier=None) -> list[tuple]:
        """Flatten nested structure to list of tuples."""
        result = [(node.get("type"), node.get("identifier"), parent_type, parent_identifier, node.get("label_description", ""), node.get("reserved", False))]
        for child in node.get("children", []):
            result.extend(self._flatten_structure(child, node.get("type"), node.get("identifier")))
        return result

    async def fetch_title_racing(self, session: aiohttp.ClientSession, title_num: int, date: str, timeout: int = 120) -> tuple[str, bytes]:
        """Fetch title from both eCFR and govinfo in parallel, return first success."""
        async def fetch(url, source):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return source, await resp.read()
            except Exception:
                pass
            return None

        tasks = [
            asyncio.create_task(fetch(f"{self.ECFR_BASE}/versioner/v1/full/{date}/title-{title_num}.xml", "ecfr")),
            asyncio.create_task(fetch(f"{self.GOVINFO_ECFR}/title-{title_num}/ECFR-title{title_num}.xml", "govinfo"))
        ]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return result
        raise aiohttp.ClientError(f"Both sources failed for title {title_num}")

    async def fetch_govinfo_volumes(self, session: aiohttp.ClientSession, year: int, title_num: int, max_volumes: int = 20) -> list[bytes]:
        """Fetch all CFR volumes for a title from govinfo bulk data."""
        volumes = []
        for vol in range(1, max_volumes + 1):
            url = f"{self.GOVINFO_CFR}/{year}/title-{title_num}/CFR-{year}-title{title_num}-vol{vol}.xml"
            for attempt in range(3):
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
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

    async def fetch_chunks_async(self, title_num: int, date: str, chunks: list[tuple[str, str]], max_concurrent: int = 2, delay: float = 0.2) -> list[bytes]:
        """Fetch multiple XML chunks with rate limiting."""
        semaphore = asyncio.Semaphore(max_concurrent)
        completed, total = [0], len(chunks)

        async def fetch_one(session, idx, chunk_type, chunk_id):
            url = f"{self.ECFR_BASE}/versioner/v1/full/{date}/title-{title_num}.xml?{chunk_type}={chunk_id}"
            async with semaphore:
                await asyncio.sleep(delay)
                for attempt in range(5):
                    try:
                        async with session.get(url) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(5 * (2 ** attempt))
                                continue
                            resp.raise_for_status()
                            completed[0] += 1
                            if completed[0] % 50 == 0:
                                print(f"    {completed[0]}/{total} parts...", flush=True)
                            return idx, await resp.read()
                    except aiohttp.ClientError:
                        if attempt == 4:
                            raise
                        await asyncio.sleep(2)
                raise aiohttp.ClientError(f"Failed after 5 attempts: {url}")

        results = [None] * len(chunks)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=max_concurrent), timeout=aiohttp.ClientTimeout(total=1800)) as session:
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
