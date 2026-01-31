import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp


@dataclass
class SearchResult:
    cluster_id: Optional[int]
    case_name: Optional[str]
    court: Optional[str]
    date_filed: Optional[str]
    opinion_ids: List[int]


class CourtListenerAsyncClient:
    def __init__(self, api_key: str, base_url: str = "https://www.courtlistener.com/api/rest/v4"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "CourtListenerAsyncClient":
        headers = {
            "Authorization": f"Token {self.api_key}",
            "Accept": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=60)
        self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()

    async def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("Client session not initialized")

        backoff = 1
        while True:
            async with self._session.get(url, params=params) as resp:
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                    continue
                resp.raise_for_status()
                return await resp.json()

    async def search_opinions(self, query: str, type_code: str = "o") -> List[SearchResult]:
        url = f"{self.base_url}/search/"
        params = {"q": query, "type": type_code}
        results: List[SearchResult] = []

        next_url: Optional[str] = url
        next_params: Optional[Dict[str, Any]] = params

        while next_url:
            data = await self._get_json(next_url, next_params)
            for item in data.get("results", []):
                opinions = item.get("opinions") or []
                opinion_ids = [op.get("id") for op in opinions if op.get("id")]
                results.append(
                    SearchResult(
                        cluster_id=item.get("cluster_id"),
                        case_name=item.get("caseName"),
                        court=item.get("court"),
                        date_filed=item.get("dateFiled"),
                        opinion_ids=opinion_ids,
                    )
                )
            next_url = data.get("next")
            next_params = None

        return results

    async def fetch_opinion(self, opinion_id: int) -> Dict[str, Any]:
        url = f"{self.base_url}/opinions/{opinion_id}/"
        return await self._get_json(url)


class OpinionMatchExtractor:
    def __init__(
        self,
        client: CourtListenerAsyncClient,
        query: str,
        output_path: str,
        concurrency: int = 10,
    ) -> None:
        self.client = client
        self.query = query
        self.output_path = output_path
        self.concurrency = max(1, concurrency)
        self.pattern = re.compile(r"29\s+CFR\.?", re.IGNORECASE)
        self._sem = asyncio.Semaphore(self.concurrency)
        self._clusters_done = 0
        self._opinions_done = 0
        self._lock = asyncio.Lock()
        self._done_event = asyncio.Event()

    async def _extract_for_opinion(self, opinion_id: int) -> Dict[str, Any]:
        async with self._sem:
            op_data = await self.client.fetch_opinion(opinion_id)
        text = op_data.get("plain_text") or ""
        matches: List[Dict[str, str]] = []
        for m in self.pattern.finditer(text):
            start = max(0, m.start() - 60)
            end = min(len(text), m.end() + 60)
            context = text[start:end].replace("\n", " ")
            matches.append({"match": text[m.start():m.end()], "context": context})
        async with self._lock:
            self._opinions_done += 1
        return {"opinion_id": opinion_id, "match_count": len(matches), "matches": matches}

    async def _extract_for_cluster(self, cluster: SearchResult) -> Dict[str, Any]:
        opinion_records: List[Dict[str, Any]] = []
        if cluster.opinion_ids:
            tasks = [self._extract_for_opinion(op_id) for op_id in cluster.opinion_ids]
            opinion_records = await asyncio.gather(*tasks)

        return {
            "cluster_id": cluster.cluster_id,
            "caseName": cluster.case_name,
            "court": cluster.court,
            "dateFiled": cluster.date_filed,
            "opinions": opinion_records,
        }

    async def _progress_reporter(self, total_clusters: int, total_opinions: int) -> None:
        start = time.time()
        while not self._done_event.is_set():
            async with self._lock:
                clusters_done = self._clusters_done
                opinions_done = self._opinions_done
            elapsed = time.time() - start
            rate = opinions_done / elapsed if elapsed > 0 else 0.0
            print(
                f"progress: clusters {clusters_done}/{total_clusters} | "
                f"opinions {opinions_done}/{total_opinions} | "
                f"{rate:.2f} opinions/s",
                flush=True,
            )
            await asyncio.sleep(1)

    async def run(self) -> None:
        clusters = await self.client.search_opinions(self.query, type_code="o")
        total_clusters = len(clusters)
        total_opinions = sum(len(c.opinion_ids) for c in clusters)
        progress_task = asyncio.create_task(self._progress_reporter(total_clusters, total_opinions))

        # Stream results to disk to avoid holding everything in memory.
        with open(self.output_path, "w", encoding="utf-8") as f:
            for idx, cluster in enumerate(clusters, 1):
                record = await self._extract_for_cluster(cluster)
                f.write(json.dumps(record, ensure_ascii=True) + "\n")
                async with self._lock:
                    self._clusters_done += 1

        self._done_event.set()
        await progress_task


class CitationIndexer:
    def __init__(self, input_path: str, db_path: str) -> None:
        self.input_path = input_path
        self.db_path = db_path
        self._patterns = [
            re.compile(
                r"(?P<title>\d{1,2})\s+C\.?\s*F\.?\s*R\.?\s*"
                r"(?P<kind>§|Sec\.|Section|Part|Pt\.)?\s*"
                r"(?P<num>\d{1,4}(?:\.\d+)*[a-zA-Z]?)"
                r"(?P<rest>(?:\s*[-–]\s*\d{1,4}(?:\.\d+)*[a-zA-Z]?)?"
                r"(?:\s*\([a-zA-Z0-9]+\))*)",
                re.IGNORECASE,
            ),
            re.compile(
                r"(?P<title>\d{1,2})\s+CFR\s*§?\s*"
                r"(?P<num>\d{1,4}(?:\.\d+)*[a-zA-Z]?)"
                r"(?P<rest>(?:\s*[-–]\s*\d{1,4}(?:\.\d+)*[a-zA-Z]?)?"
                r"(?:\s*\([a-zA-Z0-9]+\))*)",
                re.IGNORECASE,
            ),
        ]

    @staticmethod
    def _normalize_citation(match: re.Match) -> str:
        title = match.group("title") if "title" in match.groupdict() else "29"
        kind = (match.group("kind") or "").lower()
        num = match.group("num")
        rest = match.group("rest") or ""

        rest = re.sub(r"\s+", " ", rest)
        rest = rest.replace("–", "-")
        rest = re.sub(r"\s*-\s*", "-", rest)
        rest = re.sub(r"\s+\(", "(", rest)
        rest = re.sub(r"\)\s+", ") ", rest).strip()

        if kind in {"part", "pt."}:
            return f"{title} CFR part {num}{(' ' + rest) if rest else ''}".strip()
        return f"{title} CFR {num}{(' ' + rest) if rest else ''}".strip()

    def _extract_citations(self, text: str) -> List[str]:
        citations: List[str] = []
        seen_spans = set()
        for pattern in self._patterns:
            for match in pattern.finditer(text):
                span = match.span()
                if span in seen_spans:
                    continue
                seen_spans.add(span)
                citations.append(self._normalize_citation(match))
        return citations

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS citations (
                    citation TEXT PRIMARY KEY,
                    count INTEGER NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def index(self) -> None:
        self._init_db()
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            with open(self.input_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    for op in obj.get("opinions", []):
                        for match in op.get("matches", []):
                            context = match.get("context", "")
                            for citation in self._extract_citations(context):
                                cur.execute(
                                    "INSERT INTO citations (citation, count) VALUES (?, 1) "
                                    "ON CONFLICT(citation) DO UPDATE SET count = count + 1",
                                    (citation,),
                                )
            conn.commit()
        finally:
            conn.close()


async def async_main() -> int:
    parser = argparse.ArgumentParser(description="Async CourtListener opinion matcher")
    parser.add_argument("--query", default='"29 CFR."', help="Search query string")
    parser.add_argument(
        "--output",
        default="/tmp/pacer_29_cfr_opinion_matches_async.jsonl",
        help="Output JSONL path",
    )
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent requests")
    parser.add_argument(
        "--db",
        default="/tmp/pacer_29_cfr_citations.db",
        help="SQLite database path for citation counts",
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Only index citations from --output into --db",
    )
    args = parser.parse_args()

    api_key = os.environ.get("COURT_LISTENER_KEY")
    if not api_key:
        print("COURT_LISTENER_KEY not set", file=sys.stderr)
        return 1

    if not args.index_only:
        async with CourtListenerAsyncClient(api_key) as client:
            extractor = OpinionMatchExtractor(
                client=client,
                query=args.query,
                output_path=args.output,
                concurrency=args.concurrency,
            )
            start = time.time()
            await extractor.run()
            elapsed = time.time() - start
            print(f"wrote {args.output} in {elapsed:.1f}s")

    indexer = CitationIndexer(args.output, args.db)
    indexer.index()
    print(f"indexed citations into {args.db}")

    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
