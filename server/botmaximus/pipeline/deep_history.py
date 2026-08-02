"""Deep 1m OHLCV history pull — a Pass C prerequisite, not a live pipeline task.

The periodic `OhlcvBackfiller` heals holes in a recent window. This module does
the one-off job of seeding *years* of candles so the validation gate (§5.5) has
enough history to mean anything: 30 trades across ≥2 regimes and 4 walk-forward
folds is unreachable on the 7-day window the live pipeline maintains.

It is the same ingest path — same `BinanceParser`, same `QualityGate`, same
`backfill=True` semantics — with storage batched through `Writer.write_many`.
The gate is stateless for backfilled records (it skips the staleness and
continuity rules and does not update its `_prev_*` maps), so batching cannot
change a verdict.

Idempotent and resumable: already-stored minutes are skipped without an HTTP
request, so a re-run after an interruption costs one index scan.

    python -m botmaximus.pipeline.deep_history --days 730
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from botmaximus.config import settings
from botmaximus.db.mongo import get_db
from botmaximus.db.schema import DATASET_COLLECTIONS, ensure_schema
from botmaximus.pipeline.backfill import MINUTE_MS, kline_row_to_ws_shape, minute_close
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.parsers.binance import BinanceParser
from botmaximus.pipeline.quality.gate import QualityGate
from botmaximus.pipeline.writer import Writer

log = logging.getLogger(__name__)

PAGE = 1000                     # Binance kline cap per request
PAGE_MS = PAGE * MINUTE_MS
COMMIT_EVERY = 20_000           # envelopes buffered before a batch insert


class DeepOhlcvHistory:
    name = "deep-ohlcv-history"
    dataset_id = "btc_ohlcv_1m"

    def __init__(self, days: int) -> None:
        self.days = days
        self.url = f"{settings.binance_rest_url}/api/v3/klines"
        self.parser = BinanceParser()
        self.gate = QualityGate()
        self.writer = Writer()

    async def _existing_closes(self, start: datetime, end: datetime) -> set[datetime]:
        """Every candle close already stored in the window, in one index scan."""
        cursor = get_db()[DATASET_COLLECTIONS[self.dataset_id]].find(
            {"meta.dataset_id": self.dataset_id,
             "event_time": {"$gte": start, "$lte": end}},
            {"event_time": 1},
        )
        return {d["event_time"] async for d in cursor}

    async def _fetch(self, client: httpx.AsyncClient, start_ms: int, end_ms: int) -> list:
        """One page, retrying on rate limit. 429 carries Retry-After; 418 means
        we have already been banned, so back off hard rather than hammer."""
        for attempt in range(6):
            r = await client.get(self.url, params={
                "symbol": settings.symbol, "interval": "1m",
                "startTime": start_ms, "endTime": end_ms, "limit": PAGE,
            })
            if r.status_code in (429, 418):
                wait = float(r.headers.get("Retry-After", 2 ** attempt))
                log.warning("[%s] rate limited (%d) — sleeping %.0fs",
                            self.name, r.status_code, wait)
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"{self.name}: rate limited past retry budget")

    async def run(self) -> dict:
        await ensure_schema()
        now = datetime.now(timezone.utc)
        end = minute_close(now - timedelta(minutes=2))      # exclude the open minute
        start = minute_close(now - timedelta(days=self.days))

        log.info("[%s] target window %s → %s (%d days)",
                 self.name, start.isoformat(), end.isoformat(), self.days)
        existing = await self._existing_closes(start, end)
        log.info("[%s] %d candles already stored in window", self.name, len(existing))

        buf: list = []
        stored = requests = skipped_pages = 0
        cursor_ms = int(start.timestamp() * 1000) - (MINUTE_MS - 1)
        end_ms = int(end.timestamp() * 1000)

        async with httpx.AsyncClient(timeout=30) as client:
            while cursor_ms <= end_ms:
                page_end_ms = min(end_ms, cursor_ms + PAGE_MS - 1)

                # every close time this page would cover; if all are present,
                # the request is pure waste — this is what makes re-runs cheap
                wanted = set()
                t = minute_close(datetime.fromtimestamp(cursor_ms / 1000, tz=timezone.utc))
                page_end = datetime.fromtimestamp(page_end_ms / 1000, tz=timezone.utc)
                while t <= page_end:
                    if t not in existing:
                        wanted.add(t)
                    t += timedelta(milliseconds=MINUTE_MS)
                if not wanted:
                    skipped_pages += 1
                    cursor_ms = page_end_ms + 1
                    continue

                rows = await self._fetch(client, cursor_ms, page_end_ms)
                requests += 1
                if not rows:
                    cursor_ms = page_end_ms + 1
                    continue

                for row in rows:
                    close_t = datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc)
                    if close_t not in wanted:
                        continue
                    item = RawItem(
                        dataset_id=self.dataset_id, source="binance",
                        symbol=settings.symbol, raw=kline_row_to_ws_shape(row),
                        collection_time=utcnow(), backfill=True,
                    )
                    env = self.gate.check(self.parser.parse(item))
                    env.ingest_time = utcnow()
                    buf.append(env)

                if len(buf) >= COMMIT_EVERY:
                    stored += await self.writer.write_many(buf)
                    buf = []
                    log.info("[%s] %d stored · %d requests · cursor %s",
                             self.name, stored, requests,
                             datetime.fromtimestamp(cursor_ms / 1000,
                                                    tz=timezone.utc).date().isoformat())

                cursor_ms = rows[-1][6] + 1 if rows[-1][6] >= cursor_ms else page_end_ms + 1

        if buf:
            stored += await self.writer.write_many(buf)

        summary = {"stored": stored, "requests": requests,
                   "pages_skipped": skipped_pages,
                   "already_present": len(existing),
                   "window": [start.isoformat(), end.isoformat()]}
        log.info("[%s] done: %s", self.name, summary)
        return summary


async def _main(days: int) -> None:
    from botmaximus.db import mongo
    try:
        result = await DeepOhlcvHistory(days).run()
        total = await get_db()[DATASET_COLLECTIONS["btc_ohlcv_1m"]].estimated_document_count()
        log.info("btc_ohlcv_1m total documents: %d (this run stored %d)",
                 total, result["stored"])
    finally:
        await mongo.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="One-shot deep 1m OHLCV backfill")
    ap.add_argument("--days", type=int, default=730, help="how far back to pull")
    asyncio.run(_main(ap.parse_args().days))
