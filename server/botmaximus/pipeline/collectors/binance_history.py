"""REST history collectors (Decision & Execution Master Prompt §5.1, §5.3).

Two settled series the backtester and cost model need, neither available live:
- funding-8h: the actual settled funding rates the cost model charges from
  (`/fapi/v1/fundingRate`). Deep history is cheap (~3 rows/day).
- OI-5m: 5-minute open-interest history (`/futures/data/openInterestHist`),
  which Binance caps at the last 30 days — so we start accumulating now and
  the usable window grows forward.

Both inject through the normal pipeline flagged `backfill=True`: the gate
skips staleness/continuity and the writer dedupes by existence, exactly like
OHLCV backfill. Periodic top-up keeps them current; boot does the deep fill.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from botmaximus.config import settings
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


class _RestHistoryCollector:
    """Shared bounded-window sync from last-stored → now.

    Each request covers a window of at most `page_limit` rows
    (`page_limit * granularity_ms`). Keeping every window inside the page
    limit means the row ordering the endpoint uses does not matter — the
    funding endpoint returns oldest-first from startTime, the OI endpoint
    returns the newest rows in a range, and bounded windows capture the full
    span either way.
    """
    name = "history"
    dataset_id = ""
    source = "binance_futures"
    time_key = ""                 # field in each row holding the epoch-ms timestamp
    page_limit = 1000
    granularity_ms = 0            # nominal spacing between rows
    boot_lookback: timedelta = timedelta(days=30)
    topup_s = 3600

    def __init__(self, gather_q: asyncio.Queue) -> None:
        self.gather_q = gather_q

    async def _last_stored_ms(self) -> int | None:
        from botmaximus.db.mongo import get_db
        from botmaximus.db.schema import DATASET_COLLECTIONS
        doc = await get_db()[DATASET_COLLECTIONS[self.dataset_id]].find_one(
            {}, sort=[("event_time", -1)], projection={"event_time": 1}
        )
        return int(doc["event_time"].timestamp() * 1000) if doc else None

    async def _fetch_page(self, client: httpx.AsyncClient, start_ms: int, end_ms: int) -> list:
        raise NotImplementedError

    async def _sync(self, client: httpx.AsyncClient) -> int:
        last = await self._last_stored_ms()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = (last + 1) if last is not None else int(
            (datetime.now(timezone.utc) - self.boot_lookback).timestamp() * 1000
        )
        window = self.page_limit * self.granularity_ms
        stored = 0
        cursor = start_ms
        while cursor < now_ms:
            end = min(now_ms, cursor + window)
            rows = await self._fetch_page(client, cursor, end)
            for row in rows:
                await self.gather_q.put(RawItem(
                    dataset_id=self.dataset_id, source=self.source, symbol=settings.symbol,
                    raw=row, collection_time=utcnow(), backfill=True,
                ))
                telemetry.counts["gathered"] += 1
                stored += 1
            cursor = end + 1
        if stored:
            log.info("[%s] synced %d rows", self.name, stored)
        return stored

    async def run(self) -> None:
        async with httpx.AsyncClient(timeout=20) as client:
            while True:
                try:
                    await self._sync(client)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("[%s] sync failed (%s) — retrying next cycle", self.name, e)
                await asyncio.sleep(self.topup_s)


class FundingHistoryCollector(_RestHistoryCollector):
    name = "funding-8h-history"
    dataset_id = "btc_funding_8h"
    time_key = "fundingTime"
    page_limit = 1000
    granularity_ms = 8 * 3600 * 1000
    boot_lookback = timedelta(days=365 * 2)   # matches the funding TTL; deeper just expires
    topup_s = 3600

    async def _fetch_page(self, client, start_ms, end_ms):
        r = await client.get(
            f"{settings.binance_futures_rest_url}/fapi/v1/fundingRate",
            params={"symbol": settings.symbol, "startTime": start_ms,
                    "endTime": end_ms, "limit": self.page_limit},
        )
        r.raise_for_status()
        return r.json()


class OIHistoryCollector(_RestHistoryCollector):
    name = "oi-5m-history"
    dataset_id = "btc_oi_5m"
    time_key = "timestamp"
    page_limit = 500
    granularity_ms = 5 * 60 * 1000
    # Binance rejects startTime at/over 30 days; stay safely inside the window
    boot_lookback = timedelta(days=29, hours=12)
    topup_s = 300

    async def _fetch_page(self, client, start_ms, end_ms):
        r = await client.get(
            f"{settings.binance_futures_rest_url}/futures/data/openInterestHist",
            params={"symbol": settings.symbol, "period": "5m", "startTime": start_ms,
                    "endTime": end_ms, "limit": self.page_limit},
        )
        r.raise_for_status()
        return r.json()
