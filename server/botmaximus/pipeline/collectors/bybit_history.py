"""Bybit V5 REST history: settled funding, open interest, and kline backfill.

Reuses the Binance collectors' bounded-window machinery — only the fetch and the
row shape differ — so the sync logic that was already proven against real gaps
is not reimplemented per venue.

Two Bybit specifics drive everything here:

**Rows come back newest-first.** Binance's funding endpoint returned oldest-
first and its OI endpoint returned the newest rows in a range. Bounded windows
already made ordering irrelevant for the history collectors; the kline
backfiller pages explicitly backwards because it is not window-bounded.

**Open interest reaches back 2+ years** (measured against the live API, not
assumed). Binance capped `openInterestHist` at 30 days, which is why OI-based
strategies became unevaluable the moment a 90-day holdout was sealed — the
search window landed entirely before any OI existed. On Bybit that constraint
is gone and OI can be seeded as deep as the candles.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from botmaximus.config import settings
from botmaximus.pipeline.backfill import MINUTE_MS, OhlcvBackfiller
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.collectors.binance_history import _RestHistoryCollector
from botmaximus.pipeline.envelope import utcnow

log = logging.getLogger(__name__)

#: V5 caps every market history endpoint at 200 rows per request except kline,
#: which allows 1000.
V5_PAGE_LIMIT = 200
V5_KLINE_LIMIT = 1000


def _unwrap(payload: dict, endpoint: str) -> list:
    """V5 wraps everything in {retCode, retMsg, result:{list:[...]}}.

    A non-zero `retCode` arrives with HTTP 200, so `raise_for_status()` sees a
    perfectly successful request. Checking it here is what stops a rate-limit or
    a bad-symbol response from being read as "no rows available" — which would
    look exactly like a genuine gap and get silently backfilled as absence.
    """
    if payload.get("retCode") != 0:
        raise RuntimeError(
            f"bybit {endpoint} retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')!r}")
    return (payload.get("result") or {}).get("list") or []


class BybitFundingHistoryCollector(_RestHistoryCollector):
    """The settled 8h series the cost model charges from."""
    name = "bybit-funding-8h-history"
    dataset_id = "btc_funding_8h"
    source = "bybit"
    time_key = "fundingRateTimestamp"
    page_limit = V5_PAGE_LIMIT
    granularity_ms = 8 * 3600 * 1000
    boot_lookback = timedelta(days=365 * 2)
    topup_s = 3600

    async def _fetch_page(self, client, start_ms, end_ms):
        r = await client.get(
            f"{settings.bybit_rest_url}/v5/market/funding/history",
            params={"category": settings.bybit_category, "symbol": settings.symbol,
                    "startTime": start_ms, "endTime": end_ms,
                    "limit": self.page_limit},
        )
        r.raise_for_status()
        return _unwrap(r.json(), "funding/history")


class BybitOIHistoryCollector(_RestHistoryCollector):
    """5-minute open interest. Unlike Binance, seedable to the full 2 years."""
    name = "bybit-oi-5m-history"
    dataset_id = "btc_oi_5m"
    source = "bybit"
    time_key = "timestamp"
    page_limit = V5_PAGE_LIMIT
    granularity_ms = 5 * 60 * 1000
    boot_lookback = timedelta(days=365 * 2)
    topup_s = 300

    async def _fetch_page(self, client, start_ms, end_ms):
        r = await client.get(
            f"{settings.bybit_rest_url}/v5/market/open-interest",
            params={"category": settings.bybit_category, "symbol": settings.symbol,
                    "intervalTime": "5min", "startTime": start_ms,
                    "endTime": end_ms, "limit": self.page_limit},
        )
        r.raise_for_status()
        return _unwrap(r.json(), "open-interest")


def kline_row_to_ws_shape(row: list) -> dict:
    """REST kline row -> the same dict the websocket `kline` topic delivers.

    `[start, open, high, low, close, volume, turnover]`. Giving REST and
    websocket rows one shape means the parser has a single kline path, so a
    backfilled candle and a live one cannot disagree about what a candle is.
    `confirm` is True because a REST row is closed by definition.
    """
    start = int(row[0])
    return {
        "start": start,
        "end": start + MINUTE_MS - 1,
        "interval": "1",
        "open": row[1], "high": row[2], "low": row[3], "close": row[4],
        "volume": row[5], "turnover": row[6],
        "confirm": True,
        "timestamp": start + MINUTE_MS - 1,
    }


class BybitOhlcvBackfiller(OhlcvBackfiller):
    """Gap healing for 1m candles. Inherits gap detection; replaces the fetch."""

    name = "bybit-ohlcv-backfill"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        super().__init__(gather_q)
        self.url = f"{settings.bybit_rest_url}/v5/market/kline"

    async def _fetch_and_enqueue(self, client: httpx.AsyncClient,
                                 gaps: list[datetime]) -> None:
        """Pages BACKWARDS: V5 returns the newest rows inside the range, so
        walking forward from the oldest gap would re-request the same tail
        forever and never reach the older candles."""
        start_ms = int((gaps[0] - timedelta(milliseconds=MINUTE_MS - 1)).timestamp() * 1000)
        cursor_end = int(gaps[-1].timestamp() * 1000)
        wanted = {int(g.timestamp() * 1000) for g in gaps}
        filled = 0
        while cursor_end >= start_ms:
            r = await client.get(self.url, params={
                "category": settings.bybit_category, "symbol": settings.symbol,
                "interval": "1", "start": start_ms, "end": cursor_end,
                "limit": V5_KLINE_LIMIT,
            })
            r.raise_for_status()
            rows = _unwrap(r.json(), "kline")
            if not rows:
                break
            for row in rows:
                close_ms = int(row[0]) + MINUTE_MS - 1
                if close_ms in wanted:
                    await self.gather_q.put(RawItem(
                        dataset_id="btc_ohlcv_1m", source="bybit",
                        symbol=settings.symbol, raw=kline_row_to_ws_shape(row),
                        collection_time=utcnow(), backfill=True,
                    ))
                    filled += 1
            oldest = min(int(row[0]) for row in rows)
            if oldest <= start_ms:
                break
            cursor_end = oldest - 1
        log.info("[%s] backfilled %d/%d missing candles", self.name, filled, len(gaps))
