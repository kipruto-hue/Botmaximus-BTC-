"""Binance USDⓈ-M futures collectors (§11 step 4).

Since Binance's 2026-04-23 websocket migration, futures streams are routed:
`/market` carries markPrice + forceOrder (and other regular feeds), `/public`
carries depth/bookTicker. A combined stream cannot cross paths — an unrouted
connection silently pushes nothing for /market streams — so funding and
liquidations ride one socket and the order book another.

Futures streams are used (not spot) because they carry exchange event
timestamps on every message — §1.1 forbids deriving event_time from the
local clock, and the spot partial-depth stream has no timestamp. The
derivatives context (funding, liquidations, leveraged book) is what these
feeds exist to capture anyway.

- markPrice@1s  → funding rate + mark/index price; stored at most once per
  FUNDING_THROTTLE_MS (funding moves slowly, the stream is 1/s).
- forceOrder    → every liquidation, stored as-is (sparse, event-driven).
- depth20@500ms → book snapshot; stored at most once per ORDERBOOK_THROTTLE_MS.
"""
from __future__ import annotations

import asyncio
import json
import time

from botmaximus.config import settings
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.collectors.base import BaseWSCollector
from botmaximus.pipeline.envelope import utcnow


class BinanceFuturesMarketCollector(BaseWSCollector):
    """/market route: funding (markPrice@1s) + liquidations (forceOrder)."""

    name = "binance-futures-market"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        sym = settings.symbol.lower()
        url = (
            f"{settings.binance_futures_ws_url}/market/stream"
            f"?streams={sym}@markPrice@1s/{sym}@forceOrder"
        )
        super().__init__(url, gather_q)
        self._last_funding_enqueued = 0.0

    async def handle(self, message: str) -> None:
        collection_time = utcnow()
        msg = json.loads(message)
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if "@markPrice" in stream:
            now = time.monotonic()
            if (now - self._last_funding_enqueued) * 1000 >= settings.funding_throttle_ms:
                self._last_funding_enqueued = now
                await self.gather_q.put(RawItem(
                    dataset_id="btc_funding", source="binance_futures",
                    symbol=settings.symbol, raw=data, collection_time=collection_time,
                ))

        elif "@forceOrder" in stream:
            await self.gather_q.put(RawItem(
                dataset_id="btc_liquidation", source="binance_futures",
                symbol=settings.symbol, raw=data, collection_time=collection_time,
            ))


class BinanceFuturesDepthCollector(BaseWSCollector):
    """/public route: order-book snapshots (depth20@500ms)."""

    name = "binance-futures-depth"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        sym = settings.symbol.lower()
        url = f"{settings.binance_futures_ws_url}/public/stream?streams={sym}@depth20@500ms"
        super().__init__(url, gather_q)
        self._last_book_enqueued = 0.0

    async def handle(self, message: str) -> None:
        collection_time = utcnow()
        now = time.monotonic()
        if (now - self._last_book_enqueued) * 1000 < settings.orderbook_throttle_ms:
            return
        msg = json.loads(message)
        data = msg.get("data", {})
        if not data.get("b") or not data.get("a"):
            return
        self._last_book_enqueued = now
        await self.gather_q.put(RawItem(
            dataset_id="btc_orderbook", source="binance_futures",
            symbol=settings.symbol, raw=data, collection_time=collection_time,
        ))
