"""Bybit V5 public websocket collectors.

One socket carries every public topic for the symbol (`wss://.../v5/public/linear`),
which is simpler than Binance's routed `/market` + `/public` split. Written
against captured live frames.

Three behaviours have no Binance equivalent and are where the bugs would be:

**`tickers` is snapshot-then-delta.** A delta carries only the fields that
changed — `{"symbol", "tickDirection", "ask1Price", "ask1Size"}` is a complete
message. Parsing one directly would raise KeyError on `fundingRate`, or worse,
silently record a stale funding rate as current. State is merged here so the
parser always receives a full picture.

**`orderbook.50` is snapshot-then-delta too**, with quantity `"0"` meaning
delete a level. The book is maintained in memory and a synthesised snapshot is
emitted on the existing throttle. Bybit has no periodic-snapshot equivalent of
Binance's `depth20@500ms`, so this reconstruction is not optional.

**`kline` publishes the in-progress candle** (`confirm: false`) roughly once a
second. Storing those would fill the series with partial bars that look exactly
like closed ones — every one of them a lookahead artefact. Only `confirm: true`
is enqueued.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

from botmaximus.config import settings
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.collectors.base import BaseWSCollector
from botmaximus.pipeline.envelope import from_epoch_ms, utcnow
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


class BybitPublicCollector(BaseWSCollector):
    """Trades, klines, funding/OI (tickers), liquidations and the order book."""

    name = "bybit-public"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        super().__init__(settings.bybit_ws_url, gather_q)
        sym = settings.symbol
        self.topics = [
            f"publicTrade.{sym}",
            f"kline.1.{sym}",
            f"tickers.{sym}",
            f"allLiquidation.{sym}",
            f"orderbook.{settings.bybit_orderbook_depth}.{sym}",
        ]
        self._ticker: dict = {}          # merged `tickers` state
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._book_ready = False
        self._last_tick = 0.0
        self._last_funding = 0.0
        self._last_book = 0.0
        self._last_oi = 0.0

    async def on_connect(self, ws) -> None:
        await ws.send(json.dumps({"op": "subscribe", "args": self.topics}))

    def _reset_state(self) -> None:
        """A reconnect invalidates every incremental structure. Carrying a book
        or a ticker snapshot across a gap would serve stale levels as live
        ones — the exact class of failure a reconnect is supposed to end."""
        self._ticker = {}
        self._bids.clear()
        self._asks.clear()
        self._book_ready = False

    async def handle(self, message: str) -> None:
        collection_time = utcnow()
        msg = json.loads(message)

        if msg.get("op") == "subscribe":
            if not msg.get("success"):
                # Loud on purpose: an unacked subscription is the silent-death
                # failure mode. Binance once acked success and sent nothing;
                # here a refusal at least gets said out loud.
                log.error("[%s] SUBSCRIBE REJECTED: %s", self.name, msg)
            else:
                log.info("[%s] subscribed to %s", self.name, ", ".join(self.topics))
            return

        topic = msg.get("topic")
        if not topic:
            return
        kind = topic.split(".")[0]

        if kind == "publicTrade":
            await self._on_trades(msg, collection_time)
        elif kind == "kline":
            await self._on_kline(msg, collection_time)
        elif kind == "tickers":
            await self._on_ticker(msg, collection_time)
        elif kind == "allLiquidation":
            await self._on_liquidation(msg, collection_time)
        elif kind == "orderbook":
            await self._on_book(msg, collection_time)

    # ---- topics ----
    async def _on_trades(self, msg: dict, collection_time) -> None:
        rows = msg.get("data") or []
        if not rows:
            return
        # The live price is set from EVERY batch, before the storage throttle.
        # Throttling exists to bound what is written to Mongo, not what the
        # dashboard displays — gating the price on it would make the ticker lag
        # by up to a second for no benefit.
        telemetry.set_price(float(rows[-1]["p"]), from_epoch_ms(int(rows[-1]["T"])))

        now = time.monotonic()
        if (now - self._last_tick) * 1000 < settings.tick_throttle_ms:
            return
        self._last_tick = now
        # One stored tick per throttle window: the last trade in the batch is
        # the most recent price, and storing the whole batch would blow past
        # the throttle the retention policy is sized around.
        await self.gather_q.put(RawItem(
            dataset_id="btc_price_tick", source="bybit",
            symbol=settings.symbol, raw=rows[-1], collection_time=collection_time,
        ))

    async def _on_kline(self, msg: dict, collection_time) -> None:
        for row in msg.get("data") or []:
            # The in-progress bar is not storable, but its close is a perfectly
            # good live price — and on a quiet tape it refreshes when no trade
            # has printed for a while.
            telemetry.set_price(float(row["close"]), from_epoch_ms(int(row["timestamp"])))
            if not row.get("confirm"):
                continue                    # in-progress bar, not a fact yet
            await self.gather_q.put(RawItem(
                dataset_id="btc_ohlcv_1m", source="bybit",
                symbol=settings.symbol, raw=row, collection_time=collection_time,
            ))

    async def _on_ticker(self, msg: dict, collection_time) -> None:
        data = msg.get("data") or {}
        if msg.get("type") == "snapshot":
            self._ticker = dict(data)
        else:
            self._ticker.update(data)       # delta: changed fields only
        self._ticker["_ts"] = msg.get("ts")

        # Funding and OI ride the same topic but have their own budgets and
        # retention, so they are throttled independently rather than sharing one
        # clock and pulling each other off cadence.
        now = time.monotonic()
        if "fundingRate" in self._ticker and "markPrice" in self._ticker and \
                "indexPrice" in self._ticker and "nextFundingTime" in self._ticker:
            if (now - self._last_funding) * 1000 >= settings.funding_throttle_ms:
                self._last_funding = now
                await self.gather_q.put(RawItem(
                    dataset_id="btc_funding", source="bybit",
                    symbol=settings.symbol, raw=dict(self._ticker),
                    collection_time=collection_time,
                ))
        if "openInterest" in self._ticker:
            if (now - self._last_oi) * 1000 >= settings.oi_poll_s * 1000:
                self._last_oi = now
                await self.gather_q.put(RawItem(
                    dataset_id="btc_open_interest", source="bybit",
                    symbol=settings.symbol, raw=dict(self._ticker),
                    collection_time=collection_time,
                ))

    async def _on_liquidation(self, msg: dict, collection_time) -> None:
        for row in msg.get("data") or []:
            await self.gather_q.put(RawItem(
                dataset_id="btc_liquidation", source="bybit",
                symbol=settings.symbol, raw=row, collection_time=collection_time,
            ))

    async def _on_book(self, msg: dict, collection_time) -> None:
        data = msg.get("data") or {}
        if msg.get("type") == "snapshot":
            self._bids = {float(p): float(q) for p, q in data.get("b", [])}
            self._asks = {float(p): float(q) for p, q in data.get("a", [])}
            self._book_ready = True
        else:
            if not self._book_ready:
                # A delta before any snapshot cannot be applied to anything.
                # Dropping it is correct; the next snapshot re-seeds the book.
                return
            for p, q in data.get("b", []):
                price, qty = float(p), float(q)
                if qty == 0:
                    self._bids.pop(price, None)
                else:
                    self._bids[price] = qty
            for p, q in data.get("a", []):
                price, qty = float(p), float(q)
                if qty == 0:
                    self._asks.pop(price, None)
                else:
                    self._asks[price] = qty

        now = time.monotonic()
        if (now - self._last_book) * 1000 < settings.orderbook_throttle_ms:
            return
        if not self._bids or not self._asks:
            return
        self._last_book = now

        depth = settings.bybit_orderbook_store_levels
        bids = sorted(self._bids.items(), key=lambda kv: -kv[0])[:depth]
        asks = sorted(self._asks.items(), key=lambda kv: kv[0])[:depth]
        await self.gather_q.put(RawItem(
            dataset_id="btc_orderbook", source="bybit", symbol=settings.symbol,
            raw={"b": [[p, q] for p, q in bids],
                 "a": [[p, q] for p, q in asks],
                 "E": msg.get("ts")},
            collection_time=collection_time,
        ))
