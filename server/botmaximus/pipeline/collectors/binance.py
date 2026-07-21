"""Binance combined-stream collector (§11 step 2): btcusdt@kline_1m + btcusdt@aggTrade.

- aggTrade → live price on every trade; enqueued as `btc_price_tick` at most
  once per TICK_THROTTLE_MS.
- kline_1m → live price on every update; enqueued as `btc_ohlcv_1m` only when
  the candle closes (`k.x == true`).
"""
from __future__ import annotations

import asyncio
import json
import time

from botmaximus.config import settings
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.collectors.base import BaseWSCollector
from botmaximus.pipeline.envelope import from_epoch_ms, utcnow
from botmaximus.pipeline.telemetry import telemetry


class BinanceCollector(BaseWSCollector):
    name = "binance"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        sym = settings.symbol.lower()
        url = f"{settings.binance_ws_url}?streams={sym}@kline_1m/{sym}@aggTrade"
        super().__init__(url, gather_q)
        self._last_tick_enqueued = 0.0

    async def handle(self, message: str) -> None:
        collection_time = utcnow()
        msg = json.loads(message)
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if stream.endswith("@aggTrade"):
            telemetry.set_price(float(data["p"]), from_epoch_ms(data["T"]))
            now = time.monotonic()
            if (now - self._last_tick_enqueued) * 1000 >= settings.tick_throttle_ms:
                self._last_tick_enqueued = now
                await self.gather_q.put(RawItem(
                    dataset_id="btc_price_tick", source="binance",
                    symbol=settings.symbol, raw=data, collection_time=collection_time,
                ))

        elif stream.endswith("@kline_1m"):
            k = data.get("k", {})
            if k:
                telemetry.set_price(float(k["c"]), from_epoch_ms(data["E"]))
            if k.get("x"):  # candle closed → store it
                await self.gather_q.put(RawItem(
                    dataset_id="btc_ohlcv_1m", source="binance",
                    symbol=settings.symbol, raw=data, collection_time=collection_time,
                ))
