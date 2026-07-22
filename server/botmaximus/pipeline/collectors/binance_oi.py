"""Open-interest collector (§11 step 4). Binance exposes no OI websocket,
so this polls the futures REST endpoint every OI_POLL_S seconds. Transient
failures are logged and retried on the next cycle — a missed poll shows up
as growing freshness in telemetry, which is the correct signal.
"""
from __future__ import annotations

import asyncio
import logging

import httpx

from botmaximus.config import settings
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


class BinanceOICollector:
    name = "binance-oi"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        self.gather_q = gather_q
        self.url = f"{settings.binance_futures_rest_url}/fapi/v1/openInterest"

    async def run(self) -> None:
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                try:
                    r = await client.get(self.url, params={"symbol": settings.symbol})
                    r.raise_for_status()
                    telemetry.counts["gathered"] += 1
                    await self.gather_q.put(RawItem(
                        dataset_id="btc_open_interest", source="binance_futures",
                        symbol=settings.symbol, raw=r.json(), collection_time=utcnow(),
                    ))
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("[%s] poll failed (%s) — retrying next cycle", self.name, e)
                await asyncio.sleep(settings.oi_poll_s)
