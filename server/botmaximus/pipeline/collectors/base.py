"""Abstract websocket collector: connect, receive, stamp collection_time,
hand off to `handle()`. Reconnects forever with exponential backoff + jitter.
"""
from __future__ import annotations

import abc
import asyncio
import logging
import random

import websockets

from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


class BaseWSCollector(abc.ABC):
    name = "base"

    def __init__(self, url: str, gather_q: asyncio.Queue) -> None:
        self.url = url
        self.gather_q = gather_q

    @abc.abstractmethod
    async def handle(self, message: str) -> None:
        """Parse routing info from one ws message and enqueue RawItem(s)."""

    async def on_connect(self, ws) -> None:
        """Sent immediately after the socket opens. Binance encodes its streams
        in the URL and needs nothing; Bybit subscribes with a frame."""

    def _reset_state(self) -> None:
        """Drop anything accumulated from the previous connection.

        Called before every connect, including the first. A collector that
        maintains incremental state — an order book built from deltas, a merged
        ticker snapshot — must not carry it across a gap: the levels on the far
        side are stale, and serving them as live is precisely the failure a
        reconnect exists to end.
        """

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                self._reset_state()
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
                    log.info("[%s] connected to %s", self.name, self.url)
                    await self.on_connect(ws)
                    telemetry.set_ws(self.name, True)
                    backoff = 1.0
                    async for message in ws:
                        telemetry.counts["gathered"] += 1
                        await self.handle(message)
            except asyncio.CancelledError:
                telemetry.set_ws(self.name, False)
                raise
            except Exception as e:
                telemetry.set_ws(self.name, False)
                telemetry.ws_reconnects += 1
                delay = backoff + random.uniform(0, backoff / 2)
                log.warning("[%s] connection lost (%s) — reconnecting in %.1fs", self.name, e, delay)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, 60.0)
