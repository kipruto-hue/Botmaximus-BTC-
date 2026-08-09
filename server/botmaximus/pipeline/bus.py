"""Pipeline flow (§2): collectors → gather_q → parse → parse_q → quality →
store_q → writer. Bounded queues give backpressure; every stage is timed
per record into `stage_latency_ms` and the telemetry rollups.

Gather latency is measured receipt→dequeue (includes queue wait), the other
stages as function wall-time. End-to-end freshness = ingest_time − event_time.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from botmaximus.config import settings
from botmaximus.pipeline.envelope import Envelope, utcnow
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


@dataclass
class RawItem:
    dataset_id: str
    source: str
    symbol: str
    raw: dict[str, Any]
    collection_time: datetime
    backfill: bool = False


class Pipeline:
    def __init__(self, parser, gate, writer) -> None:
        self.parser = parser
        self.gate = gate
        self.writer = writer
        self.gather_q: asyncio.Queue[RawItem] = asyncio.Queue(maxsize=settings.queue_maxsize)
        self.parse_q: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=settings.queue_maxsize)
        self.store_q: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=settings.queue_maxsize)
        self._tasks: list[asyncio.Task] = []
        #: Async callbacks fired on each stored, confirmed 1m candle.
        self._bar_close_subs: list = []

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._parse_worker(), name="parse_worker"),
            asyncio.create_task(self._quality_worker(), name="quality_worker"),
            asyncio.create_task(self._store_worker(), name="store_worker"),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _parse_worker(self) -> None:
        while True:
            item = await self.gather_q.get()
            gather_ms = (utcnow() - item.collection_time).total_seconds() * 1000
            t0 = time.perf_counter()
            try:
                env = self.parser.parse(item)
            except Exception:
                log.exception("parse failed for %s", item.dataset_id)
                continue
            parse_ms = (time.perf_counter() - t0) * 1000
            env.stage_latency_ms["gather"] = round(gather_ms, 2)
            env.stage_latency_ms["parse"] = round(parse_ms, 2)
            telemetry.record_stage(env.dataset_id, "gather", gather_ms)
            telemetry.record_stage(env.dataset_id, "parse", parse_ms)
            await self.parse_q.put(env)

    async def _quality_worker(self) -> None:
        while True:
            env = await self.parse_q.get()
            t0 = time.perf_counter()
            try:
                self.gate.check(env)
            except Exception:
                log.exception("quality gate failed for %s", env.dataset_id)
                continue
            quality_ms = (time.perf_counter() - t0) * 1000
            env.stage_latency_ms["quality"] = round(quality_ms, 2)
            telemetry.record_stage(env.dataset_id, "quality", quality_ms)
            await self.store_q.put(env)

    async def _store_worker(self) -> None:
        while True:
            env = await self.store_q.get()
            env.ingest_time = utcnow()
            t0 = time.perf_counter()
            try:
                await self.writer.write(env)
            except Exception:
                log.exception("store failed for %s", env.dataset_id)
                continue
            store_ms = (time.perf_counter() - t0) * 1000
            telemetry.record_stage(env.dataset_id, "store", store_ms)
            await self._emit_bar_close(env)

    # ---- bar-close event (TradeLoop v1.0 §2.2) --------------------------
    def subscribe_bar_close(self, callback) -> None:
        """Register an async callback fired after a closed 1m candle is stored.

        The TradeLoop is event-driven on bar close (§1.6) and there was no such
        event to subscribe to — this pipeline is queues end to end. The event
        fires *after* the write, not before: a decision taken on a bar that
        failed to store would be a decision on data the system does not have.
        """
        self._bar_close_subs.append(callback)

    async def _emit_bar_close(self, env: Envelope) -> None:
        """Notify subscribers that a confirmed 1m candle landed.

        Backfilled candles are excluded: they arrive to heal a hole in history
        and are not news. Firing on them would have the loop evaluate a bar
        from last Tuesday as though it had just closed.

        A subscriber that raises must never break the store path — losing a
        trading decision is survivable, losing the data is not.
        """
        if env.dataset_id != "btc_ohlcv_1m" or env.backfill:
            return
        for cb in self._bar_close_subs:
            try:
                await cb(env)
            except Exception:                           # noqa: BLE001
                log.exception("bar-close subscriber failed for %s",
                              env.event_time)
