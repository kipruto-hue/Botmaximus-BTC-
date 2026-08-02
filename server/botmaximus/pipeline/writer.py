"""Store stage: route envelopes to their collections; quarantine hard failures.

Time-series collections are insert-only, so duplicate protection (e.g. a
reconnect replaying the last closed candle) is done here by tracking the
newest stored event_time per dataset, seeded from the DB at startup.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from botmaximus.db.mongo import get_db
from botmaximus.db.schema import DATASET_COLLECTIONS, QUARANTINE
from botmaximus.pipeline.envelope import Envelope
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


class Writer:
    def __init__(self) -> None:
        self._last_written: dict[str, datetime] = {}
        self._last_store_ms: dict[str, float] = {}

    async def seed_dedupe(self) -> None:
        db = get_db()
        for dataset_id, coll in DATASET_COLLECTIONS.items():
            doc = await db[coll].find_one(
                {"meta.dataset_id": dataset_id}, sort=[("event_time", -1)]
            )
            if doc:
                self._last_written[dataset_id] = doc["event_time"]

    async def write(self, env: Envelope) -> None:
        db = get_db()

        if env.quarantine_reasons:
            doc = env.to_doc()
            doc["quarantine_reasons"] = env.quarantine_reasons
            await db[QUARANTINE].insert_one(doc)
            telemetry.record_quarantined()
            log.warning("quarantined %s: %s", env.dataset_id, env.quarantine_reasons)
            return

        coll = DATASET_COLLECTIONS[env.dataset_id]
        last = self._last_written.get(env.dataset_id)
        if env.backfill:
            # backfill fills holes *behind* the newest record, so the monotonic
            # check can't apply — dedupe against the DB directly (rare, cheap)
            exists = await db[coll].find_one(
                {"meta.dataset_id": env.dataset_id, "event_time": env.event_time}
            )
            if exists:
                return
        elif last is not None and env.event_time <= last:
            return  # duplicate/replay — already stored

        # A record's own write duration can't be embedded in itself (time-series
        # docs are immutable), so each doc carries the previous write's measured
        # latency for its dataset; telemetry gets exact per-record timing in bus.py.
        env.stage_latency_ms["store"] = self._last_store_ms.get(env.dataset_id, 0.0)
        t0 = time.perf_counter()
        await db[coll].insert_one(env.to_doc())
        self._last_store_ms[env.dataset_id] = round((time.perf_counter() - t0) * 1000, 2)
        if last is None or env.event_time > last:
            self._last_written[env.dataset_id] = env.event_time
        telemetry.record_stored(env.dataset_id, env.ingest_time, env.event_time, backfill=env.backfill)

    async def write_many(self, envs: list[Envelope]) -> int:
        """Batch store for deep historical backfill — same storage authority as
        `write`, but one existence query and one insert per batch instead of per
        record. The per-record path's find_one dedupe is fine for healing a
        handful of gaps; at 10^6 candles it would be 2M round-trips.

        Backfill-only by contract: live records must keep the monotonic dedupe
        and per-record timing in `write`.
        """
        if not envs:
            return 0
        if not all(e.backfill for e in envs):
            raise ValueError("write_many is backfill-only (§5.1)")
        datasets = {e.dataset_id for e in envs}
        if len(datasets) != 1:
            raise ValueError(f"write_many takes one dataset at a time, got {datasets}")

        db = get_db()
        dataset_id = envs[0].dataset_id

        quarantined = [e for e in envs if e.quarantine_reasons]
        if quarantined:
            docs = []
            for e in quarantined:
                d = e.to_doc()
                d["quarantine_reasons"] = e.quarantine_reasons
                docs.append(d)
            await db[QUARANTINE].insert_many(docs)
            for _ in docs:
                telemetry.record_quarantined()
            log.warning("quarantined %d %s records", len(docs), dataset_id)

        clean = [e for e in envs if not e.quarantine_reasons]
        if not clean:
            return 0

        coll = DATASET_COLLECTIONS[dataset_id]
        lo = min(e.event_time for e in clean)
        hi = max(e.event_time for e in clean)
        cursor = db[coll].find(
            {"meta.dataset_id": dataset_id, "event_time": {"$gte": lo, "$lte": hi}},
            {"event_time": 1},
        )
        existing = {d["event_time"] async for d in cursor}

        fresh, seen = [], set()
        for e in clean:
            if e.event_time in existing or e.event_time in seen:
                continue        # already stored, or duplicated within this batch
            seen.add(e.event_time)
            e.stage_latency_ms["store"] = self._last_store_ms.get(dataset_id, 0.0)
            fresh.append(e)
        if not fresh:
            return 0

        t0 = time.perf_counter()
        await db[coll].insert_many([e.to_doc() for e in fresh])
        elapsed = (time.perf_counter() - t0) * 1000
        self._last_store_ms[dataset_id] = round(elapsed / len(fresh), 2)

        newest = max(e.event_time for e in fresh)
        last = self._last_written.get(dataset_id)
        if last is None or newest > last:
            self._last_written[dataset_id] = newest
        for e in fresh:
            telemetry.record_stored(dataset_id, e.ingest_time, e.event_time, backfill=True)
        return len(fresh)
