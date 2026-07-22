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
