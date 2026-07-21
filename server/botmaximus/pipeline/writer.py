"""Store stage: route envelopes to their collections; quarantine hard failures.

Time-series collections are insert-only, so duplicate protection (e.g. a
reconnect replaying the last closed candle) is done here by tracking the
newest stored event_time per dataset, seeded from the DB at startup.
"""
from __future__ import annotations

import logging
from datetime import datetime

from botmaximus.db.mongo import get_db
from botmaximus.db.schema import DATASET_COLLECTIONS, QUARANTINE
from botmaximus.pipeline.envelope import Envelope
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)


class Writer:
    def __init__(self) -> None:
        self._last_written: dict[str, datetime] = {}

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

        last = self._last_written.get(env.dataset_id)
        if last is not None and env.event_time <= last:
            return  # duplicate/replay — already stored

        await db[DATASET_COLLECTIONS[env.dataset_id]].insert_one(env.to_doc())
        self._last_written[env.dataset_id] = env.event_time
        telemetry.record_stored(env.dataset_id, env.ingest_time, env.event_time)
