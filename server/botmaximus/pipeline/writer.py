r"""Store stage: hand every envelope to the two-store write path.

The routing decision — clean to Postgres + Parquet, failed to quarantine — lives
in `storage/records.py` and is deliberately not repeated here. This module keeps
what is genuinely the writer's job: **not writing the same record twice**, and
reporting store latency to telemetry.

## Dedupe, and why there are still two strategies

Postgres has a real primary key now, so `ON CONFLICT (record_id, event_time) DO
NOTHING` is the backstop. But `record_id` is minted fresh per envelope, so two
observations of the same candle get two ids and the constraint would not catch
them. The in-memory watermark is what actually prevents that, exactly as it did
under Mongo:

- **Live records** arrive in order, so anything at or behind the newest stored
  `event_time` for that dataset is a replay (a reconnect re-sending the last
  closed candle) and is dropped without touching the database.
- **Backfilled records** fill holes *behind* the watermark by definition, so the
  monotonic test cannot apply and existence is checked against the store.

Dropping the watermark and relying on the constraint alone would silently
double-count every reconnect, and the coverage ledger and integrity checks are
both derived from these counts.

## Store latency

Under Mongo a time-series document was immutable, so a record could not carry
its own write duration and each one carried the *previous* write's latency for
its dataset. That constraint is gone — the row is built before it is sent — so
each record now carries its own measured store latency. Telemetry keeps exact
per-record timing either way; this only changes what is stamped in the record.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from botmaximus.db.schema import DATASETS
from botmaximus.pipeline.envelope import Envelope
from botmaximus.pipeline.telemetry import telemetry
from botmaximus.storage import records as store
from botmaximus.storage.venues import venue_of

log = logging.getLogger(__name__)


class Writer:
    def __init__(self) -> None:
        self._last_written: dict[str, datetime] = {}

    async def seed_dedupe(self) -> None:
        """Rebuild the watermark from the store at startup (§3.C).

        In-memory state is a working buffer, never the record of truth, so on
        restart it is rebuilt from what was actually committed rather than
        assumed empty — otherwise the first reconnect after every restart
        re-stores candles the system already has.
        """
        for dataset_id in DATASETS:
            newest = await store.last_event_time(
                dataset_id, venue=venue_of(_source_for(dataset_id)))
            if newest is not None:
                self._last_written[dataset_id] = newest

    async def write(self, env: Envelope) -> None:
        if not env.quarantine_reasons and env.quality_ok:
            last = self._last_written.get(env.dataset_id)
            if env.backfill:
                # Backfill lands behind the watermark, so the monotonic test
                # cannot apply. `ON CONFLICT DO NOTHING` in the write path is
                # what makes a re-offered historical record a no-op.
                pass
            elif last is not None and env.event_time <= last:
                return          # replay of an already-stored record

        t0 = time.perf_counter()
        rec = env.to_record()
        result = await store.write_records([rec])
        env.stage_latency_ms["store"] = round((time.perf_counter() - t0) * 1000, 2)

        if result.quarantined:
            telemetry.record_quarantined()
            log.warning("quarantined %s: %s", env.dataset_id,
                        env.quarantine_reasons or env.quality_flags)
            return

        last = self._last_written.get(env.dataset_id)
        if last is None or env.event_time > last:
            self._last_written[env.dataset_id] = env.event_time
        telemetry.record_stored(env.dataset_id, env.ingest_time, env.event_time,
                                backfill=env.backfill)

    async def write_many(self, envs: list[Envelope]) -> int:
        """Batch store for deep historical backfill.

        Same storage authority as `write`, one round-trip per batch instead of
        per record — at 10^6 candles the per-record path would be millions of
        round-trips. Backfill-only by contract: live records keep the monotonic
        dedupe and per-record timing above.
        """
        if not envs:
            return 0
        if not all(e.backfill for e in envs):
            raise ValueError("write_many is backfill-only (§5.1)")
        datasets = {e.dataset_id for e in envs}
        if len(datasets) != 1:
            raise ValueError(
                f"write_many takes one dataset at a time, got {datasets}")
        dataset_id = envs[0].dataset_id

        # Within-batch duplicates would each get their own record_id and so
        # survive the primary key; collapse them on event_time first.
        seen: set[datetime] = set()
        deduped: list[Envelope] = []
        for e in envs:
            if e.event_time in seen:
                continue
            seen.add(e.event_time)
            deduped.append(e)

        t0 = time.perf_counter()
        result = await store.write_records([e.to_record() for e in deduped])
        elapsed = (time.perf_counter() - t0) * 1000

        if result.quarantined:
            for _ in range(result.quarantined):
                telemetry.record_quarantined()
            log.warning("quarantined %d %s records", result.quarantined,
                        dataset_id)
        if not result.written:
            return 0

        per_record = round(elapsed / max(len(deduped), 1), 2)
        clean = [e for e in deduped
                 if not e.quarantine_reasons and e.quality_ok]
        for e in clean:
            e.stage_latency_ms["store"] = per_record
            telemetry.record_stored(dataset_id, e.ingest_time, e.event_time,
                                    backfill=True)

        if clean:
            newest = max(e.event_time for e in clean)
            last = self._last_written.get(dataset_id)
            if last is None or newest > last:
                self._last_written[dataset_id] = newest
        return result.written


def _source_for(dataset_id: str) -> str:
    """The venue a dataset's records belong to.

    Reads from `settings.venue` rather than from a record, because at seed time
    there is no record yet — this is the question "which venue am I collecting
    for right now", which is exactly what config answers.
    """
    from botmaximus.config import settings
    return settings.venue
