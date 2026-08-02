"""Coverage ledger (Decision & Execution Master Prompt §5.1).

A per-feed, per-minute record of whether data actually exists. The backtester
(Pass B) consults this and refuses or flags any evaluation window where a
required feed has holes — without it, backtests silently succeed on phantom
coverage (§2.4, and failure-mode "silent data gaps corrupting statistics").

Two kinds of feed:
- Record-backed (OHLCV, funding-8h, OI-5m): a stored record for a minute IS
  coverage. Reconciled from the collections themselves.
- Event-driven (ticks, order book, liquidations): SILENCE IS NOT ABSENCE — a
  minute with no liquidation is complete data, not a gap. Coverage is whether
  the collector socket was up, recorded by a per-minute heartbeat while the
  source's ws is connected. A minute with no heartbeat is `missing` forever
  (liquidations/book have no history endpoint).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from botmaximus.db.mongo import get_db
from botmaximus.pipeline.telemetry import telemetry

log = logging.getLogger(__name__)

COVERAGE_COLLECTION = "coverage"

COMPLETE = "complete"
PARTIAL = "partial"
MISSING = "missing"

# feed → the ws source (telemetry.ws_sources key) whose uptime defines its coverage
EVENT_DRIVEN_FEEDS = {
    "btc_price_tick": "binance",
    "btc_liquidation": "binance-futures-market",
    "btc_orderbook": "binance-futures-depth",
}
# feed → collection, granularity in seconds (a stored record = coverage for that slot)
RECORD_BACKED_FEEDS = {
    "btc_ohlcv_1m": ("btc_ohlcv_1m", 60),
    "btc_funding_8h": ("btc_funding_8h", 8 * 3600),
    "btc_oi_5m": ("btc_oi_5m", 300),
}


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def snap_slot(dt: datetime, granularity_s: int) -> datetime:
    """Snap a timestamp down to its granularity boundary. Reconcile and gaps()
    MUST use this identically or their slot grids won't align (a 5-min feed
    snapped to :30/:35 vs a gap grid on :31/:36 would report 100% missing)."""
    if granularity_s == 60:
        return dt.replace(second=0, microsecond=0)
    epoch = int(dt.timestamp())
    return datetime.fromtimestamp(epoch - epoch % granularity_s, tz=timezone.utc)


async def ensure_indexes() -> None:
    db = get_db()
    await db[COVERAGE_COLLECTION].create_index(
        [("feed", 1), ("slot", 1)], unique=True
    )
    await db[COVERAGE_COLLECTION].create_index([("feed", 1), ("state", 1)])


async def mark(feed: str, slot: datetime, state: str, source: str) -> None:
    """Upsert one coverage slot. A `complete` mark never downgrades to missing."""
    db = get_db()
    existing = await db[COVERAGE_COLLECTION].find_one(
        {"feed": feed, "slot": slot}, {"state": 1}
    )
    if existing and existing["state"] == COMPLETE and state != COMPLETE:
        return
    await db[COVERAGE_COLLECTION].replace_one(
        {"feed": feed, "slot": slot},
        {"feed": feed, "slot": slot, "state": state, "source": source,
         "updated_at": datetime.now(timezone.utc)},
        upsert=True,
    )


async def reconcile_record_feed(feed: str, since: datetime | None = None) -> int:
    """Rebuild coverage for a record-backed feed from its stored records.
    Returns the number of complete slots found."""
    if feed not in RECORD_BACKED_FEEDS:
        raise ValueError(f"{feed} is not record-backed")
    coll_name, granularity_s = RECORD_BACKED_FEEDS[feed]
    db = get_db()
    query = {}
    if since is not None:
        query = {"event_time": {"$gte": since}}
    cursor = db[coll_name].find(query, {"event_time": 1})
    count = 0
    async for doc in cursor:
        await mark(feed, snap_slot(doc["event_time"], granularity_s), COMPLETE, "record")
        count += 1
    return count


async def reconcile_record_feed_bulk(feed: str, since: datetime | None = None,
                                     batch: int = 50_000) -> int:
    """Bulk reconcile for deep history — same ledger, same `snap_slot` grid.

    `reconcile_record_feed` does a read plus a write per record, which is right
    for healing a handful of slots but is 2 × 10^6 round-trips against two years
    of 1m candles. This batches the same upserts.

    Dropping the per-slot read is safe *only* because this path writes nothing
    but COMPLETE: `mark`'s guard exists to stop a complete slot being downgraded,
    and an upsert to COMPLETE can never downgrade anything.
    """
    from pymongo import UpdateOne

    if feed not in RECORD_BACKED_FEEDS:
        raise ValueError(f"{feed} is not record-backed")
    coll_name, granularity_s = RECORD_BACKED_FEEDS[feed]
    db = get_db()
    query = {"event_time": {"$gte": since}} if since is not None else {}

    now = datetime.now(timezone.utc)
    ops: list = []
    seen: set[datetime] = set()
    count = 0
    cursor = db[coll_name].find(query, {"event_time": 1}).sort("event_time", 1)
    async for doc in cursor:
        slot = snap_slot(doc["event_time"], granularity_s)
        if slot in seen:                    # many records can share one slot
            continue
        seen.add(slot)
        ops.append(UpdateOne(
            {"feed": feed, "slot": slot},
            {"$set": {"feed": feed, "slot": slot, "state": COMPLETE,
                      "source": "record", "updated_at": now}},
            upsert=True,
        ))
        if len(ops) >= batch:
            await db[COVERAGE_COLLECTION].bulk_write(ops, ordered=False)
            count += len(ops)
            ops = []
            seen.clear()
    if ops:
        await db[COVERAGE_COLLECTION].bulk_write(ops, ordered=False)
        count += len(ops)
    log.info("coverage: reconciled %d slots for %s", count, feed)
    return count


async def gaps(feed: str, start: datetime, end: datetime) -> list[datetime]:
    """Slots in [start, end] NOT marked complete. This is what the backtester
    calls to decide whether a window is safe to evaluate."""
    db = get_db()
    granularity_s = RECORD_BACKED_FEEDS.get(feed, (None, 60))[1]
    start_slot = snap_slot(start, granularity_s)   # snap the query bound too, or
    cursor = db[COVERAGE_COLLECTION].find(          # the boundary slot is excluded
        {"feed": feed, "slot": {"$gte": start_slot, "$lte": end}, "state": COMPLETE},
        {"slot": 1},
    )
    complete = {d["slot"] async for d in cursor}

    out = []
    step = timedelta(seconds=granularity_s)
    cur = start_slot
    while cur <= end:
        if cur not in complete:
            out.append(cur)
        cur += step
    return out


class CoverageHeartbeat:
    """Keeps the ledger current:
    - event-driven feeds: mark the current minute `complete` while the source's
      ws is up. Minutes it never runs for (process/socket down) stay absent →
      reported as gaps, correctly and permanently.
    - record-backed feeds: incrementally reconcile the recent window each cycle
      so live and backfilled records both register as coverage.
    """
    name = "coverage-heartbeat"

    def __init__(self, interval_s: int = 20, record_reconcile_lookback_min: int = 15) -> None:
        self.interval_s = interval_s
        self._lookback = timedelta(minutes=record_reconcile_lookback_min)

    async def run(self) -> None:
        # one-time full reconcile so stored/backfilled history registers as
        # coverage; the loop then maintains only the recent window incrementally
        try:
            for feed in RECORD_BACKED_FEEDS:
                n = await reconcile_record_feed(feed)
                log.info("[%s] bootstrap reconciled %s: %d slots", self.name, feed, n)
        except Exception as e:
            log.warning("[%s] bootstrap reconcile failed (%s)", self.name, e)

        while True:
            try:
                slot = floor_minute(datetime.now(timezone.utc))
                for feed, source in EVENT_DRIVEN_FEEDS.items():
                    if telemetry.ws_source_up(source):
                        await mark(feed, slot, COMPLETE, "heartbeat")
                since = datetime.now(timezone.utc) - self._lookback
                for feed in RECORD_BACKED_FEEDS:
                    await reconcile_record_feed(feed, since=since)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] cycle failed (%s)", self.name, e)
            await asyncio.sleep(self.interval_s)


async def summary(feed: str, start: datetime, end: datetime) -> dict:
    total_gaps = await gaps(feed, start, end)
    granularity_s = RECORD_BACKED_FEEDS.get(feed, (None, 60))[1]
    expected = int((end - start).total_seconds() // granularity_s) + 1
    return {
        "feed": feed,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "expected_slots": expected,
        "missing_slots": len(total_gaps),
        "complete_pct": round((expected - len(total_gaps)) / expected * 100, 2) if expected else 0.0,
        "first_gap": total_gaps[0].isoformat() if total_gaps else None,
    }
