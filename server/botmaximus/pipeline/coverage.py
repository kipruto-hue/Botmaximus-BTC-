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

from botmaximus.config import settings
from botmaximus.pipeline.telemetry import telemetry
from botmaximus.storage import postgres

log = logging.getLogger(__name__)

COMPLETE = "complete"
PARTIAL = "partial"
MISSING = "missing"

# feed → the ws source (telemetry.ws_sources key) whose uptime defines its
# coverage. Bybit serves every topic over ONE socket (`bybit-public`), where
# Binance needed two because its futures streams are routed by path. That means
# a single socket death now takes all three event-driven feeds with it — which
# is worse operationally, and worth remembering when reading a coverage report
# that shows three feeds failing at exactly the same second.
EVENT_DRIVEN_FEEDS = {
    "btc_price_tick": "bybit-public",
    "btc_liquidation": "bybit-public",
    "btc_orderbook": "bybit-public",
}
# feed → dataset_id, granularity in seconds (a stored record = coverage for that slot)
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
    """No-op: the ledger's keys and indexes are declared in `schema.sql`.

    Kept so boot sequences that call it do not need to know that the ledger
    moved from a collection with runtime-created indexes to a table whose
    primary key is part of its definition.
    """
    return None


async def mark(feed: str, slot: datetime, state: str, source: str,
               venue: str | None = None) -> None:
    """Upsert one coverage slot. A `complete` mark never downgrades.

    The guard is in the statement rather than in a read-then-write pair. The old
    version fetched the row, decided, and wrote — a race in which the heartbeat
    and a reconcile could interleave and lose a `complete`. Here the WHERE on
    the DO UPDATE makes the downgrade impossible at the database, which is also
    what makes the bulk path below safe to run without reading first.
    """
    await postgres.execute(
        "INSERT INTO coverage_ledger (venue, feed, slot, state, source, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,now()) "
        "ON CONFLICT (venue, feed, slot) DO UPDATE SET "
        "  state = EXCLUDED.state, source = EXCLUDED.source, "
        "  updated_at = now() "
        "WHERE coverage_ledger.state <> 'complete' OR EXCLUDED.state = 'complete'",
        (venue or settings.venue, feed, slot, state, source))


async def reconcile_record_feed(feed: str, since: datetime | None = None,
                                venue: str | None = None) -> int:
    """Rebuild coverage for a record-backed feed from its stored records.

    One statement: the slots are derived from `market_records` in the database
    and inserted straight into the ledger, so two years of candles is a single
    server-side pass rather than 10^6 round-trips. `snap_slot`'s grid is
    reproduced by `to_timestamp(floor(extract(epoch …)))`, and a test asserts
    the SQL and the Python agree — if those grids ever diverge, a 5-minute feed
    snapped to :30 against a gap grid on :31 reports 100% missing.

    Only ever writes COMPLETE, so it cannot downgrade a slot.
    """
    if feed not in RECORD_BACKED_FEEDS:
        raise ValueError(f"{feed} is not record-backed")
    dataset_id, granularity_s = RECORD_BACKED_FEEDS[feed]
    venue = venue or settings.venue

    sql = (
        "INSERT INTO coverage_ledger (venue, feed, slot, state, source, updated_at) "
        "SELECT DISTINCT %s, %s, "
        "       to_timestamp(floor(extract(epoch FROM event_time) / %s) * %s), "
        "       'complete', 'record', now() "
        "  FROM market_records "
        " WHERE venue = %s AND dataset_id = %s AND valid_to_sys IS NULL ")
    params: list = [venue, feed, granularity_s, granularity_s, venue, dataset_id]
    if since is not None:
        sql += "   AND event_time >= %s "
        params.append(since)
    sql += ("ON CONFLICT (venue, feed, slot) DO UPDATE SET "
            "  state = EXCLUDED.state, source = EXCLUDED.source, "
            "  updated_at = now() "
            "WHERE coverage_ledger.state <> 'complete'")
    return await postgres.execute(sql, tuple(params))


async def reconcile_record_feed_bulk(feed: str, since: datetime | None = None,
                                     batch: int = 50_000) -> int:
    """Retained for callers; the set-based reconcile above is already bulk.

    Under Mongo this was a genuinely different code path — batched `UpdateOne`s
    versus a read-plus-write per record. In SQL both collapse to the same single
    statement, so keeping two implementations would only create the chance for
    their slot grids to drift apart.
    """
    n = await reconcile_record_feed(feed, since=since)
    log.info("coverage: reconciled %d slots for %s", n, feed)
    return n


async def gaps(feed: str, start: datetime, end: datetime,
               venue: str | None = None) -> list[datetime]:
    """Slots in [start, end] NOT marked complete. This is what the backtester
    calls to decide whether a window is safe to evaluate."""
    granularity_s = RECORD_BACKED_FEEDS.get(feed, (None, 60))[1]
    start_slot = snap_slot(start, granularity_s)   # snap the query bound too, or
    rows = await postgres.fetch(                    # the boundary slot is excluded
        "SELECT slot FROM coverage_ledger "
        "WHERE venue = %s AND feed = %s AND state = 'complete' "
        "  AND slot >= %s AND slot <= %s",
        (venue or settings.venue, feed, start_slot, end))
    complete = {r["slot"] for r in rows}

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
