"""OHLCV gap backfill (§11 step 4). A reconnect that spans a minute close
loses that candle from the websocket forever; this task periodically scans
the recent window for holes and fetches the missing candles from Binance
REST, injecting them through the normal pipeline (parse → quality → store)
flagged `backfill` so the gate skips the staleness rule and the writer uses
existence-based dedupe instead of the monotonic check.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from botmaximus.config import settings
from botmaximus.db.mongo import get_db
from botmaximus.db.schema import DATASET_COLLECTIONS
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import utcnow

log = logging.getLogger(__name__)

MINUTE_MS = 60_000


def minute_close(dt: datetime) -> datetime:
    """The Binance close time (k.T) of the minute containing dt: :59.999."""
    start = dt.replace(second=0, microsecond=0)
    return start + timedelta(milliseconds=MINUTE_MS - 1)


def missing_minutes(existing: set[datetime], start: datetime, end: datetime) -> list[datetime]:
    """Expected candle close times in [start, end] that are not in `existing`.
    `end` should already exclude the still-open minute."""
    out = []
    cur = minute_close(start)
    stop = minute_close(end)
    while cur <= stop:
        if cur not in existing:
            out.append(cur)
        cur += timedelta(milliseconds=MINUTE_MS)
    return out


def kline_row_to_ws_shape(row: list) -> dict:
    """Binance REST kline array → the ws-message shape BinanceParser expects."""
    return {
        "k": {
            "t": row[0], "T": row[6],
            "o": row[1], "h": row[2], "l": row[3], "c": row[4],
            "v": row[5], "q": row[7], "n": row[8],
        },
    }


class OhlcvBackfiller:
    name = "ohlcv-backfill"

    def __init__(self, gather_q: asyncio.Queue) -> None:
        self.gather_q = gather_q
        self.url = f"{settings.binance_rest_url}/api/v3/klines"

    async def _find_gaps(self, scan_minutes: int) -> list[datetime]:
        db = get_db()
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(minutes=scan_minutes)
        # exclude the current (still open) minute and the one just closed —
        # the live stream delivers that one within seconds
        window_end = now - timedelta(minutes=2)
        if window_end <= window_start:
            return []
        cursor = db[DATASET_COLLECTIONS["btc_ohlcv_1m"]].find(
            {"event_time": {"$gte": window_start}}, {"event_time": 1}
        )
        existing = {d["event_time"] async for d in cursor}  # tz_aware client → UTC datetimes
        return missing_minutes(existing, window_start, window_end)

    async def _fetch_and_enqueue(self, client: httpx.AsyncClient, gaps: list[datetime]) -> None:
        """REST klines are capped at 1000/request — paginate across the gap span."""
        cursor_ms = int((gaps[0] - timedelta(milliseconds=MINUTE_MS - 1)).timestamp() * 1000)
        end_ms = int(gaps[-1].timestamp() * 1000)
        wanted = {int(g.timestamp() * 1000) for g in gaps}
        filled = 0
        while cursor_ms <= end_ms:
            r = await client.get(self.url, params={
                "symbol": settings.symbol, "interval": "1m",
                "startTime": cursor_ms, "endTime": end_ms, "limit": 1000,
            })
            r.raise_for_status()
            rows = r.json()
            if not rows:
                break
            for row in rows:
                if row[6] in wanted:
                    await self.gather_q.put(RawItem(
                        dataset_id="btc_ohlcv_1m", source="binance",
                        symbol=settings.symbol, raw=kline_row_to_ws_shape(row),
                        collection_time=utcnow(), backfill=True,
                    ))
                    filled += 1
            cursor_ms = rows[-1][6] + 1  # continue after the last close time
        log.info("[%s] backfilled %d/%d missing candles", self.name, filled, len(gaps))

    async def run(self) -> None:
        if not settings.backfill_enabled:
            return
        scan_minutes = settings.backfill_startup_scan_minutes  # deep scan on boot
        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                try:
                    gaps = await self._find_gaps(scan_minutes)
                    if gaps:
                        log.warning("[%s] %d candle gap(s) detected, oldest %s",
                                    self.name, len(gaps), gaps[0].isoformat())
                        await self._fetch_and_enqueue(client, gaps)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("[%s] cycle failed (%s) — retrying next cycle", self.name, e)
                else:
                    # deep boot scan succeeded → drop to the light periodic window
                    scan_minutes = settings.backfill_scan_minutes
                await asyncio.sleep(settings.backfill_check_s)
