"""REST history collectors (§5.1/§5.3): bounded-window sync must capture the
full span regardless of the endpoint's row ordering."""
import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from botmaximus.pipeline.collectors.binance_history import (
    FundingHistoryCollector,
    OIHistoryCollector,
)

UTC = timezone.utc


class NewestInRangeClient:
    """Mimics openInterestHist: returns up to `limit` rows, the NEWEST within
    [startTime, endTime]. Forward pagination without bounded windows would miss
    everything older than the last page — bounded windows must not."""
    def __init__(self, first_ts, last_ts, step, time_key, limit):
        self.rows = [{time_key: t} for t in range(first_ts, last_ts + 1, step)]
        self.time_key = time_key
        self.limit = limit

    class _Resp:
        def __init__(self, data): self._d = data
        def raise_for_status(self): pass
        def json(self): return self._d

    async def get(self, url, params):
        s, e = params["startTime"], params["endTime"]
        inrange = [r for r in self.rows if s <= r[self.time_key] <= e]
        return self._Resp(inrange[-self.limit:])   # newest `limit` in the window


@pytest.mark.asyncio
async def test_oi_bounded_window_captures_full_span():
    step = 5 * 60 * 1000
    now = int(datetime.now(UTC).timestamp() * 1000)
    first = now - 3000 * step                       # 3000 rows back (6× the page limit)
    client = NewestInRangeClient(first, now, step, "timestamp", 500)

    q = asyncio.Queue()
    col = OIHistoryCollector(q)
    with patch.object(col, "_last_stored_ms", return_value=first):
        stored = await col._sync(client)

    assert stored >= 2900          # essentially the whole span, not just one page
    assert q.qsize() == stored


@pytest.mark.asyncio
async def test_funding_bounded_window_captures_full_span():
    step = 8 * 3600 * 1000
    now = int(datetime.now(UTC).timestamp() * 1000)
    first = now - 2500 * step                       # 2500 funding rows (> one page)
    client = NewestInRangeClient(first, now, step, "fundingTime", 1000)

    q = asyncio.Queue()
    col = FundingHistoryCollector(q)
    with patch.object(col, "_last_stored_ms", return_value=first):
        stored = await col._sync(client)

    assert stored >= 2400
    item = q.get_nowait()
    assert item.backfill and item.dataset_id == "btc_funding_8h"
