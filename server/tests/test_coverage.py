"""Coverage ledger tests (§5.1): gap detection, complete-never-downgrades,
record reconciliation, silence-is-not-absence for event-driven feeds."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from botmaximus.pipeline import coverage

UTC = timezone.utc


class FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def __aiter__(self):
        async def gen():
            for d in self._docs:
                yield d
        return gen()


class FakeCollection:
    def __init__(self):
        self.docs = []

    async def create_index(self, *a, **kw):
        pass

    async def find_one(self, query, projection=None):
        for d in self.docs:
            if all(d.get(k) == v for k, v in query.items()):
                return d
        return None

    async def replace_one(self, query, doc, upsert=False):
        for i, d in enumerate(self.docs):
            if all(d.get(k) == v for k, v in query.items()):
                self.docs[i] = doc
                return
        if upsert:
            self.docs.append(doc)

    def find(self, query, projection=None):
        out = []
        for d in self.docs:
            ok = True
            for k, v in query.items():
                if isinstance(v, dict):
                    slot = d.get(k)
                    if "$gte" in v and slot < v["$gte"]:
                        ok = False
                    if "$lte" in v and slot > v["$lte"]:
                        ok = False
                elif d.get(k) != v:
                    ok = False
            if ok:
                out.append(d)
        return FakeCursor(out)


class FakeDB(dict):
    def __missing__(self, key):
        self[key] = FakeCollection()
        return self[key]


@pytest.fixture
def db():
    fake = FakeDB()
    with patch("botmaximus.pipeline.coverage.get_db", return_value=fake):
        yield fake


@pytest.mark.asyncio
async def test_gaps_finds_missing_minutes(db):
    base = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    for i in [0, 1, 3, 4]:               # minute 2 missing
        await coverage.mark("btc_ohlcv_1m", base + timedelta(minutes=i),
                            coverage.COMPLETE, "record")
    g = await coverage.gaps("btc_ohlcv_1m", base, base + timedelta(minutes=4))
    assert g == [base + timedelta(minutes=2)]


@pytest.mark.asyncio
async def test_complete_never_downgrades(db):
    slot = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    await coverage.mark("btc_orderbook", slot, coverage.COMPLETE, "heartbeat")
    await coverage.mark("btc_orderbook", slot, coverage.MISSING, "heartbeat")
    doc = await db[coverage.COVERAGE_COLLECTION].find_one(
        {"feed": "btc_orderbook", "slot": slot})
    assert doc["state"] == coverage.COMPLETE


@pytest.mark.asyncio
async def test_reconcile_record_feed_marks_stored_minutes(db):
    base = datetime(2026, 7, 23, 10, 0, 30, tzinfo=UTC)   # note :30 second
    coll = db["btc_ohlcv_1m"]
    for i in range(3):
        coll.docs.append({"event_time": base + timedelta(minutes=i)})
    n = await coverage.reconcile_record_feed("btc_ohlcv_1m")
    assert n == 3
    # minute-floored slots are complete, seconds dropped
    g = await coverage.gaps("btc_ohlcv_1m",
                            base.replace(second=0),
                            base.replace(second=0) + timedelta(minutes=2))
    assert g == []


@pytest.mark.asyncio
async def test_summary_reports_completeness(db):
    base = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    for i in range(5):
        if i != 3:
            await coverage.mark("btc_ohlcv_1m", base + timedelta(minutes=i),
                                coverage.COMPLETE, "record")
    s = await coverage.summary("btc_ohlcv_1m", base, base + timedelta(minutes=4))
    assert s["expected_slots"] == 5
    assert s["missing_slots"] == 1
    assert s["complete_pct"] == 80.0


@pytest.mark.asyncio
async def test_oi_5m_snaps_to_5min_granularity(db):
    coll = db["btc_oi_5m"]
    # 10:03:20 should snap to the 10:00 slot for 5m granularity
    coll.docs.append({"event_time": datetime(2026, 7, 23, 10, 3, 20, tzinfo=UTC)})
    await coverage.reconcile_record_feed("btc_oi_5m")
    doc = await coll_first(db, "btc_oi_5m")
    assert doc["slot"] == datetime(2026, 7, 23, 10, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_gaps_grid_aligns_with_snapped_slots_5min(db):
    """Regression: a 5-min feed's stored slots and the gaps() grid must use the
    same snapping, or a fully-covered feed reads as 100% missing."""
    coll = db["btc_oi_5m"]
    base = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    for i in range(6):
        coll.docs.append({"event_time": base + timedelta(minutes=5 * i, seconds=17)})
    await coverage.reconcile_record_feed("btc_oi_5m")
    # query window starts off-boundary (10:02:09) — must still see full coverage
    g = await coverage.gaps("btc_oi_5m",
                            base + timedelta(minutes=2, seconds=9),
                            base + timedelta(minutes=25))
    assert g == []


async def coll_first(db, feed):
    for d in db[coverage.COVERAGE_COLLECTION].docs:
        if d["feed"] == feed:
            return d
    return None
