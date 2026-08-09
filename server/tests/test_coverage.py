"""Coverage ledger tests (§5.1, Storage v2.0 §3.B): gap detection,
complete-never-downgrades, record reconciliation, and the slot-grid agreement
that a fake database could never really have tested.

These used to run against a hand-written fake collection whose `find` reimplemented
a subset of Mongo's query semantics. The grid-alignment test in particular was
only as trustworthy as that reimplementation — and grid alignment is exactly the
thing that, when wrong, reports a fully-covered feed as 100% missing.
"""
from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.pipeline import coverage
from botmaximus.storage import records as store
from botmaximus.storage.archive import Archive, LocalBackend
from botmaximus.storage.envelope import Record

UTC = timezone.utc


@pytest.fixture
def local_archive(tmp_path):
    a = Archive(LocalBackend(tmp_path), "archive", "quarantine")
    store.set_archive_for_tests(a)
    yield a
    store.reset_for_tests()


async def store_records(dataset_id: str, times: list[datetime]) -> None:
    await store.write_records([
        Record.create(dataset_id=dataset_id, source="bybit_v5_ws",
                      event_time=t, payload={"v": 1}, quality_ok=True)
        for t in times])


async def slots(pg, feed: str) -> list[datetime]:
    rows = await pg.fetch(
        "SELECT slot FROM coverage_ledger WHERE feed = %s ORDER BY slot",
        (feed,))
    return [r["slot"] for r in rows]


@pytest.mark.asyncio
async def test_gaps_finds_missing_minutes(pg):
    base = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    for i in [0, 1, 3, 4]:               # minute 2 missing
        await coverage.mark("btc_ohlcv_1m", base + timedelta(minutes=i),
                            coverage.COMPLETE, "record")
    g = await coverage.gaps("btc_ohlcv_1m", base, base + timedelta(minutes=4))
    assert g == [base + timedelta(minutes=2)]


@pytest.mark.asyncio
async def test_complete_never_downgrades(pg):
    """A heartbeat that fires late must not erase a slot already known good."""
    slot = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    await coverage.mark("btc_orderbook", slot, coverage.COMPLETE, "heartbeat")
    await coverage.mark("btc_orderbook", slot, coverage.MISSING, "heartbeat")
    row = await pg.fetchrow(
        "SELECT state FROM coverage_ledger WHERE feed = %s AND slot = %s",
        ("btc_orderbook", slot))
    assert row["state"] == coverage.COMPLETE


@pytest.mark.asyncio
async def test_missing_can_still_be_upgraded_to_complete(pg):
    """The guard is one-directional: it blocks downgrades, not late good news."""
    slot = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    await coverage.mark("btc_orderbook", slot, coverage.MISSING, "heartbeat")
    await coverage.mark("btc_orderbook", slot, coverage.COMPLETE, "heartbeat")
    row = await pg.fetchrow(
        "SELECT state FROM coverage_ledger WHERE feed = %s AND slot = %s",
        ("btc_orderbook", slot))
    assert row["state"] == coverage.COMPLETE


@pytest.mark.asyncio
async def test_reconcile_record_feed_marks_stored_minutes(pg, local_archive):
    base = datetime(2026, 7, 23, 10, 0, 30, tzinfo=UTC)   # note :30 second
    await store_records("btc_ohlcv_1m",
                        [base + timedelta(minutes=i) for i in range(3)])

    n = await coverage.reconcile_record_feed("btc_ohlcv_1m")
    assert n == 3
    # minute-floored slots are complete, seconds dropped
    g = await coverage.gaps("btc_ohlcv_1m",
                            base.replace(second=0),
                            base.replace(second=0) + timedelta(minutes=2))
    assert g == []


@pytest.mark.asyncio
async def test_reconcile_ignores_superseded_records(pg, local_archive):
    """A corrected record is not coverage for its own slot twice, and a
    superseded one must not vouch for a minute on its own."""
    t = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    rec = Record.create(dataset_id="btc_ohlcv_1m", source="bybit_v5_ws",
                        event_time=t, payload={"close": 1.0}, quality_ok=True)
    await store.write_records([rec])
    await store.apply_correction(rec, {"close": 2.0}, reason="reissue")

    assert await coverage.reconcile_record_feed("btc_ohlcv_1m") == 1
    assert await slots(pg, "btc_ohlcv_1m") == [t]


@pytest.mark.asyncio
async def test_summary_reports_completeness(pg):
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
async def test_oi_5m_snaps_to_5min_granularity(pg, local_archive):
    # 10:03:20 should snap to the 10:00 slot for 5m granularity
    await store_records("btc_oi_5m",
                        [datetime(2026, 7, 23, 10, 3, 20, tzinfo=UTC)])
    await coverage.reconcile_record_feed("btc_oi_5m")
    assert await slots(pg, "btc_oi_5m") == [
        datetime(2026, 7, 23, 10, 0, tzinfo=UTC)]


@pytest.mark.asyncio
async def test_gaps_grid_aligns_with_snapped_slots_5min(pg, local_archive):
    """Regression: a 5-min feed's stored slots and the gaps() grid must use the
    same snapping, or a fully-covered feed reads as 100% missing."""
    base = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    await store_records("btc_oi_5m", [
        base + timedelta(minutes=5 * i, seconds=17) for i in range(6)])
    await coverage.reconcile_record_feed("btc_oi_5m")
    # query window starts off-boundary (10:02:09) — must still see full coverage
    g = await coverage.gaps("btc_oi_5m",
                            base + timedelta(minutes=2, seconds=9),
                            base + timedelta(minutes=25))
    assert g == []


@pytest.mark.asyncio
@pytest.mark.parametrize("granularity_s,feed,dataset", [
    (60, "btc_ohlcv_1m", "btc_ohlcv_1m"),
    (300, "btc_oi_5m", "btc_oi_5m"),
    (8 * 3600, "btc_funding_8h", "btc_funding_8h"),
])
async def test_sql_slot_grid_matches_python_snap_slot(
        pg, local_archive, granularity_s, feed, dataset):
    """The reconcile computes slots in SQL; `gaps()` walks them in Python. Two
    implementations of one grid that disagree is the failure this asserts
    against — a 5-minute feed snapped to :30 against a gap grid on :31 reports
    100% missing while the data is entirely present."""
    base = datetime(2026, 7, 23, 10, 1, 17, tzinfo=UTC)
    times = [base + timedelta(seconds=granularity_s * i) for i in range(4)]
    await store_records(dataset, times)
    await coverage.reconcile_record_feed(feed)

    from_sql = await slots(pg, feed)
    from_python = sorted({coverage.snap_slot(t, granularity_s) for t in times})
    assert from_sql == from_python


@pytest.mark.asyncio
async def test_coverage_is_kept_per_venue(pg):
    """'We have complete OHLCV for this minute' is a different claim on Bybit
    than on Binance; one venue's uptime must not vouch for the other's data."""
    slot = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    await coverage.mark("btc_ohlcv_1m", slot, coverage.COMPLETE, "record",
                        venue="bybit")
    assert await coverage.gaps("btc_ohlcv_1m", slot, slot, venue="bybit") == []
    assert await coverage.gaps("btc_ohlcv_1m", slot, slot,
                               venue="binance") == [slot]
