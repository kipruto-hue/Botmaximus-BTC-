"""The store stage, against the real two-store write path.

These previously ran against a fake Mongo collection. They now run against a
real Postgres and a filesystem-backed archive, which makes them evidence about
the system rather than about the fake: the dedupe assertions in particular were
only ever as good as the fake's `find_one`, and the behaviour they describe is
now enforced by a unique index.
"""
from datetime import timedelta

import pytest

from botmaximus.pipeline.envelope import Envelope, utcnow
from botmaximus.pipeline.writer import Writer
from botmaximus.storage import records as store
from botmaximus.storage.archive import Archive, LocalBackend


@pytest.fixture
def local_archive(tmp_path):
    a = Archive(LocalBackend(tmp_path), "archive", "quarantine")
    store.set_archive_for_tests(a)
    yield a
    store.reset_for_tests()


def candle(event_offset_s=0.0):
    now = utcnow()
    return Envelope(
        dataset_id="btc_ohlcv_1m", source="bybit_v5_ws", symbol="BTCUSDT",
        event_time=now + timedelta(seconds=event_offset_s),
        collection_time=now,
        ingest_time=now,  # stamped by bus.py's store worker in production
        payload={"open": 64000, "high": 64100, "low": 63900, "close": 64050,
                 "volume": 12.5, "quote_volume": 1.0, "trades": 100},
    )


async def _stored(pg) -> list[dict]:
    return await pg.fetch(
        "SELECT * FROM market_records WHERE valid_to_sys IS NULL "
        "ORDER BY event_time")


@pytest.mark.asyncio
async def test_store_latency_is_stamped(pg, local_archive):
    """Postgres rows are built before they are sent, so a record can carry its
    own measured store latency — under Mongo's immutable time-series documents
    each one had to carry the *previous* write's duration."""
    w = Writer()
    first = candle()
    second = candle(event_offset_s=60)
    await w.write(first)
    await w.write(second)

    assert len(await _stored(pg)) == 2
    for env in (first, second):
        assert env.stage_latency_ms["store"] >= 0.0


@pytest.mark.asyncio
async def test_duplicate_event_time_not_rewritten(pg, local_archive):
    w = Writer()
    env = candle()
    await w.write(env)
    replay = candle()
    replay.event_time = env.event_time
    await w.write(replay)

    assert len(await _stored(pg)) == 1


@pytest.mark.asyncio
async def test_replay_is_rejected_even_without_the_watermark(pg, local_archive):
    """The watermark is an optimisation; the unique index is the guarantee. A
    fresh Writer has no memory of what was stored, which is exactly the state
    after a restart."""
    await Writer().write(candle())
    env = candle()
    await Writer().write(env)          # different Writer, empty watermark
    replay = candle()
    replay.event_time = env.event_time
    await Writer().write(replay)

    rows = await _stored(pg)
    assert len({r["event_time"] for r in rows}) == len(rows)


@pytest.mark.asyncio
async def test_backfill_fills_hole_behind_newest_without_duplicating(
        pg, local_archive):
    w = Writer()
    newest = candle()
    await w.write(newest)

    gap = candle(event_offset_s=-300)   # behind the watermark
    gap.backfill = True
    await w.write(gap)
    assert len(await _stored(pg)) == 2

    replay = candle(event_offset_s=-300)
    replay.event_time = gap.event_time
    replay.backfill = True
    await w.write(replay)               # same minute again
    assert len(await _stored(pg)) == 2

    # The live high-water mark must not regress to the backfilled time,
    # otherwise the next live candle looks like a replay and is dropped.
    assert w._last_written["btc_ohlcv_1m"] == newest.event_time


@pytest.mark.asyncio
async def test_quarantined_record_routed_away_from_production(pg, local_archive):
    w = Writer()
    env = candle()
    env.quarantine_reasons = ["lookahead_violation"]
    await w.write(env)

    assert await _stored(pg) == []
    assert local_archive.backend.list("archive", "") == []
    assert local_archive.backend.list("quarantine", "") != []
    checks = [e["failing_check"] for e in
              await pg.fetch("SELECT * FROM quality_events")]
    assert "lookahead_violation" in checks


@pytest.mark.asyncio
async def test_advisory_flags_do_not_quarantine_a_passing_record(
        pg, local_archive):
    """The gate marks every record `single_source` while leaving quality_ok
    true. Treating that as a failing check would quarantine the entire feed."""
    w = Writer()
    env = candle()
    env.quality_flags = ["single_source", "illiquid_window"]
    await w.write(env)

    rows = await _stored(pg)
    assert len(rows) == 1
    assert rows[0]["quality_ok"] is True
    assert rows[0]["quality_flags"] == []
    assert set(rows[0]["annotations"]) == {"single_source", "illiquid_window"}


@pytest.mark.asyncio
async def test_failing_record_carries_its_reasons_as_flags(pg, local_archive):
    w = Writer()
    env = candle()
    env.quality_flags = ["stale", "single_source"]
    env.quality_ok = False
    await w.write(env)

    assert await _stored(pg) == []
    checks = {e["failing_check"] for e in
              await pg.fetch("SELECT * FROM quality_events")}
    assert {"stale", "single_source"} <= checks


@pytest.mark.asyncio
async def test_seed_dedupe_rebuilds_the_watermark_from_the_store(
        pg, local_archive):
    """§3.C: in-memory state is a working buffer, rebuilt on restart from what
    was actually committed — not assumed empty."""
    first = Writer()
    env = candle()
    await first.write(env)

    revived = Writer()
    assert revived._last_written == {}
    await revived.seed_dedupe()
    assert revived._last_written["btc_ohlcv_1m"] == env.event_time

    replay = candle()
    replay.event_time = env.event_time
    await revived.write(replay)
    assert len(await _stored(pg)) == 1


@pytest.mark.asyncio
async def test_write_many_is_backfill_only(pg, local_archive):
    w = Writer()
    with pytest.raises(ValueError, match="backfill-only"):
        await w.write_many([candle()])


@pytest.mark.asyncio
async def test_write_many_collapses_within_batch_duplicates(pg, local_archive):
    """Two envelopes for the same minute get different record_ids, so only the
    natural key stops them both landing."""
    w = Writer()
    a = candle(event_offset_s=-600)
    b = candle(event_offset_s=-600)
    b.event_time = a.event_time
    for e in (a, b):
        e.backfill = True

    written = await w.write_many([a, b])
    assert written == 1
    assert len(await _stored(pg)) == 1
