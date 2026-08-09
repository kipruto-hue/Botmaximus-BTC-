r"""The write path and point-in-time reads (Storage v2.0 §4, §5, §6).

The assertions here are mostly about where data does **not** go. A store that
writes clean records correctly but also leaks a failed one into the backtester's
view is worse than one that fails outright, because the leak is invisible until
a strategy is trading on it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.storage import partitions, postgres, records
from botmaximus.storage.archive import Archive, LocalBackend
from botmaximus.storage.envelope import Record, visible_at
from botmaximus.storage.venues import UnknownVenue, venue_of

UTC = timezone.utc
T0 = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


@pytest.fixture
def local_archive(tmp_path):
    a = Archive(LocalBackend(tmp_path), "archive", "quarantine")
    records.set_archive_for_tests(a)
    yield a
    records.reset_for_tests()


def make(event_time: datetime = T0, *, ok: bool = True,
         source: str = "bybit_v5_ws", dataset: str = "btc_ohlcv_1m",
         flags: tuple[str, ...] = (), close: float = 60000.0) -> Record:
    return Record.create(
        dataset_id=dataset, source=source, event_time=event_time,
        payload={"close": close}, quality_ok=ok, quality_flags=flags)


# ---------------------------------------------------------------- venue

def test_venue_is_derived_from_source_not_config():
    assert venue_of("bybit_v5_ws") == "bybit"
    assert venue_of("binance_futures_ws") == "binance"


def test_unknown_source_raises_rather_than_guessing():
    """A wrong guess about which exchange a price came from is
    indistinguishable from real data once written."""
    with pytest.raises(UnknownVenue):
        venue_of("kraken_ws")
    with pytest.raises(UnknownVenue):
        venue_of("")


# ---------------------------------------------------------------- write path

@pytest.mark.asyncio
async def test_clean_record_reaches_both_stores_and_the_manifest(pg, local_archive):
    res = await records.write_records([make()])
    assert res.written == 1 and res.quarantined == 0
    assert not res.archive_deferred

    rows = await records.read_as_of("btc_ohlcv_1m", T0 - timedelta(minutes=1),
                                    T0 + timedelta(minutes=1), venue="bybit")
    assert len(rows) == 1
    assert rows[0]["venue"] == "bybit"

    # Parquet
    assert len(res.archived_keys) == 1
    table = local_archive.read(res.archived_keys[0])
    assert table.num_rows == 1

    # ...and indexed with a checksum, which is what the weekly audit compares.
    manifest = await pg.fetch("SELECT * FROM storage_manifest")
    assert len(manifest) == 1
    assert manifest[0]["sha256"] and manifest[0]["rows"] == 1


@pytest.mark.asyncio
async def test_failed_record_reaches_quarantine_and_neither_production_store(
        pg, local_archive):
    """P4 and §5: quarantine never mixes with production."""
    res = await records.write_records(
        [make(ok=False, flags=("stale_price",))])
    assert res.quarantined == 1 and res.written == 0

    rows = await records.read_as_of("btc_ohlcv_1m", T0 - timedelta(minutes=1),
                                    T0 + timedelta(minutes=1), venue="bybit")
    assert rows == []
    assert local_archive.backend.list("archive", "") == []
    assert local_archive.backend.list("quarantine", "") != []

    ev = await pg.fetch("SELECT * FROM quality_events")
    assert [e["failing_check"] for e in ev] == ["stale_price"]


@pytest.mark.asyncio
async def test_mixed_batch_splits_by_verdict(pg, local_archive):
    res = await records.write_records([
        make(T0, close=1.0),
        make(T0 + timedelta(minutes=1), ok=False, flags=("lookahead",)),
        make(T0 + timedelta(minutes=2), close=3.0),
    ])
    assert (res.written, res.quarantined) == (2, 1)
    rows = await records.read_as_of("btc_ohlcv_1m", T0, T0 + timedelta(hours=1),
                                    venue="bybit")
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_reinserting_the_same_record_does_not_duplicate(pg, local_archive):
    """Dedupe-by-existence: a reconnect or an overlapping backfill re-offers
    records the store already holds, and counting them twice would inflate
    every figure the coverage ledger and integrity checks derive."""
    r = make()
    await records.write_records([r])
    await records.write_records([r])
    rows = await records.read_as_of("btc_ohlcv_1m", T0 - timedelta(minutes=1),
                                    T0 + timedelta(minutes=1), venue="bybit")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_archive_failure_defers_but_does_not_stop_collection(pg, monkeypatch):
    """§7: object storage down → the collector keeps writing to Postgres and
    tier-out defers. Only Postgres going down halts the system."""
    class Broken(LocalBackend):
        def put(self, bucket, key, data):
            raise OSError("object storage unreachable")

    records.set_archive_for_tests(Archive(Broken("/nonexistent"),
                                          "archive", "quarantine"))
    try:
        res = await records.write_records([make()])
    finally:
        records.reset_for_tests()

    assert res.written == 1                 # Postgres write succeeded
    assert res.archive_deferred             # ...and the archive gap is recorded
    rows = await records.read_as_of("btc_ohlcv_1m", T0 - timedelta(minutes=1),
                                    T0 + timedelta(minutes=1), venue="bybit")
    assert len(rows) == 1
    tel = await pg.fetch(
        "SELECT * FROM telemetry_events WHERE label = 'archive_write_failed'")
    assert len(tel) == 1
    # No manifest row, so tier-out cannot later believe this day was archived.
    assert await pg.fetchval("SELECT count(*) AS n FROM storage_manifest") == 0


# ---------------------------------------------------------------- venue guard

@pytest.mark.asyncio
async def test_reads_cannot_blend_two_venues(pg, local_archive):
    """The separation that used to be two databases. A window spanning both
    venues would be a price series that never existed on either."""
    await records.write_records([
        make(T0, source="bybit_v5_ws", close=60000.0),
        make(T0, source="binance_futures_ws", close=59990.0),
    ])
    bybit = await records.read_as_of("btc_ohlcv_1m", T0 - timedelta(minutes=1),
                                     T0 + timedelta(minutes=1), venue="bybit")
    binance = await records.read_as_of("btc_ohlcv_1m", T0 - timedelta(minutes=1),
                                       T0 + timedelta(minutes=1),
                                       venue="binance")
    assert len(bybit) == 1 and len(binance) == 1
    assert bybit[0]["payload"]["close"] == 60000.0
    assert binance[0]["payload"]["close"] == 59990.0


@pytest.mark.asyncio
async def test_read_requires_a_known_venue(pg):
    with pytest.raises(UnknownVenue):
        await records.read_as_of("btc_ohlcv_1m", T0, T0, venue="kraken")


# ---------------------------------------------------------------- corrections

@pytest.mark.asyncio
async def test_correction_supersedes_without_editing_history(pg, local_archive):
    """§2 invariant 3 + §6: a backtest pinned before the correction must still
    see the original. This is the whole reason the system-time axis exists."""
    original = make(close=60000.0)
    await records.write_records([original])
    before = datetime.now(UTC)

    new = await records.apply_correction(original, {"close": 60500.0},
                                         reason="venue reissued the bar")
    after = datetime.now(UTC)

    window = (T0 - timedelta(minutes=1), T0 + timedelta(minutes=1))

    current = await records.read_as_of("btc_ohlcv_1m", *window, venue="bybit")
    assert len(current) == 1
    assert current[0]["payload"]["close"] == 60500.0
    assert str(current[0]["supersedes"]) == original.record_id

    historic = await records.read_as_of("btc_ohlcv_1m", *window,
                                        venue="bybit", as_of=before)
    assert len(historic) == 1
    assert historic[0]["payload"]["close"] == 60000.0, (
        "a backtest pinned before the correction saw the corrected value — "
        "that is the silent-revision failure the bitemporal axis prevents")

    latest = await records.read_as_of("btc_ohlcv_1m", *window,
                                      venue="bybit", as_of=after)
    assert latest[0]["payload"]["close"] == 60500.0
    assert new.event_time == original.event_time


@pytest.mark.asyncio
async def test_correction_copies_the_original_to_quarantine(pg, local_archive):
    """§5.1 retroactive quarantine: the original stays in production AND gets a
    forensic copy. Both exist forever."""
    original = make()
    await records.write_records([original])
    await records.apply_correction(original, {"close": 1.0}, reason="reissue")

    checks = [e["failing_check"] for e in
              await pg.fetch("SELECT * FROM quality_events")]
    assert checks == ["retroactive:retroactive"]
    assert local_archive.backend.list("quarantine", "") != []


@pytest.mark.asyncio
async def test_correcting_a_closed_record_is_refused(pg, local_archive):
    """Forking a supersession chain would make as-of queries ambiguous — two
    rows could both claim to be current at the same instant."""
    original = make()
    await records.write_records([original])
    await records.apply_correction(original, {"close": 1.0}, reason="first")
    with pytest.raises(ValueError, match="not open for correction"):
        await records.apply_correction(original, {"close": 2.0}, reason="again")


# ------------------------------------------------- the three readers agree

@pytest.mark.asyncio
async def test_sql_and_in_memory_as_of_agree(pg, local_archive):
    """Two implementations of the point-in-time predicate that disagree is the
    silent-revision bug wearing a different hat, so they are checked against
    each other rather than each against its own expectation."""
    original = make(close=60000.0)
    await records.write_records([original])
    t_before = datetime.now(UTC)
    await records.apply_correction(original, {"close": 60500.0},
                                   reason="reissue")
    t_after = datetime.now(UTC)

    window = (T0 - timedelta(minutes=1), T0 + timedelta(minutes=1))
    for as_of in (t_before, t_after):
        sql_rows = await records.read_as_of("btc_ohlcv_1m", *window,
                                            venue="bybit", as_of=as_of)
        # Rebuild the same records in memory from the store, then apply the
        # library predicate to them.
        every = await pg.fetch(
            "SELECT * FROM market_records ORDER BY valid_from_sys")
        in_memory = visible_at(
            [Record(dataset_id=r["dataset_id"], source=r["source"],
                    event_time=r["event_time"], payload=r["payload"],
                    record_id=str(r["record_id"]),
                    valid_from_sys=r["valid_from_sys"],
                    valid_to_sys=r["valid_to_sys"],
                    quality_ok=r["quality_ok"]) for r in every],
            as_of)
        assert {str(r["record_id"]) for r in sql_rows} == \
               {r.record_id for r in in_memory}, f"disagreement at as_of={as_of}"


@pytest.mark.asyncio
async def test_last_event_time_tracks_the_newest_committed_row(pg, local_archive):
    """What a restarting collector asks before deciding what to backfill (§3.C)."""
    assert await records.last_event_time("btc_ohlcv_1m", venue="bybit") is None
    await records.write_records([make(T0), make(T0 + timedelta(minutes=5))])
    assert await records.last_event_time("btc_ohlcv_1m", venue="bybit") == \
        T0 + timedelta(minutes=5)
    # A different venue has its own answer; it must not inherit this one.
    assert await records.last_event_time("btc_ohlcv_1m", venue="binance") is None


@pytest.mark.asyncio
async def test_records_spanning_days_land_in_their_own_partitions(pg, local_archive):
    """Partitioning is by EVENT date, so a backfill of last week does not land
    in today's partition."""
    day1 = datetime(2026, 8, 9, 23, 59, tzinfo=UTC)
    day2 = datetime(2026, 8, 10, 0, 1, tzinfo=UTC)
    res = await records.write_records([make(day1), make(day2)])
    assert res.written == 2
    assert len(res.archived_keys) == 2      # one file per day, atomic
    existing = await partitions.existing_partitions()
    assert "market_records_20260809" in existing
    assert "market_records_20260810" in existing


@pytest.mark.asyncio
async def test_no_rehabilitation_path_exists():
    """§14: there is no 'clean it up and reintroduce it' pathway, and its
    absence is asserted rather than assumed."""
    import ast
    src = (postgres.SCHEMA_PATH.parent / "records.py").read_text(encoding="utf-8")
    # Parse rather than grep: the module docstring *explains* that no such path
    # exists, and a substring search cannot tell an explanation from an
    # implementation.
    tree = ast.parse(src)
    defined = {n.name for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    for banned in ("promote_from_quarantine", "unquarantine", "rehabilitate",
                   "restore_from_quarantine"):
        assert banned not in defined, (
            f"{banned}() exists — §14: there is no pathway from quarantine "
            f"back into production")
