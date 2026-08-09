r"""Tier-out, retention, integrity and degradation (Storage v2.0 §4, §7, §9, §11).

The load-bearing assertions here are all refusals and absences: that a failed
verification does NOT drop a partition, that a Postgres outage does NOT get
written around, and that a passing check still leaves evidence it ran.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from botmaximus.storage import (degrade, integrity, partitions, postgres,
                                retention, tiering)
from botmaximus.storage import records as store
from botmaximus.storage.archive import Archive, LocalBackend
from botmaximus.storage.envelope import Record

UTC = timezone.utc
DAY = date(2026, 6, 1)
T0 = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def local_archive(tmp_path):
    a = Archive(LocalBackend(tmp_path), "archive", "quarantine")
    store.set_archive_for_tests(a)
    yield a
    store.reset_for_tests()


async def seed(n: int = 3, dataset: str = "btc_ohlcv_1m"):
    await store.write_records([
        Record.create(dataset_id=dataset, source="bybit_v5_ws",
                      event_time=T0 + timedelta(minutes=i),
                      payload={"close": 60000.0 + i}, quality_ok=True)
        for i in range(n)])


# ---------------------------------------------------------------- tier-out

@pytest.mark.asyncio
async def test_a_verified_day_is_archived_then_dropped(pg, local_archive):
    await seed(3)
    assert await partitions.row_count(DAY) == 3

    result = await tiering.tier_out_day(DAY)
    assert result.verified and result.dropped
    assert partitions.partition_name(DAY) not in \
        await partitions.existing_partitions()

    ev = await pg.fetch("SELECT * FROM tier_out_events")
    assert len(ev) == 1 and ev[0]["dropped"] is True
    assert ev[0]["pg_rows"] == 3 and ev[0]["parquet_rows"] == 3


@pytest.mark.asyncio
async def test_a_corrupted_archive_never_drops_the_partition(pg, local_archive):
    """§4: 'tier-out failure never drops the Postgres partition.' This is the
    single most destructive thing this module could get wrong — Postgres would
    be holding the only copy."""
    await seed(3)
    # Corrupt the archived object so its checksum no longer matches.
    key = local_archive.backend.list("archive", "")[0]
    local_archive.backend.put("archive", key, b"not a parquet file")

    result = await tiering.tier_out_day(DAY, allow_rearchive=False)
    assert not result.verified and not result.dropped
    assert "checksum" in result.skipped_reason
    assert await partitions.row_count(DAY) == 3      # still there

    # ...and the operator is told rather than it passing quietly.
    tel = await pg.fetch(
        "SELECT * FROM telemetry_events WHERE label = 'tier_out_verification_failed'")
    assert len(tel) == 1


@pytest.mark.asyncio
async def test_a_missing_archive_is_rewritten_before_any_drop(pg, local_archive):
    """§7 lets the collector keep writing when object storage is down, which
    leaves a day in Postgres and not in Parquet. Tier-out repairs it rather
    than either dropping the day or getting stuck forever."""
    await seed(3)
    await pg.execute("DELETE FROM storage_manifest")
    for key in local_archive.backend.list("archive", ""):
        local_archive.backend.put("archive", key, b"")
    await pg.execute("DELETE FROM storage_manifest")

    result = await tiering.tier_out_day(DAY)
    assert result.verified and result.dropped
    assert await pg.fetchval(
        "SELECT count(*) AS n FROM storage_manifest") >= 1


@pytest.mark.asyncio
async def test_a_row_count_mismatch_refuses(pg, local_archive):
    await seed(3)
    # Claim the archive holds fewer rows than Postgres does.
    await pg.execute("UPDATE storage_manifest SET rows = 1")
    result = await tiering.tier_out_day(DAY, allow_rearchive=False)
    assert not result.dropped
    assert "pg=3" in result.skipped_reason


@pytest.mark.asyncio
async def test_days_inside_the_hot_window_are_never_considered(pg, local_archive):
    """The partition holds every dataset, so it may only go once the LONGEST
    hot window has passed — otherwise funding would be dropped on the tick
    window's schedule."""
    cutoff = tiering.eligible_before(datetime(2026, 6, 30, tzinfo=UTC))
    assert cutoff < date(2026, 6, 30)
    # 30d funding/liquidation window is the longest, so ~30 days back.
    assert (date(2026, 6, 30) - cutoff).days >= 29

    await store.write_records([
        Record.create(dataset_id="btc_ohlcv_1m", source="bybit_v5_ws",
                      event_time=datetime(2026, 6, 29, tzinfo=UTC),
                      payload={"close": 1.0}, quality_ok=True)])
    results = await tiering.run_tier_out(now=datetime(2026, 6, 30, tzinfo=UTC))
    assert all(r.day < cutoff for r in results)


@pytest.mark.asyncio
async def test_every_attempt_is_recorded_even_when_nothing_happens(pg, local_archive):
    """Recording only failures makes 'all clear' and 'the job died' identical."""
    result = await tiering.tier_out_day(DAY)
    assert not result.dropped
    ev = await pg.fetch("SELECT * FROM tier_out_events")
    assert len(ev) == 1 and "no rows" in ev[0]["detail"]


# ---------------------------------------------------------------- retention

def test_the_records_that_must_never_expire_have_no_expiry():
    """Not a long retention — none. A period that exists can be shortened."""
    for cls in ("trials", "orders_fills_ledger", "strategies",
                "market_data", "llm_provenance", "coverage_ledger"):
        assert retention.is_forever(cls)
        assert retention.cutoff(cls) is None


def test_quarantine_expires_but_only_as_a_report():
    assert retention.POLICY["quarantine"] == 730
    assert retention.cutoff("quarantine") is not None


def test_an_unknown_class_raises_rather_than_defaulting():
    """Every stored thing needs an explicit answer to 'how long'."""
    with pytest.raises(KeyError):
        retention.is_forever("something_new")


def test_no_automatic_deletion_path_exists():
    """§9: retention changes are operator commits, never automatic."""
    import ast
    src = (postgres.SCHEMA_PATH.parent / "retention.py").read_text(encoding="utf-8")
    names = {n.name for n in ast.walk(ast.parse(src))
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not any(w in n for n in names for w in ("delete", "purge", "drop"))


# ---------------------------------------------------------------- integrity

@pytest.mark.asyncio
async def test_a_passing_check_still_leaves_evidence(pg, local_archive):
    """A checker that only records failures cannot be distinguished from one
    that stopped running."""
    results = await integrity.run_all()
    assert results
    rows = await pg.fetch("SELECT * FROM integrity_events")
    assert len(rows) == len(results)
    assert any(r["passed"] for r in rows)


@pytest.mark.asyncio
async def test_a_future_event_time_is_caught(pg, local_archive):
    await store.write_records([
        Record.create(dataset_id="btc_ohlcv_1m", source="bybit_v5_ws",
                      event_time=datetime.now(UTC) + timedelta(days=2),
                      payload={"close": 1.0}, quality_ok=True)])
    out = await integrity.check_timestamps_are_sane()
    future = [r for r in out if r.name == "no_future_event_time"][0]
    assert not future.passed
    assert future.observed["records_in_the_future"] == 1


@pytest.mark.asyncio
async def test_a_pre_launch_event_time_is_caught(pg, local_archive):
    await store.write_records([
        Record.create(dataset_id="btc_ohlcv_1m", source="bybit_v5_ws",
                      event_time=datetime(2015, 1, 1, tzinfo=UTC),
                      payload={"close": 1.0}, quality_ok=True)])
    out = await integrity.check_timestamps_are_sane()
    early = [r for r in out if r.name == "no_pre_launch_event_time"
             and r.observed["venue"] == "bybit"][0]
    assert not early.passed


@pytest.mark.asyncio
async def test_an_order_without_a_prediction_is_caught(pg):
    await pg.execute(
        "INSERT INTO orders (order_id, client_order_id, trade_id, strategy_id, "
        " symbol, side, order_type, qty, status) "
        "VALUES ('o1','c1','t-ghost','s1','BTCUSDT','Buy','Market',0.01,'New')")
    out = await integrity.check_orders_have_predictions()
    assert not out[0].passed
    assert out[0].observed["orders_without_prediction"] == 1


@pytest.mark.asyncio
async def test_a_failing_check_escalates(pg, local_archive):
    """§11: red-lining halts writes rather than continuing on corrupt state."""
    await pg.execute(
        "INSERT INTO orders (order_id, client_order_id, trade_id, strategy_id, "
        " symbol, side, order_type, qty, status) "
        "VALUES ('o1','c1','t-ghost','s1','BTCUSDT','Buy','Market',0.01,'New')")
    await integrity.run_all()
    tel = await pg.fetch(
        "SELECT * FROM telemetry_events "
        "WHERE label = 'integrity_orders_have_predictions'")
    assert len(tel) == 1


@pytest.mark.asyncio
async def test_bit_rot_is_detected_by_the_checksum_audit(pg, local_archive):
    await seed(2)
    key = local_archive.backend.list("archive", "")[0]
    local_archive.backend.put("archive", key, b"rotted")
    out = await integrity.check_archive_checksums()
    assert not out[0].passed
    assert out[0].observed["failed"] == 1


@pytest.mark.asyncio
async def test_a_raising_check_counts_as_a_failure(pg, local_archive, monkeypatch):
    """An integrity checker that dies quietly is indistinguishable from one
    that keeps passing."""
    async def boom():
        raise RuntimeError("kaboom")
    monkeypatch.setattr(integrity, "CHECKS", (boom,))
    results = await integrity.run_all()
    assert len(results) == 1 and not results[0].passed
    assert "kaboom" in results[0].observed["error"]


# ---------------------------------------------------------------- degradation

class FakeKills:
    def __init__(self):
        self.halted = None

    async def halt_portfolio(self, reason):
        self.halted = reason


class FakeRisk:
    def __init__(self):
        self.kills = FakeKills()


@pytest.mark.asyncio
async def test_postgres_unreachable_halts_rather_than_falling_back():
    """§7/§14: no file fallback, no queued-write side-channel. A queued write
    that never lands is worse than a rejected trade."""
    degrade.reset_for_tests()
    risk = FakeRisk()
    await degrade.on_postgres_unavailable(
        risk, postgres.PostgresUnavailable("connection refused"))
    assert risk.kills.halted == degrade.L2_REASON
    assert len(degrade.buffered()) == 1


@pytest.mark.asyncio
async def test_the_ring_buffer_is_bounded():
    """A process in trouble must not also exhaust memory remembering it."""
    degrade.reset_for_tests()
    for i in range(1500):
        degrade.buffer_degraded("x", f"reason {i}")
    assert len(degrade.buffered()) == 1000


@pytest.mark.asyncio
async def test_buffered_events_are_flushed_on_recovery(pg):
    degrade.reset_for_tests()
    degrade.buffer_degraded("postgres_unreachable", "was down")
    assert await degrade.flush_buffer() == 1
    rows = await pg.fetch(
        "SELECT * FROM telemetry_events WHERE label = 'postgres_unreachable'")
    assert len(rows) == 1
    assert not degrade.buffered()


@pytest.mark.asyncio
async def test_an_archive_outage_is_degraded_not_halted(pg):
    """The asymmetry that matters: Parquet down costs archive latency,
    Postgres down costs trading."""
    class Broken(LocalBackend):
        def exists(self, bucket, key):
            raise OSError("object storage unreachable")

    store.set_archive_for_tests(Archive(Broken("/nope"), "a", "q"))
    try:
        h = await degrade.health()
    finally:
        store.reset_for_tests()
    assert h["postgres"] is True
    assert h["archive"] is False
    assert h["status"] == "degraded"        # not "halted"


@pytest.mark.asyncio
async def test_no_fallback_store_appears_in_the_degradation_module():
    """The absence is the feature, so it is asserted directly."""
    import re
    src = (postgres.SCHEMA_PATH.parent / "degrade.py").read_text(encoding="utf-8")
    for pattern in (r"(?<![.\w])open\(\s*[\"'f]", r"\.write_text\(",
                    r"sqlite3", r"\bredis\b", r"shelve", r"pickle\.dump"):
        assert not re.search(pattern, src), (
            f"{pattern!r} in degrade.py — §7 says halt, not write elsewhere")
