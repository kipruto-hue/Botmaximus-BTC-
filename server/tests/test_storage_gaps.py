r"""Provenance, nightly snapshots and feature-set storage (§3.B, §3.D, §3.G, §14).

The three parts of the spec that were still unbuilt after the Mongo migration.
Each is tested for the property that makes it worth having, not for the API:
provenance that cannot vanish, coverage state that stays answerable after the
ledger changes its mind, and feature versions that cannot be recomputed in
place.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from botmaximus.storage import features, postgres, provenance, snapshots
from botmaximus.storage import records as store
from botmaximus.storage.archive import Archive, LocalBackend

UTC = timezone.utc
DAY = date(2026, 6, 1)


@pytest.fixture
def local_archive(tmp_path):
    a = Archive(LocalBackend(tmp_path), "archive", "quarantine")
    store.set_archive_for_tests(a)
    yield a
    store.reset_for_tests()


# ---------------------------------------------------------------- §3.G / §14

@pytest.mark.asyncio
async def test_provenance_lands_in_postgres_and_parquet(pg, local_archive):
    ref = await provenance.record_generation(
        strategy_id="s1", proposer="test",
        brief={"n_requested": 3}, raw_response={"raw": "some model output"},
        model_id="gpt-5.6-luna-pro", temperature=0.9, seed=11)

    assert provenance.blob_exists(ref.blob_key)
    blob = await provenance.read_blob(ref.blob_key)
    assert "some model output" in blob["raw_response"]

    row = await pg.fetchrow("SELECT * FROM generations")
    assert row["strategy_id"] == "s1"
    assert row["provenance_blob_key"] == ref.blob_key
    assert row["model_id"] == "gpt-5.6-luna-pro"
    assert float(row["temperature"]) == 0.9


@pytest.mark.asyncio
async def test_no_local_json_store_remains(pg):
    """§14: 'If it isn't Postgres or Parquet, it does not get written.'"""
    import re
    src = (postgres.SCHEMA_PATH.parent.parent / "strategy" /
           "generator.py").read_text(encoding="utf-8")
    assert not re.search(r"\.write_text\(", src)
    assert "GENERATIONS_DIR" not in src.replace(
        "#: There is deliberately no generations directory.", "")


@pytest.mark.asyncio
async def test_a_missing_blob_is_not_provenance(pg, local_archive):
    """The check asks the archive, not a local path — a path is only true on
    the box that wrote it."""
    assert provenance.blob_exists(None) is False
    assert provenance.blob_exists("provenance/generation/nope.parquet") is False


@pytest.mark.asyncio
async def test_scrutiny_blob_is_linked_to_its_verdict(pg, local_archive):
    await pg.execute(
        "INSERT INTO scrutiny_events (intent_id, strategy_id, direction, "
        " verdict, reason, provider, provider_version) "
        "VALUES ('i1','s1','LONG','VETO','stale','test','v1')")
    key = await provenance.record_scrutiny_blob("i1", "the prompt", "the answer")
    row = await pg.fetchrow("SELECT * FROM scrutiny_events WHERE intent_id='i1'")
    assert row["prompt_blob_key"] == key
    assert (await provenance.read_blob(key))["prompt"] == "the prompt"


# ---------------------------------------------------------------- §3.B / §3.I

@pytest.mark.asyncio
async def test_coverage_snapshot_freezes_a_mutable_ledger(pg, local_archive):
    """The ledger changes its mind when a backfill lands. Without a snapshot,
    'why was this window refused in April but accepted in November?' has no
    answer."""
    from botmaximus.pipeline import coverage
    slot = datetime(2026, 6, 1, 10, 0, tzinfo=UTC)
    await coverage.mark("btc_ohlcv_1m", slot, coverage.MISSING, "heartbeat")

    first = await snapshots.snapshot_coverage(DAY)
    assert first["rows"] == 1

    # A backfill arrives and the ledger revises itself.
    await coverage.mark("btc_ohlcv_1m", slot, coverage.COMPLETE, "record")
    row = await pg.fetchrow("SELECT state FROM coverage_ledger")
    assert row["state"] == "complete"

    # The frozen copy still says what it said on the day.
    table = local_archive.read(first["key"])
    assert "missing" in table.to_pydict()["row"][0]


@pytest.mark.asyncio
async def test_money_records_are_exported_for_the_archive(pg, local_archive):
    """Postgres stays the system of record; this is the copy that survives the
    VPS."""
    t = datetime(2026, 6, 1, 12, tzinfo=UTC)
    await pg.execute(
        "INSERT INTO execution_ledger_predictions (trade_id, leg, strategy_id, "
        " direction, symbol, qty, decision_time, reference_price, "
        " predicted_fill, predicted_fee, predicted_slippage_bps, "
        " predicted_latency_ms) "
        "VALUES ('t1','entry','s1','LONG','BTCUSDT',0.01,%s,60000,60001,0.33,1,120)",
        (t,))
    await pg.execute(
        "INSERT INTO kill_events (level, reason, at) VALUES ('L2','test',%s)",
        (t,))

    out = await snapshots.snapshot_money(DAY)
    tables = {o["table"] for o in out}
    assert {"execution_ledger_predictions", "kill_events"} <= tables
    for o in out:
        assert local_archive.backend.exists("archive", o["key"])
    # Indexed, so the checksum audit can see them.
    keys = [o["key"] for o in out]
    n = await pg.fetchval(
        "SELECT count(*) AS n FROM storage_manifest WHERE object_key = ANY(%s)",
        (keys,))
    assert n == len(keys)


@pytest.mark.asyncio
async def test_one_snapshot_failing_does_not_skip_the_other(pg, monkeypatch,
                                                            local_archive):
    async def boom(day=None):
        raise RuntimeError("coverage exploded")
    monkeypatch.setattr(snapshots, "snapshot_coverage", boom)
    await pg.execute(
        "INSERT INTO kill_events (level, reason, at) VALUES ('L2','x',%s)",
        (datetime(2026, 6, 1, 12, tzinfo=UTC),))

    result = await snapshots.run_nightly(DAY)
    assert "coverage_error" in result
    assert any(o["table"] == "kill_events" for o in result["money"])


# ---------------------------------------------------------------- §3.D

@pytest.mark.asyncio
async def test_registering_a_feature_set_is_idempotent(pg):
    a = await features.register(1, {"features": ["ema"]})
    b = await features.register(1, {"features": ["ema"]})
    assert a.registry_hash == b.registry_hash
    assert len(await features.catalog()) == 1


@pytest.mark.asyncio
async def test_a_changed_registry_cannot_reuse_a_version(pg, monkeypatch):
    """§3.D: a definition change is a NEW version. Recomputing v1 in place
    would silently change what every backtest citing v1 measured."""
    await features.register(1, {"features": ["ema"]})
    monkeypatch.setattr(features, "registry_hash", lambda *a, **k: "deadbeef")
    with pytest.raises(features.FeatureVersionConflict, match="NEW version"):
        await features.register(1, {"features": ["ema", "adx"]})


@pytest.mark.asyncio
async def test_feature_vectors_are_versioned_and_not_overwritten(pg, local_archive):
    await features.register(1, {"features": ["ema"]})
    key = await features.write_vectors(1, DAY, {"t": [1, 2], "ema": [1.0, 2.0]})
    assert local_archive.backend.exists("archive", key)

    with pytest.raises(features.FeatureVersionConflict, match="new version"):
        await features.write_vectors(1, DAY, {"t": [1, 2], "ema": [9.0, 9.0]})

    # v2 coexists with v1 rather than replacing it.
    await features.register(2, {"features": ["ema", "adx"]})
    key2 = await features.write_vectors(2, DAY, {"t": [1], "adx": [5.0]})
    assert key != key2
    assert features.read_vectors(1, DAY).to_pydict()["ema"] == [1.0, 2.0]


@pytest.mark.asyncio
async def test_retiring_a_version_keeps_its_files(pg, local_archive):
    """§9 keeps features forever, and a retired version is still what some past
    backtest cites."""
    await features.register(1, {"features": ["ema"]})
    key = await features.write_vectors(1, DAY, {"t": [1], "ema": [1.0]})
    await features.retire(1)

    assert await features.current_version() is None
    assert local_archive.backend.exists("archive", key)
    assert (await features.catalog())[0]["retired_at"] is not None


@pytest.mark.asyncio
async def test_an_empty_feature_day_is_refused(pg, local_archive):
    await features.register(1, {"features": ["ema"]})
    with pytest.raises(ValueError, match="empty"):
        await features.write_vectors(1, DAY, {})
