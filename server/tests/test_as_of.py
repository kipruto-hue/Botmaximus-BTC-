r"""Point-in-time pinning (Storage v2.0 §6, §14).

Written after a mutation check found this guarantee unguarded: deleting the
`as_of` refusal in `save_run` broke nothing in the suite. §6 calls an unpinned
backtest a defect, so the defect needs a test that fails.

Why it matters concretely: without a pinned instant, re-running the same
backtest months later silently reads whatever corrections have landed since.
The config hash is unchanged, the run looks identical, and the numbers differ —
which is the silent-revision failure the bitemporal axis exists to prevent,
arriving through the runner instead of through the store.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.backtest import store
from botmaximus.backtest.runner import pin_as_of

UTC = timezone.utc
T0 = datetime(2026, 6, 1, tzinfo=UTC)


class _Verdict:
    passed = False
    reasons = ["insufficient_trades"]
    metrics = {"deflated_sharpe": 0.0}


class _Result:
    equity_curve = [(T0, 10_000.0), (T0 + timedelta(minutes=1), 10_001.0)]
    trades: list = []
    params = {"time_stop_bars": 5}


def _doc(as_of):
    return store.build_run_doc(
        "s1",
        {"strategy_id": "s1", "window": [T0.isoformat(),
                                         (T0 + timedelta(days=1)).isoformat()]},
        {"ohlcv": {"missing_slots": 0}}, _Result(), _Verdict(), as_of=as_of)


def test_pin_as_of_defaults_to_now_rather_than_leaving_it_unset():
    """§6: 'default = run start'. Refusing outright would be worse rather than
    stricter — callers would pass datetime.now() themselves and the pin would
    drift across the several loads one run performs."""
    before = datetime.now(UTC)
    pinned = pin_as_of(None)
    assert before <= pinned <= datetime.now(UTC)
    assert pinned.tzinfo is not None


def test_pin_as_of_refuses_a_naive_instant():
    """A naive datetime means something different on the Tokyo VPS than here."""
    with pytest.raises(ValueError, match="timezone-aware"):
        pin_as_of(datetime(2026, 6, 1))


def test_pin_as_of_passes_an_explicit_instant_through():
    assert pin_as_of(T0) == T0


@pytest.mark.asyncio
async def test_saving_a_run_without_as_of_is_refused(pg):
    """The defect §6 names. `backtest_runs.as_of` is NOT NULL so such a run is
    unrecordable anyway; failing here gives the operator the reason instead of
    a constraint violation from three layers down."""
    with pytest.raises(ValueError, match="as_of"):
        await store.save_run(_doc(None))
    assert await pg.fetchval("SELECT count(*) AS n FROM backtest_runs") == 0


@pytest.mark.asyncio
async def test_a_pinned_run_persists_with_its_instant(pg, tmp_path):
    from botmaximus.storage import records as store_mod
    from botmaximus.storage.archive import Archive, LocalBackend
    store_mod.set_archive_for_tests(
        Archive(LocalBackend(tmp_path), "archive", "quarantine"))
    try:
        await store.save_run(_doc(T0))
    finally:
        store_mod.reset_for_tests()

    row = await pg.fetchrow("SELECT * FROM backtest_runs")
    assert row["as_of"] == T0
    assert row["passed"] is False
    assert row["curve_blob_key"]


def test_as_of_is_not_part_of_the_config_hash():
    """It changes on every run, so hashing it would give every run a unique
    hash — defeating the replay-dedupe the hash exists for, and inflating the
    lifetime trial ledger on every restart. That is precisely the
    multiple-testing correction this system is trying to keep honest."""
    a = _doc(T0)
    b = _doc(T0 + timedelta(days=30))
    assert a["config_hash"] == b["config_hash"]
    assert a["as_of"] != b["as_of"]


@pytest.mark.asyncio
async def test_two_runs_at_the_same_as_of_do_not_double_count_trials(pg):
    """The consequence of the hash rule above, stated as behaviour: replaying
    an identical evaluation is not another look at the data."""
    from botmaximus.strategy import trials
    from botmaximus.strategy.seeds import seed_definitions

    defn = seed_definitions()[0]
    cfg_hash = store.config_hash(_doc(T0)["config"])
    assert await trials.record(defn, cfg_hash) == 1
    assert await trials.record(defn, cfg_hash) == 1
