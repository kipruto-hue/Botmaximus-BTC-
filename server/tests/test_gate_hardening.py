"""Pre-C2 gate hardening: the lifetime trial ledger, the sealed holdout, and
the raised trade floor.

These three exist because the gate as it stood would have certified noise the
moment a generator started producing in bulk: the deflated Sharpe was corrected
for one trial, every candidate was selected on the same bars with nothing held
back, and 30 trades is not enough to estimate a Sharpe from.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.backtest import holdout, metrics
from botmaximus.backtest.engine import BacktestResult, Trade
from botmaximus.backtest.validation import validate
from botmaximus.config import settings
from botmaximus.strategy import trials
from botmaximus.strategy.seeds import seed_definitions

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


# =====================================================================
# Fake Mongo — enough of the update_one/$inc/$setOnInsert contract
# =====================================================================
class FakeCursor:
    """Enough of the PyMongo cursor contract for find().sort().limit()."""
    def __init__(self, docs: list[dict]):
        self.docs = docs

    def sort(self, field, direction=1):
        self.docs = sorted(self.docs, key=lambda d: d.get(field) or 0,
                           reverse=direction < 0)
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    async def __aiter__(self):
        for d in self.docs:
            yield d


class FakeCollection:
    def __init__(self):
        self.docs: list[dict] = []

    @staticmethod
    def _match(doc: dict, query: dict) -> bool:
        return all(doc.get(k) == v for k, v in query.items())

    def find(self, query=None, projection=None):
        return FakeCursor([dict(d) for d in self.docs
                           if self._match(d, query or {})])

    async def find_one(self, query, projection=None):
        return next((d for d in self.docs if self._match(d, query)), None)

    async def insert_one(self, doc):
        self.docs.append(dict(doc))

    async def update_one(self, query, update, upsert=False):
        doc = await self.find_one(query)
        if doc is None:
            if not upsert:
                return
            doc = dict(update.get("$setOnInsert", {}))
            self.docs.append(doc)
        doc.update(update.get("$set", {}))
        for k, v in update.get("$inc", {}).items():
            doc[k] = doc.get(k, 0) + v

    async def count_documents(self, query):
        return sum(1 for d in self.docs if self._match(d, query))

    async def distinct(self, field):
        return list({d.get(field) for d in self.docs if field in d})

    async def create_index(self, *a, **kw):
        return None


class FakeDB(dict):
    def __missing__(self, key):
        self[key] = FakeCollection()
        return self[key]


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()
    from botmaximus.db import mongo
    monkeypatch.setattr(mongo, "get_db", lambda: fake)
    return fake


# =====================================================================
# The trial ledger
# =====================================================================
def test_the_fallback_trial_count_disables_the_correction():
    """Why the ledger has to exist at all: `expected_max_sharpe` returns a
    benchmark of 0.0 at n<=1, so the shipped default silently turned the
    multiple-testing correction into a plain probabilistic Sharpe."""
    assert metrics.expected_max_sharpe(0.5, settings.bt_candidate_trials) == 0.0
    assert metrics.expected_max_sharpe(0.5, 500) > 0.0


def test_more_trials_raises_the_bar_the_winner_must_clear():
    assert (metrics.expected_max_sharpe(0.5, 1000)
            > metrics.expected_max_sharpe(0.5, 100)
            > metrics.expected_max_sharpe(0.5, 10)
            > 0.0)


def _profitable_result(n: int = 250) -> BacktestResult:
    trades, equity, curve = [], 10_000.0, []
    for i in range(n):
        pnl = 12.0 if i % 4 else -9.0
        t = T0 + timedelta(minutes=5 * i)
        trades.append(Trade("s", "LONG", t, t + timedelta(minutes=5),
                            100.0, 100.1, 1.0, "target", pnl, 0.0, 0.0))
        equity += pnl
        curve.append((t, equity))
    return BacktestResult(trades, curve, 10_000.0, n * 5, 0)


def test_a_strategy_that_passes_at_one_trial_is_rejected_after_many():
    """The whole point. Identical trades, identical everything — only the
    lifetime search intensity differs, and that is enough to change the
    verdict. This is the certification that was being handed out for free."""
    res = _profitable_result()
    solo = validate(res, lambda _t: "uptrend", n_trials=1)
    searched = validate(res, lambda _t: "uptrend", n_trials=5000)
    assert searched.metrics["deflated_sharpe"] < solo.metrics["deflated_sharpe"]
    assert any(r.startswith("deflated_sharpe_below_threshold")
               for r in searched.reasons)


@pytest.mark.asyncio
async def test_ledger_counts_every_distinct_evaluation(db):
    defns = seed_definitions()
    n1 = await trials.record(defns[0], "cfg-a")
    n2 = await trials.record(defns[1], "cfg-a")
    n3 = await trials.record(defns[0], "cfg-b")
    assert (n1, n2, n3) == (1, 2, 3)


@pytest.mark.asyncio
async def test_replaying_an_identical_evaluation_is_not_a_new_trial(db):
    """A restart, a retry, or a dev re-run is not another look at the data.
    Anything that actually differs is."""
    defn = seed_definitions()[0]
    assert await trials.record(defn, "cfg-a") == 1
    assert await trials.record(defn, "cfg-a") == 1
    assert await trials.record(defn, "cfg-a") == 1
    assert await trials.record(defn, "cfg-c") == 2


@pytest.mark.asyncio
async def test_ledger_survives_a_restart(db):
    """Lifetime means lifetime: search intensity does not reset when the
    process does. The ledger is the only thing standing between a bulk
    generator and a DSR that flatters it."""
    defn = seed_definitions()[0]
    await trials.record(defn, "cfg-a")
    await trials.record(defn, "cfg-b")
    from botmaximus.db import mongo          # a "new process" reading the same db
    assert await trials.count() == 2
    assert mongo.get_db() is db


@pytest.mark.asyncio
async def test_variants_of_one_idea_still_each_cost_a_trial(db):
    """Fifty variants of one idea are fifty looks at the data. Counting them as
    one would under-correct, and under-correcting is the failure that matters —
    over-correcting only makes the gate harder to pass."""
    defn = seed_definitions()[0]
    for i in range(50):
        await trials.record(defn, f"cfg-{i}")
    assert await trials.count() == 50
    assert await trials.distinct_ideas() == 1


def test_signature_hash_is_stable_across_processes():
    """Hashed from a sorted signature — a frozenset has no order, and a key
    that changes between runs would silently restart the count."""
    defn = seed_definitions()[0]
    assert trials.signature_hash(defn) == trials.signature_hash(copy.deepcopy(defn))
    assert trials.signature_hash(defn) != trials.signature_hash(seed_definitions()[1])


def test_no_api_can_lower_the_trial_count():
    """A count that can be reset is a count that will be, on the day it is
    inconvenient. There is deliberately no reset/decrement entry point."""
    api = {n for n in dir(trials) if not n.startswith("_")}
    assert not any(w in n.lower() for n in api
                   for w in ("reset", "clear", "delete", "decrement", "prune"))


# =====================================================================
# The sealed holdout
# =====================================================================
def test_holdout_seals_the_most_recent_window():
    now = datetime(2026, 8, 2, tzinfo=timezone.utc)
    assert holdout.boundary(now) == now - timedelta(days=settings.holdout_days)


def test_search_window_reaching_into_the_holdout_is_refused():
    now = datetime(2026, 8, 2, tzinfo=timezone.utc)
    with pytest.raises(holdout.HoldoutViolation):
        holdout.assert_outside(now - timedelta(days=365), now, now)


def test_search_window_stopping_at_the_boundary_is_allowed():
    now = datetime(2026, 8, 2, tzinfo=timezone.utc)
    holdout.assert_outside(now - timedelta(days=365), holdout.boundary(now), now)


def test_clip_end_never_extends_a_window():
    now = datetime(2026, 8, 2, tzinfo=timezone.utc)
    early = now - timedelta(days=300)
    assert holdout.clip_end(early, now) == early          # already legal, untouched
    assert holdout.clip_end(now, now) == holdout.boundary(now)


@pytest.mark.asyncio
async def test_holdout_can_be_spent_once_per_strategy(db):
    await holdout.assert_unburned("seed_x")               # unspent — fine
    await holdout.record_burn("seed_x", {"passed": False}, holdout.window())
    with pytest.raises(holdout.HoldoutViolation):
        await holdout.assert_unburned("seed_x")


@pytest.mark.asyncio
async def test_burning_one_strategy_does_not_burn_another(db):
    await holdout.record_burn("seed_x", {"passed": True}, holdout.window())
    await holdout.assert_unburned("seed_y")


@pytest.mark.asyncio
async def test_a_burn_is_recorded_even_for_a_failing_verdict(db):
    """The holdout is spent by *looking*, not by passing. A failed run that
    left the window reusable would make repeated attempts free."""
    await holdout.record_burn("seed_x", {"passed": False, "reasons": ["x"]},
                              holdout.window())
    assert await holdout.is_burned("seed_x")


# =====================================================================
# The trade floor
# =====================================================================
def test_trade_floor_is_high_enough_to_estimate_a_sharpe():
    assert settings.bt_min_trades >= 200


def test_thirty_trades_no_longer_clears_the_floor():
    """Four of five Gate-3 seeds were judged on merit at the old floor of 30,
    which is far too few trades to distinguish an edge from noise."""
    res = _profitable_result(30)
    v = validate(res, lambda _t: "uptrend", n_trials=1)
    assert any(r.startswith("insufficient_trades") for r in v.reasons)
