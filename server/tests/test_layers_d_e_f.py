"""Arbiter, scrutiny gate, generator, decay monitor and paper engine.

Written around the properties that make these layers safe rather than around
their API surface: veto-on-anything-unclear, one position, no size stacking,
trial-per-attempt, no margin leakage, and a stop that is never optional.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.arbiter.core import Arbiter, StrategySignal
from botmaximus.config import settings
from botmaximus.execution.session import Session
from botmaximus.obs import degradation
from botmaximus.risk.state import OpenPosition, OrderIntent
from botmaximus.scrutiny.analog import AnalogRules, RuleBasedAnalog
from botmaximus.scrutiny.gate import ScrutinyGate
from botmaximus.scrutiny.provider import APPROVE, VETO, ScrutinyProvider, ScrutinyVerdict
from botmaximus.strategy import decay as decay_mod
from botmaximus.strategy.decay import DecayMonitor, DecayTrigger, current_losing_run
from botmaximus.strategy.generator import Generator, NullProposer, coarsen


# =====================================================================
# fakes
# =====================================================================
class FakeKills:
    def __init__(self):
        self.suspended = {}
        self.block = None
        self.halted = None

    def blocks_trading(self):
        return self.block

    def strategy_suspended(self, sid):
        return self.suspended.get(sid)

    async def suspend_strategy(self, sid, reason):
        self.suspended[sid] = reason

    async def halt_portfolio(self, reason):
        self.halted = reason


class FakePortfolio:
    def __init__(self):
        self.open_positions = []
        self.equity = 10_000.0


class FakeRisk:
    def __init__(self):
        self.kills = FakeKills()
        self.portfolio = FakePortfolio()

    @staticmethod
    def _check_edge_over_cost(intent):
        if intent.expected_edge_pct is None or intent.expected_cost_pct is None:
            return []
        return ([] if intent.expected_edge_pct >= intent.expected_cost_pct * 1.5
                else ["edge_below_cost_margin"])


@pytest.fixture
def db(pg):
    """Kept under its old name so the tests below read unchanged; it is now a
    real, empty Postgres schema rather than a fake collection."""
    return pg


def sig(sid="s1", direction="LONG", conf=0.8, **kw):
    return StrategySignal(strategy_id=sid, direction=direction, confidence=conf,
                          entry_price=64_000.0, stop_price=63_500.0, **kw)


def _fresh_all():
    from botmaximus.pipeline.telemetry import telemetry
    for ds in ("btc_price_tick", "btc_ohlcv_1m", "btc_funding",
               "btc_open_interest", "btc_orderbook"):
        telemetry._last_event[ds] = datetime.now(timezone.utc)


# =====================================================================
# ARBITER
# =====================================================================
@pytest.mark.asyncio
async def test_one_signal_becomes_one_intent(db):
    a = Arbiter(FakeRisk())
    d = await a.decide([sig()])
    assert d.reason == "ok" and d.intent.strategy_id == "s1"


@pytest.mark.asyncio
async def test_opposite_directions_stand_aside(db):
    """Disagreement is evidence the situation is outside at least one
    strategy's competence -- not a weak signal to be netted."""
    a = Arbiter(FakeRisk())
    d = await a.decide([sig("s1", "LONG"), sig("s2", "SHORT")])
    assert d.intent is None and d.reason == "conflict"


@pytest.mark.asyncio
async def test_same_direction_signals_do_not_stack_size(db):
    """Two strategies agreeing does not authorise double size: risk_per_trade
    is a per-trade budget."""
    a = Arbiter(FakeRisk())
    d = await a.decide([sig("s1", conf=0.5), sig("s2", conf=0.9)])
    assert d.intent is not None
    assert d.intent.strategy_id == "s2"          # highest score wins outright
    assert not hasattr(d.intent, "qty")          # sizing is risk's job, later


@pytest.mark.asyncio
async def test_an_open_position_blocks_every_new_entry(db):
    r = FakeRisk()
    r.portfolio.open_positions.append(
        OpenPosition("s1", "LONG", 0.01, 64_000.0, 63_500.0, 5.0))
    d = await Arbiter(r).decide([sig()])
    assert d.intent is None and d.reason == "position_open"


@pytest.mark.asyncio
async def test_cooldown_blocks_churn(db):
    a = Arbiter(FakeRisk(), cooldown_s=300)
    a.note_entry()
    d = await a.decide([sig()])
    assert d.reason == "cooldown"


@pytest.mark.asyncio
async def test_outside_the_session_window_nothing_enters(db):
    closed = Session(enabled=True, tz="Africa/Nairobi",
                     start=datetime(2026, 1, 1, 3, 0).time(),
                     end=datetime(2026, 1, 1, 3, 1).time())
    d = await Arbiter(FakeRisk(), session=closed).decide([sig()])
    assert d.reason == "outside_window"


@pytest.mark.asyncio
async def test_an_unknown_regime_drops_a_scoped_strategy(db):
    """Never guess a regime."""
    a = Arbiter(FakeRisk())
    d = await a.decide([sig(regime_scope=("uptrend",))], regime=None)
    assert d.intent is None and d.reason == "all_stale"
    assert d.dropped["s1"] == "regime_unknown"


@pytest.mark.asyncio
async def test_an_unscoped_strategy_survives_an_unknown_regime(db):
    d = await Arbiter(FakeRisk()).decide([sig()], regime=None)
    assert d.reason == "ok"


@pytest.mark.asyncio
async def test_every_decision_is_recorded_including_the_refusals(db):
    """'Why didn't it trade?' is unanswerable from a log of trades that did."""
    a = Arbiter(FakeRisk())
    await a.decide([sig("s1", "LONG"), sig("s2", "SHORT")])
    rows = await db.fetch("SELECT * FROM arbiter_events")
    assert len(rows) == 1
    assert rows[0]["reason"] == "conflict"


# =====================================================================
# SCRUTINY
# =====================================================================
class AlwaysApprove(ScrutinyProvider):
    name = "test"

    @property
    def version(self):
        return "test_v1"

    async def evaluate(self, context):
        return ScrutinyVerdict(APPROVE, "ok", self.name, self.version)


class Hangs(ScrutinyProvider):
    name = "hangs"

    @property
    def version(self):
        return "hangs"

    async def evaluate(self, context):
        await asyncio.sleep(5)
        return ScrutinyVerdict(APPROVE, "late", self.name, self.version)


class Raises(ScrutinyProvider):
    name = "raises"

    @property
    def version(self):
        return "raises"

    async def evaluate(self, context):
        raise ValueError("boom")


def _intent(**kw):
    base = dict(strategy_id="s1", direction="LONG", entry_price=64_000.0,
                stop_price=63_500.0, required_feeds=("ohlcv",))
    base.update(kw)
    return OrderIntent(**base)


@pytest.mark.asyncio
async def test_a_kill_vetoes_without_ever_consulting_the_provider(db):
    """Scrutiny cannot un-veto a deterministic risk block."""
    r = FakeRisk()
    r.kills.block = "L3_master_kill:test"
    g = ScrutinyGate(r, AlwaysApprove())
    v = await g.review(_intent(), {})
    assert v.verdict == VETO
    assert v.provider == "deterministic"


@pytest.mark.asyncio
async def test_a_stale_required_feed_vetoes(db):
    from botmaximus.pipeline.telemetry import telemetry
    telemetry._last_event["btc_ohlcv_1m"] = (
        datetime.now(timezone.utc) - timedelta(minutes=30))
    v = await ScrutinyGate(FakeRisk(), AlwaysApprove()).review(_intent(), {})
    assert v.verdict == VETO and "btc_ohlcv_1m" in v.reason


@pytest.mark.asyncio
async def test_a_timeout_is_a_veto_not_a_wait(db, monkeypatch):
    _fresh_all()
    monkeypatch.setattr(settings, "scrutiny_latency_budget_ms", 50)
    v = await ScrutinyGate(FakeRisk(), Hangs()).review(_intent(), {})
    assert v.verdict == VETO and v.reason == "provider_timeout"
    assert degradation.counts().get("scrutiny_timeout") == 1


@pytest.mark.asyncio
async def test_a_raising_provider_is_a_veto(db):
    _fresh_all()
    v = await ScrutinyGate(FakeRisk(), Raises()).review(_intent(), {})
    assert v.verdict == VETO and "provider_error" in v.reason


@pytest.mark.asyncio
async def test_zero_analogs_is_a_veto_not_a_pass():
    """Missing evidence is not approval."""
    p = RuleBasedAnalog(AnalogRules(min_analogs=20), bars_provider=None)
    v = await p.evaluate({"direction": "LONG", "regime": "uptrend"})
    assert v.verdict == VETO and "insufficient_analogs" in v.reason


@pytest.mark.asyncio
async def test_analogs_that_mostly_went_badly_veto():
    async def provider(key, start, cutoff, ctx):
        return [{"adverse_pct": 5.0, "spread_pct": 0.01} for _ in range(30)]

    p = RuleBasedAnalog(AnalogRules(min_analogs=20), bars_provider=provider)
    v = await p.evaluate({"direction": "LONG", "regime": "uptrend"})
    assert v.verdict == VETO and "analogs_adverse" in v.reason


@pytest.mark.asyncio
async def test_benign_analogs_approve():
    async def provider(key, start, cutoff, ctx):
        return [{"adverse_pct": 0.2, "spread_pct": 0.01} for _ in range(30)]

    p = RuleBasedAnalog(AnalogRules(min_analogs=20), bars_provider=provider)
    v = await p.evaluate({"direction": "LONG", "regime": "uptrend"})
    assert v.verdict == APPROVE


@pytest.mark.asyncio
async def test_analogs_are_drawn_from_before_the_embargo():
    """Without the embargo the 'historical' windows overlap the bar being
    judged, and the gate reads the answer off the exam paper."""
    seen = {}

    async def provider(key, start, cutoff, ctx):
        seen["cutoff"] = cutoff
        seen["now"] = ctx["now"]
        return [{"adverse_pct": 0.1, "spread_pct": 0.01} for _ in range(25)]

    now = datetime(2026, 8, 1, tzinfo=timezone.utc)
    p = RuleBasedAnalog(AnalogRules(min_analogs=20, embargo_s=3600),
                        bars_provider=provider)
    await p.evaluate({"direction": "LONG", "regime": "uptrend", "now": now})
    assert seen["cutoff"] == now - timedelta(seconds=3600)


@pytest.mark.asyncio
async def test_funding_against_the_position_vetoes():
    p = RuleBasedAnalog(AnalogRules(funding_veto_abs=0.0005))
    v = await p.evaluate({"direction": "LONG", "regime": "uptrend",
                          "funding_rate": 0.001})
    assert v.verdict == VETO and "funding_against_position" in v.reason


def test_the_provider_cannot_modify_an_order():
    """Veto-only is structural: the verdict type carries no size, price, stop
    or direction field to modify."""
    fields = set(ScrutinyVerdict.__dataclass_fields__)
    assert not (fields & {"qty", "size", "price", "stop", "stop_price",
                          "direction", "target"})


def test_the_llm_provider_is_unreachable_without_credentials():
    from botmaximus.scrutiny.provider import LLMProvider
    with pytest.raises(RuntimeError, match="missing required secret"):
        LLMProvider()


# =====================================================================
# GENERATOR
# =====================================================================
def test_feedback_is_coarsened_to_check_names_only():
    """A margin tells an optimiser exactly how far to push. Names do not."""
    raw = ["deflated_sharpe_below_threshold:0.412<0.95",
           "insufficient_trades:30<200",
           "walkforward_inconsistent:1/4"]
    out = coarsen(raw)
    assert out == ["deflated_sharpe_below_threshold", "insufficient_trades",
                   "walkforward_inconsistent"]
    assert not any(ch.isdigit() for ch in "".join(out))


@pytest.mark.asyncio
async def test_the_brief_never_carries_a_margin(db):
    g = Generator(NullProposer(), generations_dir=None) if False else \
        Generator(NullProposer())
    brief = await g.build_brief(
        population=[], recent_failures=[["deflated_sharpe_below_threshold:0.4<0.95"]])
    blob = str(brief["recent_failure_kinds"])
    assert "0.4" not in blob and "<" not in blob


@pytest.mark.asyncio
async def test_generation_requires_an_explicit_candidate_cap(db, monkeypatch):
    monkeypatch.setattr(settings, "candidate_cap_per_cycle", None)
    with pytest.raises(RuntimeError, match="multiple-testing"):
        await Generator(NullProposer()).generate()


@pytest.mark.asyncio
async def test_validation_is_refused_without_provenance(db):
    from botmaximus.strategy.generator import Proposal
    from botmaximus.strategy.seeds import seed_definitions

    p = Proposal(definition=seed_definitions()[0], generation_id="x",
                 provenance_path=None)
    with pytest.raises(RuntimeError, match="no provenance"):
        await Generator(NullProposer()).validate_proposal(p, None, None)


@pytest.mark.asyncio
async def test_a_repair_beyond_the_lineage_cap_is_refused(db, monkeypatch):
    monkeypatch.setattr(settings, "candidate_cap_per_cycle", 2)
    monkeypatch.setattr(settings, "lineage_repair_cap", 3)
    with pytest.raises(RuntimeError, match="chained Goodharting"):
        await Generator(NullProposer()).generate(lineage_depth=4)


@pytest.mark.asyncio
async def test_proposals_get_provenance_files(db, monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "candidate_cap_per_cycle", 2)
    g = Generator(NullProposer(), generations_dir=tmp_path)
    out = await g.generate()
    assert out.proposed == 2
    for p in out.accepted:
        assert p.provenance_path.exists()


# =====================================================================
# DECAY
# =====================================================================
def test_the_trigger_refuses_to_invent_a_significance_level():
    with pytest.raises(RuntimeError, match="invented significance"):
        DecayTrigger.parse(None)


def test_the_trigger_parses_the_documented_form():
    t = DecayTrigger.parse("sequential:min_trades=50,alpha=0.01")
    assert (t.kind, t.min_trades, t.alpha) == ("sequential", 50, 0.01)


def test_losing_runs_are_counted_from_the_end():
    assert current_losing_run([1.0, -1, -1, -1]) == 3
    assert current_losing_run([-1, -1, 1.0]) == 0


@pytest.mark.asyncio
async def test_a_normal_losing_streak_does_not_retire_a_working_strategy(db):
    """A 45%-win strategy throws a run of four regularly. Retiring on that
    replaces a working strategy with one that has not had its streak yet."""
    m = DecayMonitor(FakeRisk(), DecayTrigger("sequential", 50, 0.01))
    pnls = [1.0] * 56 + [-1.0] * 4
    v = await m.evaluate("s1", pnls, validated_win_rate=0.45)
    assert v.decayed is False


@pytest.mark.asyncio
async def test_an_improbable_losing_run_fires(db):
    m = DecayMonitor(FakeRisk(), DecayTrigger("sequential", 50, 0.01))
    pnls = [1.0] * 50 + [-1.0] * 12
    v = await m.evaluate("s1", pnls, validated_win_rate=0.55)
    assert v.decayed and v.cause == "alpha_decay"


@pytest.mark.asyncio
async def test_decay_suspends_the_strategy(db):
    r = FakeRisk()
    m = DecayMonitor(r, DecayTrigger("sequential", 50, 0.01))
    v = await m.evaluate("s1", [1.0] * 50 + [-1.0] * 12, 0.55)
    await m.on_decay(v)
    assert r.kills.suspended["s1"] == "alpha_decay"
    rows = await db.fetch(
        "SELECT * FROM strategy_events WHERE event = 'decay'")
    assert len(rows) == 1
    assert rows[0]["detail"]["cause"] == "alpha_decay"
