"""Strategy DSL tests (Strategy DSL & Generation Master Prompt §3–§5, §8).

Most of this file tests things the DSL must *refuse*. That is the point of
having a DSL instead of letting a model write code: the §10 failure-mode table
lists what GPT-5.5 will eventually try, and each row should have a test here
proving the attempt dies at the boundary rather than reaching the backtester.
"""
from __future__ import annotations

import copy
import math
from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.backtest.data import Bar, MarketWindow
from botmaximus.backtest.engine import DEFAULT_EXIT_POLICY, Backtester, ExitPolicy
from botmaximus.backtest.strategy import Strategy
from botmaximus.backtest.validation import ValidationVerdict
from botmaximus.config import settings
from botmaximus.strategy import grammar, lifecycle
from botmaximus.strategy.compiler import compile_strategy
from botmaximus.strategy.schema import StrategyDefinition
from botmaximus.strategy.seeds import SEED_PAYLOADS, seed_definitions
from botmaximus.strategy.validator import (
    parse_and_validate, signature, similarity, validate,
)

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def mkbars(n: int, fn=None) -> list[Bar]:
    fn = fn or (lambda i: 100 + math.sin(i / 40) * 7 + i * 0.002)
    out = []
    for i in range(n):
        s = T0 + timedelta(minutes=i)
        c = fn(i)
        out.append(Bar(s, s + timedelta(minutes=1, milliseconds=-1),
                       c, c + 0.5, c - 0.5, c, 10.0))
    return out


@pytest.fixture
def base_payload() -> dict:
    return copy.deepcopy(SEED_PAYLOADS[0])


# =====================================================================
# §8.1 seeds
# =====================================================================
def test_all_seeds_parse_validate_and_compile():
    defns = seed_definitions()
    assert len(defns) == 5
    bars = mkbars(6000)
    for d in defns:
        _, res = parse_and_validate(d.to_dict())
        assert res.ok, (d.id, res.reasons)
        compiled = compile_strategy(d, MarketWindow(bars=bars))
        assert isinstance(compiled, Strategy)


def test_every_seed_states_an_economic_rationale():
    """§1.7 — a proposal without a `why` is rejected. Rationale is what makes
    the population auditable and repairable, not decoration."""
    for d in seed_definitions():
        assert len(d.rationale) > 80, d.id


def test_seeds_are_macro_structural_only():
    """§8.1: order book and liquidations have no venue history, so a
    microstructure seed could not be validated today."""
    for d in seed_definitions():
        assert not ({"orderbook", "liquidations"} & set(d.required_feeds)), d.id


def test_seed_ids_are_unique_and_diverse():
    defns = seed_definitions()
    assert len({d.id for d in defns}) == len(defns)
    sigs = [(d.id, signature(d)) for d in defns]
    for i, (id_a, a) in enumerate(sigs):
        for id_b, b in sigs[i + 1:]:
            assert similarity(a, b) < settings.diversity_threshold, (id_a, id_b)


# =====================================================================
# §5.1 structural — the absences that carry the safety weight
# =====================================================================
def test_no_size_field_exists(base_payload):
    """§3, §10: the grammar has no field for absolute size, so 'tries to set
    position size' is not a check that can be missed — it cannot be written."""
    base_payload["size"] = 0.5
    _, res = parse_and_validate(base_payload)
    assert not res.ok and "unknown_keys" in res.reasons[0]
    assert not hasattr(StrategyDefinition, "size")
    assert "size" not in StrategyDefinition.ALLOWED_KEYS
    assert "qty" not in StrategyDefinition.ALLOWED_KEYS


@pytest.mark.parametrize("key,value", [
    ("leverage", 10), ("equity", 1000), ("max_drawdown", 5),
    ("risk_per_trade", 2), ("notional", 5000),
])
def test_account_and_risk_fields_are_rejected(base_payload, key, value):
    """§4.3: the DSL cannot read equity, raise limits, or touch the risk core."""
    base_payload[key] = value
    _, res = parse_and_validate(base_payload)
    assert not res.ok, f"{key} was accepted"


def test_missing_stop_fails_to_parse(base_payload):
    """§4.3: `exit.stop` is required — a stopless strategy does not parse."""
    base_payload["exit"].pop("stop")
    _, res = parse_and_validate(base_payload)
    assert not res.ok and "missing_stop" in res.reasons[0]


def test_stop_basis_is_an_alias_and_cannot_disagree():
    d = seed_definitions()[0]
    assert d.stop_basis is d.exit.stop


def test_unknown_top_level_key_is_rejected_not_ignored(base_payload):
    base_payload["notes"] = "hello"
    _, res = parse_and_validate(base_payload)
    assert not res.ok


@pytest.mark.parametrize("field", ["rationale", "entry", "exit", "timeframes",
                                   "required_feeds", "regime_scope", "direction"])
def test_required_fields_are_required(base_payload, field):
    base_payload.pop(field)
    _, res = parse_and_validate(base_payload)
    assert not res.ok, field


def test_unknown_direction_rejected(base_payload):
    base_payload["direction"] = "sideways"
    _, res = parse_and_validate(base_payload)
    assert not res.ok


# =====================================================================
# §5.2 registry — the anti-hallucination gate
# =====================================================================
def test_unknown_feature_is_a_hard_reject(base_payload):
    base_payload["entry"]["and"].append(
        {"left": {"feature": "supertrend_v2", "timeframe": "1h", "args": {"n": 10}},
         "op": "gt", "right": 0})
    _, res = parse_and_validate(base_payload)
    assert any(r.startswith("unknown_feature:supertrend_v2") for r in res.reasons)


def test_unknown_arg_is_rejected(base_payload):
    base_payload["entry"]["and"][0]["right"]["args"] = {"n": 20, "smoothing": 3}
    _, res = parse_and_validate(base_payload)
    assert any("unknown_arg" in r for r in res.reasons)


def test_categorical_feature_cannot_be_compared(base_payload):
    """regime_label has no ordering; gt/lt on it is meaningless. Regimes are
    declared through regime_scope."""
    base_payload["entry"]["and"].append(
        {"left": {"feature": "regime_label"}, "op": "gt", "right": 1})
    _, res = parse_and_validate(base_payload)
    assert any("categorical_feature_in_predicate" in r for r in res.reasons)


def test_missing_timeframe_on_timeframed_feature(base_payload):
    base_payload["entry"]["and"][0]["right"].pop("timeframe")
    _, res = parse_and_validate(base_payload)
    assert any("missing_timeframe" in r for r in res.reasons)


def test_unknown_timeframe_rejected(base_payload):
    base_payload["entry"]["and"][0]["right"]["timeframe"] = "3s"
    base_payload["timeframes"].append("3s")
    _, res = parse_and_validate(base_payload)
    assert any("unknown_timeframe" in r for r in res.reasons)


# =====================================================================
# §5.3 bounds
# =====================================================================
@pytest.mark.parametrize("n", [1, 99999, -5, 0])
def test_feature_arg_bounds_enforced(base_payload, n):
    base_payload["entry"]["and"][0]["right"]["args"] = {"n": n}
    _, res = parse_and_validate(base_payload)
    assert not res.ok, n


def test_param_outside_its_own_bounds_rejected(base_payload):
    base_payload["params"]["adx_floor"]["value"] = 999
    _, res = parse_and_validate(base_payload)
    assert any("param_out_of_bounds" in r for r in res.reasons)


def test_undefined_param_reference_rejected(base_payload):
    base_payload["entry"]["and"][1]["right"] = {"param": "ghost"}
    _, res = parse_and_validate(base_payload)
    assert any("undefined_param_ref:ghost" in r for r in res.reasons)


def test_time_exit_bounded_by_max_holding_bars(base_payload):
    base_payload["exit"]["time_exit"] = settings.max_holding_bars_cap + 1
    _, res = parse_and_validate(base_payload)
    assert any("time_exit_exceeds_cap" in r for r in res.reasons)


def test_time_exit_at_the_cap_is_allowed(base_payload):
    base_payload["exit"]["time_exit"] = settings.max_holding_bars_cap
    _, res = parse_and_validate(base_payload)
    assert res.ok, res.reasons


# =====================================================================
# §5.4 consistency — features ↔ feeds ↔ timeframes
# =====================================================================
def test_undeclared_feed_is_rejected(base_payload):
    """The dangerous direction: an undeclared feed is never checked against the
    coverage ledger, which is how a strategy gets backtested over absent data."""
    base_payload["entry"]["and"].append(
        {"left": {"feature": "funding_zscore", "args": {"n": 30}},
         "op": "gt", "right": 2})
    _, res = parse_and_validate(base_payload)
    assert any("undeclared_feed:funding" in r for r in res.reasons)


def test_undeclared_timeframe_is_rejected(base_payload):
    base_payload["timeframes"] = ["1m", "15m"]          # drops 1h
    _, res = parse_and_validate(base_payload)
    assert any("undeclared_timeframe:1h" in r for r in res.reasons)


def test_declared_but_unused_feed_is_rejected(base_payload):
    base_payload["required_feeds"] = ["ohlcv", "orderbook"]
    _, res = parse_and_validate(base_payload)
    assert any("declared_unused_feed:orderbook" in r for r in res.reasons)


def test_oi_strategy_warns_about_the_30_day_history_cap():
    oi_seed = [d for d in seed_definitions() if "oi" in d.required_feeds][0]
    res = validate(oi_seed)
    assert res.ok
    assert any("history_capped_30d" in w for w in res.warnings)


# =====================================================================
# §5.6 lookahead — inexpressible, then asserted anyway
# =====================================================================
def test_grammar_ops_are_exactly_the_declared_set():
    assert set(grammar.OPS) == {"gt", "lt", "gte", "lte",
                                "cross_above", "cross_below", "between"}


@pytest.mark.parametrize("arg", ["shift", "offset", "lead", "forward", "ahead"])
def test_lookahead_style_args_are_rejected(base_payload, arg):
    base_payload["entry"]["and"][0]["right"]["args"] = {"n": 20, arg: -3}
    _, res = parse_and_validate(base_payload)
    assert not res.ok, arg


def test_no_grammar_production_can_name_a_future_bar(base_payload):
    """There is no term type for a bar index, so 'enter at the low' has no
    spelling. Anything that tries becomes an unknown key or unknown term."""
    for attempt in ({"bar": 5}, {"index": -1}, {"future": True},
                    {"expr": "close[i+1]"}, "close[i+1]", [1, 2]):
        p = copy.deepcopy(base_payload)
        p["entry"]["and"].append({"left": attempt, "op": "gt", "right": 0})
        _, res = parse_and_validate(p)
        assert not res.ok, attempt


def test_no_code_can_be_smuggled_through_a_field(base_payload):
    for payload in ("__import__('os').system('echo hi')", "lambda x: x", {"eval": "1+1"}):
        p = copy.deepcopy(base_payload)
        p["entry"]["and"][0]["right"] = payload
        _, res = parse_and_validate(p)
        assert not res.ok, payload


def test_constant_only_comparison_rejected(base_payload):
    """§4.1: the left side is always a Feature. `5 > 3` is not a market
    condition and would let a generator emit trivially-true entries."""
    base_payload["entry"]["and"].append({"left": 5, "op": "gt", "right": 3})
    _, res = parse_and_validate(base_payload)
    assert any("left_must_be_feature" in r for r in res.reasons)


def test_unknown_operator_rejected(base_payload):
    base_payload["entry"]["and"][0]["op"] = "eval"
    _, res = parse_and_validate(base_payload)
    assert any("unknown_op" in r for r in res.reasons)


# =====================================================================
# §5.7 diversity
# =====================================================================
def test_near_duplicate_is_rejected(base_payload):
    original = seed_definitions()[0]
    clone = copy.deepcopy(base_payload)
    clone["id"] = "clone"
    clone["params"]["adx_floor"]["value"] = 23.0        # trivially different
    _, res = parse_and_validate(clone, population=[(original.id, signature(original))])
    assert any("near_duplicate_of" in r for r in res.reasons)


def test_a_genuinely_different_idea_passes_dedupe():
    a, c = seed_definitions()[0], seed_definitions()[2]
    _, res = parse_and_validate(c.to_dict(), population=[(a.id, signature(a))])
    assert res.ok, res.reasons


def test_signature_ignores_trivial_param_drift():
    """Two strategies differing only by an EMA length of 50 vs 51 are one idea.
    Counting them as two independent trials corrupts the deflated Sharpe."""
    a = seed_definitions()[0]
    b_payload = copy.deepcopy(SEED_PAYLOADS[0])
    b_payload["entry"]["and"][0]["right"]["args"] = {"n": 51}
    b = StrategyDefinition.parse(b_payload)
    assert similarity(signature(a), signature(b)) >= settings.diversity_threshold


# =====================================================================
# §5.8 compile
# =====================================================================
def test_compiled_strategy_satisfies_the_protocol():
    d = seed_definitions()[0]
    c = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    assert isinstance(c, Strategy)
    assert c.id == d.id and c.required_feeds == list(d.required_feeds)


def test_compilation_is_deterministic():
    d = seed_definitions()[1]
    a = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    b = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    assert a.signals == b.signals
    assert a.stop_distance == b.stop_distance


def test_compiled_signals_have_no_lookahead():
    d = seed_definitions()[1]
    split = 3000
    bars = mkbars(4000)
    ref = compile_strategy(d, MarketWindow(bars=bars))

    tampered = mkbars(4000)
    for i in range(split, 4000):
        b = tampered[i]
        tampered[i] = Bar(b.open_time, b.close_time, 9e5, 9e5, 9e5, 9e5, 1e6)
    other = compile_strategy(d, MarketWindow(bars=tampered))
    assert ref.signals[:split] == other.signals[:split]
    assert ref.stop_distance[:split] == other.stop_distance[:split]


@pytest.mark.parametrize("kind", ["atr", "percent", "structural"])
def test_every_stop_kind_produces_a_positive_distance(base_payload, kind):
    base_payload["exit"]["stop"] = {"kind": kind, "timeframe": "15m",
                                    "n": 14, "mult": 1.5}
    defn, res = parse_and_validate(base_payload)
    assert res.ok, res.reasons
    c = compile_strategy(defn, MarketWindow(bars=mkbars(6000)))
    defined = [v for v in c.stop_distance if v is not None]
    assert defined, kind
    assert all(v > 0 for v in defined), kind


def test_structural_stop_tracks_the_rolling_low(base_payload):
    """A long's structural stop sits below the recent swing low, so the distance
    must grow when price rises away from it."""
    base_payload["exit"]["stop"] = {"kind": "structural", "timeframe": "1m",
                                    "n": 20, "mult": 1.0}
    defn, _ = parse_and_validate(base_payload)
    bars = mkbars(600, lambda i: 100.0 if i < 300 else 100.0 + (i - 300) * 0.1)
    c = compile_strategy(defn, MarketWindow(bars=bars))
    assert c.stop_distance[350] > c.stop_distance[305]


def test_structural_stop_has_no_lookahead(base_payload):
    base_payload["exit"]["stop"] = {"kind": "structural", "timeframe": "5m",
                                    "n": 20, "mult": 1.0}
    defn, _ = parse_and_validate(base_payload)
    split = 3000
    ref = compile_strategy(defn, MarketWindow(bars=mkbars(4000)))
    tampered = mkbars(4000)
    for i in range(split, 4000):
        b = tampered[i]
        tampered[i] = Bar(b.open_time, b.close_time, 1.0, 1.0, 0.01, 1.0, b.volume)
    other = compile_strategy(defn, MarketWindow(bars=tampered))
    assert ref.stop_distance[:split] == other.stop_distance[:split]


def test_missing_feature_value_never_triggers_an_entry():
    """A comparison against None is False. Warmup silence must not be a signal."""
    d = seed_definitions()[0]
    c = compile_strategy(d, MarketWindow(bars=mkbars(200)))   # far too short
    assert not any(c.signals)


def test_evaluate_returns_none_without_a_stop_distance():
    d = seed_definitions()[0]
    c = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    c.signals = [True] * len(c.signals)
    c.stop_distance = [None] * len(c.stop_distance)
    from botmaximus.backtest.data import PointInTimeView
    assert c.evaluate(PointInTimeView(mkbars(4000), 3000)) is None


def test_intent_carries_no_quantity():
    """Sizing is an output of the risk core; an intent states direction and
    prices only."""
    d = seed_definitions()[0]
    c = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    c.signals = [True] * len(c.signals)
    c.stop_distance = [1.0] * len(c.stop_distance)
    c.entry_regime_ok = None
    from botmaximus.backtest.data import PointInTimeView
    bars = mkbars(4000)
    intent = c.evaluate(PointInTimeView(bars, 3500))
    assert intent is not None
    assert not hasattr(intent, "qty") and not hasattr(intent, "size")
    assert intent.stop_price < intent.entry_price       # long


def test_short_strategy_places_its_stop_above_entry():
    d = [x for x in seed_definitions() if x.direction == "short"][0]
    c = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    c.signals = [True] * len(c.signals)
    c.stop_distance = [1.0] * len(c.stop_distance)
    c.entry_regime_ok = None
    from botmaximus.backtest.data import PointInTimeView
    intent = c.evaluate(PointInTimeView(mkbars(4000), 3500))
    assert intent.direction == "SHORT" and intent.stop_price > intent.entry_price


def test_entry_is_gated_by_regime_scope():
    d = seed_definitions()[0]
    c = compile_strategy(d, MarketWindow(bars=mkbars(4000)))
    c.signals = [True] * len(c.signals)
    c.stop_distance = [1.0] * len(c.stop_distance)
    c.entry_regime_ok = [False] * len(c.signals)
    from botmaximus.backtest.data import PointInTimeView
    assert c.evaluate(PointInTimeView(mkbars(4000), 3500)) is None


@pytest.mark.parametrize("op,left,right,expected", [
    ("gt", [2.0], [1.0], True), ("lt", [2.0], [1.0], False),
    ("gte", [1.0], [1.0], True), ("lte", [1.0], [1.0], True),
])
def test_comparison_ops(op, left, right, expected):
    from botmaximus.strategy.compiler import _compare
    assert _compare(op, left, right, None, 1)[0] is expected


def test_cross_operators_need_the_previous_bar():
    from botmaximus.strategy.compiler import _compare
    left, right = [1.0, 3.0], [2.0, 2.0]
    out = _compare("cross_above", left, right, None, 2)
    assert out == [False, True]
    assert _compare("cross_below", [3.0, 1.0], [2.0, 2.0], None, 2) == [False, True]


def test_between_is_order_insensitive():
    from botmaximus.strategy.compiler import _compare
    assert _compare("between", [5.0], [1.0], [9.0], 1)[0] is True
    assert _compare("between", [5.0], [9.0], [1.0], 1)[0] is True
    assert _compare("between", [50.0], [1.0], [9.0], 1)[0] is False


def test_none_never_evaluates_true():
    from botmaximus.strategy.compiler import _compare
    for op in grammar.OPS:
        assert _compare(op, [None], [1.0], [2.0], 1) == [False], op
        assert _compare(op, [1.0], [None], [2.0], 1) == [False], op


# =====================================================================
# Exit policy (engine)
# =====================================================================
def test_default_policy_reproduces_pre_pass_c_behaviour():
    assert DEFAULT_EXIT_POLICY.target_r == 1.0
    assert DEFAULT_EXIT_POLICY.time_exit_bars is None
    assert DEFAULT_EXIT_POLICY.trail_distance is None
    assert DEFAULT_EXIT_POLICY.regime_ok is None


def _run_with(policy, bars, direction="LONG", stop_dist=2.0):
    from botmaximus.backtest.costs import CostModel
    from botmaximus.risk.core import RiskCore
    from botmaximus.risk.state import OrderIntent, PortfolioState

    class Once:
        id = "once"
        required_feeds = ["ohlcv"]

        def __init__(self):
            self.fired = False

        def evaluate(self, view):
            if self.fired or view.i != 60:
                return None
            self.fired = True
            p = view.now.close
            return OrderIntent(strategy_id=self.id, direction=direction,
                               entry_price=p,
                               stop_price=p - stop_dist if direction == "LONG"
                               else p + stop_dist)

    risk = RiskCore.__new__(RiskCore)
    risk.portfolio = PortfolioState(equity=10_000, peak_equity=10_000,
                                    day_start_equity=10_000, day_start_date="2025-01-01")
    risk.db = None
    bt = Backtester(CostModel(funding=[]), risk, policy=policy)
    from botmaximus.risk.state import SizedOrder

    def _size(intent):
        return SizedOrder(intent=intent, qty=0.1, notional_usd=10.0,
                          risk_usd=1.0, implied_leverage=1.0,
                          est_liquidation_price=0.0)
    risk.size_intent = _size
    return bt.run(bars, Once(), warmup=60)


def test_time_exit_from_the_policy_is_honoured():
    bars = mkbars(400, lambda i: 100.0)          # flat: only time can exit
    res = _run_with(ExitPolicy(target_r=1.0, time_exit_bars=7), bars)
    assert res.trades and res.trades[0].exit_reason == "time"
    held = (res.trades[0].exit_time - res.trades[0].entry_time).total_seconds() / 60
    assert held == pytest.approx(7)


def test_target_r_multiple_is_honoured():
    bars = mkbars(400, lambda i: 100.0 + max(0, i - 61) * 0.5)   # rises after entry
    r1 = _run_with(ExitPolicy(target_r=1.0, time_exit_bars=100), bars)
    r3 = _run_with(ExitPolicy(target_r=3.0, time_exit_bars=100), bars)
    assert r1.trades[0].exit_reason == "target"
    assert r3.trades[0].exit_reason == "target"
    assert r3.trades[0].exit_price > r1.trades[0].exit_price


def test_no_target_means_only_stop_or_time_can_exit():
    bars = mkbars(400, lambda i: 100.0 + max(0, i - 61) * 5)
    res = _run_with(ExitPolicy(target_r=None, time_exit_bars=20), bars)
    assert res.trades[0].exit_reason == "time"


def test_trailing_stop_ratchets_and_exits_as_trail():
    # rise then collapse: a trailing stop must capture the give-back
    def path(i):
        if i < 62:
            return 100.0
        return 100.0 + (i - 62) * 1.0 if i < 80 else 118.0 - (i - 80) * 2.0
    bars = mkbars(400, path)
    res = _run_with(ExitPolicy(target_r=None, time_exit_bars=100,
                               trail_distance=[3.0] * 400), bars)
    assert res.trades and res.trades[0].exit_reason == "trail"
    assert res.trades[0].exit_price > 100.0, "the ratchet locked in a profit"


def test_trailing_stop_never_loosens():
    """A widening trail distance must not walk the stop back down. If the
    ratchet were missing, the stop would jump from ~108 to ~60 at bar 75 and
    the position would run to the time exit instead."""
    def path(i):
        if i < 62:
            return 100.0
        return 100.0 + (i - 62) * 0.5 if i < 82 else 110.0 - (i - 82) * 1.0
    bars = mkbars(400, path)
    widening = [2.0] * 400
    for i in range(75, 400):
        widening[i] = 50.0                  # would loosen the stop enormously
    res = _run_with(ExitPolicy(target_r=None, time_exit_bars=100,
                               trail_distance=widening), bars)
    assert res.trades and res.trades[0].exit_reason == "trail"
    # the stop froze at the level it reached just before the trail widened
    assert res.trades[0].exit_price > 104.0

    # contrast: a trail so wide it never ratchets above the original stop
    # leaves the original stop in charge — a loss instead of a locked-in gain
    loose = _run_with(ExitPolicy(target_r=None, time_exit_bars=100,
                                 trail_distance=[50.0] * 400), bars)
    assert loose.trades[0].exit_reason == "stop"
    assert loose.trades[0].net_pnl < 0 < res.trades[0].net_pnl


def test_regime_invalidation_closes_the_position():
    bars = mkbars(400, lambda i: 100.0)
    ok = [True] * 400
    for i in range(70, 400):
        ok[i] = False                            # regime leaves scope at bar 70
    res = _run_with(ExitPolicy(target_r=1.0, time_exit_bars=100, regime_ok=ok), bars)
    assert res.trades and res.trades[0].exit_reason == "regime"


def test_stop_still_wins_over_regime_invalidation():
    """Stop-first worst case is not negotiable — it must not be pre-empted by a
    softer exit that happens to be checked nearby."""
    def path(i):
        return 100.0 if i < 62 else 90.0
    bars = mkbars(400, path)
    ok = [True] * 400
    for i in range(62, 400):
        ok[i] = False
    res = _run_with(ExitPolicy(target_r=1.0, time_exit_bars=100, regime_ok=ok), bars)
    assert res.trades[0].exit_reason == "stop"


# =====================================================================
# Lifecycle
# =====================================================================
def _verdict(passed: bool) -> ValidationVerdict:
    return ValidationVerdict(passed=passed,
                             reasons=[] if passed else ["nonpositive_net_expectancy"],
                             metrics={"trades": 40})


def test_candidate_cannot_be_promoted_without_a_verdict():
    with pytest.raises(lifecycle.LifecycleError, match="requires_validation_verdict"):
        lifecycle.promote("s", "candidate", None)


def test_candidate_cannot_be_promoted_on_a_failing_verdict():
    with pytest.raises(lifecycle.LifecycleError, match="requires_passing_verdict"):
        lifecycle.promote("s", "candidate", _verdict(False))


def test_candidate_promotes_to_paper_on_a_passing_verdict():
    t = lifecycle.promote("s", "candidate", _verdict(True))
    assert (t.from_state, t.to_state) == ("candidate", "paper")
    assert t.verdict["passed"] is True


def test_promotion_is_one_rung_at_a_time():
    assert lifecycle.next_state("paper") == "micro"
    assert lifecycle.next_state("micro") == "full"
    assert lifecycle.next_state("full") is None


def test_retired_is_terminal():
    """§7.4: a repair is a new candidate that re-earns its way through the
    gate, never a patch applied to something still trading."""
    with pytest.raises(lifecycle.LifecycleError, match="terminal"):
        lifecycle.promote("s", "retired", _verdict(True))
    with pytest.raises(lifecycle.LifecycleError, match="already_retired"):
        lifecycle.retire("s", "retired", "because")


@pytest.mark.parametrize("state", ["candidate", "paper", "micro", "full"])
def test_retirement_is_always_available(state):
    t = lifecycle.retire("s", state, "decay_detected")
    assert t.to_state == "retired"


def test_retirement_requires_a_reason():
    with pytest.raises(lifecycle.LifecycleError, match="requires_reason"):
        lifecycle.retire("s", "paper", "")


def test_rejection_records_the_reasons():
    t = lifecycle.reject("s", _verdict(False))
    assert t.to_state == "retired"
    assert "nonpositive_net_expectancy" in t.reason


def test_cannot_reject_a_passing_verdict():
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.reject("s", _verdict(True))


def test_unknown_state_is_rejected():
    with pytest.raises(lifecycle.LifecycleError, match="unknown_state"):
        lifecycle.promote("s", "live", _verdict(True))


# =====================================================================
# Round-trip
# =====================================================================
def test_definition_round_trips_through_json():
    for d in seed_definitions():
        again = StrategyDefinition.parse(d.to_dict())
        assert again.to_dict() == d.to_dict()


def test_long_equity_curves_are_downsampled_under_the_bson_limit():
    """A 365-day 1m run produces ~525k curve points, which serialises to ~33MB
    and exceeds Mongo's 16MB document cap. The curve is a display artefact —
    the trade list is the reproducible record — so it is strided, not dropped,
    and the final equity is kept exact."""
    from botmaximus.backtest.store import MAX_CURVE_POINTS, downsample_curve
    curve = [(datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i),
              10_000 + i) for i in range(525_600)]
    out, stride = downsample_curve(curve)
    assert stride > 1
    assert len(out) <= MAX_CURVE_POINTS + 1
    assert out[0] == curve[0] and out[-1] == curve[-1]


def test_short_equity_curves_are_kept_at_full_resolution():
    from botmaximus.backtest.store import downsample_curve
    curve = [(datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i),
              10_000.0) for i in range(100)]
    out, stride = downsample_curve(curve)
    assert stride == 1 and out == curve


def test_upsert_never_clobbers_lifecycle_state_or_last_verdict():
    """Re-registering a strategy is not a new verdict and not a resurrection.
    These three fields must be insert-only, or a re-run of the seed gate would
    erase the gate history the dashboard shows and C2 learns from."""
    from botmaximus.strategy import store
    doc = store.build_doc(seed_definitions()[0])
    insert_only = {"lifecycle_state", "created_at", "last_verdict"}
    assert insert_only <= set(doc), doc.keys()


def test_config_declares_c2_params_unset():
    """§2: the build must fail loudly on unset generator params rather than
    invent a value. They are declared here, decided in Pass C2."""
    assert settings.generation_llm is None
    assert settings.candidate_cap_per_cycle is None
    assert settings.generation_cadence_s is None
    assert settings.vector_store == "chroma"
