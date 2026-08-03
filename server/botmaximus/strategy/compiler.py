"""Compiler (§5.8): a validated StrategyDefinition → a vectorized evaluator.

The compiled object satisfies the existing `Strategy` protocol
(`backtest/strategy.py`), so the backtester needs no special case for DSL
strategies. **The same compiled object is what runs live**, which is what closes
the research/production gap: there is no second implementation of the entry
logic to drift from this one.

Compilation is where the predicate tree stops being data and becomes a boolean
array — evaluated once over the whole window, then indexed per bar. Besides
being ~10^6× cheaper than walking the tree inside the replay loop, it means the
signal at bar *i* is fixed before the replay starts and cannot depend on
anything the replay does.

The compiler never emits, compiles or executes source code. It builds arrays.
"""
from __future__ import annotations

from dataclasses import dataclass

from botmaximus.backtest.data import MarketWindow, PointInTimeView
from botmaximus.config import settings
from botmaximus.backtest.engine import ExitPolicy
from botmaximus.features.compute import FeatureContext, FeatureRef
from botmaximus.risk.state import OrderIntent
from botmaximus.strategy.grammar import (
    And, Comparison, ConstTerm, FeatureTerm, Not, Or, ParamTerm, Predicate, Term,
)
from botmaximus.strategy.schema import StrategyDefinition

BoolSeries = list[bool]
Series = list[float | None]


class CompileError(Exception):
    pass


# --------------------------------------------------------------------------
# Predicate → boolean array
# --------------------------------------------------------------------------
def _term_series(t: Term, ctx: FeatureContext, params: dict[str, float],
                 n: int) -> Series:
    if isinstance(t, ConstTerm):
        return [t.value] * n
    if isinstance(t, ParamTerm):
        if t.name not in params:
            raise CompileError(f"undefined_param:{t.name}")
        return [params[t.name]] * n
    if isinstance(t, FeatureTerm):
        return ctx.series(t.ref)
    raise CompileError(f"unknown_term:{type(t).__name__}")


def _compare(op: str, left: Series, right: Series, right2: Series | None,
             n: int) -> BoolSeries:
    """Pointwise, except the cross operators which read i-1 as well.

    A comparison against a missing value is False, never True: an undefined
    feature must not be able to *trigger* an entry. Warmup silence is the
    correct behaviour, not a signal.
    """
    out = [False] * n
    for i in range(n):
        a, b = left[i], right[i]
        if a is None or b is None:
            continue
        if op == "gt":
            out[i] = a > b
        elif op == "lt":
            out[i] = a < b
        elif op == "gte":
            out[i] = a >= b
        elif op == "lte":
            out[i] = a <= b
        elif op == "between":
            c = right2[i] if right2 else None
            if c is not None:
                lo, hi = (b, c) if b <= c else (c, b)
                out[i] = lo <= a <= hi
        elif op in ("cross_above", "cross_below"):
            if i == 0:
                continue
            pa, pb = left[i - 1], right[i - 1]
            if pa is None or pb is None:
                continue
            out[i] = (pa <= pb and a > b) if op == "cross_above" else (pa >= pb and a < b)
        else:
            raise CompileError(f"unknown_op:{op}")
    return out


def compile_predicate(p: Predicate, ctx: FeatureContext,
                      params: dict[str, float], n: int) -> BoolSeries:
    if isinstance(p, And):
        parts = [compile_predicate(c, ctx, params, n) for c in p.children]
        return [all(part[i] for part in parts) for i in range(n)]
    if isinstance(p, Or):
        parts = [compile_predicate(c, ctx, params, n) for c in p.children]
        return [any(part[i] for part in parts) for i in range(n)]
    if isinstance(p, Not):
        inner = compile_predicate(p.child, ctx, params, n)
        return [not v for v in inner]
    if isinstance(p, Comparison):
        left = _term_series(p.left, ctx, params, n)
        right = _term_series(p.right, ctx, params, n)
        right2 = _term_series(p.right2, ctx, params, n) if p.right2 is not None else None
        return _compare(p.op, left, right, right2, n)
    raise CompileError(f"unknown_predicate:{type(p).__name__}")


# --------------------------------------------------------------------------
# Compiled strategy
# --------------------------------------------------------------------------
@dataclass
class CompiledStrategy:
    """Implements the `Strategy` protocol. Entry signals, stop distances,
    trailing distances and regime validity are all precomputed arrays."""
    id: str
    required_feeds: list[str]
    definition: StrategyDefinition
    signals: BoolSeries
    stop_distance: Series
    exit_policy: ExitPolicy
    #: whether each bar's regime is inside regime_scope. Entries always respect
    #: this; whether it also *closes* an open position is the separate
    #: regime_invalidation flag, carried on the exit policy.
    entry_regime_ok: list[bool] | None = None

    def evaluate(self, view: PointInTimeView) -> OrderIntent | None:
        i = view.i
        if i >= len(self.signals) or not self.signals[i]:
            return None
        if self.entry_regime_ok is not None and not self.entry_regime_ok[i]:
            return None                     # outside regime_scope — stand aside
        dist = self.stop_distance[i]
        if dist is None or dist <= 0:
            return None                     # no valid stop → no trade, ever
        price = view.now.close
        long = self.definition.direction == "long"
        stop = price - dist if long else price + dist
        if stop <= 0:
            return None
        return OrderIntent(
            strategy_id=self.id,
            direction="LONG" if long else "SHORT",
            entry_price=price,
            stop_price=stop,
            thesis=self.definition.rationale[:200],
        )


def _stop_distance_series(defn: StrategyDefinition, ctx: FeatureContext,
                          n: int) -> Series:
    """Stop distance in price units per bar — the risk core's sizing input.

    This is the only number the DSL contributes to sizing. It cannot state a
    quantity, and a bar with no computable stop distance yields None, which
    `evaluate` turns into "no trade" rather than a guessed default.
    """
    stop = defn.exit.stop
    bars = ctx.bars
    if stop.kind == "atr":
        atr = ctx.series(FeatureRef("atr", stop.timeframe, (("n", stop.n),)))
        return [None if v is None else v * stop.mult for v in atr]
    if stop.kind == "percent":
        return [b.close * stop.mult / 100 for b in bars]
    if stop.kind == "structural":
        # distance to the extreme of the last n bars on the stop timeframe:
        # a swing low for longs, a swing high for shorts
        long = defn.direction == "long"
        frame = ctx.frame(stop.timeframe)
        fb = frame.bars
        # rolling extreme over the last n frame bars, via a monotonic deque —
        # O(n) rather than the O(n*window) the obvious slice-and-min would cost
        from collections import deque
        vals: Series = [None] * len(fb)
        dq: deque[int] = deque()
        series = [b.low for b in fb] if long else [b.high for b in fb]
        for k, v in enumerate(series):
            while dq and dq[0] <= k - stop.n:
                dq.popleft()
            while dq and ((series[dq[-1]] >= v) if long else (series[dq[-1]] <= v)):
                dq.pop()
            dq.append(k)
            vals[k] = series[dq[0]]
        from botmaximus.features.compute import project
        extreme = project(frame, vals)
        out: Series = [None] * n
        for i, b in enumerate(bars):
            e = extreme[i]
            if e is None:
                continue
            d = (b.close - e) if long else (e - b.close)
            out[i] = d * stop.mult if d > 0 else None
        return out
    raise CompileError(f"unknown_stop_kind:{stop.kind}")


def compile_strategy(defn: StrategyDefinition, market: MarketWindow) -> CompiledStrategy:
    """§5.8. Assumes the definition already passed validation — compiling an
    unvalidated definition is a programming error, not a runtime path."""
    bars = market.bars
    n = len(bars)
    if n == 0:
        raise CompileError("empty_window")

    tfs = set(defn.timeframes) | {"1m", defn.exit.stop.timeframe}
    if defn.exit.trailing:
        tfs.add(defn.exit.trailing.timeframe)
    ctx = FeatureContext(market, tfs)

    params = {k: p.value for k, p in defn.params.items()}
    signals = compile_predicate(defn.entry, ctx, params, n)
    stop_distance = _stop_distance_series(defn, ctx, n)

    trail = None
    if defn.exit.trailing:
        t = defn.exit.trailing
        atr = ctx.series(FeatureRef("atr", t.timeframe, (("n", t.n),)))
        trail = [None if v is None else v * t.mult for v in atr]

    regime_ok = None
    scope = set(defn.regime_scope)
    if scope:
        labels = ctx.regime_labels()
        regime_ok = [lab in scope for lab in labels]

    policy = ExitPolicy(
        target_r=defn.exit.target.r if defn.exit.target else None,
        time_exit_bars=defn.exit.time_exit,
        trail_distance=trail,
        # regime_ok is always supplied so `evaluate` can gate entries on scope;
        # only regime_invalidation decides whether it also closes a live position
        regime_ok=regime_ok if defn.exit.regime_invalidation else None,
        regime_confirm_bars=settings.regime_invalidation_confirm_bars,
    )
    return CompiledStrategy(
        id=defn.id,
        required_feeds=list(defn.required_feeds),
        definition=defn,
        signals=signals,
        stop_distance=stop_distance,
        exit_policy=policy,
        entry_regime_ok=regime_ok,
    )
