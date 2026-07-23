"""Validation gate (§5.5). A strategy must pass ALL checks, measured gate-off,
on coverage-complete windows, to leave `candidate`. A negative verdict is a
valid and useful outcome — it is far cheaper to learn a strategy has no edge
here than with capital committed.

Checks:
- minimum trade count (statistical power)
- positive net expectancy after full costs
- walk-forward out-of-sample consistency (not in-sample fit)
- deflated Sharpe clears threshold (multiple-testing correction)
- stability across ≥ N distinct regimes
- max drawdown within ceiling
Purge/embargo helpers guard train/test boundaries against label leakage.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from botmaximus.backtest import metrics
from botmaximus.backtest.engine import BacktestResult, Trade
from botmaximus.config import settings

PSR_THRESHOLD = 0.95        # ≥95% confidence the deflated Sharpe beats the benchmark


@dataclass
class ValidationVerdict:
    passed: bool
    reasons: list[str] = field(default_factory=list)     # why it failed
    metrics: dict = field(default_factory=dict)


def purge_embargo(n_samples: int, test_start: int, test_end: int,
                  purge: int, embargo: int) -> set[int]:
    """Training indices to DROP around a test block [test_start, test_end):
    `purge` bars each side (label horizon overlap) plus `embargo` after. This
    is what stops a label that spans the boundary from leaking (§5.4)."""
    drop = set(range(max(0, test_start - purge), test_start))
    drop |= set(range(test_end, min(n_samples, test_end + purge + embargo)))
    return drop


def _returns(trades: list[Trade], equity: float) -> list[float]:
    """Per-trade net returns as a fraction of equity at risk."""
    return [t.net_pnl / equity for t in trades]


def _regime_expectancy(trades: list[Trade], regime_of) -> dict:
    buckets: dict[str, float] = {}
    for t in trades:
        r = regime_of(t.entry_time)
        buckets[r] = buckets.get(r, 0.0) + t.net_pnl
    return buckets


def validate(result: BacktestResult, regime_of, n_trials: int | None = None,
             trial_sr_std: float | None = None) -> ValidationVerdict:
    reasons: list[str] = []
    trades = result.trades
    n_trials = n_trials if n_trials is not None else settings.bt_candidate_trials

    if len(trades) < settings.bt_min_trades:
        reasons.append(f"insufficient_trades:{len(trades)}<{settings.bt_min_trades}")

    rets = _returns(trades, result.starting_equity)
    net_total = sum(t.net_pnl for t in trades)
    gross_total = sum(t.gross_pnl for t in trades)
    if net_total <= 0:
        reasons.append("nonpositive_net_expectancy")

    dsr = metrics.deflated_sharpe(rets, n_trials, trial_sr_std) if len(rets) >= 3 else 0.0
    if dsr < PSR_THRESHOLD:
        reasons.append(f"deflated_sharpe_below_threshold:{dsr:.3f}<{PSR_THRESHOLD}")

    eq = [e for _, e in result.equity_curve]
    dd = metrics.max_drawdown_pct(eq)
    if dd > settings.bt_max_drawdown_pct:
        reasons.append(f"drawdown_exceeds_ceiling:{dd:.1f}%>{settings.bt_max_drawdown_pct}%")

    regimes = _regime_expectancy(trades, regime_of)
    positive_regimes = sum(1 for v in regimes.values() if v > 0)
    if positive_regimes < settings.bt_min_regimes_positive:
        reasons.append(
            f"unstable_across_regimes:{positive_regimes}<{settings.bt_min_regimes_positive}")

    wf = walk_forward_consistency(trades, result.starting_equity)
    if not wf["consistent"]:
        reasons.append(f"walkforward_inconsistent:{wf['positive_folds']}/{wf['folds']}")

    return ValidationVerdict(
        passed=not reasons,
        reasons=reasons,
        metrics={
            "trades": len(trades),
            "gross_pnl": round(gross_total, 2),
            "net_pnl": round(net_total, 2),
            "friction": round(gross_total - net_total, 2),
            "net_return_pct": round(result.net_return_pct, 3),
            "deflated_sharpe": round(dsr, 4),
            "max_drawdown_pct": round(dd, 2),
            "regime_expectancy": {k: round(v, 2) for k, v in regimes.items()},
            "positive_regimes": positive_regimes,
            "walk_forward": wf,
        },
    )


def walk_forward_consistency(trades: list[Trade], equity: float,
                             folds: int | None = None) -> dict:
    """Split the trade sequence into sequential folds; require a majority to be
    net-positive. Sequential (not shuffled) folds are the out-of-sample test —
    consistency across time, not in-sample fit."""
    folds = folds or settings.bt_walkforward_folds
    if len(trades) < folds:
        return {"consistent": False, "folds": folds, "positive_folds": 0}
    size = len(trades) // folds
    positive = 0
    for f in range(folds):
        chunk = trades[f * size: (f + 1) * size] if f < folds - 1 else trades[f * size:]
        if sum(t.net_pnl for t in chunk) > 0:
            positive += 1
    return {"consistent": positive >= (folds // 2 + 1),
            "folds": folds, "positive_folds": positive}
