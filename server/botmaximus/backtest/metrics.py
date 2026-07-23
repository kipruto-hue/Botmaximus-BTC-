"""Performance statistics for validation (§5.5). Includes the deflated Sharpe
machinery that corrects for multiple testing — generating many candidates
guarantees some look good by chance, and an uncorrected Sharpe rewards exactly
that (failure mode: dead strategies that only backtested well once).

References: Bailey & López de Prado, "The Deflated Sharpe Ratio" (2014).
"""
from __future__ import annotations

import math
from statistics import NormalDist

_N = NormalDist()
_EULER = 0.5772156649015329


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _skew(xs: list[float]) -> float:
    n = len(xs)
    s = _std(xs)
    if n < 3 or s == 0:
        return 0.0
    m = _mean(xs)
    return sum(((x - m) / s) ** 3 for x in xs) * n / ((n - 1) * (n - 2))


def _kurtosis(xs: list[float]) -> float:
    """Non-excess (normal ≈ 3)."""
    n = len(xs)
    s = _std(xs)
    if n < 4 or s == 0:
        return 3.0
    m = _mean(xs)
    return sum(((x - m) / s) ** 4 for x in xs) / n


def sharpe(returns: list[float]) -> float:
    """Per-observation Sharpe (not annualised) of a trade-return series."""
    s = _std(returns)
    return _mean(returns) / s if s > 0 else 0.0


def probabilistic_sharpe(returns: list[float], benchmark_sr: float = 0.0) -> float:
    """P(true SR > benchmark_sr) given track-record length, skew and kurtosis."""
    n = len(returns)
    if n < 3:
        return 0.0
    sr = sharpe(returns)
    sk, ku = _skew(returns), _kurtosis(returns)
    denom = math.sqrt(max(1e-12, 1 - sk * sr + (ku - 1) / 4 * sr * sr))
    return _N.cdf((sr - benchmark_sr) * math.sqrt(n - 1) / denom)


def expected_max_sharpe(trial_sr_std: float, n_trials: int) -> float:
    """Expected maximum Sharpe from n_trials independent strategies with
    per-trial Sharpe std `trial_sr_std` — the benchmark the winner must beat."""
    if n_trials <= 1 or trial_sr_std <= 0:
        return 0.0
    a = _N.inv_cdf(1 - 1 / n_trials)
    b = _N.inv_cdf(1 - 1 / (n_trials * math.e))
    return trial_sr_std * ((1 - _EULER) * a + _EULER * b)


def deflated_sharpe(returns: list[float], n_trials: int,
                    trial_sr_std: float | None = None) -> float:
    """PSR against the expected-max-Sharpe benchmark for n_trials. With one
    trial this reduces to the ordinary probabilistic Sharpe (benchmark 0)."""
    if trial_sr_std is None:
        trial_sr_std = _std(returns) and abs(sharpe(returns)) / 2 or 0.0
    bench = expected_max_sharpe(trial_sr_std, n_trials)
    return probabilistic_sharpe(returns, benchmark_sr=bench)


def max_drawdown_pct(equity_curve: list[float]) -> float:
    peak = -math.inf
    worst = 0.0
    for e in equity_curve:
        peak = max(peak, e)
        if peak > 0:
            worst = max(worst, (peak - e) / peak * 100)
    return worst
