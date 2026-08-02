"""Regime labelling for the stability check (§5.5). A strategy must hold up in
more than one regime — an edge that exists only in, say, a strong uptrend is a
regime artefact, not an edge. Simple, transparent buckets from higher-timeframe
return: uptrend / downtrend / range.
"""
from __future__ import annotations

from datetime import datetime

from botmaximus.backtest.data import Bar


def build_regime_map(bars: list[Bar], lookback: int = 60,
                     flat_threshold: float = 0.002) -> dict[datetime, str]:
    """Label each bar by the sign of its trailing `lookback`-bar return."""
    labels: dict[datetime, str] = {}
    for i, b in enumerate(bars):
        if i < lookback:
            labels[b.close_time] = "range"
            continue
        ret = (b.close - bars[i - lookback].close) / bars[i - lookback].close
        if ret > flat_threshold:
            labels[b.close_time] = "uptrend"
        elif ret < -flat_threshold:
            labels[b.close_time] = "downtrend"
        else:
            labels[b.close_time] = "range"
    return labels


def regime_lookup(regime_map: dict[datetime, str]):
    def _of(t: datetime) -> str:
        return regime_map.get(t, "range")
    return _of


# ---------------------------------------------------------------------------
# REGIME_BUCKETS (Strategy DSL §2) — direction × volatility, 6 labels.
#
# These are what a strategy scopes itself to via `regime_scope`. The 3-label
# direction axis above is deliberately left alone: validation's ≥2-regime
# stability check counts positive regimes off `build_regime_map`, and
# repointing it at 6 sparser buckets would weaken that gate without anyone
# deciding to. Scope uses 6; the gate keeps counting 3.
# ---------------------------------------------------------------------------

DIRECTIONS = ("uptrend", "downtrend", "range")
VOL_LEVELS = ("low_vol", "high_vol")
REGIME_BUCKETS: tuple[str, ...] = tuple(
    f"{d}_{v}" for d in DIRECTIONS for v in VOL_LEVELS
)


def build_vol_axis(bars: list[Bar], lookback: int = 60,
                   ref_lookback: int = 1440) -> list[str]:
    """Label each bar low_vol / high_vol by comparing short-horizon realised
    volatility against its own trailing average.

    Causality is the whole difficulty here. A full-sample volatility percentile
    — the obvious implementation — is lookahead: it decides whether *today* is
    volatile using data from months ahead. The reference is therefore a trailing
    window, and before that window fills it is an **expanding** mean over the
    bars seen so far, so there is no stretch of fabricated labels at the start.
    """
    from botmaximus.features.kernels import realized_vol

    vol = realized_vol([b.close for b in bars], lookback)
    out: list[str] = []
    run_sum = 0.0
    seen: list[float] = []
    for i in range(len(bars)):
        v = vol[i]
        if v is None:
            out.append("low_vol")
            continue
        seen.append(v)
        run_sum += v
        if len(seen) > ref_lookback:
            run_sum -= seen[-ref_lookback - 1]
            ref = run_sum / ref_lookback
        else:
            ref = run_sum / len(seen)          # expanding mean during warmup
        out.append("high_vol" if v > ref else "low_vol")
    return out


def build_regime_buckets(bars: list[Bar], lookback: int = 60,
                         flat_threshold: float = 0.002,
                         vol_lookback: int = 60,
                         vol_ref_lookback: int = 1440) -> dict[datetime, str]:
    """The 6 composite REGIME_BUCKETS labels, keyed by bar close time."""
    direction = build_regime_map(bars, lookback, flat_threshold)
    vol = build_vol_axis(bars, vol_lookback, vol_ref_lookback)
    return {b.close_time: f"{direction[b.close_time]}_{vol[i]}"
            for i, b in enumerate(bars)}


def bucket_series(bars: list[Bar], **kw) -> list[str]:
    """Same labels as `build_regime_buckets`, positionally indexed for the
    feature layer (which works in 1m bar indices, not timestamps)."""
    labels = build_regime_buckets(bars, **kw)
    return [labels[b.close_time] for b in bars]
