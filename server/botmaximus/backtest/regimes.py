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
