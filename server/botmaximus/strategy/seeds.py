"""The §8.1 seed playbook — 5 hand-written macro/structural strategies.

They are here for two reasons: to give the population a baseline, and to give
the Pass-C2 generator worked examples of the DSL in the themes §8.1 names.

**They are not blessed edges.** Each passes §5.5 validation like anything else,
on coverage-complete windows, gate-off. The expectation is that most fail — a
1m/5-minute-hold strategy pays taker fees twice plus funding plus slippage, and
Pass B's live proof already showed friction turning a −$154 gross into a −$635
net. A seed that sails through on first contact is the result to distrust.

The class is macro/structural on purpose (OHLCV + funding only). Order book and
liquidations have no venue history endpoint at all, so a microstructure seed
could not be validated today over anything but the handful of days we have
forward-collected — which is not enough for 30 trades across 2 regimes.
"""
from __future__ import annotations

from botmaximus.strategy.schema import StrategyDefinition

ALL_REGIMES = ["uptrend_low_vol", "uptrend_high_vol", "downtrend_low_vol",
               "downtrend_high_vol", "range_low_vol", "range_high_vol"]


SEED_PAYLOADS: list[dict] = [
    # ---------------------------------------------------------------- 1 ----
    {
        "id": "seed_htf_trend_continuation",
        "version": 1,
        "origin": "seed",
        "direction": "long",
        "rationale": (
            "Higher-timeframe trend continuation. When the 1h trend is up and "
            "confirmed by ADX, short-term pullbacks into the 15m mean are more "
            "often continuation than reversal: trend-followers reload at value "
            "while the impatient exit. The edge, if any, is that the pullback "
            "offers a tighter stop than the trend's own volatility, improving "
            "reward-to-risk rather than win rate."
        ),
        "timeframes": ["1m", "15m", "1h"],
        "required_feeds": ["ohlcv"],
        "regime_scope": ["uptrend_low_vol", "uptrend_high_vol"],
        "entry": {"and": [
            {"left": {"feature": "close", "timeframe": "1h"},
             "op": "gt",
             "right": {"feature": "ema", "timeframe": "1h", "args": {"n": 50}}},
            {"left": {"feature": "adx", "timeframe": "1h", "args": {"n": 14}},
             "op": "gt", "right": {"param": "adx_floor"}},
            {"left": {"feature": "rsi", "timeframe": "15m", "args": {"n": 14}},
             "op": "lt", "right": {"param": "pullback_rsi"}},
        ]},
        "exit": {
            "stop": {"kind": "atr", "timeframe": "15m", "n": 14, "mult": 1.5},
            "target": {"kind": "r_multiple", "r": 2.0},
            "time_exit": 30,
            "regime_invalidation": True,
        },
        "params": {
            "adx_floor": {"value": 22.0, "lo": 15.0, "hi": 40.0},
            "pullback_rsi": {"value": 45.0, "lo": 30.0, "hi": 55.0},
        },
    },
    # ---------------------------------------------------------------- 2 ----
    {
        "id": "seed_vol_extreme_mean_reversion",
        "version": 1,
        "origin": "seed",
        "direction": "long",
        "rationale": (
            "Mean reversion at volatility extremes. A violent push to the lower "
            "Bollinger band in a ranging market is usually forced flow — "
            "liquidations and stop cascades — rather than information. Forced "
            "flow exhausts, so price tends to revert toward the band mean once "
            "the seller is done. Scoped to range regimes because the identical "
            "signal in a downtrend is simply the trend continuing."
        ),
        "timeframes": ["1m", "5m"],
        "required_feeds": ["ohlcv"],
        "regime_scope": ["range_low_vol", "range_high_vol"],
        "entry": {"and": [
            {"left": {"feature": "bb_position", "timeframe": "5m", "args": {"n": 20}},
             "op": "lt", "right": {"param": "band_floor"}},
            {"left": {"feature": "rsi", "timeframe": "5m", "args": {"n": 14}},
             "op": "lt", "right": {"param": "rsi_floor"}},
        ]},
        "exit": {
            "stop": {"kind": "atr", "timeframe": "5m", "n": 14, "mult": 1.2},
            "target": {"kind": "r_multiple", "r": 1.5},
            "time_exit": 20,
            "regime_invalidation": True,
        },
        "params": {
            "band_floor": {"value": 0.05, "lo": 0.0, "hi": 0.25},
            "rsi_floor": {"value": 25.0, "lo": 10.0, "hi": 40.0},
        },
    },
    # ---------------------------------------------------------------- 3 ----
    {
        "id": "seed_funding_extreme_contrarian",
        "version": 1,
        "origin": "seed",
        "direction": "short",
        "rationale": (
            "Funding as a positioning gauge. Persistently high funding means "
            "longs are paying shorts to hold — crowded, leveraged and paying "
            "rent for the privilege. Crowded leveraged positioning is fragile: "
            "a small adverse move forces the marginal long out. This fades the "
            "crowd when funding is extreme by its own recent history rather "
            "than by an absolute threshold, since the regime-average rate drifts."
        ),
        "timeframes": ["1m", "1h"],
        "required_feeds": ["ohlcv", "funding"],
        "regime_scope": ["uptrend_high_vol", "range_high_vol"],
        "entry": {"and": [
            {"left": {"feature": "funding_zscore", "args": {"n": 30}},
             "op": "gt", "right": {"param": "funding_z"}},
            {"left": {"feature": "rsi", "timeframe": "1h", "args": {"n": 14}},
             "op": "gt", "right": {"param": "overbought"}},
        ]},
        "exit": {
            "stop": {"kind": "atr", "timeframe": "1h", "n": 14, "mult": 1.5},
            "target": {"kind": "r_multiple", "r": 2.0},
            "time_exit": 60,
            "regime_invalidation": True,
        },
        "params": {
            "funding_z": {"value": 1.8, "lo": 1.0, "hi": 3.5},
            "overbought": {"value": 65.0, "lo": 55.0, "hi": 85.0},
        },
    },
    # ---------------------------------------------------------------- 4 ----
    {
        "id": "seed_oi_divergence_exhaustion",
        "version": 1,
        "origin": "seed",
        "direction": "short",
        "rationale": (
            "Open-interest divergence. Rising open interest alongside a stalling "
            "price means new leveraged positions are being added into a move "
            "that is no longer paying them. That is late-cycle positioning: the "
            "marginal buyer is levered and underwater, so the next move down is "
            "amplified by forced exits rather than absorbed. Only backtestable "
            "over the venue's 30-day OI history, which the validator flags."
        ),
        "timeframes": ["1m", "15m"],
        "required_feeds": ["ohlcv", "oi"],
        "regime_scope": ["uptrend_high_vol", "range_high_vol"],
        "entry": {"and": [
            {"left": {"feature": "oi_change", "args": {"n": 24}},
             "op": "gt", "right": {"param": "oi_surge"}},
            {"left": {"feature": "ret_pct", "timeframe": "15m", "args": {"n": 8}},
             "op": "between", "right": {"param": "stall_lo"}, "right2": {"param": "stall_hi"}},
        ]},
        "exit": {
            "stop": {"kind": "atr", "timeframe": "15m", "n": 14, "mult": 1.5},
            "target": {"kind": "r_multiple", "r": 1.8},
            "time_exit": 45,
            "regime_invalidation": True,
        },
        "params": {
            "oi_surge": {"value": 3.0, "lo": 1.0, "hi": 10.0},
            "stall_lo": {"value": -0.3, "lo": -2.0, "hi": 0.0},
            "stall_hi": {"value": 0.3, "lo": 0.0, "hi": 2.0},
        },
    },
    # ---------------------------------------------------------------- 5 ----
    {
        "id": "seed_vol_regime_breakout",
        "version": 1,
        "origin": "seed",
        "direction": "long",
        "rationale": (
            "Volatility-regime breakout. Volatility clusters: a quiet stretch "
            "compresses the range, and the resolution out of compression tends "
            "to run because the positioning built during the quiet period is "
            "offside all at once. Entry requires price to actually clear the "
            "compressed range rather than anticipating it, and the trailing stop "
            "exists because the payoff distribution is fat-tailed — the rare "
            "long run is what pays for the many small false breaks."
        ),
        "timeframes": ["1m", "5m", "1h"],
        "required_feeds": ["ohlcv"],
        "regime_scope": ["uptrend_low_vol", "range_low_vol"],
        "entry": {"and": [
            {"left": {"feature": "atr_pct", "timeframe": "1h", "args": {"n": 14}},
             "op": "lt", "right": {"param": "compression"}},
            {"left": {"feature": "close", "timeframe": "5m"},
             "op": "cross_above",
             "right": {"feature": "high", "timeframe": "1h"}},
        ]},
        "exit": {
            "stop": {"kind": "atr", "timeframe": "5m", "n": 14, "mult": 1.5},
            "target": {"kind": "r_multiple", "r": 3.0},
            "trailing": {"kind": "atr", "timeframe": "5m", "n": 14, "mult": 2.0},
            "time_exit": 60,
            "regime_invalidation": True,
        },
        "params": {
            "compression": {"value": 0.35, "lo": 0.05, "hi": 1.5},
        },
    },
]


def seed_definitions() -> list[StrategyDefinition]:
    """Parsed seeds. Raises if a seed no longer conforms — a seed that stops
    parsing is a broken grammar change, and should fail loudly at import time
    rather than quietly drop out of the population."""
    return [StrategyDefinition.parse(p) for p in SEED_PAYLOADS]
