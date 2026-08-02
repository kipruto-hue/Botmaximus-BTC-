"""FEATURE_REGISTRY — the whitelist (§2, §5.2).

This is the anti-hallucination gate. A generated strategy may reference nothing
outside this table: an unknown feature name, an unknown argument, or an argument
outside its declared bounds is a hard reject, not a coerced default. When the
Pass-C2 generator invents `ema_supertrend_v2(3)`, it dies here.

The registry is also what ties a feature to a *feed*, which the validator uses
to enforce §5.4 (features ↔ `required_feeds`) and which the coverage ledger then
governs. That chain is why a microstructure strategy cannot be silently
backtested over a window where the order book was never collected.

`backtestable_days` records the venue's history limit for the feed, so the
validator can warn when a strategy is only testable on forward-collected data:
- ohlcv/funding: deep REST history (years)
- oi: Binance caps openInterestHist at 30 days
- orderbook/liquidations: no history endpoint at all — forward coverage only
"""
from __future__ import annotations

from dataclasses import dataclass, field

from botmaximus.features.frames import TIMEFRAME_MINUTES

FEEDS = ("ohlcv", "funding", "oi", "orderbook", "liquidations")

# Venue history limits per feed. None = effectively unlimited via REST.
FEED_HISTORY_DAYS: dict[str, int | None] = {
    "ohlcv": None,
    "funding": None,
    "oi": 30,
    "orderbook": 0,
    "liquidations": 0,
}


@dataclass(frozen=True)
class ArgSpec:
    name: str
    lo: float
    hi: float
    integer: bool = True

    def check(self, value) -> str | None:
        if self.integer and not isinstance(value, int):
            return f"arg_{self.name}_not_integer:{value!r}"
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return f"arg_{self.name}_not_numeric:{value!r}"
        if not (self.lo <= value <= self.hi):
            # ASCII only: reason strings are logged and returned over the API,
            # and a cp1252 console cannot encode the set-membership glyphs
            return f"arg_{self.name}_out_of_bounds:{value}_not_in[{self.lo},{self.hi}]"
        return None


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    feed: str
    kind: str                       # "numeric" | "categorical"
    timeframed: bool                # takes a timeframe argument
    args: tuple[ArgSpec, ...] = ()
    doc: str = ""
    #: computed on its native series, then projected to 1m — see compute.py
    compute: str = ""
    allowed_timeframes: tuple[str, ...] = field(
        default_factory=lambda: tuple(TIMEFRAME_MINUTES)
    )

    @property
    def backtestable_days(self) -> int | None:
        return FEED_HISTORY_DAYS[self.feed]


def _n(lo: int, hi: int) -> ArgSpec:
    return ArgSpec("n", lo, hi, integer=True)


FEATURE_REGISTRY: dict[str, FeatureSpec] = {
    # ---- price / OHLCV -------------------------------------------------
    "close": FeatureSpec("close", "ohlcv", "numeric", True, (),
                         "Close of the last fully-closed bar on the timeframe.",
                         "close"),
    "high": FeatureSpec("high", "ohlcv", "numeric", True, (),
                        "High of the last fully-closed bar.", "high"),
    "low": FeatureSpec("low", "ohlcv", "numeric", True, (),
                       "Low of the last fully-closed bar.", "low"),
    "ema": FeatureSpec("ema", "ohlcv", "numeric", True, (_n(2, 400),),
                       "Exponential moving average of closes.", "ema"),
    "sma": FeatureSpec("sma", "ohlcv", "numeric", True, (_n(2, 400),),
                       "Simple moving average of closes.", "sma"),
    "rsi": FeatureSpec("rsi", "ohlcv", "numeric", True, (_n(2, 100),),
                       "Wilder RSI, 0–100.", "rsi"),
    "atr": FeatureSpec("atr", "ohlcv", "numeric", True, (_n(2, 200),),
                       "Average true range, in price units.", "atr"),
    "adx": FeatureSpec("adx", "ohlcv", "numeric", True, (_n(2, 100),),
                       "Trend strength 0–100, direction-agnostic.", "adx"),
    "bb_position": FeatureSpec("bb_position", "ohlcv", "numeric", True, (_n(5, 200),),
                               "Position in the Bollinger band: 0 lower, 1 upper.",
                               "bb_position"),
    "vwap_dist": FeatureSpec("vwap_dist", "ohlcv", "numeric", True, (),
                             "Percent distance from the running UTC-session VWAP.",
                             "vwap_dist"),
    "realized_vol": FeatureSpec("realized_vol", "ohlcv", "numeric", True, (_n(5, 400),),
                                "Annualised realised volatility, percent.",
                                "realized_vol"),
    "atr_pct": FeatureSpec("atr_pct", "ohlcv", "numeric", True, (_n(2, 200),),
                           "ATR as a percent of price — scale-free volatility.",
                           "atr_pct"),
    "ret_pct": FeatureSpec("ret_pct", "ohlcv", "numeric", True, (_n(1, 400),),
                           "Percent return over the last n bars.", "ret_pct"),

    # ---- funding -------------------------------------------------------
    "funding_rate": FeatureSpec("funding_rate", "funding", "numeric", False, (),
                                "Most recent settled 8h funding rate.",
                                "funding_rate"),
    "funding_zscore": FeatureSpec("funding_zscore", "funding", "numeric", False,
                                  (_n(3, 200),),
                                  "Z-score of funding over the last n settlements.",
                                  "funding_zscore"),

    # ---- open interest (30-day venue history cap) ----------------------
    "oi_change": FeatureSpec("oi_change", "oi", "numeric", False, (_n(1, 288),),
                             "Percent change in open interest over n 5m periods.",
                             "oi_change"),

    # ---- order book (forward coverage only) ----------------------------
    "book_imbalance": FeatureSpec("book_imbalance", "orderbook", "numeric", False, (),
                                  "Top-20 depth imbalance, −1 all asks … +1 all bids.",
                                  "book_imbalance"),

    # ---- liquidations (forward coverage only) --------------------------
    "liq_cluster_above": FeatureSpec("liq_cluster_above", "liquidations", "numeric",
                                     False, (_n(1, 1440),),
                                     "USD liquidated above price in the last n minutes.",
                                     "liq_cluster_above"),
    "liq_cluster_below": FeatureSpec("liq_cluster_below", "liquidations", "numeric",
                                     False, (_n(1, 1440),),
                                     "USD liquidated below price in the last n minutes.",
                                     "liq_cluster_below"),

    # ---- regime --------------------------------------------------------
    # Categorical: it has no ordering, so none of the §4.1 operators (gt, lt,
    # cross_above, …) are meaningful on it. The validator rejects it inside a
    # Predicate; `regime_scope` is where a strategy declares its regimes.
    "regime_label": FeatureSpec("regime_label", "ohlcv", "categorical", False, (),
                                "Composite regime bucket; use via regime_scope.",
                                "regime_label"),
}


def resolve(name: str) -> FeatureSpec | None:
    return FEATURE_REGISTRY.get(name)


def feeds_for(names) -> set[str]:
    """Feeds implied by a set of feature names (§5.4 consistency check)."""
    return {FEATURE_REGISTRY[n].feed for n in names if n in FEATURE_REGISTRY}
