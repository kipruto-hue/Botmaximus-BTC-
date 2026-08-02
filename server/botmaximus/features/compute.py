"""Feature computation: registry reference → a 1m-indexed causal series.

Two rules hold everything together:

1. **Higher timeframes are read one bar late.** A feature on `1h` is computed on
   the 1h frame and projected back to 1m through `Frame.avail`, which only ever
   points at a *fully closed* 1h bar. The forming bar is unreachable.
2. **Non-OHLCV feeds join backwards.** Funding, OI, book and liquidation records
   arrive on their own clocks; each 1m bar takes the last value whose event_time
   is ≤ that bar's close. A forward or nearest join would hand the strategy a
   record published after the decision — the subtlest lookahead in the layer,
   and the reason this is one shared helper rather than per-feature code.

Everything is computed once per window and indexed per bar. Recomputing inside
the replay loop would be ~10^6 × slower and, worse, would make lookahead a
per-call property instead of a structural one.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import timedelta

from botmaximus.backtest.data import Bar, MarketWindow
from botmaximus.features import kernels
from botmaximus.features.frames import TIMEFRAME_MINUTES, Frame, build_frames
from botmaximus.features.registry import FEATURE_REGISTRY, FeatureSpec

Series = list[float | None]

_MINUTES_PER_YEAR = 525_600.0


@dataclass(frozen=True)
class FeatureRef:
    """One resolved registry call: `ema("1h", n=50)`."""
    name: str
    timeframe: str | None = None
    args: tuple[tuple[str, float], ...] = ()

    @property
    def key(self) -> str:
        a = ",".join(f"{k}={v}" for k, v in self.args)
        return f"{self.name}({self.timeframe or '-'}{',' + a if a else ''})"

    def arg(self, name: str, default=None):
        for k, v in self.args:
            if k == name:
                return v
        return default

    @property
    def spec(self) -> FeatureSpec:
        return FEATURE_REGISTRY[self.name]


def as_of(bars: list[Bar], times: list, values: list[float]) -> Series:
    """Backward as-of join: out[i] = last value with time ≤ bars[i].close_time.

    None before the first record — an absent value is absent, not zero.
    """
    out: Series = [None] * len(bars)
    if not times:
        return out
    j = 0
    n = len(times)
    for i, b in enumerate(bars):
        while j < n and times[j] <= b.close_time:
            j += 1
        if j > 0:
            out[i] = values[j - 1]
    return out


def project(frame: Frame, series: Series) -> Series:
    """Frame-indexed series → 1m-indexed, via the fully-closed-bar map."""
    out: Series = [None] * len(frame.avail)
    for i, n in enumerate(frame.avail):
        if n > 0:
            out[i] = series[n - 1]
    return out


class FeatureContext:
    """Computes and caches the series a strategy references over one window."""

    def __init__(self, market: MarketWindow, timeframes=("1m",)) -> None:
        self.market = market
        self.bars = market.bars
        self.frames: dict[str, Frame] = build_frames(self.bars, set(timeframes) | {"1m"})
        self._cache: dict[str, Series] = {}
        self._regime: list[str] | None = None

    # ---- public ----------------------------------------------------------
    def series(self, ref: FeatureRef) -> Series:
        if ref.key not in self._cache:
            self._cache[ref.key] = self._compute(ref)
        return self._cache[ref.key]

    def regime_labels(self) -> list[str]:
        if self._regime is None:
            from botmaximus.backtest.regimes import bucket_series
            self._regime = bucket_series(self.bars)
        return self._regime

    def frame(self, timeframe: str) -> Frame:
        if timeframe not in self.frames:
            self.frames[timeframe] = build_frames(self.bars, [timeframe])[timeframe]
        return self.frames[timeframe]

    # ---- dispatch --------------------------------------------------------
    def _compute(self, ref: FeatureRef) -> Series:
        spec = ref.spec
        if spec.feed == "ohlcv" and spec.name != "regime_label":
            return self._ohlcv_feature(ref)
        return {
            "funding_rate": self._funding_rate,
            "funding_zscore": self._funding_zscore,
            "oi_change": self._oi_change,
            "book_imbalance": self._book_imbalance,
            "liq_cluster_above": lambda r: self._liq_cluster(r, above=True),
            "liq_cluster_below": lambda r: self._liq_cluster(r, above=False),
            "regime_label": lambda r: [None] * len(self.bars),
        }[spec.name](ref)

    # ---- OHLCV-derived ---------------------------------------------------
    def _ohlcv_feature(self, ref: FeatureRef) -> Series:
        tf = ref.timeframe or "1m"
        frame = self.frame(tf)
        fb = frame.bars
        if not fb:
            return [None] * len(self.bars)

        closes = [b.close for b in fb]
        highs = [b.high for b in fb]
        lows = [b.low for b in fb]
        n = int(ref.arg("n", 0) or 0)
        name = ref.name

        if name == "close":
            out = list(closes)
        elif name == "high":
            out = list(highs)
        elif name == "low":
            out = list(lows)
        elif name == "ema":
            out = kernels.ema(closes, n)
        elif name == "sma":
            out = kernels.sma(closes, n)
        elif name == "rsi":
            out = kernels.rsi(closes, n)
        elif name == "atr":
            out = kernels.atr(highs, lows, closes, n)
        elif name == "adx":
            out = kernels.adx(highs, lows, closes, n)
        elif name == "bb_position":
            out = kernels.bb_position(closes, n)
        elif name == "ret_pct":
            out = kernels.pct_change(list(closes), n)
        elif name == "atr_pct":
            a = kernels.atr(highs, lows, closes, n)
            out = [None if v is None or c <= 0 else v / c * 100
                   for v, c in zip(a, closes)]
        elif name == "realized_vol":
            per_year = _MINUTES_PER_YEAR / TIMEFRAME_MINUTES[tf]
            out = kernels.realized_vol(closes, n, per_year)
        elif name == "vwap_dist":
            days = [b.open_time.toordinal() for b in fb]
            typical = [(b.high + b.low + b.close) / 3 for b in fb]
            out = kernels.session_vwap_dist(days, typical, [b.volume for b in fb])
        else:                                            # unreachable via the validator
            raise ValueError(f"no computation for ohlcv feature {name!r}")

        return project(frame, out)

    # ---- funding ---------------------------------------------------------
    def _funding_rate(self, ref: FeatureRef) -> Series:
        f = self.market.funding
        return as_of(self.bars, [p.time for p in f], [p.rate for p in f])

    def _funding_zscore(self, ref: FeatureRef) -> Series:
        """Z-scored over the last n *settlements*, not the last n minutes — the
        series only moves every 8h, so a bar-count window would be meaningless."""
        f = self.market.funding
        n = int(ref.arg("n", 30))
        z = kernels.zscore([p.rate for p in f], n)
        times = [p.time for p, v in zip(f, z) if v is not None]
        vals = [v for v in z if v is not None]
        return as_of(self.bars, times, vals)

    # ---- open interest ---------------------------------------------------
    def _oi_change(self, ref: FeatureRef) -> Series:
        oi = self.market.oi
        n = int(ref.arg("n", 12))
        ch = kernels.pct_change([p.open_interest for p in oi], n)
        times = [p.time for p, v in zip(oi, ch) if v is not None]
        vals = [v for v in ch if v is not None]
        return as_of(self.bars, times, vals)

    # ---- order book ------------------------------------------------------
    def _book_imbalance(self, ref: FeatureRef) -> Series:
        b = self.market.book
        return as_of(self.bars, [p.time for p in b], [p.imbalance for p in b])

    # ---- liquidations ----------------------------------------------------
    def _liq_cluster(self, ref: FeatureRef, above: bool) -> Series:
        """USD liquidated in the trailing n minutes at prices above (or below)
        the current close. The price comparison is against *this* bar's close,
        so it cannot be precomputed as a plain rolling sum — but liquidations
        are sparse, so the sliding window stays small.

        Zero (not None) when the feed is live but quiet: silence is a real
        reading here. None only when the feed was never collected at all.
        """
        liqs = self.market.liquidations
        if not liqs:
            return [None] * len(self.bars)
        n = int(ref.arg("n", 60))
        span = timedelta(minutes=n)
        times = [p.time for p in liqs]
        out: Series = [None] * len(self.bars)
        lo = 0
        for i, bar in enumerate(self.bars):
            hi = bisect_right(times, bar.close_time)
            while lo < hi and times[lo] < bar.close_time - span:
                lo += 1
            total = 0.0
            for k in range(lo, hi):
                p = liqs[k]
                if (p.price > bar.close) == above:
                    total += p.notional_usd
            out[i] = total
        return out


def build_context(market: MarketWindow, refs) -> FeatureContext:
    """Context pre-warmed with every series a strategy will read."""
    tfs = {r.timeframe for r in refs if r.timeframe} | {"1m"}
    ctx = FeatureContext(market, tfs)
    for r in refs:
        ctx.series(r)
    return ctx
