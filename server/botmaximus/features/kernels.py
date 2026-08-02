"""Indicator kernels. Every one is causal: output[i] is a function of inputs
[0..i] only. There is no centering, no forward fill, and no reverse pass —
those are the ways a feature library silently leaks the future.

Each returns a list the same length as its input, with None during the warmup
where the value is not yet defined. None propagates to "no signal" in a
predicate rather than being coerced to a number, because a fabricated warmup
value is indistinguishable from a real one to the backtester.

Pure stdlib and single-pass: this runs over ~10^6 bars.
"""
from __future__ import annotations

import math
from collections import deque

Series = list[float | None]


def sma(values: list[float], n: int) -> Series:
    out: Series = [None] * len(values)
    total = 0.0
    for i, v in enumerate(values):
        total += v
        if i >= n:
            total -= values[i - n]
        if i >= n - 1:
            out[i] = total / n
    return out


def ema(values: list[float], n: int) -> Series:
    """Seeded with the first n-bar SMA so the series does not depend on how much
    history happens to precede the window."""
    out: Series = [None] * len(values)
    if len(values) < n:
        return out
    k = 2.0 / (n + 1)
    prev = sum(values[:n]) / n
    out[n - 1] = prev
    for i in range(n, len(values)):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def rsi(closes: list[float], n: int) -> Series:
    """Wilder's smoothing. 100 when there are no losses in the window — that is
    the defined limit, not a sentinel."""
    out: Series = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_g, avg_l = gains / n, losses / n
    out[n] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (n - 1) + max(d, 0.0)) / n
        avg_l = (avg_l * (n - 1) + max(-d, 0.0)) / n
        out[i] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)
    return out


def true_range(high: list[float], low: list[float], close: list[float]) -> Series:
    out: Series = [None] * len(high)
    for i in range(len(high)):
        if i == 0:
            out[i] = high[i] - low[i]
        else:
            pc = close[i - 1]
            out[i] = max(high[i] - low[i], abs(high[i] - pc), abs(low[i] - pc))
    return out


def atr(high: list[float], low: list[float], close: list[float], n: int) -> Series:
    tr = [t for t in true_range(high, low, close)]
    out: Series = [None] * len(high)
    if len(high) < n:
        return out
    prev = sum(tr[:n]) / n           # type: ignore[arg-type]
    out[n - 1] = prev
    for i in range(n, len(high)):
        prev = (prev * (n - 1) + tr[i]) / n      # type: ignore[operator]
        out[i] = prev
    return out


def adx(high: list[float], low: list[float], close: list[float], n: int) -> Series:
    """Wilder's ADX — trend *strength*, direction-agnostic. Needs 2n bars before
    the first value (n to smooth DI, n more to smooth DX into ADX)."""
    size = len(high)
    out: Series = [None] * size
    if size < 2 * n + 1:
        return out

    tr = true_range(high, low, close)
    plus_dm, minus_dm = [0.0] * size, [0.0] * size
    for i in range(1, size):
        up = high[i] - high[i - 1]
        dn = low[i - 1] - low[i]
        plus_dm[i] = up if (up > dn and up > 0) else 0.0
        minus_dm[i] = dn if (dn > up and dn > 0) else 0.0

    str_ = sum(tr[1:n + 1])          # type: ignore[arg-type]
    sp = sum(plus_dm[1:n + 1])
    sm = sum(minus_dm[1:n + 1])
    dx: list[float | None] = [None] * size

    def _dx(s_tr: float, s_p: float, s_m: float) -> float:
        if s_tr == 0:
            return 0.0
        pdi, mdi = 100 * s_p / s_tr, 100 * s_m / s_tr
        return 0.0 if pdi + mdi == 0 else 100 * abs(pdi - mdi) / (pdi + mdi)

    dx[n] = _dx(str_, sp, sm)
    for i in range(n + 1, size):
        str_ = str_ - str_ / n + tr[i]           # type: ignore[operator]
        sp = sp - sp / n + plus_dm[i]
        sm = sm - sm / n + minus_dm[i]
        dx[i] = _dx(str_, sp, sm)

    first = 2 * n
    prev = sum(dx[n:first]) / n                  # type: ignore[arg-type]
    out[first - 1] = prev
    for i in range(first, size):
        prev = (prev * (n - 1) + dx[i]) / n      # type: ignore[operator]
        out[i] = prev
    return out


def rolling_std(values: list[float], n: int) -> Series:
    """Population stddev over a trailing window, via rolling sums."""
    out: Series = [None] * len(values)
    s = s2 = 0.0
    for i, v in enumerate(values):
        s += v
        s2 += v * v
        if i >= n:
            old = values[i - n]
            s -= old
            s2 -= old * old
        if i >= n - 1:
            var = max(0.0, s2 / n - (s / n) ** 2)
            out[i] = math.sqrt(var)
    return out


def bb_position(closes: list[float], n: int, mult: float = 2.0) -> Series:
    """Where price sits inside its Bollinger band: 0 = lower, 1 = upper, and
    outside the band when it breaks out. Flat band → 0.5 (no information)."""
    mid = sma(closes, n)
    sd = rolling_std(closes, n)
    out: Series = [None] * len(closes)
    for i in range(len(closes)):
        m, s = mid[i], sd[i]
        if m is None or s is None:
            continue
        width = 2 * mult * s
        out[i] = 0.5 if width == 0 else (closes[i] - (m - mult * s)) / width
    return out


def realized_vol(closes: list[float], n: int, periods_per_year: float = 525_600.0) -> Series:
    """Annualised stddev of log returns over a trailing window, in percent.
    `periods_per_year` defaults to the 1m bar count and is scaled per frame."""
    rets: list[float] = [0.0]
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        rets.append(math.log(closes[i] / prev) if prev > 0 and closes[i] > 0 else 0.0)
    sd = rolling_std(rets, n)
    scale = math.sqrt(periods_per_year)
    return [None if s is None else s * scale * 100 for s in sd]


def zscore(values: list[float | None], n: int) -> Series:
    """Trailing z-score. Undefined (None) while the window is incomplete or the
    trailing series is constant — a zero-variance z-score is a divide-by-zero,
    not a zero."""
    out: Series = [None] * len(values)
    window: deque[float] = deque(maxlen=n)
    for i, v in enumerate(values):
        if v is None:
            window.clear()
            continue
        window.append(v)
        if len(window) < n:
            continue
        m = sum(window) / n
        var = sum((x - m) ** 2 for x in window) / n
        if var > 0:
            out[i] = (v - m) / math.sqrt(var)
    return out


def pct_change(values: list[float | None], n: int) -> Series:
    """Percent change over n bars. None where either endpoint is missing."""
    out: Series = [None] * len(values)
    for i in range(n, len(values)):
        a, b = values[i - n], values[i]
        if a is None or b is None or a == 0:
            continue
        out[i] = (b - a) / abs(a) * 100
    return out


def rolling_sum(values: list[float], n: int) -> Series:
    out: Series = [None] * len(values)
    total = 0.0
    for i, v in enumerate(values):
        total += v
        if i >= n:
            total -= values[i - n]
        if i >= n - 1:
            out[i] = total
    return out


def session_vwap_dist(days: list[int], typical: list[float],
                      volume: list[float]) -> Series:
    """Percent distance of the typical price from the running session VWAP.

    `days` is a UTC day ordinal per bar; the accumulator resets when it changes,
    so VWAP at bar i covers [session_start .. i] and never the rest of the day.
    None until the session has traded volume.
    """
    out: Series = [None] * len(typical)
    cum_pv = cum_v = 0.0
    cur_day: int | None = None
    for i, d in enumerate(days):
        if d != cur_day:
            cur_day, cum_pv, cum_v = d, 0.0, 0.0
        cum_pv += typical[i] * volume[i]
        cum_v += volume[i]
        if cum_v > 0:
            vwap = cum_pv / cum_v
            out[i] = (typical[i] - vwap) / vwap * 100
    return out
