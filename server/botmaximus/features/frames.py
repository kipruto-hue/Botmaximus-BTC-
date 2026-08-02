"""Higher-timeframe frames built from 1m bars, point-in-time by construction.

The DSL lets a strategy reference `ema("1h", 50)` while the engine still walks
1m bars. That is only safe if, at 1m bar *i*, the strategy sees the last 1h bar
that has **fully closed** at or before `bars[i].close_time` — never the one
still forming, which contains the future.

`Frame.avail[i]` is that guarantee, precomputed: the number of higher-timeframe
bars closed as of 1m index *i*. A feature value at *i* is read from position
`avail[i] - 1`; `avail[i] == 0` means not enough history yet and the feature is
None, which predicates treat as "no signal".

Availability is derived from each bucket's **nominal** close (bucket start + the
timeframe), not from the last 1m bar actually present. A missing minute inside a
bucket must not make that bucket look closed early.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from botmaximus.backtest.data import Bar

# The §3 `timeframes` set. Minutes, so every frame is an integer multiple of 1m.
TIMEFRAME_MINUTES: dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "24h": 1440,
}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def bucket_start(t: datetime, minutes: int) -> datetime:
    """Floor `t` to the timeframe grid. The grid is anchored at the Unix epoch,
    which is UTC midnight — so 24h buckets are UTC days and every smaller
    timeframe nests inside them cleanly."""
    elapsed = int((t - _EPOCH).total_seconds() // 60)
    return _EPOCH + timedelta(minutes=(elapsed // minutes) * minutes)


@dataclass(frozen=True)
class Frame:
    """One timeframe's aggregated bars plus the 1m→frame availability map."""
    timeframe: str
    bars: list[Bar]
    avail: list[int]        # len == len(1m bars); count of frame bars closed by then

    def index_at(self, i: int) -> int | None:
        """Position of the newest fully-closed frame bar as of 1m index i."""
        n = self.avail[i]
        return n - 1 if n > 0 else None


def build_frame(bars: list[Bar], timeframe: str) -> Frame:
    if timeframe not in TIMEFRAME_MINUTES:
        raise ValueError(f"unknown timeframe {timeframe!r}")
    minutes = TIMEFRAME_MINUTES[timeframe]

    if minutes == 1:
        return Frame(timeframe, list(bars), list(range(1, len(bars) + 1)))

    step = timedelta(minutes=minutes)
    agg: list[Bar] = []
    closes: list[datetime] = []          # nominal close of each aggregated bar
    cur_start: datetime | None = None
    o = h = l = c = 0.0
    v = 0.0

    for b in bars:
        bs = bucket_start(b.open_time, minutes)
        if cur_start is None or bs != cur_start:
            if cur_start is not None:
                agg.append(Bar(open_time=cur_start, close_time=cur_start + step
                               - timedelta(milliseconds=1),
                               open=o, high=h, low=l, close=c, volume=v))
                closes.append(cur_start + step - timedelta(milliseconds=1))
            cur_start = bs
            o, h, l, c, v = b.open, b.high, b.low, b.close, b.volume
        else:
            h = max(h, b.high)
            l = min(l, b.low)
            c = b.close
            v += b.volume
    if cur_start is not None:
        agg.append(Bar(open_time=cur_start,
                       close_time=cur_start + step - timedelta(milliseconds=1),
                       open=o, high=h, low=l, close=c, volume=v))
        closes.append(cur_start + step - timedelta(milliseconds=1))

    # avail[i] = how many frame bars have nominally closed by bars[i].close_time.
    # Both sequences are ascending, so one forward walk suffices.
    avail: list[int] = []
    k = 0
    for b in bars:
        while k < len(closes) and closes[k] <= b.close_time:
            k += 1
        avail.append(k)

    return Frame(timeframe, agg, avail)


def build_frames(bars: list[Bar], timeframes: set[str] | list[str]) -> dict[str, Frame]:
    return {tf: build_frame(bars, tf) for tf in sorted(set(timeframes))}
