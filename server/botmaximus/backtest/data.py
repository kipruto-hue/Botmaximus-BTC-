"""Point-in-time data access (§5.4). At bar *t*, a strategy may see only data
whose event_time ≤ the close of bar t — the view makes lookahead structurally
impossible, it isn't a convention the strategy is trusted to honour.

Loading consults the coverage ledger first (§2.4): a window with holes in a
required feed is refused, or flagged if the caller allows it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from botmaximus.backtest.costs import FundingPoint
from botmaximus.pipeline import coverage


@dataclass(frozen=True)
class Bar:
    open_time: datetime
    close_time: datetime        # == event_time; the point-in-time boundary
    open: float
    high: float
    low: float
    close: float
    volume: float


class CoverageError(Exception):
    """Raised when a required feed has gaps in the evaluation window."""


class PointInTimeView:
    """Exposes bars[0 .. i] — the just-closed bar and everything before it.
    A strategy physically cannot reach bar i+1; there is no method for it."""

    def __init__(self, bars: list[Bar], i: int):
        self._bars = bars
        self._i = i

    @property
    def now(self) -> Bar:
        return self._bars[self._i]

    def bars(self, lookback: int) -> list[Bar]:
        return self._bars[max(0, self._i - lookback + 1): self._i + 1]

    def closes(self, lookback: int) -> list[float]:
        return [b.close for b in self.bars(lookback)]

    def sma(self, lookback: int) -> float | None:
        c = self.closes(lookback)
        return sum(c) / len(c) if len(c) == lookback else None


async def assert_coverage(feed: str, start: datetime, end: datetime,
                          allow_gaps: bool = False) -> dict:
    """Refuse (or flag) an evaluation window with incomplete coverage."""
    summ = await coverage.summary(feed, start, end)
    if summ["missing_slots"] > 0 and not allow_gaps:
        raise CoverageError(
            f"{feed}: {summ['missing_slots']}/{summ['expected_slots']} slots missing "
            f"in [{start.isoformat()}, {end.isoformat()}] — refusing backtest (§2.4). "
            f"first gap {summ['first_gap']}"
        )
    return summ


async def load_ohlcv(start: datetime, end: datetime) -> list[Bar]:
    from botmaximus.db.mongo import get_db
    from botmaximus.db.schema import DATASET_COLLECTIONS
    db = get_db()
    cursor = db[DATASET_COLLECTIONS["btc_ohlcv_1m"]].find(
        {"event_time": {"$gte": start, "$lte": end}}
    ).sort("event_time", 1)
    out = []
    async for d in cursor:
        p = d["payload"]
        out.append(Bar(
            open_time=p["open_time"], close_time=d["event_time"],
            open=p["open"], high=p["high"], low=p["low"], close=p["close"],
            volume=p["volume"],
        ))
    return out


async def load_funding(start: datetime, end: datetime) -> list[FundingPoint]:
    from botmaximus.db.mongo import get_db
    from botmaximus.db.schema import DATASET_COLLECTIONS
    db = get_db()
    cursor = db[DATASET_COLLECTIONS["btc_funding_8h"]].find(
        {"event_time": {"$gte": start, "$lte": end}}
    ).sort("event_time", 1)
    return [FundingPoint(time=d["event_time"], rate=d["payload"]["funding_rate"])
            async for d in cursor]
