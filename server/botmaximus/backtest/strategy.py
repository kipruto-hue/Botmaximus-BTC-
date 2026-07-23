"""Strategy protocol — the backtester's input contract (§6.1 forward-reference).

A strategy is a pure function of a point-in-time view → an OrderIntent or None.
It never sizes (risk does that, §4.1) and never sees the future (the view
forbids it). Pass C's DSL strategies will conform to or adapt to this shape;
until then only test fixtures implement it. No model-authored code runs here.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from botmaximus.backtest.data import PointInTimeView
from botmaximus.risk.state import OrderIntent


@runtime_checkable
class Strategy(Protocol):
    id: str
    required_feeds: list[str]

    def evaluate(self, view: PointInTimeView) -> OrderIntent | None:
        """Return an entry intent for a flat book, or None to stand aside.
        Called only when no position is open."""
        ...
