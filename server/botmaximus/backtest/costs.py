"""Cost model (§5.3). Every simulated trade pays fees, funding, slippage, and
latency. A strategy profitable only under a frictionless model is rejected —
so gross and net are always reported side by side, and slippage is never zero.

Taker-only (ORDER_STYLE locked to taker): fills cross the spread, so entry and
exit take the adverse side plus a conservative slippage floor. Funding is
charged from the actual settled `btc_funding_8h` series, not an estimate.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import datetime

from botmaximus.config import settings


@dataclass(frozen=True)
class FundingPoint:
    time: datetime          # settlement time (00/08/16:00 UTC)
    rate: float             # signed 8h rate; positive → longs pay


class CostModel:
    def __init__(self, funding: list[FundingPoint] | None = None,
                 taker_fee_rate: float | None = None, slippage_bps: float | None = None):
        self.taker_fee_rate = settings.taker_fee_rate if taker_fee_rate is None else taker_fee_rate
        self.slippage_bps = settings.slippage_bps if slippage_bps is None else slippage_bps
        self._funding = sorted(funding or [], key=lambda f: f.time)
        self._funding_times = [f.time for f in self._funding]

    # ---- fills ----
    def fill_price(self, mid: float, direction: str, side: str) -> float:
        """Adverse-side taker fill. `side` is 'entry' or 'exit'.
        A long buys to enter (pays up) and sells to exit (receives less);
        a short is the mirror. Slippage always worsens the fill."""
        slip = self.slippage_bps / 10_000
        buying = (direction == "LONG" and side == "entry") or (direction == "SHORT" and side == "exit")
        return mid * (1 + slip) if buying else mid * (1 - slip)

    def fee(self, notional: float) -> float:
        return abs(notional) * self.taker_fee_rate

    # ---- funding ----
    def funding_cost(self, direction: str, qty: float, avg_price: float,
                     entry_time: datetime, exit_time: datetime) -> float:
        """Sum funding over every settlement strictly inside (entry, exit].
        Positive result = a cost to this position. Longs pay when the rate is
        positive; shorts pay when it is negative."""
        if not self._funding or exit_time <= entry_time:
            return 0.0
        lo = bisect.bisect_right(self._funding_times, entry_time)
        hi = bisect.bisect_right(self._funding_times, exit_time)
        notional = qty * avg_price
        sign = 1.0 if direction == "LONG" else -1.0
        return sum(sign * self._funding[i].rate * notional for i in range(lo, hi))
