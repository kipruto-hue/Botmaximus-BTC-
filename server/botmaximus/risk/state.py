"""Risk-layer value objects and portfolio state tracking.

Sizes are OUTPUTS of the risk core (§4.1): an OrderIntent carries direction,
prices and rationale — never a quantity. Rejections are typed and logged,
never silent clamps.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class OrderIntent:
    """What a strategy (via the arbiter) asks for. No size — sizing is risk's job."""
    strategy_id: str
    direction: str                  # "LONG" | "SHORT"
    entry_price: float
    stop_price: float
    symbol: str = "BTCUSDT"
    expected_edge_pct: float | None = None   # gross expected move, for edge-over-cost
    expected_cost_pct: float | None = None   # round-trip cost estimate (cost model, Pass B)
    thesis: str = ""


@dataclass(frozen=True)
class SizedOrder:
    intent: OrderIntent
    qty: float                      # base asset (BTC)
    notional_usd: float
    risk_usd: float                 # equity at risk if the stop is hit
    implied_leverage: float
    est_liquidation_price: float


@dataclass(frozen=True)
class Rejection:
    intent: OrderIntent
    reasons: list[str]


@dataclass
class OpenPosition:
    """Minimal open-exposure record used for cumulative risk accounting."""
    strategy_id: str
    direction: str
    qty: float
    entry_price: float
    stop_price: float
    risk_usd: float


@dataclass
class PortfolioState:
    """Equity/exposure snapshot the risk core checks intents against.

    Paper-oriented for now. When execution exists (Pass F), venue state is
    truth and reconciliation (§4.4) rebuilds this from the exchange, never
    the other way around.
    """
    equity: float
    peak_equity: float
    day_start_equity: float
    day_start_date: str             # UTC YYYY-MM-DD of day_start_equity
    open_positions: list[OpenPosition] = field(default_factory=list)

    @property
    def open_risk_usd(self) -> float:
        return sum(p.risk_usd for p in self.open_positions)

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - self.equity) / self.peak_equity * 100)

    @property
    def day_pnl_pct(self) -> float:
        if self.day_start_equity <= 0:
            return 0.0
        return (self.equity - self.day_start_equity) / self.day_start_equity * 100

    def update_equity(self, equity: float) -> None:
        """Roll the day boundary and the peak. L3 evaluation is the kill
        stack's job — callers must invoke it on every update (§4.3)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.day_start_date:
            self.day_start_date = today
            self.day_start_equity = equity
        self.equity = equity
        self.peak_equity = max(self.peak_equity, equity)
