"""Replay engine (§5.3–5.4). Walks 1m bars, asks the strategy for an intent
while flat, sizes it through the risk core, and simulates a taker fill one or
more bars later (latency) at an adverse, slippage-loaded price. Positions exit
on stop / target / time-stop, checked intrabar with a stop-first worst-case
assumption. Fees and funding are charged exactly as the cost model defines.

Every trade records gross and net PnL; the two diverge by exactly the modelled
friction, which is the diagnostic §5.3 asks for.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from botmaximus.backtest.costs import CostModel
from botmaximus.backtest.data import Bar, PointInTimeView
from botmaximus.backtest.strategy import Strategy
from botmaximus.config import settings
from botmaximus.risk.core import RiskCore
from botmaximus.risk.state import Rejection, SizedOrder


@dataclass
class Trade:
    strategy_id: str
    direction: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float          # actual fill (with slippage)
    exit_price: float
    qty: float
    exit_reason: str            # "stop" | "target" | "time"
    gross_pnl: float
    fees: float
    funding: float

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.fees - self.funding


@dataclass
class BacktestResult:
    trades: list[Trade]
    equity_curve: list[tuple[datetime, float]]
    starting_equity: float
    bars_evaluated: int
    warmup_bars: int
    params: dict = field(default_factory=dict)

    @property
    def net_return_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        return (self.equity_curve[-1][1] / self.starting_equity - 1) * 100


class Backtester:
    def __init__(self, cost_model: CostModel, risk: RiskCore,
                 time_stop_bars: int | None = None):
        self.costs = cost_model
        self.risk = risk
        self.latency = settings.latency_bars
        self.time_stop_bars = time_stop_bars or max(1, settings.holding_period_target_s // 60)

    def run(self, bars: list[Bar], strategy: Strategy, warmup: int = 60) -> BacktestResult:
        trades: list[Trade] = []
        equity = self.risk.portfolio.equity
        start_equity = equity
        curve: list[tuple[datetime, float]] = []

        i = warmup
        n = len(bars)
        while i < n - self.latency - 1:
            self.risk.portfolio.equity = equity          # size against current equity
            intent = strategy.evaluate(PointInTimeView(bars, i))
            if intent is None:
                curve.append((bars[i].close_time, equity))
                i += 1
                continue

            sized = self.risk.size_intent(intent)
            if isinstance(sized, Rejection):
                curve.append((bars[i].close_time, equity))
                i += 1
                continue

            trade, exit_idx = self._simulate(bars, i, intent.direction, sized)
            if trade is not None:
                trades.append(trade)
                equity += trade.net_pnl
                i = exit_idx + 1                          # flat again after exit
            else:
                i += 1
            curve.append((bars[min(i, n - 1)].close_time, equity))

        return BacktestResult(
            trades=trades, equity_curve=curve, starting_equity=start_equity,
            bars_evaluated=n - warmup, warmup_bars=warmup,
            params={"time_stop_bars": self.time_stop_bars, "latency_bars": self.latency},
        )

    def _simulate(self, bars: list[Bar], signal_idx: int, direction: str,
                  sized: SizedOrder):
        """Fill at signal_idx + latency open, then walk forward to the exit."""
        entry_idx = signal_idx + self.latency
        entry_bar = bars[entry_idx]
        entry_fill = self.costs.fill_price(entry_bar.open, direction, "entry")
        qty = sized.qty
        stop = sized.intent.stop_price
        # symmetric target at the intent's reward = risk distance (1:1) for the harness
        stop_dist = abs(entry_fill - stop)
        target = entry_fill + stop_dist if direction == "LONG" else entry_fill - stop_dist

        last = min(len(bars) - 1, entry_idx + self.time_stop_bars)
        for j in range(entry_idx, last + 1):
            bar = bars[j]
            if j == last:
                exit_mid, reason = bar.close, "time"
            elif direction == "LONG":
                if bar.low <= stop:                      # stop-first worst case
                    exit_mid, reason = stop, "stop"
                elif bar.high >= target:
                    exit_mid, reason = target, "target"
                else:
                    continue
            else:
                if bar.high >= stop:
                    exit_mid, reason = stop, "stop"
                elif bar.low <= target:
                    exit_mid, reason = target, "target"
                else:
                    continue

            exit_fill = self.costs.fill_price(exit_mid, direction, "exit")
            gross = (exit_fill - entry_fill) * qty if direction == "LONG" else (entry_fill - exit_fill) * qty
            fees = self.costs.fee(qty * entry_fill) + self.costs.fee(qty * exit_fill)
            funding = self.costs.funding_cost(
                direction, qty, entry_fill, entry_bar.close_time, bar.close_time)
            return Trade(
                strategy_id=sized.intent.strategy_id, direction=direction,
                entry_time=entry_bar.close_time, exit_time=bar.close_time,
                entry_price=entry_fill, exit_price=exit_fill, qty=qty,
                exit_reason=reason, gross_pnl=gross, fees=fees, funding=funding,
            ), j
        return None, entry_idx
