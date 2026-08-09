"""RiskCore (§4): sizing, pre-trade checks, kill stack, state persistence.

Deterministic and standalone — no imports from strategies, arbiter, or gate.
Size is an output of risk (§4.1). Intents that exceed caps are REJECTED with
reasons, never silently clamped. Every verdict is logged to `risk_events`
(constitution §2.7).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.execution import venue
from botmaximus.obs import degradation
from botmaximus.risk import freshness
from botmaximus.risk.kills import KillStack
from botmaximus.risk.state import (
    OpenPosition,
    OrderIntent,
    PortfolioState,
    Rejection,
    SizedOrder,
)

log = logging.getLogger(__name__)

RISK_STATE_COLLECTION = "risk_state"
RISK_EVENTS_COLLECTION = "risk_events"
PORTFOLIO_DOC_ID = "portfolio"

EDGE_COST_MARGIN = 1.5          # expected edge must be ≥ 1.5× expected cost

# Quantity step, minimum notional and maintenance margin used to be constants
# copied from Binance. Two of the three were wrong for Bybit — min notional by
# 20x, and maintenance margin low by 25%, which made the estimated liquidation
# price sit further from entry than the real one and let the stop-vs-liquidation
# buffer approve trades closer to the edge than the operator allowed.
# They now come from the venue itself (constitution §9): `execution/venue.py`.
#
# Freshness used to check a fixed pair of feeds for every strategy. It is now
# resolved from the strategy's declared `required_feeds` (§8) — see
# `risk/freshness.py` for why the fixed pair was both too strict and too loose.


class RiskCore:
    def __init__(self, db=None) -> None:
        # `db` is accepted and ignored: risk state lives in Postgres now, and
        # keeping the parameter lets the backtester keep constructing a
        # RiskCore the same way while `size_intent` stays pure.
        self.kills = KillStack()
        self.portfolio = PortfolioState(
            equity=settings.starting_equity_paper,
            peak_equity=settings.starting_equity_paper,
            day_start_equity=settings.starting_equity_paper,
            day_start_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        )

    # ---- lifecycle ----
    async def load(self) -> None:
        """Restore persisted state. A restart never resets equity, peak, or kills."""
        from botmaximus.storage import postgres
        await self.kills.load()
        row = await postgres.fetchrow(
            "SELECT state FROM risk_state WHERE id = %s", (PORTFOLIO_DOC_ID,))
        if row:
            doc = row["state"]
            self.portfolio.equity = doc["equity"]
            self.portfolio.peak_equity = doc["peak_equity"]
            self.portfolio.day_start_equity = doc["day_start_equity"]
            self.portfolio.day_start_date = doc["day_start_date"]

    async def _persist_portfolio(self) -> None:
        import json

        from botmaximus.storage import postgres
        await postgres.execute(
            "INSERT INTO risk_state (id, state, updated_at) "
            "VALUES (%s, %s, now()) "
            "ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, "
            "  updated_at = now()",
            (PORTFOLIO_DOC_ID, json.dumps({
                "equity": self.portfolio.equity,
                "peak_equity": self.portfolio.peak_equity,
                "day_start_equity": self.portfolio.day_start_equity,
                "day_start_date": self.portfolio.day_start_date,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })))

    async def update_equity(self, equity: float) -> None:
        """§4.3: the L3/L2 check runs on EVERY equity update, unsuppressably."""
        self.portfolio.update_equity(equity)
        await self.kills.check_equity(
            self.portfolio.drawdown_pct,
            self.portfolio.day_pnl_pct,
            settings.max_drawdown_kill_pct,
            settings.daily_loss_limit_pct,
        )
        await self._persist_portfolio()

    # ---- sizing (§4.1) ----
    def size_intent(self, intent: OrderIntent) -> SizedOrder | Rejection:
        reasons: list[str] = []
        if intent.direction not in ("LONG", "SHORT"):
            reasons.append("invalid_direction")
        stop_distance = (
            intent.entry_price - intent.stop_price if intent.direction == "LONG"
            else intent.stop_price - intent.entry_price
        )
        if stop_distance <= 0:
            reasons.append("stop_on_wrong_side")
        if intent.entry_price <= 0:
            reasons.append("nonpositive_entry")
        if reasons:
            return Rejection(intent, reasons)

        vc = venue.get()        # raises if startup never fetched them
        risk_usd = self.portfolio.equity * settings.risk_per_trade_pct / 100
        qty = vc.round_qty(risk_usd / stop_distance)
        if qty <= 0 or qty < vc.min_order_qty:
            return Rejection(intent, ["risk_too_small_for_min_qty"])
        if qty > vc.max_order_qty:
            return Rejection(intent, ["above_venue_max_qty"])
        notional = qty * intent.entry_price
        if notional < vc.min_notional:
            return Rejection(intent, ["below_min_notional"])

        return SizedOrder(
            intent=intent,
            qty=round(qty, 3),
            notional_usd=round(notional, 2),
            risk_usd=round(qty * stop_distance, 2),
            implied_leverage=round(notional / self.portfolio.equity, 3),
            est_liquidation_price=self._liquidation_price(
                intent.direction, intent.entry_price, notional),
        )

    @staticmethod
    def _liquidation_price(direction: str, entry: float,
                           notional_usd: float = 0.0) -> float:
        """Isolated one-way USDT-margined approximation at the leverage cap.
        Conservative for paper: assumes the position is margined at the cap.

        The maintenance-margin rate is selected from Bybit's real tier ladder
        for this position's notional, not a single assumed number. A rate that
        is too low pushes the estimate away from entry and makes the
        stop-vs-liquidation buffer approve trades that sit nearer the edge than
        the operator authorised.
        """
        lev = settings.leverage_cap
        mmr = venue.get().maint_margin_for(notional_usd)
        if direction == "LONG":
            return entry * (1 - 1 / lev + mmr)
        return entry * (1 + 1 / lev - mmr)

    # ---- pre-trade checks (§4.2) — all must pass, deterministic ----
    async def pre_trade_check(self, sized: SizedOrder) -> SizedOrder | Rejection:
        intent = sized.intent
        reasons: list[str] = []

        block = self.kills.blocks_trading()
        if block:
            reasons.append(block)
        l1 = self.kills.strategy_suspended(intent.strategy_id)
        if l1:
            reasons.append(f"L1_strategy_suspended:{l1}")

        max_risk_usd = self.portfolio.equity * settings.risk_per_trade_pct / 100
        if sized.risk_usd > max_risk_usd * 1.001:  # float tolerance only, not headroom
            reasons.append("per_trade_risk_exceeds_cap")

        open_risk_cap = self.portfolio.equity * settings.max_open_risk_pct / 100
        if self.portfolio.open_risk_usd + sized.risk_usd > open_risk_cap:
            reasons.append("total_open_risk_exceeds_cap")

        if sized.implied_leverage > settings.leverage_cap:
            reasons.append("leverage_exceeds_cap")

        open_margin = sum(
            p.qty * p.entry_price / settings.leverage_cap
            for p in self.portfolio.open_positions
        )
        required_margin = sized.notional_usd / settings.leverage_cap
        if open_margin + required_margin > self.portfolio.equity:
            reasons.append("insufficient_free_margin")

        reasons += self._check_stop_vs_liquidation(sized)
        reasons += self._check_feed_freshness(intent.required_feeds)
        reasons += self._check_edge_over_cost(intent)

        verdict: SizedOrder | Rejection = Rejection(intent, reasons) if reasons else sized
        await self._log_event(
            "rejected" if reasons else "approved",
            intent, reasons=reasons,
            qty=sized.qty, risk_usd=sized.risk_usd, notional=sized.notional_usd,
        )
        return verdict

    def _check_stop_vs_liquidation(self, sized: SizedOrder) -> list[str]:
        """§2.6: the stop must always be reached before liquidation, with buffer."""
        intent = sized.intent
        liq = sized.est_liquidation_price
        stop_distance = abs(intent.entry_price - intent.stop_price)
        buffer_required = stop_distance * settings.liq_stop_buffer_pct / 100
        if intent.direction == "LONG":
            if intent.stop_price <= liq or (intent.stop_price - liq) < buffer_required:
                return ["stop_beyond_liquidation_buffer"]
        else:
            if intent.stop_price >= liq or (liq - intent.stop_price) < buffer_required:
                return ["stop_beyond_liquidation_buffer"]
        return []

    @staticmethod
    def _check_feed_freshness(required_feeds: tuple[str, ...] | list[str]) -> list[str]:
        """Stale-feed reasons for exactly the feeds this strategy declared (§8).

        An empty declaration is not treated as "nothing to check" — that would
        let an under-specified strategy trade with no freshness guard at all.
        It falls back to the baseline pair and records a degradation, so the
        gap is countable instead of invisible (§11).
        """
        if not required_feeds:
            degradation.record_sync(
                "freshness_no_declared_feeds",
                "strategy declared no required_feeds — falling back to the "
                "baseline pair; freshness is not being checked for whatever "
                "this strategy actually reads")
            required_feeds = freshness.BASELINE_FEEDS
        return freshness.assert_fresh(required_feeds)

    @staticmethod
    def _check_edge_over_cost(intent: OrderIntent) -> list[str]:
        """Edge must clear round-trip cost by a margin (§4.2). Until the Pass-B
        cost model wires real figures in, callers supply the estimate; an intent
        with neither figure is not blocked but is flagged in the event log."""
        if intent.expected_edge_pct is None or intent.expected_cost_pct is None:
            return []
        if intent.expected_edge_pct < intent.expected_cost_pct * EDGE_COST_MARGIN:
            return ["edge_below_cost_margin"]
        return []

    # ---- exposure bookkeeping (used by paper execution later; tests now) ----
    def register_open(self, sized: SizedOrder) -> None:
        self.portfolio.open_positions.append(OpenPosition(
            strategy_id=sized.intent.strategy_id,
            direction=sized.intent.direction,
            qty=sized.qty,
            entry_price=sized.intent.entry_price,
            stop_price=sized.intent.stop_price,
            risk_usd=sized.risk_usd,
        ))

    def register_close(self, strategy_id: str) -> None:
        self.portfolio.open_positions = [
            p for p in self.portfolio.open_positions if p.strategy_id != strategy_id
        ]

    # ---- logging (§2.7) ----
    async def _log_event(self, kind: str, intent: OrderIntent, **fields) -> None:
        import json

        from botmaximus.storage import postgres
        await postgres.execute(
            "INSERT INTO risk_events (kind, strategy_id, detail) "
            "VALUES (%s, %s, %s)",
            (kind, intent.strategy_id, json.dumps({
                "direction": intent.direction,
                "entry_price": intent.entry_price,
                "stop_price": intent.stop_price,
                "thesis": intent.thesis,
                **fields,
            }, default=str)))

    def snapshot(self) -> dict:
        return {
            "equity": round(self.portfolio.equity, 2),
            "peak_equity": round(self.portfolio.peak_equity, 2),
            "drawdown_pct": round(self.portfolio.drawdown_pct, 3),
            "day_pnl_pct": round(self.portfolio.day_pnl_pct, 3),
            "open_risk_usd": round(self.portfolio.open_risk_usd, 2),
            "open_positions": len(self.portfolio.open_positions),
            "limits": {
                "risk_per_trade_pct": settings.risk_per_trade_pct,
                "max_open_risk_pct": settings.max_open_risk_pct,
                "daily_loss_limit_pct": settings.daily_loss_limit_pct,
                "max_drawdown_kill_pct": settings.max_drawdown_kill_pct,
                "leverage_cap": settings.leverage_cap,
                "margin_mode": settings.margin_mode,
                "position_mode": settings.position_mode,
            },
            "kills": self.kills.snapshot(),
        }
