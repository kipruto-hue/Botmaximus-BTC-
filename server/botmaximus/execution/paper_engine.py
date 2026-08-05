r"""Paper execution engine — the only consumer of scrutiny-approved intents,
and the only caller of `BybitClient`.

Constitution §7: risk-core pre-check → arbiter → scrutiny → risk-core final
approve → execute. This module is the last step and it re-runs the risk check
itself, because state moves between the first approval and the order: equity
changes, a kill can arm, a feed can go stale. Approval is not a token that stays
valid.

## The stop is not a nice-to-have

A position opens and its stop is placed **broker-side, reduce-only, in the same
action**. If the stop fails to place, the just-opened position is closed
immediately and the strategy is suspended. The alternative — an open position
with no stop, waiting for a retry — is the single worst state this system can be
in, and it is reachable in one failed HTTP call.

A stop that exists only in this process's memory is not a stop. It dies with the
process, and the position it was protecting does not.

## Predictions are written before the order, not after the fill

`execution_ledger` records what the cost model *expected* at decision time. If
that were written after the fill, the drift would be computed against a
prediction contaminated by the outcome, and would converge to zero by
construction — which is exactly the reassuring, useless number the ledger was
built to avoid.

## Reconciliation treats the venue as truth

Local bookkeeping is a hypothesis; `fetch_position` is the fact. On divergence
the engine halts (L2) rather than guessing which side is right — resuming blind
after a mismatch is how a phantom position becomes a real loss.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.execution import venue
from botmaximus.execution.bybit_client import BybitClient
from botmaximus.execution.ledger import (
    Prediction,
    Realization,
    mark_unfilled,
    record_prediction,
    record_realization,
)
from botmaximus.obs import degradation
from botmaximus.risk.state import OrderIntent, Rejection, SizedOrder

log = logging.getLogger(__name__)

SIDE = {"LONG": "Buy", "SHORT": "Sell"}


class PaperEngine:
    def __init__(self, risk_core, client: BybitClient | None = None) -> None:
        self.risk = risk_core
        self.client = client or BybitClient()
        self._last_entry_ts: float = 0.0

    # =================================================================
    # entry
    # =================================================================
    async def execute(self, intent: OrderIntent) -> dict:
        """Size, re-check, place, protect, record. Returns an outcome dict."""
        trade_id = f"t-{uuid.uuid4().hex[:16]}"

        sized = self.risk.size_intent(intent)
        if isinstance(sized, Rejection):
            return await self._reject(trade_id, intent, "sizing_rejected",
                                      sized.reasons)

        # Re-check rather than trust the earlier approval: equity, kills and
        # feed freshness all move between the arbiter's decision and this line.
        checked = await self.risk.pre_trade_check(sized)
        if isinstance(checked, Rejection):
            return await self._reject(trade_id, intent, "pre_trade_rejected",
                                      checked.reasons)

        vc = venue.get()
        if settings.order_entry_style == "maker_first_then_taker":
            fill = await self._enter_maker_first(trade_id, sized, vc)
        elif settings.order_entry_style == "taker":
            fill = await self._enter_taker(trade_id, sized, vc)
        else:
            raise RuntimeError(
                f"unknown order_entry_style {settings.order_entry_style!r}")

        if fill is None:
            return {"trade_id": trade_id, "status": "unfilled"}

        protected = await self._place_protective_stop(trade_id, sized, vc)
        if not protected:
            return {"trade_id": trade_id, "status": "closed_unprotected"}

        self.risk.register_open(sized)
        self._last_entry_ts = time.monotonic()
        return {"trade_id": trade_id, "status": "open",
                "qty": sized.qty, "entry": fill["price"]}

    async def _enter_taker(self, trade_id: str, sized: SizedOrder, vc) -> dict | None:
        await self._predict(trade_id, sized, "entry", sized.intent.entry_price, vc)
        try:
            order = await self.client.place_market(
                SIDE[sized.intent.direction], sized.qty)
        except Exception as e:                          # noqa: BLE001
            await mark_unfilled(trade_id, "entry", f"place_failed:{e}")
            return None
        return await self._confirm_fill(trade_id, "entry", order, sized)

    async def _enter_maker_first(self, trade_id: str, sized: SizedOrder,
                                 vc) -> dict | None:
        """Post-only at the touch, then cross for whatever is left.

        Each leg gets its own prediction and its own reconciliation: a maker
        fill and a taker fill have different costs, and averaging them would
        hide the very difference this entry style exists to measure.
        """
        price = vc.round_price(sized.intent.entry_price)
        await self._predict(trade_id, sized, "entry", price, vc, maker=True)
        try:
            order = await self.client.place_limit_maker(
                SIDE[sized.intent.direction], sized.qty, price)
        except Exception as e:                          # noqa: BLE001
            await degradation.record(
                "maker_entry_rejected",
                f"post-only entry refused ({e}) — falling back to taker",
                trade_id=trade_id)
            await mark_unfilled(trade_id, "entry", f"maker_rejected:{e}")
            return await self._enter_taker(trade_id, sized, vc)

        deadline = time.monotonic() + settings.maker_timeout_ms / 1000
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            pos = await self.client.fetch_position()
            if pos and float(pos["size"]) >= sized.qty:
                return await self._confirm_fill(trade_id, "entry", order, sized)

        # Timed out: cancel the remainder and cross for the shortfall.
        try:
            await self.client.cancel(order["orderId"])
        except Exception:                               # noqa: BLE001
            pass
        pos = await self.client.fetch_position()
        filled = float(pos["size"]) if pos else 0.0
        shortfall = vc.round_qty(sized.qty - filled)
        if shortfall <= 0:
            return await self._confirm_fill(trade_id, "entry", order, sized)

        await degradation.record(
            "maker_entry_timeout",
            "post-only entry did not fill inside the timeout — crossing for the "
            "shortfall, which pays the taker fee this style exists to avoid",
            trade_id=trade_id, filled=filled, shortfall=shortfall)
        await mark_unfilled(trade_id, "entry", "maker_timeout_partial")
        return await self._enter_taker(trade_id, sized, vc)

    # =================================================================
    # protection
    # =================================================================
    async def _place_protective_stop(self, trade_id: str, sized: SizedOrder,
                                     vc) -> bool:
        """Broker-side reduce-only stop. Failure closes the position at once."""
        exit_side = "Sell" if sized.intent.direction == "LONG" else "Buy"
        trigger = vc.round_price(sized.intent.stop_price)
        try:
            await self.client.place_stop_market(exit_side, sized.qty, trigger)
            return True
        except Exception as e:                          # noqa: BLE001
            log.critical("[%s] STOP PLACEMENT FAILED (%s) — closing immediately",
                         trade_id, e)
            await degradation.record(
                "stop_place_failed",
                "protective stop could not be placed; closing the position "
                "rather than holding it unprotected",
                trade_id=trade_id, strategy_id=sized.intent.strategy_id)
            try:
                await self.client.close_position_market()
            except Exception as close_err:              # noqa: BLE001
                # An open position with no stop AND no way to close it is the
                # worst state available. Halt the portfolio and shout.
                log.critical("[%s] could not close after stop failure: %s",
                             trade_id, close_err)
                await self.risk.kills.halt_portfolio(
                    f"unprotected_position:{trade_id}")
            await mark_unfilled(trade_id, "entry", "stop_place_failed")
            await self.risk.kills.suspend_strategy(
                sized.intent.strategy_id, "stop_place_failed")
            return False

    # =================================================================
    # ledger
    # =================================================================
    async def _predict(self, trade_id: str, sized: SizedOrder, leg: str,
                       reference_price: float, vc, maker: bool = False) -> None:
        fee_rate = vc.maker_fee_rate if maker else vc.taker_fee_rate
        slip = 0.0 if maker else settings.slippage_bps
        buying = (sized.intent.direction == "LONG") == (leg == "entry")
        fill = reference_price * (1 + slip / 10_000) if buying \
            else reference_price * (1 - slip / 10_000)
        await record_prediction(Prediction(
            trade_id=trade_id,
            strategy_id=sized.intent.strategy_id,
            leg=leg,
            direction=sized.intent.direction,
            symbol=settings.symbol,
            qty=sized.qty,
            decision_time=datetime.now(timezone.utc),
            reference_price=reference_price,
            predicted_fill=fill,
            predicted_fee=abs(sized.qty * fill) * fee_rate,
            predicted_slippage_bps=slip,
            predicted_latency_ms=0.0,
        ))

    async def _confirm_fill(self, trade_id: str, leg: str, order: dict,
                            sized: SizedOrder) -> dict | None:
        """Read the venue for what actually happened and reconcile the leg."""
        pos = await self.client.fetch_position()
        if pos is None or float(pos["size"]) <= 0:
            await mark_unfilled(trade_id, leg, "no_position_after_place")
            return None
        price = float(pos.get("avgPrice") or pos.get("entryPrice") or 0.0)
        qty = float(pos["size"])
        vc = venue.get()
        await record_realization(Realization(
            trade_id=trade_id, leg=leg,
            fill_time=datetime.now(timezone.utc),
            realized_fill=price,
            realized_fee=abs(qty * price) * vc.taker_fee_rate,
            realized_qty=qty,
            venue_order_id=order.get("orderId"),
            partial=qty < sized.qty,
        ))
        return {"price": price, "qty": qty}

    async def _reject(self, trade_id: str, intent: OrderIntent, status: str,
                      reasons: list[str]) -> dict:
        await mark_unfilled(trade_id, "entry", f"{status}:{','.join(reasons)}")
        return {"trade_id": trade_id, "status": status, "reasons": reasons}

    # =================================================================
    # flatten — audit B1
    # =================================================================
    async def flatten_all(self) -> dict:
        """Cancel every order, close every position, verify flat.

        This is what makes a kill a kill. Blocking new entries while leaving
        exposure on is not a kill; it is a pause during the event the kill was
        armed for.
        """
        t0 = time.monotonic()
        cancelled = 0
        errors: list[str] = []
        try:
            cancelled = await self.client.cancel_all()
        except Exception as e:                          # noqa: BLE001
            errors.append(f"cancel_all:{e}")

        closed = None
        try:
            closed = await self.client.close_position_market()
        except Exception as e:                          # noqa: BLE001
            errors.append(f"close:{e}")

        # Verify against the venue. "We sent a close" is not "we are flat".
        flat = False
        for _ in range(10):
            try:
                if await self.client.fetch_position() is None:
                    flat = True
                    break
            except Exception as e:                      # noqa: BLE001
                errors.append(f"verify:{e}")
            await asyncio.sleep(0.15)

        elapsed_ms = (time.monotonic() - t0) * 1000
        result = {"cancelled_orders": cancelled, "closed": bool(closed),
                  "flat": flat, "elapsed_ms": round(elapsed_ms, 1),
                  "errors": errors}
        if not flat:
            log.critical("FLATTEN DID NOT VERIFY FLAT: %s", result)
            await degradation.record(
                "flatten_unverified",
                "master kill could not verify a flat position — exposure may "
                "remain and this needs an operator now", **result)
        else:
            log.warning("flatten complete in %.0fms: %s", elapsed_ms, result)
        return result

    # =================================================================
    # reconciliation
    # =================================================================
    async def reconcile(self) -> dict:
        """Local intent vs venue truth. Divergence halts rather than guesses."""
        try:
            pos = await self.client.fetch_position()
            orders = await self.client.fetch_open_orders()
        except Exception as e:                          # noqa: BLE001
            await degradation.record("reconcile_read_failed", str(e))
            return {"ok": False, "reason": "read_failed"}

        local_open = len(self.risk.portfolio.open_positions)
        venue_open = 1 if pos else 0
        if local_open == venue_open:
            return {"ok": True, "positions": venue_open, "orders": len(orders)}

        await degradation.record(
            "reconcile_divergence",
            "local position count disagrees with the venue — halting rather "
            "than guessing which is right",
            local=local_open, venue=venue_open)
        await self.risk.kills.halt_portfolio("reconcile_divergence")
        return {"ok": False, "local": local_open, "venue": venue_open}
