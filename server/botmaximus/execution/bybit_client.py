r"""Thin Bybit V5 adapter — the only module in the system that talks to a venue.

Constitution §1-§3: Bybit V5, `linear`, `BTCUSDT`, one-way, isolated, **testnet
only in this build**. Nothing here can reach a live account, and nothing here
can be pointed at another symbol or category — both are asserted before a
request is built, not validated afterwards by the venue.

## Three independent guards, on purpose

Every mutating call passes:

1. `settings.require("bybit_api_key", "bybit_api_secret")` — configured at all;
2. `assert settings.bybit_testnet is True` — belt-and-braces over the config,
   so a future edit that loosens config still trips here;
3. `settings.require_trading()` — the sanctioned pre-order check.

Three checks for one property is redundant by design. The property is "this
build cannot place an order against real money", and redundancy is cheap next
to the cost of being wrong once.

**Read methods carry guards 1 and 2 but not 3.** This is a deliberate deviation
from a literal reading of the spec: position and order reads must keep working
while trading is halted, because that is exactly when reconciliation and the
kill-flatten verification need them. A halt that also blinds you is worse than
the failure it was protecting against.

## pybit is synchronous; this system is not

Every call runs on a worker thread via `asyncio.to_thread`. A blocking HTTP
round-trip on the event loop would stall the collector — the websocket feeds
share that loop, and stalling them loses data that cannot be backfilled.

## Order placement is never retried

A retry after an ambiguous response is how one intent becomes two positions.
On any uncertainty the client reconciles against `fetch_open_orders` /
`fetch_position` instead. `orderLinkId` (Bybit's client order id) is set on
every order, so a reconciling read can recognise our own order.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from pybit.unified_trading import HTTP

from botmaximus.config import settings
from botmaximus.obs import degradation

log = logging.getLogger(__name__)

#: Bybit rate-limit / system-busy codes worth a bounded retry on READ calls.
RETRYABLE_CODES = {10006, 10016, 10429}
MAX_READ_RETRIES = 3


class VenueScopeError(RuntimeError):
    """Raised when a call would leave the locked venue/symbol/category scope."""


class BybitClient:
    def __init__(self) -> None:
        self._assert_scope()
        settings.require("bybit_api_key", "bybit_api_secret")
        self._http = HTTP(
            testnet=settings.bybit_testnet,
            api_key=settings.bybit_api_key.get_secret_value(),
            api_secret=settings.bybit_api_secret.get_secret_value(),
        )

    # ---- guards -------------------------------------------------------
    @staticmethod
    def _assert_scope() -> None:
        if settings.venue != "bybit":
            raise VenueScopeError(f"venue is {settings.venue!r}, not bybit")
        if settings.bybit_category != "linear":
            raise VenueScopeError(
                f"category is {settings.bybit_category!r}. Only `linear` is "
                f"supported: `inverse` is coin-margined with non-linear PnL and "
                f"the risk core's sizing does not model it.")
        if settings.symbol != "BTCUSDT":
            raise VenueScopeError(
                f"symbol is {settings.symbol!r}; this build is locked to BTCUSDT")
        assert settings.bybit_testnet is True, (
            "live account reached in a build that must not touch live")

    def _guard_read(self) -> None:
        self._assert_scope()
        settings.require("bybit_api_key", "bybit_api_secret")

    def _guard_trade(self) -> None:
        self._guard_read()
        settings.require_trading()

    # ---- transport ----------------------------------------------------
    async def _call(self, method: str, retry: bool, **params) -> dict:
        """One pybit call on a worker thread, with `retCode` checked.

        V5 returns errors with HTTP 200, so pybit raising nothing does not mean
        the request succeeded. An unchecked retCode turns a rate limit into an
        empty result set — which downstream reads as "no open orders" and is
        how a reconciler concludes a live position does not exist.
        """
        fn = getattr(self._http, method)
        attempt = 0
        while True:
            resp: dict[str, Any] = await asyncio.to_thread(fn, **params)
            code = resp.get("retCode")
            if code == 0:
                return resp
            if retry and code in RETRYABLE_CODES and attempt < MAX_READ_RETRIES:
                delay = 0.4 * (2 ** attempt)
                await degradation.record(
                    "bybit_read_retry",
                    f"{method} returned retCode={code}; retrying in {delay:.1f}s",
                    method=method, attempt=attempt + 1)
                await asyncio.sleep(delay)
                attempt += 1
                continue
            raise RuntimeError(
                f"bybit {method} retCode={code} retMsg={resp.get('retMsg')!r}")

    @staticmethod
    def _link_id(prefix: str) -> str:
        """Bybit's client order id. Idempotency key and ownership marker: a
        reconciling read uses it to tell our orders from anything else on the
        account."""
        return f"bmx-{prefix}-{uuid.uuid4().hex[:16]}"

    # ---- reads --------------------------------------------------------
    async def fetch_instrument_info(self) -> dict:
        self._guard_read()
        r = await self._call("get_instruments_info", True,
                             category=settings.bybit_category,
                             symbol=settings.symbol)
        return r["result"]["list"][0]

    async def fetch_fee_rate(self) -> dict:
        """Account fee schedule. The cost model's dominant term — an assumed
        rate makes every validation verdict an estimate of the wrong thing."""
        self._guard_read()
        r = await self._call("get_fee_rates", True,
                             category=settings.bybit_category,
                             symbol=settings.symbol)
        row = r["result"]["list"][0]
        return {"taker": float(row["takerFeeRate"]),
                "maker": float(row["makerFeeRate"])}

    async def fetch_position(self) -> dict | None:
        """The single open position, or None. Venue state is truth (§4.4):
        never inferred from local bookkeeping."""
        self._guard_read()
        r = await self._call("get_positions", True,
                             category=settings.bybit_category,
                             symbol=settings.symbol)
        for row in r["result"]["list"]:
            if float(row.get("size") or 0) > 0:
                return row
        return None

    async def fetch_open_orders(self) -> list[dict]:
        self._guard_read()
        r = await self._call("get_open_orders", True,
                             category=settings.bybit_category,
                             symbol=settings.symbol)
        return r["result"]["list"]

    async def fetch_wallet_equity(self) -> float:
        self._guard_read()
        r = await self._call("get_wallet_balance", True, accountType="UNIFIED")
        rows = r["result"]["list"]
        return float(rows[0]["totalEquity"]) if rows else 0.0

    # ---- account setup -------------------------------------------------
    async def configure_account(self, leverage: int) -> None:
        """Isolated margin at the operator's leverage cap, one-way mode.

        Both calls are idempotent at the venue and both tolerate "already set"
        responses -- Bybit returns a non-zero retCode for a no-op change, which
        is not an error worth aborting startup over.
        """
        self._guard_trade()
        for method, params, benign in (
            ("switch_position_mode",
             {"category": settings.bybit_category, "symbol": settings.symbol,
              "mode": 0}, {110025}),
            ("set_leverage",
             {"category": settings.bybit_category, "symbol": settings.symbol,
              "buyLeverage": str(leverage), "sellLeverage": str(leverage)},
             {110043}),
        ):
            try:
                await self._call(method, False, **params)
            except RuntimeError as e:
                if not any(str(c) in str(e) for c in benign):
                    raise
                log.info("%s already configured", method)

    # ---- orders -------------------------------------------------------
    async def place_market(self, side: str, qty: float,
                           reduce_only: bool = False) -> dict:
        """Never retried. A retry after an ambiguous response is how one intent
        becomes two positions; the caller reconciles instead."""
        self._guard_trade()
        link = self._link_id("mkt")
        r = await self._call(
            "place_order", False,
            category=settings.bybit_category, symbol=settings.symbol,
            side=side, orderType="Market", qty=str(qty),
            reduceOnly=reduce_only, orderLinkId=link,
            timeInForce="IOC",
        )
        return {"orderId": r["result"]["orderId"], "orderLinkId": link}

    async def place_limit_maker(self, side: str, qty: float, price: float) -> dict:
        """PostOnly: rejected rather than filled if it would cross. That is the
        point — a maker order that crosses pays the taker fee, which is the
        cost this entry style exists to avoid."""
        self._guard_trade()
        link = self._link_id("mkr")
        r = await self._call(
            "place_order", False,
            category=settings.bybit_category, symbol=settings.symbol,
            side=side, orderType="Limit", qty=str(qty), price=str(price),
            timeInForce="PostOnly", orderLinkId=link,
        )
        return {"orderId": r["result"]["orderId"], "orderLinkId": link}

    async def place_stop_market(self, side: str, qty: float,
                                trigger_price: float) -> dict:
        """Broker-side reduce-only stop. Lives at the venue so it survives this
        process dying — a stop that only exists in local memory is not a stop."""
        self._guard_trade()
        link = self._link_id("stp")
        r = await self._call(
            "place_order", False,
            category=settings.bybit_category, symbol=settings.symbol,
            side=side, orderType="Market", qty=str(qty),
            triggerPrice=str(trigger_price),
            triggerDirection=2 if side == "Buy" else 1,
            triggerBy="LastPrice", reduceOnly=True,
            orderLinkId=link, timeInForce="IOC",
        )
        return {"orderId": r["result"]["orderId"], "orderLinkId": link}

    async def cancel(self, order_id: str) -> None:
        self._guard_trade()
        await self._call("cancel_order", False,
                         category=settings.bybit_category,
                         symbol=settings.symbol, orderId=order_id)

    async def cancel_all(self) -> int:
        self._guard_trade()
        r = await self._call("cancel_all_orders", False,
                             category=settings.bybit_category,
                             symbol=settings.symbol)
        return len(r["result"].get("list") or [])

    async def close_position_market(self) -> dict | None:
        """Flatten whatever is open, reduce-only. Returns None if already flat.

        Side is derived from the venue's reported position, never from local
        state: closing in the wrong direction doubles the position instead of
        removing it, and local state is precisely what is suspect when this is
        being called.
        """
        self._guard_trade()
        pos = await self.fetch_position()
        if pos is None:
            return None
        close_side = "Sell" if pos["side"] == "Buy" else "Buy"
        return await self.place_market(close_side, float(pos["size"]),
                                       reduce_only=True)
