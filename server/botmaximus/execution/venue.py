r"""Bybit venue constants — constitution §9: *Bybit constants come from Bybit.*

Closes audit B5. Until now `risk/core.py` sized orders against three constants
copied from Binance, and two of them were wrong for the venue we actually trade:

| constant | hardcoded | Bybit actual | consequence |
|---|---|---|---|
| qty step | 0.001 | 0.001 | correct by luck |
| min notional | 100.0 USD | **5.0 USD** | rejected valid orders at 20x the real floor |
| maintenance margin | 0.004 | **0.005** | the dangerous one |

The maintenance-margin error is not cosmetic. `_liquidation_price()` uses it,
and `_check_stop_vs_liquidation()` refuses trades whose stop sits too close to
liquidation. Understating maintenance margin places the *estimated* liquidation
further from entry than the real one, so the buffer check passes on trades that
in reality sit nearer the edge than the operator allowed. A safety check
calibrated with another exchange's number is not a safety check.

Testnet and mainnet also disagree (`maxOrderQty` 1190 vs 1500,
`maxMktOrderQty` 500 vs 150), so the values are fetched from whichever host
this build is pointed at.

## Fetched, cached, and never silently defaulted

`init()` is called once at startup. `get()` raises if it was not — there is no
lazy fetch and no default instance, because a default would be exactly the
class of wrong number this module exists to remove.

Fee rates are the one value that needs credentials (`/v5/account/fee-rate`
returns `retCode 10001` unauthenticated). Without keys the configured
`taker_fee_rate` is used **and a degradation is recorded** (§11), because
trading on an assumed fee schedule is worth knowing about: fee is the dominant
term in this system's economics.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx

from botmaximus.config import settings
from botmaximus.obs import degradation

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RiskTier:
    """One rung of Bybit's maintenance-margin ladder."""
    limit_value: float          # position notional ceiling for this tier
    maint_margin: float
    initial_margin: float
    max_leverage: float


@dataclass(frozen=True)
class VenueConstants:
    symbol: str
    category: str
    qty_step: float
    min_order_qty: float
    max_order_qty: float
    max_market_qty: float
    min_notional: float
    tick_size: float
    max_leverage: float
    tiers: tuple[RiskTier, ...]
    taker_fee_rate: float
    maker_fee_rate: float
    #: "bybit" when read from the venue; "config" when we had to fall back.
    fee_source: str = "bybit"
    host: str = ""

    def maint_margin_for(self, notional_usd: float) -> float:
        """Maintenance margin for a position of this size.

        A single "lowest tier" number is right only while positions stay small.
        Selecting the tier keeps the liquidation estimate honest if size ever
        grows, and costs nothing today.
        """
        for tier in self.tiers:
            if notional_usd <= tier.limit_value:
                return tier.maint_margin
        return self.tiers[-1].maint_margin if self.tiers else 0.005

    def round_qty(self, qty: float) -> float:
        """Floor to the venue's step. Always down: rounding a size *up* silently
        increases risk past what the risk core authorised."""
        if self.qty_step <= 0:
            return qty
        steps = int(qty / self.qty_step + 1e-9)
        return round(steps * self.qty_step, 8)

    def round_price(self, price: float) -> float:
        if self.tick_size <= 0:
            return price
        ticks = round(price / self.tick_size)
        return round(ticks * self.tick_size, 8)


_cached: VenueConstants | None = None


def rest_host() -> str:
    return (settings.bybit_testnet_rest_url if settings.bybit_testnet
            else settings.bybit_rest_url)


def _unwrap(payload: dict, endpoint: str) -> list:
    """V5 returns errors with HTTP 200. Unchecked, a rate limit or a bad symbol
    reads as an empty list, and an empty instrument list would leave the caller
    to invent constants — the exact failure this module removes."""
    if payload.get("retCode") != 0:
        raise RuntimeError(f"bybit {endpoint} retCode={payload.get('retCode')} "
                           f"retMsg={payload.get('retMsg')!r}")
    return (payload.get("result") or {}).get("list") or []


async def _fetch_fee_rates(client: httpx.AsyncClient, host: str) -> tuple[float, float, str]:
    """Account fee rates. Needs credentials; degrades visibly without them."""
    if settings.bybit_api_key is None or settings.bybit_api_secret is None:
        await degradation.record(
            "venue_fee_rate_unavailable",
            "no Bybit credentials — using the configured taker fee instead of "
            "the account's real schedule",
            configured_taker=settings.taker_fee_rate)
        return settings.taker_fee_rate, settings.taker_fee_rate, "config"

    # Signing is the client's job; importing it here would invert the
    # dependency. The executor refreshes these once it is constructed.
    try:
        from botmaximus.execution.bybit_client import BybitClient
        rates = await BybitClient().fetch_fee_rate()
        return rates["taker"], rates["maker"], "bybit"
    except Exception as e:                      # noqa: BLE001
        await degradation.record(
            "venue_fee_rate_fetch_failed",
            f"fee-rate lookup failed ({e}) — using the configured taker fee",
            configured_taker=settings.taker_fee_rate)
        return settings.taker_fee_rate, settings.taker_fee_rate, "config"


async def fetch(host: str | None = None) -> VenueConstants:
    host = host or rest_host()
    params = {"category": settings.bybit_category, "symbol": settings.symbol}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{host}/v5/market/instruments-info", params=params)
        r.raise_for_status()
        rows = _unwrap(r.json(), "instruments-info")
        if not rows:
            raise RuntimeError(
                f"bybit returned no instrument info for {settings.symbol} "
                f"{settings.bybit_category} — refusing to size orders against "
                f"assumed constants")
        info = rows[0]

        if info.get("contractType") != "LinearPerpetual":
            raise RuntimeError(
                f"{settings.symbol} is {info.get('contractType')!r}, not "
                f"LinearPerpetual. The risk core's sizing and liquidation "
                f"estimate assume a linear, USDT-settled contract.")

        r = await client.get(f"{host}/v5/market/risk-limit", params=params)
        r.raise_for_status()
        tier_rows = _unwrap(r.json(), "risk-limit")

        taker, maker, fee_source = await _fetch_fee_rates(client, host)

    lot = info["lotSizeFilter"]
    price = info["priceFilter"]
    lev = info.get("leverageFilter", {})
    tiers = tuple(sorted(
        (RiskTier(limit_value=float(t["riskLimitValue"]),
                  maint_margin=float(t["maintenanceMargin"]),
                  initial_margin=float(t["initialMargin"]),
                  max_leverage=float(t["maxLeverage"]))
         for t in tier_rows),
        key=lambda t: t.limit_value))

    if not tiers:
        raise RuntimeError(
            "bybit returned no risk-limit tiers — the liquidation estimate "
            "would fall back to an assumed maintenance margin, which is the "
            "defect this module closes")

    return VenueConstants(
        symbol=settings.symbol,
        category=settings.bybit_category,
        qty_step=float(lot["qtyStep"]),
        min_order_qty=float(lot["minOrderQty"]),
        max_order_qty=float(lot["maxOrderQty"]),
        max_market_qty=float(lot.get("maxMktOrderQty", lot["maxOrderQty"])),
        min_notional=float(lot.get("minNotionalValue", 5.0)),
        tick_size=float(price["tickSize"]),
        max_leverage=float(lev.get("maxLeverage", 100.0)),
        tiers=tiers,
        taker_fee_rate=taker,
        maker_fee_rate=maker,
        fee_source=fee_source,
        host=host,
    )


async def init(force: bool = False) -> VenueConstants:
    """Fetch once at startup. Idempotent unless forced."""
    global _cached
    if _cached is not None and not force:
        return _cached
    _cached = await fetch()
    log.info("venue constants (%s): qty_step=%s min_notional=%s tick=%s "
             "maint_margin(lowest)=%s taker=%s (%s)",
             _cached.host, _cached.qty_step, _cached.min_notional,
             _cached.tick_size, _cached.tiers[0].maint_margin,
             _cached.taker_fee_rate, _cached.fee_source)
    return _cached


def get() -> VenueConstants:
    """The cached constants.

    Raises rather than fetching lazily or returning a default. A default here
    would be an assumed number reaching the sizing path — which is the whole
    bug this module was written to remove.
    """
    if _cached is None:
        raise RuntimeError(
            "venue constants not initialised — call `await venue.init()` at "
            "startup. Refusing to size an order against assumed constants.")
    return _cached


def set_for_tests(vc: VenueConstants | None) -> None:
    global _cached
    _cached = vc
