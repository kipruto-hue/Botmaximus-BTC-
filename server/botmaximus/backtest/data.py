"""Point-in-time data access (§5.4). At bar *t*, a strategy may see only data
whose event_time ≤ the close of bar t — the view makes lookahead structurally
impossible, it isn't a convention the strategy is trusted to honour.

Loading consults the coverage ledger first (§2.4): a window with holes in a
required feed is refused, or flagged if the caller allows it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
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
    def i(self) -> int:
        """Index of the just-closed bar. Compiled DSL strategies read their
        precomputed feature arrays at this position — it exposes *where* we are,
        never anything at i+1."""
        return self._i

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


async def load_ohlcv(start: datetime, end: datetime,
                     as_of: datetime | None = None,
                     venue: str | None = None) -> list[Bar]:
    out = []
    for d in await _load_range("btc_ohlcv_1m", start, end, as_of, venue):
        p = d["payload"]
        out.append(Bar(
            open_time=_as_dt(p["open_time"]), close_time=d["event_time"],
            open=p["open"], high=p["high"], low=p["low"], close=p["close"],
            volume=p["volume"],
        ))
    return out


def _as_dt(v):
    """Payloads round-trip through JSON, so a datetime comes back as a string.

    Under Mongo a BSON date survived the round trip and this was free. Silently
    returning a str here would put a string where a Bar's `open_time` is
    expected and only fail much later, somewhere unrelated.
    """
    if isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v))


@dataclass(frozen=True)
class OIPoint:
    time: datetime
    open_interest: float


@dataclass(frozen=True)
class BookPoint:
    time: datetime
    imbalance: float
    spread: float


@dataclass(frozen=True)
class LiqPoint:
    time: datetime
    price: float
    notional_usd: float
    side: str                   # SELL = a long was liquidated, BUY = a short


@dataclass
class MarketWindow:
    """Every feed a strategy may reference over one evaluation window, loaded
    once. The feature layer aligns the non-OHLCV series onto the 1m bar grid;
    only feeds a strategy actually declares are populated."""
    bars: list[Bar]
    funding: list[FundingPoint] = field(default_factory=list)
    oi: list[OIPoint] = field(default_factory=list)
    book: list[BookPoint] = field(default_factory=list)
    liquidations: list[LiqPoint] = field(default_factory=list)


#: DSL `required_feeds` name → the coverage-ledger dataset it is governed by.
FEED_DATASETS: dict[str, str] = {
    "ohlcv": "btc_ohlcv_1m",
    "funding": "btc_funding_8h",
    "oi": "btc_oi_5m",
    "orderbook": "btc_orderbook",
    "liquidations": "btc_liquidation",
    "ticks": "btc_price_tick",
}


async def load_funding(start: datetime, end: datetime,
                       as_of: datetime | None = None,
                       venue: str | None = None) -> list[FundingPoint]:
    return [FundingPoint(time=d["event_time"], rate=d["payload"]["funding_rate"])
            for d in await _load_range("btc_funding_8h", start, end,
                                       as_of, venue)]


async def _load_range(dataset_id: str, start: datetime, end: datetime,
                      as_of: datetime | None = None,
                      venue: str | None = None) -> list[dict]:
    """Every historical read in the backtester goes through here.

    `venue` defaults to the configured one rather than being optional in the
    dangerous sense: `read_as_of` will not accept an unknown venue, so a window
    can never silently span both. `as_of=None` means current truth, which is
    correct for live consumers; the backtest runner supplies a pinned instant
    (§6) so a re-run months later reads the same rows.
    """
    from botmaximus.config import settings
    from botmaximus.storage import records as store
    return await store.read_as_of(dataset_id, start, end,
                                  venue=venue or settings.venue, as_of=as_of)


async def load_oi(start: datetime, end: datetime,
                  as_of: datetime | None = None,
                  venue: str | None = None) -> list[OIPoint]:
    return [OIPoint(time=d["event_time"], open_interest=d["payload"]["open_interest"])
            for d in await _load_range("btc_oi_5m", start, end, as_of, venue)]


async def load_orderbook(start: datetime, end: datetime,
                         as_of: datetime | None = None,
                         venue: str | None = None) -> list[BookPoint]:
    """Summary fields only — the 20 depth levels are heavy and no registry
    feature reads them."""
    return [BookPoint(time=d["event_time"], imbalance=d["payload"]["imbalance"],
                      spread=d["payload"]["spread"])
            for d in await _load_range("btc_orderbook", start, end, as_of, venue)]


async def load_liquidations(start: datetime, end: datetime,
                            as_of: datetime | None = None,
                            venue: str | None = None) -> list[LiqPoint]:
    return [LiqPoint(time=d["event_time"], price=d["payload"]["price"],
                     notional_usd=d["payload"]["notional_usd"], side=d["payload"]["side"])
            for d in await _load_range("btc_liquidation", start, end, as_of, venue)]


async def load_window(feeds, start: datetime, end: datetime,
                      as_of: datetime | None = None,
                      venue: str | None = None) -> MarketWindow:
    """Load the feeds a strategy declared, plus the two the engine needs no
    matter what it declared: `ohlcv` (the engine walks 1m bars) and `funding`
    (the cost model charges every settlement held, whether or not the strategy
    reads funding as a feature). Declaring a feed still governs the coverage
    assertion and which features are available.

    `as_of` is threaded through every feed rather than applied to some of them:
    a window whose bars are pinned to April but whose funding is current would
    be a world that never existed, and the cost model charges from funding.
    """
    feeds = set(feeds)
    unknown = feeds - set(FEED_DATASETS)
    if unknown:
        raise ValueError(f"unknown feed(s) {sorted(unknown)}")
    return MarketWindow(
        bars=await load_ohlcv(start, end, as_of, venue),
        funding=await load_funding(start, end, as_of, venue),
        oi=await load_oi(start, end, as_of, venue) if "oi" in feeds else [],
        book=await load_orderbook(start, end, as_of, venue) if "orderbook" in feeds else [],
        liquidations=await load_liquidations(start, end, as_of, venue) if "liquidations" in feeds else [],
    )
