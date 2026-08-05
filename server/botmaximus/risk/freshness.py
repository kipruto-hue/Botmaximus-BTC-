r"""Feed freshness resolved from a strategy's declared `required_feeds`.

Closes audit B6. Constitution §8: *a trade dies if any feed that strategy needs
is stale by its budget.*

The previous rule checked a fixed pair — `btc_price_tick` and `btc_ohlcv_1m` —
for every strategy. That is simultaneously too strict and far too loose:

- **too strict** for a strategy that reads only closed 1m candles, which is
  blocked by a hiccup in a tick feed it never consults;
- **too loose**, and this is the one that costs money, for a strategy whose
  edge *is* funding or open interest. `seed_funding_extreme_contrarian` would
  have traded happily on a funding rate hours out of date, because the two
  feeds being checked were both perfectly fresh. The check would report
  healthy while the strategy acted on a stale premise.

One function answers the question for every caller — the risk core's pre-trade
check, the scrutiny gate's deterministic pre-check, and the executor's
pre-flight. Three copies of a freshness rule become three subtly different
rules; this is the same reasoning as `ops/healthcheck.py` being the single
definition of "unhealthy" for both supervisors.

## Event-driven feeds are never stale

Liquidations legitimately go quiet for hours. `BUDGETS_MS` carries `None` for
them, and a `None` budget means silence is not absence. Inventing a budget
would make quiet markets untradeable.
"""
from __future__ import annotations

from botmaximus.pipeline.telemetry import BUDGETS_MS, telemetry

#: DSL `required_feeds` name -> the dataset id telemetry tracks freshness for.
#: Mirrors `backtest/data.FEED_DATASETS`; kept here so the risk layer does not
#: import the backtest layer to answer a live-trading question.
FEED_DATASETS: dict[str, str] = {
    "ohlcv": "btc_ohlcv_1m",
    "funding": "btc_funding",
    "oi": "btc_open_interest",
    "orderbook": "btc_orderbook",
    "liquidations": "btc_liquidation",
    "ticks": "btc_price_tick",
}

#: Used only when a caller declares nothing. Matches the historical behaviour,
#: but reaching it is recorded as a degradation rather than assumed correct.
BASELINE_FEEDS: tuple[str, ...] = ("btc_price_tick", "btc_ohlcv_1m")


def resolve(required_feeds: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """DSL feed names -> dataset ids. Unknown names pass through unchanged so a
    caller may hand over dataset ids directly."""
    return tuple(FEED_DATASETS.get(f, f) for f in required_feeds)


def stale_feeds(required_feeds: tuple[str, ...] | list[str]) -> list[str]:
    """Dataset ids that are past their staleness budget.

    A feed with no recorded event at all counts as stale: "we have never seen
    this feed" is not evidence that it is current.
    """
    out: list[str] = []
    for ds in resolve(required_feeds):
        budget = BUDGETS_MS.get(ds)
        if budget is None:
            continue                    # event-driven; silence is not absence
        fresh = telemetry.freshness_ms(ds)
        if fresh is None or fresh > budget:
            out.append(ds)
    return out


def assert_fresh(required_feeds: tuple[str, ...] | list[str]) -> list[str]:
    """Reason strings for a rejection, in the risk core's `feed_stale:<ds>`
    format. Empty means everything the strategy needs is current."""
    return [f"feed_stale:{ds}" for ds in stale_feeds(required_feeds)]
