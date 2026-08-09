r"""Dataset registry and hot-window policy (Storage v2.0 §3.A).

What this file used to do — create Mongo time-series collections with TTL
indexes — has no Postgres equivalent, and the replacement is not a translation
but a different mechanism:

- **Time-series collections** → daily RANGE partitions of `market_records`,
  managed by `storage/partitions.py`.
- **TTL indexes** → the nightly tier-out job (§4). A TTL index deletes data
  once it is old enough; tier-out only drops a Postgres partition *after*
  verifying the Parquet copy exists and its checksum matches. The difference
  matters: a TTL index would happily expire the only copy of a day the archive
  never received, and it would do it silently.
- **A quarantine collection** → a separate Object Storage bucket that no hot
  path reads (§3.J).

So what remains here is the registry: which datasets exist, and how long each
one's hot window is. `ensure_schema()` is kept as the boot-time entry point so
callers do not need to know that the mechanism underneath changed.
"""
from __future__ import annotations

import logging

from botmaximus.config import settings
from botmaximus.storage import partitions, postgres

log = logging.getLogger(__name__)

#: Every dataset the collectors produce. The value is the hot-window length in
#: hours (§3.A); everything older lives only in Parquet.
#:
#: These are per-dataset on purpose. Ticks age out in a day because nothing
#: reads a week-old tick from Postgres, while funding is kept for 30 days
#: because the cost model charges from settled funding across every settlement
#: a position spans and reaches back further than the rest.
DATASETS: dict[str, str] = {
    "btc_price_tick": "hot_window_hours_ticks",
    "btc_ohlcv_1m": "hot_window_hours_ohlcv",
    "btc_funding": "hot_window_hours_funding",
    "btc_open_interest": "hot_window_hours_open_interest",
    "btc_liquidation": "hot_window_hours_liquidations",
    "btc_orderbook": "hot_window_hours_orderbook",
    # settled historical series used by the cost model and coverage
    "btc_funding_8h": "hot_window_hours_funding",
    "btc_oi_5m": "hot_window_hours_open_interest",
}

#: Feeds with no history endpoint on any venue. An hour of downtime is an hour
#: gone permanently, which is why uptime — not backfill — is what protects them.
UNRECOVERABLE = ("btc_liquidation", "btc_orderbook", "btc_price_tick")


def hot_window_hours(dataset_id: str) -> int:
    attr = DATASETS.get(dataset_id)
    if attr is None:
        return settings.hot_window_hours_default
    return getattr(settings, attr, settings.hot_window_hours_default)


async def ensure_schema() -> None:
    """Idempotent boot-time storage setup: apply the DDL, then make sure the
    partitions the collector is about to write into already exist."""
    await postgres.bootstrap()
    made = await partitions.ensure_ahead(days=3)
    log.info("storage ready: %d dataset(s), partitions through %s",
             len(DATASETS), made[-1] if made else "n/a")
