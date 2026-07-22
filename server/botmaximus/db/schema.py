"""Collection setup (§3.2) — idempotent, run at startup.

High-frequency series live in Mongo time-series collections (timeField
`event_time`, metaField `meta`). Time-series collections are insert-only,
so dedupe happens in the writer, not via unique indexes.
"""
import logging

from botmaximus.config import settings
from botmaximus.db.mongo import get_db

log = logging.getLogger(__name__)

# dataset_id → collection name
DATASET_COLLECTIONS = {
    "btc_price_tick": "btc_price_ticks",
    "btc_ohlcv_1m": "btc_ohlcv_1m",
    "btc_funding": "btc_funding",
    "btc_open_interest": "btc_open_interest",
    "btc_liquidation": "btc_liquidations",
    "btc_orderbook": "btc_orderbook",
}

TIMESERIES = {
    "btc_price_ticks": {"granularity": "seconds", "ttl_s": settings.ttl_price_ticks_s},
    "btc_ohlcv_1m": {"granularity": "minutes", "ttl_s": settings.ttl_ohlcv_1m_s},
    "btc_funding": {"granularity": "seconds", "ttl_s": settings.ttl_funding_s},
    "btc_open_interest": {"granularity": "seconds", "ttl_s": settings.ttl_open_interest_s},
    "btc_liquidations": {"granularity": "seconds", "ttl_s": settings.ttl_liquidations_s},
    "btc_orderbook": {"granularity": "seconds", "ttl_s": settings.ttl_orderbook_s},
}

QUARANTINE = "quarantine"


async def ensure_schema() -> None:
    db = get_db()
    existing = set(await db.list_collection_names())

    for name, opts in TIMESERIES.items():
        if name not in existing:
            await db.create_collection(
                name,
                timeseries={
                    "timeField": "event_time",
                    "metaField": "meta",
                    "granularity": opts["granularity"],
                },
                expireAfterSeconds=opts["ttl_s"],
            )
            log.info("created time-series collection %s", name)
        coll = db[name]
        await coll.create_index([("meta.dataset_id", 1), ("event_time", 1)])

    if QUARANTINE not in existing:
        await db.create_collection(QUARANTINE)
        log.info("created collection %s", QUARANTINE)
    await db[QUARANTINE].create_index([("event_time", 1)])
    await db[QUARANTINE].create_index([("meta.dataset_id", 1), ("event_time", 1)])
