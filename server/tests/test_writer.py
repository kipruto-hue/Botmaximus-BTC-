from datetime import timedelta
from unittest.mock import patch

import pytest

from botmaximus.pipeline.envelope import Envelope, utcnow
from botmaximus.pipeline.writer import Writer


class FakeCollection:
    def __init__(self) -> None:
        self.docs = []

    async def insert_one(self, doc):
        self.docs.append(doc)

    async def find_one(self, query=None, *a, **kw):
        if query and "event_time" in query:
            for d in self.docs:
                if d["event_time"] == query["event_time"]:
                    return d
        return None


class FakeDB(dict):
    def __missing__(self, key):
        self[key] = FakeCollection()
        return self[key]


def candle(event_offset_s=0.0):
    now = utcnow()
    return Envelope(
        dataset_id="btc_ohlcv_1m", source="binance", symbol="BTCUSDT",
        event_time=now + timedelta(seconds=event_offset_s),
        collection_time=now,
        ingest_time=now,  # stamped by bus.py's store worker in production
        payload={"open": 64000, "high": 64100, "low": 63900, "close": 64050,
                 "volume": 12.5, "quote_volume": 1.0, "trades": 100},
    )


@pytest.mark.asyncio
async def test_store_latency_stamped_in_doc():
    db = FakeDB()
    with patch("botmaximus.pipeline.writer.get_db", return_value=db):
        w = Writer()
        await w.write(candle())
        await w.write(candle(event_offset_s=60))

    docs = db["btc_ohlcv_1m"].docs
    assert len(docs) == 2
    # First write has no prior measurement; second carries the first's duration.
    assert docs[0]["stage_latency_ms"]["store"] == 0.0
    assert docs[1]["stage_latency_ms"]["store"] >= 0.0
    for doc in docs:
        assert set(doc["stage_latency_ms"]) >= {"store"}


@pytest.mark.asyncio
async def test_duplicate_event_time_not_rewritten():
    db = FakeDB()
    with patch("botmaximus.pipeline.writer.get_db", return_value=db):
        w = Writer()
        env = candle()
        await w.write(env)
        replay = candle()
        replay.event_time = env.event_time
        await w.write(replay)

    assert len(db["btc_ohlcv_1m"].docs) == 1


@pytest.mark.asyncio
async def test_backfill_fills_hole_behind_newest_without_duplicating():
    db = FakeDB()
    with patch("botmaximus.pipeline.writer.get_db", return_value=db):
        w = Writer()
        newest = candle()
        await w.write(newest)

        gap = candle(event_offset_s=-300)   # older than newest → monotonic check would drop it
        gap.backfill = True
        await w.write(gap)
        assert len(db["btc_ohlcv_1m"].docs) == 2

        replay = candle(event_offset_s=-300)
        replay.event_time = gap.event_time
        replay.backfill = True
        await w.write(replay)               # same minute again → deduped via existence check
        assert len(db["btc_ohlcv_1m"].docs) == 2

        # live high-water mark must not regress to the backfilled time
        assert w._last_written["btc_ohlcv_1m"] == newest.event_time


@pytest.mark.asyncio
async def test_quarantined_record_routed_to_quarantine():
    db = FakeDB()
    with patch("botmaximus.pipeline.writer.get_db", return_value=db):
        w = Writer()
        env = candle()
        env.quarantine_reasons = ["lookahead_violation"]
        await w.write(env)

    assert len(db["quarantine"].docs) == 1
    assert db["btc_ohlcv_1m"].docs == []
