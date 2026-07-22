from datetime import datetime, timezone

import pytest

from botmaximus.pipeline.envelope import Envelope, from_epoch_ms, utcnow


def make_env(**kw):
    now = utcnow()
    defaults = dict(
        dataset_id="btc_price_tick", source="binance", symbol="BTCUSDT",
        event_time=now, collection_time=now, payload={"price": 64000.0},
    )
    defaults.update(kw)
    return Envelope(**defaults)


def test_naive_datetime_rejected():
    with pytest.raises(ValueError):
        make_env(event_time=datetime(2026, 1, 1))


def test_from_epoch_ms_is_utc():
    t = from_epoch_ms(1_700_000_000_000)
    assert t.tzinfo == timezone.utc


def test_to_doc_shape():
    env = make_env()
    doc = env.to_doc()
    assert doc["meta"] == {"dataset_id": "btc_price_tick", "source": "binance", "symbol": "BTCUSDT"}
    assert doc["event_time"] == env.event_time
    assert doc["quality_ok"] is True
    assert doc["reaction_ref"] is None
    assert set(doc) == {
        "event_time", "meta", "collection_time", "ingest_time", "payload",
        "quality_flags", "quality_ok", "reaction_ref", "stage_latency_ms", "backfill",
    }


def test_freshness_ms():
    env = make_env()
    assert env.freshness_ms is None
    env.ingest_time = env.event_time
    assert env.freshness_ms == 0
