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


def test_to_record_carries_the_envelope_across():
    """`to_doc` (a Mongo document) is gone; `to_record` is the real write path.

    The two models disagree about one field, and this is where that is
    reconciled: the pipeline uses `quality_flags` for advisory marks on records
    that PASSED, while Storage v2.0 §2 defines them as the names of checks a
    record FAILED. Advisory marks become `annotations`.
    """
    env = make_env()
    rec = env.to_record()
    assert rec.dataset_id == "btc_price_tick"
    assert rec.source == "binance"
    assert rec.event_time == env.event_time
    assert rec.quality_ok is True
    assert rec.quality_flags == ()


def test_a_failed_envelope_carries_its_reasons_as_flags():
    env = make_env()
    env.quarantine_reasons = ["lookahead_violation"]
    rec = env.to_record()
    assert rec.quality_ok is False
    assert "lookahead_violation" in rec.quality_flags
    assert rec.annotations == ()


def test_advisory_flags_survive_as_annotations():
    """The gate marks every record `single_source`; folding that into
    quality_flags would quarantine the entire feed."""
    env = make_env()
    env.quality_flags = ["single_source"]
    rec = env.to_record()
    assert rec.quality_ok is True
    assert rec.quality_flags == ()
    assert rec.annotations == ("single_source",)


def test_freshness_ms():
    env = make_env()
    assert env.freshness_ms is None
    env.ingest_time = env.event_time
    assert env.freshness_ms == 0
