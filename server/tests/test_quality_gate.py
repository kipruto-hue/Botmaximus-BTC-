from datetime import timedelta

from botmaximus.pipeline.envelope import Envelope, utcnow
from botmaximus.pipeline.quality.gate import QualityGate


def tick(price=64000.0, event_offset_s=0.0, **kw):
    now = utcnow()
    return Envelope(
        dataset_id="btc_price_tick", source="binance", symbol="BTCUSDT",
        event_time=now + timedelta(seconds=event_offset_s),
        collection_time=now,
        payload={"price": price, "qty": 0.01},
        **kw,
    )


def candle(o=64000, h=64100, l=63900, c=64050, vol=12.5, event_offset_s=0.0):
    now = utcnow()
    return Envelope(
        dataset_id="btc_ohlcv_1m", source="binance", symbol="BTCUSDT",
        event_time=now + timedelta(seconds=event_offset_s),
        collection_time=now,
        payload={"open": o, "high": h, "low": l, "close": c, "volume": vol,
                 "quote_volume": 1.0, "trades": 100},
    )


def test_good_tick_passes():
    env = QualityGate().check(tick())
    assert env.quality_ok
    assert not env.quarantine_reasons
    assert "single_source" in env.quality_flags  # layer 5 always flags for now


def test_lookahead_quarantined():
    env = QualityGate().check(tick(event_offset_s=30))
    assert "lookahead_violation" in env.quarantine_reasons
    assert not env.quality_ok


def test_nonpositive_price_quarantined():
    env = QualityGate().check(tick(price=0))
    assert "nonpositive_price" in env.quarantine_reasons


def test_ohlc_incoherent_quarantined():
    env = QualityGate().check(candle(h=63000))  # high below open/close
    assert "ohlc_incoherent" in env.quarantine_reasons


def test_phantom_jump_flagged_not_stored_as_ok():
    gate = QualityGate()
    gate.check(tick(price=64000))
    env = gate.check(tick(price=80000))  # +25% in one tick
    assert "phantom_suspect" in env.quality_flags
    assert not env.quality_ok
    assert not env.quarantine_reasons  # stored, but unusable live


def test_stale_record_flagged():
    env = QualityGate().check(tick(event_offset_s=-10))  # 10s old > 3s budget
    assert "stale" in env.quality_flags
    assert not env.quality_ok


def test_zero_volume_candle_flagged_illiquid():
    env = QualityGate().check(candle(vol=0))
    assert "illiquid_window" in env.quality_flags
    assert env.quality_ok  # informational only


def test_tick_gap_flagged_illiquid():
    gate = QualityGate()
    a = tick(price=64000)
    a.event_time -= timedelta(seconds=120)      # previous tick 2 minutes ago
    a.collection_time -= timedelta(seconds=120)
    gate.check(a)
    b = gate.check(tick(price=64010))
    assert "illiquid_window" in b.quality_flags


def test_quarantined_record_does_not_poison_prev_price():
    gate = QualityGate()
    gate.check(tick(price=64000))
    gate.check(tick(price=0))          # quarantined
    env = gate.check(tick(price=64010))
    assert env.quality_ok               # compared against 64000, not the bad record
