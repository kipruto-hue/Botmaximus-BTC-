"""Feature layer tests (Strategy DSL §4.2, §9).

The load-bearing test in this file is `test_no_lookahead_*`: rewrite every bar
after index i and assert nothing at or before i moved. Indicator maths can be
checked by inspection; a lookahead leak cannot, because it produces plausible
numbers that are simply too good.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.backtest.data import (
    Bar, BookPoint, FundingPoint, LiqPoint, MarketWindow, OIPoint,
)
from botmaximus.backtest.regimes import REGIME_BUCKETS, build_regime_buckets, build_vol_axis
from botmaximus.features import kernels
from botmaximus.features.compute import FeatureContext, FeatureRef, as_of, project
from botmaximus.features.frames import TIMEFRAME_MINUTES, bucket_start, build_frame
from botmaximus.features.registry import FEATURE_REGISTRY, FEED_HISTORY_DAYS

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def mkbars(n: int, fn=None) -> list[Bar]:
    fn = fn or (lambda i: 100 + math.sin(i / 30) * 6 + i * 0.002)
    out = []
    for i in range(n):
        s = T0 + timedelta(minutes=i)
        c = fn(i)
        out.append(Bar(s, s + timedelta(minutes=1, milliseconds=-1),
                       c, c + 0.4, c - 0.4, c, 10.0 + (i % 7)))
    return out


# ---------------------------------------------------------------- frames --
def test_bucket_start_anchors_on_utc_midnight():
    t = datetime(2025, 3, 7, 13, 47, tzinfo=timezone.utc)
    assert bucket_start(t, 60) == datetime(2025, 3, 7, 13, 0, tzinfo=timezone.utc)
    assert bucket_start(t, 1440) == datetime(2025, 3, 7, 0, 0, tzinfo=timezone.utc)
    assert bucket_start(t, 5) == datetime(2025, 3, 7, 13, 45, tzinfo=timezone.utc)


@pytest.mark.parametrize("tf,minutes", sorted(TIMEFRAME_MINUTES.items()))
def test_frame_bar_becomes_available_exactly_at_its_close(tf, minutes):
    bars = mkbars(minutes * 3)
    f = build_frame(bars, tf)
    # the first frame bar covers 1m indices [0, minutes-1] and closes with it
    assert f.avail[minutes - 2] == 0 or minutes == 1
    assert f.avail[minutes - 1] == 1


def test_frame_aggregation_is_ohlc_correct():
    bars = mkbars(20)
    f = build_frame(bars, "5m")
    first = f.bars[0]
    src = bars[:5]
    assert first.open == src[0].open
    assert first.close == src[-1].close
    assert first.high == max(b.high for b in src)
    assert first.low == min(b.low for b in src)
    assert first.volume == pytest.approx(sum(b.volume for b in src))


def test_missing_minute_does_not_close_a_frame_bar_early():
    """Availability comes from the bucket's nominal close, not from whichever
    1m bar happens to be last — otherwise a gap makes a frame bar look done."""
    bars = mkbars(10)
    del bars[4]                      # drop the minute that ends the first 5m bucket
    f = build_frame(bars, "5m")
    # index 3 is now minute 3; the 5m bar must still not be available
    assert f.avail[3] == 0
    assert f.bars[0].close_time == T0 + timedelta(minutes=5, milliseconds=-1)


def test_project_maps_frame_values_to_1m_indices():
    bars = mkbars(15)
    f = build_frame(bars, "5m")
    vals = [10.0, 20.0, 30.0]
    out = project(f, vals)
    assert out[3] is None            # no 5m bar closed yet
    assert out[4] == 10.0
    assert out[9] == 20.0
    assert out[14] == 30.0


# ------------------------------------------------------------- causality --
CAUSALITY_REFS = [
    FeatureRef("ema", "1h", (("n", 5),)),
    FeatureRef("sma", "5m", (("n", 10),)),
    FeatureRef("rsi", "5m", (("n", 14),)),
    FeatureRef("atr", "1m", (("n", 14),)),
    FeatureRef("atr_pct", "15m", (("n", 14),)),
    FeatureRef("adx", "1h", (("n", 5),)),
    FeatureRef("bb_position", "5m", (("n", 20),)),
    FeatureRef("realized_vol", "5m", (("n", 20),)),
    FeatureRef("ret_pct", "15m", (("n", 4),)),
    FeatureRef("vwap_dist", "1m", ()),
    FeatureRef("close", "1h", ()),
]


@pytest.mark.parametrize("ref", CAUSALITY_REFS, ids=lambda r: r.key)
def test_no_lookahead_feature_values(ref):
    """Rewriting every bar after the split must not change any value at or
    before it. This is the structural lookahead test."""
    split = 2000
    bars = mkbars(3000)
    before = FeatureContext(MarketWindow(bars=bars),
                            ("1m", "5m", "15m", "1h")).series(ref)[:split]

    tampered = mkbars(3000)
    for i in range(split, 3000):
        b = tampered[i]
        tampered[i] = Bar(b.open_time, b.close_time, 9e5, 9e5, 9e5, 9e5, 1e6)
    after = FeatureContext(MarketWindow(bars=tampered),
                           ("1m", "5m", "15m", "1h")).series(ref)[:split]
    assert before == after


def test_no_lookahead_in_regime_labels():
    """The vol axis compares realised vol to a *trailing* reference. A
    full-sample percentile — the obvious implementation — would fail this:
    a violent future would retroactively relabel a calm past as low_vol."""
    split = 2000
    bars = mkbars(3000)
    before = build_vol_axis(bars)[:split]

    tampered = mkbars(3000)                     # identical history …
    for i in range(split, 3000):                # … then a violent future
        b = tampered[i]
        shock = 100.0 + (i - split) * 50
        tampered[i] = Bar(b.open_time, b.close_time, shock, shock * 1.1,
                          shock * 0.9, shock, b.volume)
    after = build_vol_axis(tampered)[:split]
    assert before == after


# ------------------------------------------------------------- as-of join --
def test_as_of_is_backward_and_strict():
    bars = mkbars(10)
    # a record stamped exactly at T0+5m is NOT knowable at the bar closing
    # T0+5m-1ms; it appears on the next bar
    out = as_of(bars, [T0 + timedelta(minutes=5)], [42.0])
    assert out[4] is None
    assert out[5] == 42.0


def test_as_of_is_none_before_the_first_record():
    bars = mkbars(10)
    out = as_of(bars, [T0 + timedelta(minutes=8)], [1.0])
    assert all(v is None for v in out[:8])
    assert out[8] == 1.0


def test_funding_features_join_backwards():
    bars = mkbars(400)
    fund = [FundingPoint(T0 + timedelta(minutes=100), 0.0001),
            FundingPoint(T0 + timedelta(minutes=300), -0.0002)]
    ctx = FeatureContext(MarketWindow(bars=bars, funding=fund))
    fr = ctx.series(FeatureRef("funding_rate"))
    assert fr[99] is None and fr[100] == 0.0001
    assert fr[299] == 0.0001 and fr[300] == -0.0002


def test_oi_change_is_percent_over_n_periods():
    bars = mkbars(100)
    oi = [OIPoint(T0 + timedelta(minutes=m), 1000.0 + m) for m in range(0, 100, 5)]
    ctx = FeatureContext(MarketWindow(bars=bars, oi=oi))
    v = ctx.series(FeatureRef("oi_change", None, (("n", 2),)))[60]
    assert v == pytest.approx((1060 - 1050) / 1050 * 100, rel=1e-6)


def test_book_imbalance_uses_last_snapshot():
    bars = mkbars(20)
    book = [BookPoint(T0 + timedelta(minutes=5), 0.3, 1.0),
            BookPoint(T0 + timedelta(minutes=10), -0.4, 1.0)]
    ctx = FeatureContext(MarketWindow(bars=bars, book=book))
    v = ctx.series(FeatureRef("book_imbalance"))
    assert v[4] is None and v[5] == 0.3 and v[9] == 0.3 and v[10] == -0.4


def test_liq_clusters_split_by_price_and_expire():
    bars = mkbars(300, lambda i: 100.0)
    liq = [LiqPoint(T0 + timedelta(minutes=50), 150.0, 5e6, "SELL"),
           LiqPoint(T0 + timedelta(minutes=51), 50.0, 2e6, "BUY")]
    ctx = FeatureContext(MarketWindow(bars=bars, liquidations=liq))
    above = ctx.series(FeatureRef("liq_cluster_above", None, (("n", 60),)))
    below = ctx.series(FeatureRef("liq_cluster_below", None, (("n", 60),)))
    assert above[49] == 0.0 and above[50] == 5e6
    assert below[51] == 2e6
    assert above[200] == 0.0, "the window must expire"


def test_liq_cluster_is_none_when_feed_absent_but_zero_when_quiet():
    """Silence and absence are different readings and must not look alike."""
    bars = mkbars(50)
    absent = FeatureContext(MarketWindow(bars=bars)).series(
        FeatureRef("liq_cluster_above", None, (("n", 60),)))
    assert all(v is None for v in absent)

    quiet = FeatureContext(MarketWindow(
        bars=bars,
        liquidations=[LiqPoint(T0 + timedelta(minutes=1), 1e9, 1.0, "SELL")],
    )).series(FeatureRef("liq_cluster_below", None, (("n", 5),)))
    assert quiet[40] == 0.0


# ---------------------------------------------------------------- kernels --
def test_ema_matches_manual_recursion():
    vals = [float(i) for i in range(1, 21)]
    out = kernels.ema(vals, 5)
    assert out[3] is None
    expected = sum(vals[:5]) / 5
    assert out[4] == pytest.approx(expected)
    k = 2 / 6
    for i in range(5, 20):
        expected = vals[i] * k + expected * (1 - k)
    assert out[19] == pytest.approx(expected)


def test_rsi_bounds_and_all_gains():
    rising = [float(i) for i in range(1, 40)]
    out = kernels.rsi(rising, 14)
    assert out[20] == 100.0
    mixed = kernels.rsi([100 + math.sin(i) * 5 for i in range(80)], 14)
    assert all(0 <= v <= 100 for v in mixed if v is not None)


def test_zscore_is_none_when_variance_is_zero():
    out = kernels.zscore([5.0] * 30, 10)
    assert all(v is None for v in out), "constant series has no defined z-score"


def test_bb_position_bounds():
    closes = [100 + math.sin(i / 5) * 3 for i in range(200)]
    out = kernels.bb_position(closes, 20)
    defined = [v for v in out if v is not None]
    assert defined and all(-1.0 < v < 2.0 for v in defined)


def test_session_vwap_resets_each_day():
    days = [0] * 5 + [1] * 5
    typical = [100.0] * 5 + [200.0] * 5
    vol = [1.0] * 10
    out = kernels.session_vwap_dist(days, typical, vol)
    assert out[5] == pytest.approx(0.0), "new session starts from its own first bar"


def test_atr_is_positive_and_warms_up():
    bars = mkbars(100)
    out = kernels.atr([b.high for b in bars], [b.low for b in bars],
                      [b.close for b in bars], 14)
    assert out[12] is None and out[13] is not None
    assert all(v > 0 for v in out if v is not None)


# ---------------------------------------------------------------- regimes --
def test_regime_buckets_are_the_six_declared_labels():
    bars = mkbars(4000)
    labels = build_regime_buckets(bars)
    assert set(labels.values()) <= set(REGIME_BUCKETS)
    assert len(REGIME_BUCKETS) == 6


def test_vol_axis_has_no_warmup_hole():
    bars = mkbars(500)
    axis = build_vol_axis(bars)
    assert len(axis) == len(bars)
    assert all(v in ("low_vol", "high_vol") for v in axis)


# --------------------------------------------------------------- registry --
def test_every_registry_feature_declares_a_known_feed():
    for name, spec in FEATURE_REGISTRY.items():
        assert spec.feed in FEED_HISTORY_DAYS, name
        assert spec.kind in ("numeric", "categorical"), name


def test_forward_only_feeds_are_marked_as_such():
    """Order book and liquidations have no venue history endpoint. If this ever
    reads otherwise, a strategy could be 'validated' over data we never had."""
    assert FEED_HISTORY_DAYS["orderbook"] == 0
    assert FEED_HISTORY_DAYS["liquidations"] == 0
    assert FEED_HISTORY_DAYS["oi"] == 30
