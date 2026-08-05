"""Audit B5 (venue constants) and B6 (freshness by required_feeds).

Both were cases of a number or a rule that looked right and was wrong for the
venue actually being traded. The tests below are written around the *behaviour
that was broken*, not around the new API surface.
"""
from __future__ import annotations

import pytest

from botmaximus.execution import venue
from botmaximus.obs import degradation
from botmaximus.pipeline.telemetry import telemetry
from botmaximus.risk import freshness
from botmaximus.risk.core import RiskCore
from botmaximus.risk.state import OrderIntent
from tests.conftest import BYBIT_BTCUSDT


# =====================================================================
# B5 -- constants come from the venue
# =====================================================================
def test_uninitialised_constants_raise_rather_than_defaulting():
    """A default here would be an assumed number reaching the sizing path,
    which is the entire bug this module removes."""
    venue.set_for_tests(None)
    with pytest.raises(RuntimeError, match="not initialised"):
        venue.get()
    venue.set_for_tests(BYBIT_BTCUSDT)


def test_the_two_binance_hardcodes_were_wrong_for_bybit():
    """Documents the defect so a future 'simplification' back to constants has
    to argue with a failing test."""
    vc = venue.get()
    assert vc.min_notional == 5.0          # the hardcode said 100.0 -- 20x out
    assert vc.tiers[0].maint_margin == 0.005   # the hardcode said 0.004


def test_maintenance_margin_is_selected_by_position_size():
    vc = venue.get()
    assert vc.maint_margin_for(1_000_000) == 0.005
    assert vc.maint_margin_for(2_500_000) == 0.0056
    # Beyond the last known tier, use the most conservative rate rather than
    # extrapolating -- an invented tier is an invented liquidation price.
    assert vc.maint_margin_for(9_000_000) == 0.0056


def test_understating_maintenance_margin_moves_liquidation_away_from_entry():
    """Why the wrong number was dangerous rather than merely inaccurate: a low
    maintenance margin pushes the ESTIMATED liquidation further from entry, so
    the stop-vs-liquidation buffer approves trades sitting nearer the edge than
    the operator authorised."""
    entry = 64_000.0
    old = entry * (1 - 1 / 3 + 0.004)      # Binance hardcode
    new = entry * (1 - 1 / 3 + 0.005)      # Bybit actual
    assert new > old                       # real liquidation is CLOSER to entry
    assert RiskCore._liquidation_price("LONG", entry, 100_000.0) == pytest.approx(new)


def test_quantity_is_always_floored_never_rounded_up():
    """Rounding a size up silently increases risk past what the risk core
    authorised."""
    vc = venue.get()
    assert vc.round_qty(0.0019) == pytest.approx(0.001)
    assert vc.round_qty(0.0010) == pytest.approx(0.001)
    assert vc.round_qty(0.0009) == pytest.approx(0.0)


def test_price_rounds_to_the_venue_tick():
    assert venue.get().round_price(64_000.07) == pytest.approx(64_000.1)


def test_a_nonzero_retcode_is_an_error_not_an_empty_instrument_list():
    """V5 returns errors with HTTP 200. Unchecked, a rate limit reads as 'no
    instruments', and the caller would be left inventing constants."""
    with pytest.raises(RuntimeError, match="10006"):
        venue._unwrap({"retCode": 10006, "retMsg": "rate limit",
                       "result": {}}, "instruments-info")


# =====================================================================
# B6 -- freshness follows the strategy's declared feeds
# =====================================================================
def _fresh(dataset_id: str, age_ms: float = 0.0):
    from datetime import datetime, timedelta, timezone
    telemetry._last_event[dataset_id] = (
        datetime.now(timezone.utc) - timedelta(milliseconds=age_ms))


def test_dsl_feed_names_resolve_to_dataset_ids():
    assert freshness.resolve(("ohlcv", "funding")) == ("btc_ohlcv_1m", "btc_funding")
    # dataset ids pass through, so callers may use either vocabulary
    assert freshness.resolve(("btc_price_tick",)) == ("btc_price_tick",)


def test_a_funding_strategy_is_blocked_by_stale_funding():
    """The defect in one sentence: the old check looked at price ticks and 1m
    candles for every strategy, so a funding-driven strategy would trade on a
    funding rate hours out of date while the check reported healthy."""
    _fresh("btc_price_tick")
    _fresh("btc_ohlcv_1m")
    _fresh("btc_funding", age_ms=10 * 60 * 1000)      # ten minutes stale

    assert freshness.assert_fresh(("ticks", "ohlcv")) == []          # old rule: fine
    assert freshness.assert_fresh(("funding",)) == ["feed_stale:btc_funding"]


def test_a_candle_only_strategy_is_not_blocked_by_an_unrelated_tick_hiccup():
    """The mirror failure: too strict. A strategy reading only closed candles
    was blocked by a feed it never consults."""
    _fresh("btc_ohlcv_1m")
    _fresh("btc_price_tick", age_ms=60 * 1000)
    assert freshness.assert_fresh(("ohlcv",)) == []
    assert freshness.assert_fresh(("ticks",)) == ["feed_stale:btc_price_tick"]


def test_event_driven_feeds_are_never_stale():
    """Liquidations legitimately go quiet for hours; their budget is None.
    Inventing one would make quiet markets untradeable."""
    telemetry._last_event.pop("btc_liquidation", None)
    assert freshness.assert_fresh(("liquidations",)) == []


def test_a_feed_that_has_never_reported_counts_as_stale():
    """'We have never seen this feed' is not evidence that it is current."""
    telemetry._last_event.pop("btc_orderbook", None)
    assert freshness.assert_fresh(("orderbook",)) == ["feed_stale:btc_orderbook"]


def test_an_undeclared_feed_set_falls_back_visibly_not_silently():
    """An empty declaration must not mean 'skip the freshness check'. It uses
    the baseline pair AND records a degradation, so the gap is countable."""
    _fresh("btc_price_tick")
    _fresh("btc_ohlcv_1m")
    degradation.reset_for_tests()

    assert RiskCore._check_feed_freshness(()) == []
    assert degradation.counts().get("freshness_no_declared_feeds") == 1


def test_intent_carries_required_feeds_to_the_pre_trade_check():
    intent = OrderIntent(strategy_id="s1", direction="LONG", entry_price=64_000.0,
                         stop_price=63_500.0, required_feeds=("funding",))
    _fresh("btc_funding", age_ms=10 * 60 * 1000)
    assert RiskCore._check_feed_freshness(intent.required_feeds) == \
        ["feed_stale:btc_funding"]


# =====================================================================
# degradation is countable
# =====================================================================
def test_degradations_increment_a_counter_that_app_code_cannot_clear():
    degradation.reset_for_tests()
    degradation.record_sync("test_label", "because")
    degradation.record_sync("test_label", "again")
    assert degradation.counts()["test_label"] == 2
    assert degradation.total() == 2
    api = {n for n in dir(degradation) if not n.startswith("_")}
    clearers = {n for n in api if any(w in n for w in ("reset", "clear"))}
    assert clearers == {"reset_for_tests"}, clearers
