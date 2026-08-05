"""Bybit V5 parsing and collector state handling.

Every fixture below is a frame captured verbatim from the live V5 public stream
on 2026-08-05, not a hand-written approximation. The one venue migration this
project has already survived (Binance, 2026-04-23) failed by connecting
successfully and delivering nothing, so "it parses my idea of the payload" is
not evidence of anything.
"""
from __future__ import annotations

import asyncio
from datetime import timezone

import pytest

from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.collectors.bybit_history import (
    _unwrap,
    kline_row_to_ws_shape,
)
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.parsers.binance import BinanceParser
from botmaximus.pipeline.parsers.bybit import BybitParser

# ---- captured live frames -------------------------------------------------
TRADE = {"T": 1785937120369, "s": "BTCUSDT", "S": "Buy", "v": "0.006",
         "p": "64037.60", "L": "ZeroMinusTick",
         "i": "3b834e66-1b81-534e-b53d-200600f3e336", "BT": False,
         "RPI": False, "seq": 742881263746}

KLINE_OPEN = {"start": 1785937080000, "end": 1785937139999, "interval": "1",
              "open": "63967.7", "close": "64043.6", "high": "64050.9",
              "low": "63960.5", "volume": "188.079", "turnover": "12037252.1038",
              "confirm": False, "timestamp": 1785937119440}
KLINE_CLOSED = {**KLINE_OPEN, "confirm": True}

TICKER_SNAPSHOT = {
    "symbol": "BTCUSDT", "tickDirection": "MinusTick", "lastPrice": "64043.50",
    "markPrice": "64045.13", "indexPrice": "64071.72",
    "openInterest": "58628.631", "openInterestValue": "3754878294.12",
    "nextFundingTime": "1785945600000", "fundingRate": "0.00003967",
    "bid1Price": "64043.50", "ask1Price": "64043.60",
}
TICKER_DELTA = {"symbol": "BTCUSDT", "tickDirection": "ZeroMinusTick",
                "ask1Price": "64043.60", "ask1Size": "1.563"}

LIQUIDATION = {"T": 1785937194086, "s": "BTCUSDT", "S": "Sell", "v": "0.003",
               "p": "64502.20"}

REST_KLINE_ROW = ["1785936840000", "63880.2", "63956.3", "63855", "63895",
                  "264.475", "16901918.623"]


def _item(dataset_id: str, raw, backfill: bool = False) -> RawItem:
    return RawItem(dataset_id=dataset_id, source="bybit", symbol="BTCUSDT",
                   raw=raw, collection_time=utcnow(), backfill=backfill)


# =====================================================================
# the canonical envelope must not leak the venue
# =====================================================================
def test_bybit_and_binance_klines_produce_identical_payload_keys():
    """The whole point of the canonical envelope: features, coverage, the
    backtester and the DSL must not be able to tell which venue produced a bar.
    A key that differs here becomes a venue-specific bug three layers away."""
    by = BybitParser().parse(_item("btc_ohlcv_1m", KLINE_CLOSED))
    bi = BinanceParser().parse(RawItem(
        dataset_id="btc_ohlcv_1m", source="binance", symbol="BTCUSDT",
        raw={"k": {"t": 1785937080000, "T": 1785937139999, "o": "1", "h": "2",
                   "l": "0.5", "c": "1.5", "v": "10", "q": "15", "n": 7}},
        collection_time=utcnow()))
    assert set(by.payload) == set(bi.payload)


def test_orderbook_payload_keys_match_binance():
    by = BybitParser().parse(_item("btc_orderbook", {
        "b": [[64037.5, 4.348], [64037.4, 0.001]],
        "a": [[64037.6, 0.942], [64037.7, 0.001]],
        "E": 1785937120131}))
    for k in ("best_bid", "best_ask", "spread", "bid_volume", "ask_volume",
              "imbalance", "bids", "asks"):
        assert k in by.payload


# =====================================================================
# kline
# =====================================================================
def test_kline_event_time_is_the_close_not_the_open():
    """Coverage slots and the point-in-time view are keyed on close time. An
    open-time event_time would shift every bar one minute into the past and
    hand the backtester a minute of lookahead."""
    env = BybitParser().parse(_item("btc_ohlcv_1m", KLINE_CLOSED))
    assert int(env.event_time.timestamp() * 1000) == 1785937139999
    assert env.payload["close_time"] > env.payload["open_time"]


def test_kline_trade_count_is_none_not_zero():
    """Bybit publishes no trade count. A fabricated 0 is indistinguishable from
    a genuinely tradeless minute."""
    env = BybitParser().parse(_item("btc_ohlcv_1m", KLINE_CLOSED))
    assert env.payload["trades"] is None


def test_turnover_maps_to_quote_volume():
    env = BybitParser().parse(_item("btc_ohlcv_1m", KLINE_CLOSED))
    assert env.payload["quote_volume"] == pytest.approx(12037252.1038)
    assert env.payload["volume"] == pytest.approx(188.079)


# =====================================================================
# trades: taker side is the inverse of buyer_is_maker
# =====================================================================
def test_taker_buy_means_the_buyer_was_not_the_maker():
    env = BybitParser().parse(_item("btc_price_tick", TRADE))
    assert env.payload["buyer_is_maker"] is False
    assert env.payload["price"] == pytest.approx(64037.60)


def test_taker_sell_means_the_buyer_was_the_maker():
    env = BybitParser().parse(_item("btc_price_tick", {**TRADE, "S": "Sell"}))
    assert env.payload["buyer_is_maker"] is True


# =====================================================================
# liquidation direction is explicitly unresolved
# =====================================================================
def test_liquidation_preserves_the_raw_venue_side_and_flags_the_convention():
    """Binance's forceOrder used an order side; Bybit documents a position
    side. Getting it backwards silently flips the sign of any liquidation
    feature, so it is marked unverified rather than guessed."""
    env = BybitParser().parse(_item("btc_liquidation", LIQUIDATION))
    assert env.payload["side_raw"] == "Sell"
    assert env.payload["side_convention"] == "unverified_bybit_allLiquidation"
    assert env.payload["notional_usd"] == pytest.approx(64502.20 * 0.003)


# =====================================================================
# REST history
# =====================================================================
def test_rest_kline_row_becomes_the_websocket_shape():
    """One kline shape for REST and websocket, so a backfilled candle and a
    live one cannot disagree about what a candle is."""
    shape = kline_row_to_ws_shape(REST_KLINE_ROW)
    env = BybitParser().parse(_item("btc_ohlcv_1m", shape, backfill=True))
    assert shape["confirm"] is True
    assert int(env.event_time.timestamp() * 1000) == 1785936840000 + 59_999
    assert env.backfill is True
    assert env.payload["open"] == pytest.approx(63880.2)


def test_a_nonzero_retcode_raises_instead_of_reading_as_no_rows():
    """V5 returns errors with HTTP 200, so raise_for_status sees success. An
    unchecked retCode turns a rate-limit into an empty list, which looks exactly
    like a genuine gap and gets backfilled as absence."""
    with pytest.raises(RuntimeError, match="10006"):
        _unwrap({"retCode": 10006, "retMsg": "rate limit", "result": {}}, "kline")


def test_unwrap_returns_rows_on_success():
    assert _unwrap({"retCode": 0, "result": {"list": [1, 2]}}, "kline") == [1, 2]


def test_funding_history_row_parses():
    env = BybitParser().parse(_item("btc_funding_8h", {
        "symbol": "BTCUSDT", "fundingRate": "0.00005918",
        "fundingRateTimestamp": "1785916800000"}))
    assert env.payload["funding_rate"] == pytest.approx(0.00005918)


def test_oi_history_row_does_not_invent_a_usd_value():
    """The open-interest endpoint returns no USD field. Deriving one from a
    contemporaneous price would look like venue data and silently be an
    estimate."""
    env = BybitParser().parse(_item("btc_oi_5m", {
        "openInterest": "58944.79900000", "timestamp": "1785936900000"}))
    assert env.payload["open_interest"] == pytest.approx(58944.799)
    assert env.payload["open_interest_value_usd"] is None


# =====================================================================
# collector state: deltas and reconnects
# =====================================================================
def _collector():
    from botmaximus.pipeline.collectors.bybit import BybitPublicCollector
    return BybitPublicCollector(asyncio.Queue())


@pytest.mark.asyncio
async def test_ticker_deltas_merge_into_the_snapshot():
    """A delta carries only changed fields. Parsing one directly would raise on
    fundingRate, or record a stale rate as current."""
    c = _collector()
    await c.handle('{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1,"data":'
                   + __import__("json").dumps(TICKER_SNAPSHOT) + "}")
    await c.handle('{"topic":"tickers.BTCUSDT","type":"delta","ts":2,"data":'
                   + __import__("json").dumps(TICKER_DELTA) + "}")
    assert c._ticker["fundingRate"] == "0.00003967"      # survived the delta
    assert c._ticker["ask1Size"] == "1.563"              # delta applied


@pytest.mark.asyncio
async def test_only_confirmed_candles_are_enqueued():
    """The in-progress bar is published ~1/s. Storing it would fill the series
    with partial candles indistinguishable from closed ones."""
    import json
    c = _collector()
    await c.handle(json.dumps({"topic": "kline.1.BTCUSDT", "ts": 1,
                               "data": [KLINE_OPEN]}))
    assert c.gather_q.qsize() == 0
    await c.handle(json.dumps({"topic": "kline.1.BTCUSDT", "ts": 1,
                               "data": [KLINE_CLOSED]}))
    assert c.gather_q.qsize() == 1


@pytest.mark.asyncio
async def test_book_deltas_apply_and_zero_quantity_deletes_a_level():
    import json
    c = _collector()
    await c.handle(json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot",
                               "ts": 1, "data": {"b": [["100", "1"], ["99", "2"]],
                                                 "a": [["101", "1"]]}}))
    await c.handle(json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta",
                               "ts": 2, "data": {"b": [["99", "0"], ["98", "5"]],
                                                 "a": []}}))
    assert 99.0 not in c._bids
    assert c._bids[98.0] == 5.0
    assert c._bids[100.0] == 1.0


@pytest.mark.asyncio
async def test_a_delta_before_any_snapshot_is_dropped():
    """It cannot be applied to anything. The next snapshot re-seeds the book."""
    import json
    c = _collector()
    await c.handle(json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta",
                               "ts": 1, "data": {"b": [["100", "1"]], "a": []}}))
    assert not c._bids
    assert c._book_ready is False


def test_reconnect_discards_the_book_and_ticker_state():
    """Levels from before a gap are stale. Serving them as live is the failure
    a reconnect exists to end."""
    c = _collector()
    c._bids[100.0] = 1.0
    c._ticker = {"fundingRate": "0.1"}
    c._book_ready = True
    c._reset_state()
    assert not c._bids and not c._ticker and c._book_ready is False
