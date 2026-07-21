"""Parser tests against real captured Binance stream payload shapes."""
from datetime import timezone

from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.parsers.binance import BinanceParser

AGG_TRADE = {
    "e": "aggTrade", "E": 1751234567890, "s": "BTCUSDT",
    "a": 3268503412, "p": "64210.50", "q": "0.01543",
    "f": 5011223344, "l": 5011223345, "T": 1751234567885, "m": True, "M": True,
}

KLINE_CLOSED = {
    "e": "kline", "E": 1751234580012, "s": "BTCUSDT",
    "k": {
        "t": 1751234520000, "T": 1751234579999, "s": "BTCUSDT", "i": "1m",
        "f": 100, "L": 200, "o": "64180.00", "c": "64210.50", "h": "64251.10",
        "l": "64170.00", "v": "18.44532", "n": 412, "x": True,
        "q": "1183920.55", "V": "9.20000", "Q": "590000.10", "B": "0",
    },
}


def test_agg_trade_parses():
    item = RawItem("btc_price_tick", "binance", "BTCUSDT", AGG_TRADE, utcnow())
    env = BinanceParser().parse(item)
    assert env.payload["price"] == 64210.50
    assert env.payload["qty"] == 0.01543
    assert env.event_time.tzinfo == timezone.utc
    assert int(env.event_time.timestamp() * 1000) == AGG_TRADE["T"]


def test_kline_parses():
    item = RawItem("btc_ohlcv_1m", "binance", "BTCUSDT", KLINE_CLOSED, utcnow())
    env = BinanceParser().parse(item)
    p = env.payload
    assert (p["open"], p["high"], p["low"], p["close"]) == (64180.0, 64251.1, 64170.0, 64210.5)
    assert p["trades"] == 412
    # event_time is the candle CLOSE time
    assert int(env.event_time.timestamp() * 1000) == KLINE_CLOSED["k"]["T"]
