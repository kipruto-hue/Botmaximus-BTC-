"""§11 step 4: futures feeds (funding, OI, liquidations, order book) and
OHLCV gap backfill."""
from datetime import datetime, timedelta, timezone

from botmaximus.pipeline.backfill import kline_row_to_ws_shape, minute_close, missing_minutes
from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.parsers.binance import BinanceParser
from botmaximus.pipeline.quality.gate import QualityGate

UTC = timezone.utc


def raw(dataset_id, payload, backfill=False):
    return RawItem(
        dataset_id=dataset_id, source="binance_futures", symbol="BTCUSDT",
        raw=payload, collection_time=utcnow(), backfill=backfill,
    )


def now_ms(offset_s=0.0):
    return int((utcnow() + timedelta(seconds=offset_s)).timestamp() * 1000)


# ---------------- parsers ----------------

def test_parse_mark_price_funding():
    env = BinanceParser().parse(raw("btc_funding", {
        "E": now_ms(), "p": "66000.10", "i": "65990.55", "r": "0.00010000", "T": now_ms(3600),
    }))
    assert env.payload["mark_price"] == 66000.10
    assert env.payload["funding_rate"] == 0.0001
    assert env.payload["next_funding_time"].tzinfo is not None


def test_parse_force_order_liquidation():
    env = BinanceParser().parse(raw("btc_liquidation", {
        "o": {"S": "SELL", "q": "0.5", "p": "66000", "ap": "65990", "X": "FILLED", "T": now_ms()},
    }))
    assert env.payload["side"] == "SELL"
    assert env.payload["price"] == 65990          # avg fill price preferred
    assert env.payload["notional_usd"] == 65990 * 0.5


def test_parse_depth_snapshot():
    env = BinanceParser().parse(raw("btc_orderbook", {
        "E": now_ms(), "T": now_ms(),
        "b": [["66000.0", "2.0"], ["65999.0", "1.0"]],
        "a": [["66001.0", "1.0"], ["66002.0", "1.0"]],
    }))
    p = env.payload
    assert p["best_bid"] == 66000.0 and p["best_ask"] == 66001.0
    assert p["spread"] == 1.0
    assert p["imbalance"] == (3.0 - 2.0) / 5.0


def test_parse_open_interest():
    env = BinanceParser().parse(raw("btc_open_interest", {
        "symbol": "BTCUSDT", "openInterest": "81000.5", "time": now_ms(),
    }))
    assert env.payload["open_interest"] == 81000.5


# ---------------- quality gate ----------------

def test_liquidation_not_quarantined_as_priceless():
    env = QualityGate().check(BinanceParser().parse(raw("btc_liquidation", {
        "o": {"S": "BUY", "q": "0.1", "p": "66000", "ap": "66000", "X": "FILLED", "T": now_ms()},
    })))
    assert not env.quarantine_reasons


def test_crossed_book_quarantined():
    env = QualityGate().check(BinanceParser().parse(raw("btc_orderbook", {
        "E": now_ms(), "b": [["66002.0", "1.0"]], "a": [["66001.0", "1.0"]],
    })))
    assert "crossed_book" in env.quarantine_reasons


def test_implausible_funding_rate_quarantined():
    env = QualityGate().check(BinanceParser().parse(raw("btc_funding", {
        "E": now_ms(), "p": "66000", "i": "66000", "r": "0.05", "T": now_ms(3600),
    })))
    assert "funding_rate_implausible" in env.quarantine_reasons


def test_backfill_candle_not_stale_and_flagged():
    k = {"k": {"t": now_ms(-3600), "T": now_ms(-3540), "o": "66000", "h": "66010",
               "l": "65990", "c": "66005", "v": "10", "q": "660000", "n": 500}}
    env = QualityGate().check(BinanceParser().parse(
        raw("btc_ohlcv_1m", k, backfill=True)))
    assert "backfill" in env.quality_flags
    assert "stale" not in env.quality_flags
    assert env.quality_ok
    assert not env.quarantine_reasons


def test_backfill_does_not_trip_phantom_or_poison_continuity():
    gate = QualityGate()
    parser = BinanceParser()
    live = {"k": {"t": now_ms(-60), "T": now_ms(-1), "o": "66000", "h": "66010",
                  "l": "65990", "c": "66005", "v": "10", "q": "1", "n": 5}}
    gate.check(parser.parse(raw("btc_ohlcv_1m", live)))
    # backfilled candle from a very different price hours ago
    old = {"k": {"t": now_ms(-7200), "T": now_ms(-7140), "o": "60000", "h": "60010",
                 "l": "59990", "c": "60005", "v": "10", "q": "1", "n": 5}}
    env = gate.check(parser.parse(raw("btc_ohlcv_1m", old, backfill=True)))
    assert "phantom_suspect" not in env.quality_flags
    # and the live continuity price is still the live one
    assert gate._prev_price["btc_ohlcv_1m"] == 66005.0


# ---------------- backfill gap math ----------------

def test_minute_close_is_59_999():
    t = datetime(2026, 7, 22, 11, 24, 30, tzinfo=UTC)
    assert minute_close(t) == datetime(2026, 7, 22, 11, 24, 59, 999000, tzinfo=UTC)


def test_missing_minutes_finds_interior_hole():
    base = datetime(2026, 7, 22, 11, 20, tzinfo=UTC)
    closes = [minute_close(base + timedelta(minutes=i)) for i in range(5)]
    existing = set(closes) - {closes[2]}          # 11:22 missing
    gaps = missing_minutes(existing, base, base + timedelta(minutes=4))
    assert gaps == [closes[2]]


def test_missing_minutes_none_when_complete():
    base = datetime(2026, 7, 22, 11, 20, tzinfo=UTC)
    existing = {minute_close(base + timedelta(minutes=i)) for i in range(5)}
    assert missing_minutes(existing, base, base + timedelta(minutes=4)) == []


def test_fetch_paginates_past_1000_kline_limit():
    """A gap span longer than 1000 minutes needs multiple REST requests."""
    import asyncio

    from botmaximus.pipeline.backfill import MINUTE_MS, OhlcvBackfiller

    base_close = 1_753_000_000_000 - (1_753_000_000_000 % MINUTE_MS) + MINUTE_MS - 1
    gaps = [datetime.fromtimestamp((base_close + i * MINUTE_MS) / 1000, tz=UTC)
            for i in range(1500)]

    class FakeResp:
        def __init__(self, rows):
            self._rows = rows
        def raise_for_status(self):
            pass
        def json(self):
            return self._rows

    class FakeClient:
        def __init__(self):
            self.calls = []
        async def get(self, url, params):
            self.calls.append(params)
            start, end = params["startTime"], params["endTime"]
            rows = []
            t = start - (start % MINUTE_MS)
            while len(rows) < 1000 and t + MINUTE_MS - 1 <= end:
                close = t + MINUTE_MS - 1
                if close >= start:
                    rows.append([t, "1", "1", "1", "1", "1", close, "1", 1])
                t += MINUTE_MS
            return FakeResp(rows)

    q = asyncio.Queue()
    bf = OhlcvBackfiller(q)
    client = FakeClient()
    asyncio.run(bf._fetch_and_enqueue(client, gaps))
    assert len(client.calls) == 2          # 1000 + 500
    assert q.qsize() == 1500
    item = q.get_nowait()
    assert item.backfill


def test_kline_row_round_trips_through_parser():
    row = [1753181040000, "66000", "66010", "65990", "66005", "10.5",
           1753181099999, "693000.0", 812]
    env = BinanceParser().parse(RawItem(
        dataset_id="btc_ohlcv_1m", source="binance", symbol="BTCUSDT",
        raw=kline_row_to_ws_shape(row), collection_time=utcnow(), backfill=True,
    ))
    assert env.backfill
    assert env.payload["close"] == 66005.0
    assert env.event_time == datetime.fromtimestamp(1753181099.999, tz=UTC)
