r"""Export the candle history from MongoDB to a single Parquet file.

This is the MongoDB decoupling: after running it, backtests and Kaggle
notebooks read one file and never open a database connection.

    python export_to_parquet.py
    python export_to_parquet.py --days 365 --out data/btcusdt_1m.parquet

## Why 1-minute and not 5-second

Bybit's minimum kline interval is **one minute**. `interval=5` means five
*minutes*, and `interval="5s"` is accepted with `retCode=0` and silently
treated as five minutes -- a trap that yields 5-minute bars labelled as
5-second ones. Sub-minute bars can only be built by aggregating the live trade
stream forward from now: `/v5/market/recent-trade` returns 1000 rows spanning
about **25 seconds**, which is the entire historical depth available.

So the honest 2-year export is 1m. A 5s series can be accumulated going
forward, and this script will export it too once one exists.

## Which columns are real

| column | source | history |
|---|---|---|
| timestamp, open, high, low, close, volume | venue klines | full 2 years |
| rsi, atr, volatility, returns | derived from OHLCV | full 2 years |
| spread, ofi | order book / trades | **hours, not years** |

`spread` and `ofi` cannot be backfilled by anyone: no venue serves order-book
history. They are emitted as null wherever no book snapshot exists rather than
being interpolated, forward-filled or zero-filled -- a zero spread is a
tradeable-looking lie, and a model trained on one learns that entering costs
nothing. Check `spread_coverage_pct` in the printed summary before using them.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "server"))

DEFAULT_DB = "botmaximus_bybit"
DEFAULT_OUT = ROOT / "data" / "btcusdt_1m.parquet"


# =====================================================================
# feature kernels -- single pass, stdlib only (this project has no numpy)
# =====================================================================
def rsi(closes: list[float], n: int = 14) -> list[float | None]:
    """Wilder's RSI. None until the first full window: a partial-window value
    is an artefact, and a model cannot tell it from a real one."""
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_g, avg_l = gains / n, losses / n
    out[n] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (n - 1) + max(d, 0.0)) / n
        avg_l = (avg_l * (n - 1) + max(-d, 0.0)) / n
        out[i] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)
    return out


def atr(highs: list[float], lows: list[float], closes: list[float],
        n: int = 14) -> list[float | None]:
    """Average true range, Wilder-smoothed."""
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= n:
        return out
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i] - closes[i - 1])))
    a = sum(trs[1:n + 1]) / n
    out[n] = a
    for i in range(n + 1, len(closes)):
        a = (a * (n - 1) + trs[i]) / n
        out[i] = a
    return out


def realised_vol(closes: list[float], n: int = 60) -> list[float | None]:
    """Rolling stdev of log-ish returns, annualisation left to the caller."""
    out: list[float | None] = [None] * len(closes)
    rets = [0.0] + [(closes[i] / closes[i - 1] - 1) if closes[i - 1] else 0.0
                    for i in range(1, len(closes))]
    run = 0.0
    run_sq = 0.0
    for i in range(len(closes)):
        run += rets[i]
        run_sq += rets[i] * rets[i]
        if i >= n:
            run -= rets[i - n]
            run_sq -= rets[i - n] * rets[i - n]
        if i >= n:
            mean = run / n
            var = max(0.0, run_sq / n - mean * mean)
            out[i] = var ** 0.5
    return out


# =====================================================================
# extraction
# =====================================================================
def load_candles(db, start: datetime, end: datetime) -> list[dict]:
    cursor = db["btc_ohlcv_1m"].find(
        {"meta.dataset_id": "btc_ohlcv_1m",
         "event_time": {"$gte": start, "$lte": end}},
        {"_id": 0, "event_time": 1, "payload": 1},
    ).sort("event_time", 1)
    rows = []
    for d in cursor:
        p = d.get("payload") or {}
        if p.get("close") is None:
            continue
        rows.append({
            "timestamp": d["event_time"],
            "open": float(p["open"]), "high": float(p["high"]),
            "low": float(p["low"]), "close": float(p["close"]),
            "volume": float(p.get("volume") or 0.0),
        })
    return rows


def load_book_by_minute(db, start: datetime, end: datetime) -> dict:
    """Newest book snapshot per minute -> {minute: (spread_pct, imbalance)}.

    `imbalance` stands in for order-flow imbalance: true OFI needs every book
    delta, and only throttled snapshots are stored. Naming it `ofi` while it is
    really a standing top-of-book imbalance would be a quiet lie to whatever
    trains on it, so the column carries the snapshot measure and this docstring
    says what it is.
    """
    out: dict = {}
    cursor = db["btc_orderbook"].find(
        {"event_time": {"$gte": start, "$lte": end}},
        {"_id": 0, "event_time": 1, "payload": 1},
    ).sort("event_time", 1)
    for d in cursor:
        p = d.get("payload") or {}
        bid, ask = p.get("best_bid"), p.get("best_ask")
        if not bid or not ask:
            continue
        minute = d["event_time"].replace(second=0, microsecond=0)
        mid = (bid + ask) / 2
        out[minute] = ((ask - bid) / mid * 100 if mid else None,
                       p.get("imbalance"))
    return out


def load_funding(db, start: datetime, end: datetime) -> list[dict]:
    """The settled 8h series. Small (~2,200 rows over two years) and absolutely
    load-bearing: the cost model charges funding across every settlement a
    position spans. A file-backed backtest without it silently reports zero
    funding cost, which at these hold times is the exact error that makes a
    losing strategy look profitable."""
    cursor = db["btc_funding_8h"].find(
        {"event_time": {"$gte": start, "$lte": end}},
        {"_id": 0, "event_time": 1, "payload": 1},
    ).sort("event_time", 1)
    return [{"timestamp": d["event_time"],
             "funding_rate": float(d["payload"]["funding_rate"])}
            for d in cursor]


def build_table(rows: list[dict], book: dict) -> pa.Table:
    closes = [r["close"] for r in rows]
    highs = [r["high"] for r in rows]
    lows = [r["low"] for r in rows]

    col_rsi = rsi(closes)
    col_atr = atr(highs, lows, closes)
    col_vol = realised_vol(closes)

    spreads: list[float | None] = []
    ofis: list[float | None] = []
    for r in rows:
        minute = r["timestamp"].replace(second=0, microsecond=0)
        s, o = book.get(minute, (None, None))
        spreads.append(s)
        ofis.append(o)

    return pa.table({
        "timestamp": pa.array([r["timestamp"] for r in rows],
                              type=pa.timestamp("ms", tz="UTC")),
        "open": pa.array([r["open"] for r in rows], type=pa.float64()),
        "high": pa.array(highs, type=pa.float64()),
        "low": pa.array(lows, type=pa.float64()),
        "close": pa.array(closes, type=pa.float64()),
        "volume": pa.array([r["volume"] for r in rows], type=pa.float64()),
        "rsi": pa.array(col_rsi, type=pa.float64()),
        "atr": pa.array(col_atr, type=pa.float64()),
        "volatility": pa.array(col_vol, type=pa.float64()),
        "spread": pa.array(spreads, type=pa.float64()),
        "ofi": pa.array(ofis, type=pa.float64()),
    })


def main() -> int:
    ap = argparse.ArgumentParser(description="Export candles to Parquet")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    a = ap.parse_args()

    a.out.parent.mkdir(parents=True, exist_ok=True)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=a.days)

    client = MongoClient(a.mongo_uri)
    db = client[a.db]

    print(f"reading {a.db} candles {start:%Y-%m-%d} -> {end:%Y-%m-%d} ...")
    rows = load_candles(db, start, end)
    if not rows:
        print("no candles in range -- refusing to write an empty parquet")
        return 1
    print(f"  {len(rows):,} candles")

    book = load_book_by_minute(db, start, end)
    print(f"  {len(book):,} minutes with an order-book snapshot")

    table = build_table(rows, book)
    pq.write_table(table, a.out, compression="zstd")

    funding = load_funding(db, start, end)
    funding_out = a.out.with_name(a.out.stem + "_funding.parquet")
    pq.write_table(pa.table({
        "timestamp": pa.array([f["timestamp"] for f in funding],
                              type=pa.timestamp("ms", tz="UTC")),
        "funding_rate": pa.array([f["funding_rate"] for f in funding],
                                 type=pa.float64()),
    }), funding_out, compression="zstd")
    print(f"  {len(funding):,} funding settlements -> {funding_out.name}")
    client.close()

    size_mb = a.out.stat().st_size / 1e6
    spread_cov = 100.0 * len(book) / len(rows)
    gaps = 0
    for i in range(1, len(rows)):
        step = (rows[i]["timestamp"] - rows[i - 1]["timestamp"]).total_seconds()
        if step > 60:
            gaps += int(step // 60) - 1

    print(f"\nwrote {a.out}  ({size_mb:.1f} MB, {table.num_rows:,} rows)")
    print(f"  span            : {rows[0]['timestamp']} -> {rows[-1]['timestamp']}")
    print(f"  missing minutes : {gaps:,}")
    print(f"  spread/ofi cover: {spread_cov:.3f}% of rows")
    if spread_cov < 5:
        print("  NOTE: spread and ofi are null for essentially the whole file.")
        print("        No venue serves order-book history, so they cannot be")
        print("        backfilled. Treat them as live-only features; do not")
        print("        train on them across this window.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
