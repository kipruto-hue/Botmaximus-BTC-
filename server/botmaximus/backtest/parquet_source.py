r"""File-backed market data: the same `MarketWindow` the Mongo path builds,
loaded from Parquet with no database connection.

This is what makes backtests portable. A Kaggle notebook, a CI job or a laptop
on a plane can run the full harness against `data/btcusdt_1m.parquet`; nothing
here imports pymongo, and nothing here can reach a database even by accident.

## The coverage gate still applies

The Mongo path refuses windows with holes by consulting the coverage ledger
(§2.4). There is no ledger in a file, so completeness is derived from the file
itself: the bar grid is walked and any missing minute is counted. That is a
weaker guarantee in one specific way and it is worth being precise about it --
the ledger distinguishes *"the venue had no data"* from *"we were not running"*,
and a bare file cannot. It is a stronger guarantee in another: the ledger can
disagree with the records it describes, and this cannot, because it *is* the
records.

Either way the refusal is the same and it is not optional: a window with gaps
does not get backtested.

## Funding is required, not optional

`CostModel` charges funding across every settlement a position spans. Loading
bars without the funding sidecar would produce a backtest that silently reports
zero funding cost -- and at ~5-minute taker holds, friction is the term that
decides whether a strategy is profitable. So a missing sidecar is an error,
never a shrug and a default of zero.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from botmaximus.backtest.costs import FundingPoint
from botmaximus.backtest.data import Bar, CoverageError, MarketWindow

MINUTE = timedelta(minutes=1)


def funding_path(parquet: Path) -> Path:
    return parquet.with_name(parquet.stem + "_funding.parquet")


def _as_utc(ts) -> datetime:
    dt = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _window_table(path: Path, columns: list[str], start: datetime | None,
                  end: datetime | None):
    """Read only the rows in [start, end], pushed down to the reader.

    The naive version -- read the whole file, filter in Python -- turned a
    120-day window into a full pass over 1,051,200 rows and made the file path
    *three times slower* than Mongo, which is the opposite of the point.
    Filtering in the dataset layer lets Arrow skip row groups on their
    timestamp statistics and materialises only the window.
    """
    dataset = ds.dataset(path, format="parquet")
    expr = None
    if start is not None:
        expr = ds.field("timestamp") >= pa.scalar(start, type=pa.timestamp("ms", tz="UTC"))
    if end is not None:
        upper = ds.field("timestamp") <= pa.scalar(end, type=pa.timestamp("ms", tz="UTC"))
        expr = upper if expr is None else (expr & upper)
    return dataset.to_table(columns=columns, filter=expr)


def load_bars(parquet: Path, start: datetime | None = None,
              end: datetime | None = None) -> list[Bar]:
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    table = _window_table(parquet, cols, start, end)
    # Column-at-a-time: pulling six Python lists once is markedly cheaper than
    # indexing six Arrow columns per row.
    ts = table.column("timestamp").to_pylist()
    op = table.column("open").to_pylist()
    hi = table.column("high").to_pylist()
    lo = table.column("low").to_pylist()
    cl = table.column("close").to_pylist()
    vo = table.column("volume").to_pylist()

    step = MINUTE - timedelta(milliseconds=1)
    bars: list[Bar] = []
    for i in range(table.num_rows):
        close_time = _as_utc(ts[i])
        bars.append(Bar(
            open_time=close_time - step,
            close_time=close_time,
            open=op[i], high=hi[i], low=lo[i], close=cl[i],
            volume=vo[i] or 0.0,
        ))
    return bars


def load_funding(parquet: Path, start: datetime | None = None,
                 end: datetime | None = None) -> list[FundingPoint]:
    fp = funding_path(parquet)
    if not fp.exists():
        raise CoverageError(
            f"funding sidecar {fp.name} is missing. Refusing to backtest: the "
            f"cost model would charge zero funding, and at these hold times "
            f"that is the difference between a losing strategy and one that "
            f"looks profitable. Re-run export_to_parquet.py.")
    d = pq.read_table(fp).to_pydict()
    out = []
    for i in range(len(d["timestamp"])):
        t = _as_utc(d["timestamp"][i])
        if start and t < start:
            continue
        if end and t > end:
            continue
        out.append(FundingPoint(time=t, rate=d["funding_rate"][i]))
    return out


def assert_complete(bars: list[Bar], allow_gaps: bool = False) -> dict:
    """Derive coverage from the bars themselves. Refuses on any missing minute
    unless explicitly overridden."""
    if not bars:
        raise CoverageError("no bars in window")
    missing = 0
    first_gap = None
    for i in range(1, len(bars)):
        step = (bars[i].close_time - bars[i - 1].close_time).total_seconds()
        if step > 60:
            n = int(step // 60) - 1
            missing += n
            if first_gap is None:
                first_gap = bars[i - 1].close_time
    expected = missing + len(bars)
    summary = {
        "bars": len(bars),
        "missing_slots": missing,
        "completeness_pct": round(100.0 * len(bars) / expected, 4) if expected else 0.0,
        "first_gap": first_gap.isoformat() if first_gap else None,
        "start": bars[0].close_time.isoformat(),
        "end": bars[-1].close_time.isoformat(),
    }
    if missing and not allow_gaps:
        raise CoverageError(
            f"btc_ohlcv_1m: {missing}/{expected} minutes missing in "
            f"[{summary['start']}, {summary['end']}] — refusing backtest "
            f"(§2.4). first gap {summary['first_gap']}")
    return summary


def load_window(parquet: str | Path, start: datetime | None = None,
                end: datetime | None = None,
                allow_gaps: bool = False) -> tuple[MarketWindow, dict]:
    """The file-backed equivalent of `data.load_window`.

    Returns the window and its coverage summary, so a caller can record what it
    actually evaluated rather than assuming.
    """
    parquet = Path(parquet)
    if not parquet.exists():
        raise CoverageError(
            f"{parquet} not found — run `python export_to_parquet.py` first.")
    bars = load_bars(parquet, start, end)
    summary = assert_complete(bars, allow_gaps)
    funding = load_funding(parquet, start, end)
    if not funding:
        raise CoverageError(
            "no funding settlements in window — see load_funding(); a "
            "zero-funding backtest understates cost at every settlement held.")
    return MarketWindow(bars=bars, funding=funding), summary
