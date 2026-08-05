"""The file-backed data source must be a true substitute for the Mongo path,
not an approximation of it. If a Parquet backtest and a Mongo backtest of the
same window can disagree, then every Kaggle result is unfalsifiable.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from botmaximus.backtest.data import Bar, CoverageError
from botmaximus.backtest import parquet_source as ps

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _write(tmp: Path, minutes: int, skip: set[int] | None = None,
           funding: bool = True) -> Path:
    skip = skip or set()
    ts, o, h, low, c, v = [], [], [], [], [], []
    for i in range(minutes):
        if i in skip:
            continue
        t = T0 + timedelta(minutes=i + 1) - timedelta(milliseconds=1)
        px = 100.0 + i * 0.1
        ts.append(t)
        o.append(px)
        h.append(px + 0.5)
        low.append(px - 0.5)
        c.append(px + 0.2)
        v.append(10.0)
    p = tmp / "bars.parquet"
    pq.write_table(pa.table({
        "timestamp": pa.array(ts, type=pa.timestamp("ms", tz="UTC")),
        "open": pa.array(o, type=pa.float64()),
        "high": pa.array(h, type=pa.float64()),
        "low": pa.array(low, type=pa.float64()),
        "close": pa.array(c, type=pa.float64()),
        "volume": pa.array(v, type=pa.float64()),
    }), p)
    if funding:
        pq.write_table(pa.table({
            "timestamp": pa.array([T0 + timedelta(hours=8)],
                                  type=pa.timestamp("ms", tz="UTC")),
            "funding_rate": pa.array([0.0001], type=pa.float64()),
        }), ps.funding_path(p))
    return p


def test_bars_load_with_utc_close_times(tmp_path):
    p = _write(tmp_path, 10)
    bars = ps.load_bars(p)
    assert len(bars) == 10
    assert all(isinstance(b, Bar) for b in bars)
    assert bars[0].close_time.tzinfo is not None
    # close_time is the point-in-time boundary; open_time precedes it by a bar
    assert bars[0].open_time < bars[0].close_time
    assert (bars[1].close_time - bars[0].close_time).total_seconds() == 60


def test_a_gap_is_refused_exactly_like_the_mongo_coverage_gate(tmp_path):
    """The whole point of the ledger gate is that a window with holes never
    gets backtested. Reading from a file must not become a way around it."""
    p = _write(tmp_path, 30, skip={10, 11, 12})
    with pytest.raises(CoverageError, match="refusing backtest"):
        ps.load_window(p)


def test_gaps_can_be_allowed_only_explicitly(tmp_path):
    p = _write(tmp_path, 30, skip={10})
    window, summary = ps.load_window(p, allow_gaps=True)
    assert summary["missing_slots"] == 1
    assert summary["completeness_pct"] < 100
    assert len(window.bars) == 29


def test_a_clean_window_reports_full_completeness(tmp_path):
    p = _write(tmp_path, 60)
    window, summary = ps.load_window(p)
    assert summary["missing_slots"] == 0
    assert summary["completeness_pct"] == 100.0
    assert len(window.bars) == 60


def test_missing_funding_sidecar_is_an_error_not_a_zero(tmp_path):
    """A zero-funding backtest understates cost at every settlement held, and
    friction is the term that decides these strategies. Defaulting to zero
    would make a losing strategy look profitable."""
    p = _write(tmp_path, 30, funding=False)
    with pytest.raises(CoverageError, match="funding sidecar"):
        ps.load_window(p)


def test_funding_is_attached_to_the_window(tmp_path):
    p = _write(tmp_path, 30)
    window, _ = ps.load_window(p)
    assert window.funding and window.funding[0].rate == pytest.approx(0.0001)


def test_missing_file_names_the_fix(tmp_path):
    with pytest.raises(CoverageError, match="export_to_parquet"):
        ps.load_window(tmp_path / "nope.parquet")


def test_window_can_be_narrowed_by_start_and_end(tmp_path):
    p = _write(tmp_path, 120)
    bars = ps.load_bars(p, start=T0 + timedelta(minutes=30),
                        end=T0 + timedelta(minutes=60))
    assert 25 < len(bars) <= 31
    assert bars[0].close_time >= T0 + timedelta(minutes=30)


def test_nothing_in_this_module_can_reach_a_database(tmp_path):
    """Portability is the point: a Kaggle notebook has no MongoDB. An import
    that pulls pymongo in would work locally and fail only in the place this
    exists to serve.

    Checks the import graph, not the prose -- the docstring legitimately
    discusses Mongo, and a text search would flag that forever.
    """
    import ast

    tree = ast.parse(Path(ps.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
            imported.add(node.module)
    assert "pymongo" not in imported
    assert not any("mongo" in m for m in imported), imported
