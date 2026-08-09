"""Persist backtest runs (§5.6). Every run is stored with a config hash so it
is reproducible from the recorded inputs: config, coverage summary, gross/net
metrics, trade list, equity curve, and the validation verdict with reasons.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone

from botmaximus.backtest.engine import BacktestResult
from botmaximus.backtest.validation import ValidationVerdict

log = logging.getLogger(__name__)


#: Downsampling limit for the curve returned to *callers and the dashboard*.
#:
#: This used to be a storage constraint: one point per evaluated bar means a
#: multi-year 1m window is ~10^6 points, which blew past Mongo's 16MB document
#: limit. Parquet has no such limit and is columnar, so the archive now keeps
#: the curve at full resolution (§3.H) and this is only about what a JSON
#: response should carry.
MAX_CURVE_POINTS = 5_000


def config_hash(config: dict) -> str:
    blob = json.dumps(config, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def downsample_curve(curve: list, max_points: int = MAX_CURVE_POINTS) -> tuple[list, int]:
    """Stride the curve down to `max_points`, always keeping the last point so
    the final equity is exact. Returns (points, stride)."""
    n = len(curve)
    if n <= max_points:
        return list(curve), 1
    stride = (n + max_points - 1) // max_points
    out = curve[::stride]
    if out[-1] is not curve[-1]:
        out.append(curve[-1])
    return out, stride


def build_run_doc(strategy_id: str, config: dict, coverage_summary: dict,
                  result: BacktestResult, verdict: ValidationVerdict,
                  as_of: datetime | None = None) -> dict:
    """Assemble the run record.

    `as_of` is required to persist (§6) but deliberately NOT part of
    `config_hash`. It changes on every run, so hashing it would give every run
    a unique hash — defeating the replay-dedupe the hash exists for, and
    inflating the lifetime trial ledger on every restart, which is precisely
    the multiple-testing correction this system is trying to keep honest.
    """
    curve, stride = downsample_curve(result.equity_curve)
    return {
        "strategy_id": strategy_id,
        "config_hash": config_hash(config),
        "config": config,
        "as_of": as_of,
        "created_at": datetime.now(timezone.utc),
        "coverage": coverage_summary,
        "verdict": {"passed": verdict.passed, "reasons": verdict.reasons},
        "metrics": verdict.metrics,
        "params": result.params,
        "equity_curve_points": len(result.equity_curve),
        "equity_curve_stride": stride,       # 1 = full resolution
        "equity_curve": [[t.isoformat(), round(e, 2)] for t, e in curve],
        "trades": [
            {
                "direction": t.direction,
                "entry_time": t.entry_time.isoformat(),
                "exit_time": t.exit_time.isoformat(),
                "entry_price": t.entry_price, "exit_price": t.exit_price,
                "qty": t.qty, "exit_reason": t.exit_reason,
                "gross_pnl": round(t.gross_pnl, 4),
                "net_pnl": round(t.net_pnl, 4),
                "fees": round(t.fees, 4), "funding": round(t.funding, 6),
            }
            for t in result.trades
        ],
    }


async def save_run(doc: dict) -> str:
    """Metadata to Postgres, curve and trades to Parquet (§3.H).

    Refuses without `as_of`: §6 calls an unpinned backtest a defect, and
    `backtest_runs.as_of` is NOT NULL so such a run is unrecordable anyway.
    Failing here gives the operator the reason rather than a constraint
    violation from three layers down.
    """
    import uuid

    import pyarrow as pa

    from botmaximus.storage import postgres, records as store

    if not doc.get("as_of"):
        raise ValueError(
            "refusing to persist a backtest run with no `as_of` (§6): without "
            "a pinned instant the run cannot be reproduced, because a re-run "
            "would silently read any corrections that landed since.")

    run_id = str(uuid.uuid4())
    curve_key = None
    try:
        # Full resolution in the archive — the downsampled copy in `doc` is for
        # display. This is the reproducible record.
        table = pa.table({
            "kind": (["curve"] * len(doc["equity_curve"])
                     + ["trade"] * len(doc["trades"])),
            "json": ([json.dumps(p) for p in doc["equity_curve"]]
                     + [json.dumps(t) for t in doc["trades"]]),
        })
        written = store.archive().write_blob(
            f"backtest/{run_id}.parquet", table, dataset_id="backtest_run",
            partition=f"run={run_id}")
        curve_key = written.key
        await store._record_manifest(written)
    except Exception as e:                              # noqa: BLE001
        # §7: the archive being unreachable must not lose the verdict. The
        # metadata row still lands, with a null blob key that the §11 integrity
        # check will report rather than hide.
        log.error("backtest curve archive failed for %s: %s", run_id, e)

    v = doc["verdict"]
    await postgres.execute(
        "INSERT INTO backtest_runs (backtest_run_id, strategy_id, config_hash, "
        " as_of, window_start, window_end, n_trials, passed, reasons, metrics, "
        " coverage, curve_blob_key) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (run_id, doc["strategy_id"], doc["config_hash"], doc["as_of"],
         _window(doc, 0), _window(doc, 1), doc.get("n_trials", 1),
         v["passed"], v["reasons"], json.dumps(doc["metrics"], default=str),
         json.dumps(doc["coverage"], default=str), curve_key))
    return doc["config_hash"]


def _window(doc: dict, i: int) -> datetime:
    return datetime.fromisoformat(doc["config"]["window"][i])
