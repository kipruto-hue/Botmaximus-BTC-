"""Persist backtest runs (§5.6). Every run is stored with a config hash so it
is reproducible from the recorded inputs: config, coverage summary, gross/net
metrics, trade list, equity curve, and the validation verdict with reasons.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from botmaximus.backtest.engine import BacktestResult
from botmaximus.backtest.validation import ValidationVerdict

BACKTEST_RUNS_COLLECTION = "backtest_runs"

#: The equity curve carries one point per evaluated bar, so a multi-year 1m
#: window is ~10^6 points and blows past Mongo's 16MB document limit. It is a
#: display artefact — the trade list is the reproducible record, and the curve
#: can be rebuilt from it — so it is stored downsampled rather than dropped.
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
                  result: BacktestResult, verdict: ValidationVerdict) -> dict:
    curve, stride = downsample_curve(result.equity_curve)
    return {
        "strategy_id": strategy_id,
        "config_hash": config_hash(config),
        "config": config,
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
    from botmaximus.db.mongo import get_db
    await get_db()[BACKTEST_RUNS_COLLECTION].insert_one(doc)
    return doc["config_hash"]
