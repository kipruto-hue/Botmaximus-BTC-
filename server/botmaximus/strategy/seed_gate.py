"""Gate 3: register the §8.1 seeds and put each one through the §5.5 gate.

Every seed is parsed, validated, compiled, replayed over coverage-complete
history and judged **gate-off**. Whatever comes back is recorded — a passing
seed is promoted to `paper`, a failing one is retired with its reasons.

Expect failures. A 1m strategy holding ~5 minutes pays a taker fee twice, plus
funding across any settlement it spans, plus slippage; Pass B's live proof
already showed that friction turning a −$154 gross into a −$635 net. Seeds are
baselines, not blessed edges (§8.1). A wall of rejections means the gate works.

    python -m botmaximus.strategy.seed_gate --days 365
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from botmaximus.backtest import holdout
from botmaximus.backtest.runner import run_dsl_backtest
from botmaximus.features.registry import FEED_HISTORY_DAYS
from botmaximus.strategy import lifecycle, store, trials
from botmaximus.strategy.schema import StrategyDefinition
from botmaximus.strategy.seeds import seed_definitions
from botmaximus.strategy.validator import validate

log = logging.getLogger(__name__)


def window_for(defn: StrategyDefinition, days: int,
               end: datetime) -> tuple[datetime, datetime, str]:
    """Clip the evaluation window to the shortest venue history any declared
    feed actually has. Running a 2-year window on a feed with 30 days of history
    does not produce a longer backtest — it produces a coverage refusal, or
    worse, a silent hole."""
    limit = days
    note = ""
    for feed in defn.required_feeds:
        cap = FEED_HISTORY_DAYS.get(feed)
        if cap is not None and cap > 0 and cap < limit:
            limit = cap - 1                     # stay inside the venue's edge
            note = f"clipped to {limit}d by {feed} history cap"
        elif cap == 0:
            limit = min(limit, 7)
            note = f"clipped to {limit}d — {feed} is forward-coverage only"
    return end - timedelta(days=limit), end, note


async def run(days: int, warmup: int, persist: bool = True,
              end: datetime | None = None) -> list[dict]:
    """`end` defaults to now. Pass an earlier boundary when the live pipeline
    has been down: the coverage gate will (correctly) refuse a window whose tail
    has holes, and moving the boundary is the honest fix — widening the gate is
    not."""
    await store.ensure_indexes()
    await trials.ensure_indexes()
    end = end or datetime.now(timezone.utc) - timedelta(minutes=5)
    # The seed gate is a *search* path, so it stops at the sealed window. Clip
    # rather than refuse: an operator asking for 365 days wants the largest legal
    # 365 days, and a hard error here would only teach them to pass --end by hand.
    end = holdout.clip_end(end)
    results: list[dict] = []

    for defn in seed_definitions():
        # §5.2–5.7 first: nothing reaches the backtester unvalidated
        res = validate(defn)
        await store.upsert(defn, warnings=res.warnings)
        if not res.ok:
            log.error("[%s] REJECTED at validation: %s", defn.id, res.reasons)
            results.append({"id": defn.id, "stage": "validation",
                            "passed": False, "reasons": res.reasons})
            continue

        start, stop, note = window_for(defn, days, end)
        try:
            out = await run_dsl_backtest(defn, start, stop, warmup=warmup,
                                         persist=persist)
        except Exception as e:                  # coverage refusal is a result
            log.warning("[%s] backtest refused: %s", defn.id, e)
            results.append({"id": defn.id, "stage": "coverage",
                            "passed": False, "reasons": [str(e)[:300]],
                            "window_note": note})
            continue

        verdict = out["verdict"]
        results.append({
            "id": defn.id, "stage": "backtest", "passed": verdict["passed"],
            "reasons": verdict["reasons"], "metrics": out["metrics"],
            "signals": out["signals"], "exit_reasons": out["exit_reasons"],
            "window": [start.date().isoformat(), stop.date().isoformat()],
            "window_note": note, "warnings": res.warnings,
            "n_trials": out.get("n_trials"),
        })

        # record the outcome — a rejection is kept, not deleted: its reasons are
        # the training signal Pass C2's generator needs
        from botmaximus.backtest.validation import ValidationVerdict
        v = ValidationVerdict(passed=verdict["passed"], reasons=verdict["reasons"],
                              metrics=out["metrics"])
        t = (lifecycle.promote(defn.id, "candidate", v) if v.passed
             else lifecycle.reject(defn.id, v))
        await store.record_transition(t)

    return results


def report(results: list[dict]) -> None:
    print("\n" + "=" * 78)
    print("GATE 3 — SEED VALIDATION (gate-off, coverage-complete windows)")
    print("=" * 78)
    for r in results:
        status = "PASS" if r["passed"] else "REJECTED"
        print(f"\n{r['id']}  [{status}]  stage={r['stage']}")
        if r.get("window"):
            print(f"  window   : {r['window'][0]} -> {r['window'][1]}"
                  + (f"  ({r['window_note']})" if r.get("window_note") else ""))
        m = r.get("metrics")
        if m:
            print(f"  trades   : {m['trades']}   signals: {r.get('signals')}")
            print(f"  gross    : {m['gross_pnl']:>12,.2f}")
            print(f"  net      : {m['net_pnl']:>12,.2f}")
            line = f"  friction : {m['friction']:>12,.2f}"
            if m["gross_pnl"]:
                line += f"   ({abs(m['friction'] / m['gross_pnl']):.1f}x gross)"
            print(line)
            print(f"  DSR      : {m['deflated_sharpe']}   maxDD: {m['max_drawdown_pct']}%"
                  f"   (corrected for {r.get('n_trials')} lifetime trials)")
            print(f"  regimes  : {m['positive_regimes']} positive  {m['regime_expectancy']}")
            print(f"  walkfwd  : {m['walk_forward']['positive_folds']}"
                  f"/{m['walk_forward']['folds']} folds positive")
            print(f"  exits    : {r.get('exit_reasons')}")
        for reason in r["reasons"]:
            print(f"  reject   : {reason}")
        for w in r.get("warnings", []):
            print(f"  warn     : {w}")

    passed = sum(1 for r in results if r["passed"])
    print("\n" + "-" * 78)
    print(f"{passed}/{len(results)} seeds cleared the gate.")
    if passed == 0:
        print("Zero survivors is the expected outcome, not a failure of the "
              "harness:\nfriction is the adversary at 1m/5-minute holds, and the "
              "gate exists to\nfind that out here rather than with capital.")
    print("-" * 78)


async def _main(days: int, warmup: int, end: datetime | None) -> None:
    from botmaximus.storage import postgres
    try:
        report(await run(days, warmup, end=end))
    finally:
        await postgres.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Gate 3 seed validation")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--warmup", type=int, default=5000,
                    help="1m bars; must exceed the slowest HTF feature's warmup")
    ap.add_argument("--end", type=str, default=None,
                    help="UTC window end, ISO date (default: now)")
    a = ap.parse_args()
    end_dt = (datetime.fromisoformat(a.end).replace(tzinfo=timezone.utc)
              if a.end else None)
    asyncio.run(_main(a.days, a.warmup, end_dt))
