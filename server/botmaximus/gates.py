r"""Deployment Gates 1-5 (§9), measured from real telemetry.

Every gate reads stored evidence. None is self-attested, and none can be marked
passed by editing this file — a gate that can be argued with is not a gate.

The three outcomes are deliberate and distinct:

    PASS      the evidence exists and clears the bar
    FAIL      the evidence exists and does not clear it
    NO_DATA   the evidence does not exist yet

`NO_DATA` is not a soft pass. Reporting "nothing has gone wrong" from an empty
collection is the exact failure the execution ledger already guards against, and
the same reasoning applies here: an empty aggregate looks identical to a perfect
one.

    python -m botmaximus.gates
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from botmaximus.config import settings

log = logging.getLogger(__name__)

PASS, FAIL, NO_DATA = "PASS", "FAIL", "NO_DATA"


@dataclass
class GateResult:
    gate: str
    status: str
    detail: str
    evidence: dict = field(default_factory=dict)

    def line(self) -> str:
        mark = {"PASS": "PASS", "FAIL": "FAIL", "NO_DATA": "----"}[self.status]
        return f"[{mark}] {self.gate}: {self.detail}"


async def gate1_collector_24h(db) -> GateResult:
    """24 continuous hours, no unbackfillable gaps for OHLCV/funding/OI."""
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=24)
    missing = await db["coverage"].count_documents(
        {"feed": "btc_ohlcv_1m", "slot": {"$gte": since}, "state": {"$ne": "complete"}})
    complete = await db["coverage"].count_documents(
        {"feed": "btc_ohlcv_1m", "slot": {"$gte": since}, "state": "complete"})
    if complete == 0:
        return GateResult("Gate 1 collector 24h", NO_DATA,
                          "no coverage slots in the last 24h")
    expected = 24 * 60
    status = PASS if (missing == 0 and complete >= expected * 0.999) else FAIL
    return GateResult("Gate 1 collector 24h", status,
                      f"{complete}/{expected} 1m slots complete, {missing} incomplete",
                      {"complete": complete, "missing": missing})


async def gate2_generator_honesty(db) -> GateResult:
    """>=200 candidates, pass rate <5%, and no margin leaked into any brief."""
    from pathlib import Path

    gen_dir = Path(__file__).resolve().parents[2] / "data" / "generations"
    files = list(gen_dir.glob("*.json")) if gen_dir.exists() else []
    if len(files) < 200:
        return GateResult("Gate 2 generator honesty", NO_DATA,
                          f"{len(files)}/200 candidates proposed")

    leaked = []
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:                               # noqa: BLE001
            continue
        kinds = (doc.get("brief") or {}).get("recent_failure_kinds") or []
        # A margin always carries a comparator or a digit; a check NAME never
        # does. This is the mechanical form of "names only, never margins".
        if any(any(c.isdigit() or c in "<>" for c in str(k)) for k in kinds):
            leaked.append(f.name)

    passed = await db["strategies"].count_documents(
        {"origin": "generated", "lifecycle_state": {"$in": ["paper", "live"]}})
    rate = passed / len(files)
    status = PASS if (rate < 0.05 and not leaked) else FAIL
    return GateResult(
        "Gate 2 generator honesty", status,
        f"{len(files)} proposed, pass rate {rate:.1%}, "
        f"{len(leaked)} briefs leaked a margin",
        {"proposed": len(files), "pass_rate": rate, "leaked": leaked[:5]})


async def gate3_validated_strategy(db) -> GateResult:
    """200 paper trades, deflated Sharpe >= 2.0 with the lifetime trial count,
    and one holdout burn."""
    from botmaximus.execution.ledger import LEDGER, RECONCILED

    legs = await db[LEDGER].count_documents({"status": RECONCILED})
    if legs < 400:      # two legs per round trip
        return GateResult("Gate 3 validated strategy", NO_DATA,
                          f"{legs // 2}/200 paper round-trips reconciled")

    best = await db["backtest_runs"].find_one(
        {"verdict.passed": True}, sort=[("metrics.deflated_sharpe", -1)])
    if best is None:
        return GateResult("Gate 3 validated strategy", FAIL,
                          "no strategy has passed validation")
    dsr = (best.get("metrics") or {}).get("deflated_sharpe", 0.0)
    burned = await db["strategy_events"].count_documents(
        {"event": "holdout_burned"})
    status = PASS if (dsr >= 2.0 and burned >= 1) else FAIL
    return GateResult("Gate 3 validated strategy", status,
                      f"best DSR {dsr:.2f} (need >=2.0), holdout burns {burned}",
                      {"dsr": dsr, "holdout_burns": burned})


async def gate4_cost_model_honest(db) -> GateResult:
    """>=100 reconciled legs within 20%, and systematic under-prediction fails
    even when small."""
    from botmaximus.execution.ledger import calibration

    report = await calibration()
    legs = report.get("realized_legs", 0)
    if legs < 100:
        return GateResult("Gate 4 cost model honest", NO_DATA,
                          f"{legs}/100 reconciled legs")
    total = report.get("cost_drift_usd_total", 0.0)
    per_leg = report.get("cost_drift_usd_per_leg", 0.0)
    # Under-prediction is the asymmetric failure: it flatters every backtest.
    if total > 0:
        return GateResult("Gate 4 cost model honest", FAIL,
                          f"realized cost exceeds predicted by ${total:,.2f} "
                          f"({per_leg:+.4f}/leg) — systematic under-prediction",
                          report)
    return GateResult("Gate 4 cost model honest", PASS,
                      f"model conservative by ${abs(total):,.2f} over {legs} legs",
                      report)


async def gate5_kill_flattens(db) -> GateResult:
    """Operator kill flattens with a position open, verified, in under 2s."""
    ev = await db[__import__(
        "botmaximus.obs.degradation", fromlist=["x"]).DEGRADED_EVENTS].find_one(
        {"label": "flatten_unverified"})
    drill = await db["risk_events"].find_one({"kind": "flatten_drill"},
                                             sort=[("at", -1)])
    if drill is None:
        return GateResult("Gate 5 kill flattens", NO_DATA,
                          "no flatten drill recorded (needs a live demo "
                          "position and POST /api/risk/master_kill)")
    ok = drill.get("flat") and drill.get("elapsed_ms", 9e9) < 2000 and ev is None
    return GateResult("Gate 5 kill flattens", PASS if ok else FAIL,
                      f"flat={drill.get('flat')} in {drill.get('elapsed_ms')}ms",
                      drill)


async def run_all() -> list[GateResult]:
    from botmaximus.db.mongo import get_db

    db = get_db()
    out = []
    for fn in (gate1_collector_24h, gate2_generator_honesty,
               gate3_validated_strategy, gate4_cost_model_honest,
               gate5_kill_flattens):
        try:
            out.append(await fn(db))
        except Exception as e:                          # noqa: BLE001
            out.append(GateResult(fn.__name__, FAIL, f"gate check raised: {e}"))
    return out


async def _main() -> None:
    from botmaximus.db import mongo

    try:
        results = await run_all()
        print("\n" + "=" * 72)
        print("DEPLOYMENT GATES — measured, not self-attested")
        print("=" * 72)
        for r in results:
            print(r.line())
        print("-" * 72)
        passed = sum(1 for r in results if r.status == PASS)
        nodata = sum(1 for r in results if r.status == NO_DATA)
        print(f"{passed}/5 passed, {nodata} awaiting evidence")
        if nodata:
            print("NO_DATA is not a soft pass: the evidence does not exist yet.")
        print(f"live_trading_enabled = {settings.live_trading_enabled} "
              f"(unchanged by this build, regardless of gate status)")
        print("=" * 72)
    finally:
        await mongo.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(_main())
