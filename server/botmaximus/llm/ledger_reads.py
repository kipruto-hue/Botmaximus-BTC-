r"""The ledger read seam — where prompt context comes from.

§4: the four ledgers *are* the memory. `trial_ledger` holds what passed
validation and why, `execution_ledger` holds predicted-vs-realized cost,
`scrutiny_events` holds every verdict, `generations/*.json` holds every
proposal. On each call we assemble context from them and hand it to a fresh,
stateless model.

Every read the prompt builders need goes through this one module. Two reasons,
and the second is the operational one:

1. **Coarsening happens at a single boundary.** Digests are reduced to names,
   buckets and counts here, so there is exactly one place to audit for
   leakage rather than a rule repeated at each call site.
2. **The store is swappable.** These functions define *what* is read, not
   *where from*. When the collected-history database arrives, it is
   substituted here and neither prompt builder changes.

Nothing in this module returns a margin, a price, or a P&L figure.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from botmaximus.strategy.generator import coarsen


@dataclass(frozen=True)
class PopulationSummary:
    """Counts and gaps — never definitions.

    Full definitions of live strategies are withheld deliberately: showing the
    model exactly what passed produces imitations of it, which the diversity
    gate rejects, wasting trials and narrowing the population toward one bet.
    """
    live_count: int
    by_regime: dict[str, int]
    by_feature_family: dict[str, int]
    gaps: list[str]


async def population_summary(db) -> PopulationSummary:
    rows = [d async for d in db["strategies"].find(
        {"lifecycle_state": {"$in": ["paper", "live", "candidate"]}},
        {"_id": 0, "definition": 1, "lifecycle_state": 1, "signature": 1})]

    by_regime: dict[str, int] = {}
    by_family: dict[str, int] = {}
    for r in rows:
        defn = r.get("definition") or {}
        for bucket in (defn.get("regime_scope") or ["unscoped"]):
            by_regime[bucket] = by_regime.get(bucket, 0) + 1
        for token in (r.get("signature") or []):
            if token.startswith("f:"):
                fam = token[2:].split("@")[0]
                by_family[fam] = by_family.get(fam, 0) + 1

    from botmaximus.backtest.regimes import REGIME_BUCKETS
    labels = (REGIME_BUCKETS if isinstance(REGIME_BUCKETS, (list, tuple))
              else ["uptrend", "downtrend", "range"])
    gaps = [f"no live strategy scoped to {b}" for b in labels
            if by_regime.get(b, 0) == 0]

    return PopulationSummary(len(rows), by_regime, by_family, gaps)


async def recent_failure_kinds(db, limit: int = 50) -> list[str]:
    """Coarsened failing-check NAMES from recent rejected runs (§2.C.6).

    This is the generator's "learning from mistakes": it sees which shapes keep
    failing and steers away. It cannot tune toward a threshold, because the
    numbers never leave this function.
    """
    rows = [d async for d in db["backtest_runs"].find(
        {"verdict.passed": False}, {"_id": 0, "verdict": 1}
    ).sort("created_at", -1).limit(limit)]
    reasons = [r for row in rows for r in (row.get("verdict") or {}).get("reasons", [])]
    return coarsen(reasons)


async def recent_acceptance_signatures(db, limit: int = 10) -> list[str]:
    """Structural signatures of what recently passed — not definitions (§2.C.7).

    A signature says "this shape worked"; a definition is a template to copy.
    """
    rows = [d async for d in db["strategies"].find(
        {"lifecycle_state": {"$in": ["paper", "live"]}},
        {"_id": 0, "signature": 1}
    ).sort("updated_at", -1).limit(limit)]
    out = []
    for r in rows:
        sig = sorted(r.get("signature") or [])
        out.append("+".join(t for t in sig if t.startswith(("f:", "dir:"))))
    return [s for s in out if s]


async def coverage_summary(db, feeds: list[str]) -> dict[str, str]:
    """How much clean forward history each feed has, as a coarse label.

    Tells the generator which strategy classes are testable at all — proposing
    an order-book strategy when the book has hours of history wastes a trial on
    something that can only ever be refused for coverage.
    """
    out: dict[str, str] = {}
    for feed in feeds:
        oldest = await db["coverage"].find_one({"feed": feed}, sort=[("slot", 1)])
        newest = await db["coverage"].find_one({"feed": feed}, sort=[("slot", -1)])
        if not oldest or not newest:
            out[feed] = "none"
            continue
        days = (newest["slot"] - oldest["slot"]).days
        out[feed] = ("years" if days > 365 else "months" if days > 60
                     else "weeks" if days > 14 else "days")
    return out


async def scrutiny_outcome_digest(db, window: int = 100) -> dict:
    """How recent verdicts turned out (§3.C.5).

    Scrutiny's form of learning from its own mistakes: if approving under some
    conditions has been costly, the digest shows it, and a near-zero temperature
    means the model responds to that consistently rather than creatively.

    Counts and rates only — no per-trade P&L reaches the prompt.
    """
    rows = [d async for d in db["scrutiny_events"].find(
        {"realized_known": True}, {"_id": 0, "verdict": 1, "realized_adverse": 1}
    ).sort("at", -1).limit(window)]
    approves = [r for r in rows if r.get("verdict") == "APPROVE"]
    vetoes = [r for r in rows if r.get("verdict") == "VETO"]
    bad_approves = sum(1 for r in approves if r.get("realized_adverse"))
    good_vetoes = sum(1 for r in vetoes if r.get("realized_adverse"))
    return {
        "sample": len(rows),
        "approvals": len(approves),
        "approvals_that_went_adverse": bad_approves,
        "vetoes": len(vetoes),
        "vetoes_that_avoided_adverse": good_vetoes,
    }


def generations_count(gen_dir: Path | None = None) -> int:
    d = gen_dir or (Path(__file__).resolve().parents[3] / "data" / "generations")
    return len(list(d.glob("*.json"))) if d.exists() else 0
