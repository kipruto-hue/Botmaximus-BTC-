"""Strategy population store: `strategies` + `strategy_events`.

The population is what §5.7 dedupes against and what §6.2 summarises into the
generator's brief, so it has to hold rejected and retired strategies too — not
just the survivors. A store that only remembers what passed would let the
generator re-propose the same failure forever, and would understate the trial
count the deflated Sharpe is correcting for.

`strategy_events` is the audit log: every transition, with the verdict that
justified it. Lifecycle state is never edited in place without an event.
"""
from __future__ import annotations

from datetime import datetime, timezone

from botmaximus.strategy.lifecycle import Transition
from botmaximus.strategy.schema import StrategyDefinition
from botmaximus.strategy.validator import signature

STRATEGIES = "strategies"
STRATEGY_EVENTS = "strategy_events"


async def ensure_indexes() -> None:
    from botmaximus.db.mongo import get_db
    db = get_db()
    await db[STRATEGIES].create_index([("strategy_id", 1)], unique=True)
    await db[STRATEGIES].create_index([("lifecycle_state", 1)])
    await db[STRATEGY_EVENTS].create_index([("strategy_id", 1), ("at", -1)])


def build_doc(defn: StrategyDefinition, warnings: list[str] | None = None) -> dict:
    return {
        "strategy_id": defn.id,
        "version": defn.version,
        "definition": defn.to_dict(),
        "signature": sorted(signature(defn)),
        "lifecycle_state": defn.lifecycle_state,
        "origin": defn.origin,
        "rationale": defn.rationale,
        "warnings": warnings or [],
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "last_verdict": None,
    }


async def upsert(defn: StrategyDefinition, warnings: list[str] | None = None) -> None:
    """Register (or refresh) a strategy.

    Three fields are insert-only. Lifecycle state, because only
    `record_transition` may move it — otherwise re-registering a strategy would
    silently resurrect a retired one. `created_at`, because it is the original
    registration. And `last_verdict`, because re-registration is not a new
    verdict: overwriting it with None here would erase the gate result that the
    dashboard reads and that Pass C2's generator learns from.
    """
    from botmaximus.db.mongo import get_db
    doc = build_doc(defn, warnings)
    insert_only = {k: doc.pop(k) for k in
                   ("lifecycle_state", "created_at", "last_verdict")}
    await get_db()[STRATEGIES].update_one(
        {"strategy_id": defn.id},
        {"$set": doc, "$setOnInsert": insert_only},
        upsert=True,
    )


async def record_transition(t: Transition) -> None:
    """Append the event *and* move the state — one call, so an audit log entry
    without a corresponding state change (or the reverse) is not possible."""
    from botmaximus.db.mongo import get_db
    db = get_db()
    await db[STRATEGY_EVENTS].insert_one(t.to_doc())
    update = {"lifecycle_state": t.to_state, "updated_at": t.at}
    if t.verdict is not None:
        update["last_verdict"] = t.verdict
    await db[STRATEGIES].update_one({"strategy_id": t.strategy_id}, {"$set": update})


async def record_verdict(strategy_id: str, verdict: dict) -> None:
    from botmaximus.db.mongo import get_db
    await get_db()[STRATEGIES].update_one(
        {"strategy_id": strategy_id},
        {"$set": {"last_verdict": verdict,
                  "updated_at": datetime.now(timezone.utc)}},
    )


async def get(strategy_id: str) -> dict | None:
    from botmaximus.db.mongo import get_db
    return await get_db()[STRATEGIES].find_one({"strategy_id": strategy_id},
                                               {"_id": 0})


async def list_population(states: list[str] | None = None) -> list[dict]:
    from botmaximus.db.mongo import get_db
    q = {"lifecycle_state": {"$in": states}} if states else {}
    cursor = get_db()[STRATEGIES].find(q, {"_id": 0}).sort("created_at", 1)
    return [d async for d in cursor]


async def signatures_for_dedupe(exclude_retired: bool = True):
    """(id, signature) pairs for the §5.7 diversity check.

    Retired strategies are excluded by default: §5.7 exists to keep the *live*
    population diverse, and a repair (§7.3) necessarily resembles the strategy
    it replaces — blocking it against its own retired ancestor would make the
    repair loop unable to ever produce anything.
    """
    from botmaximus.db.mongo import get_db
    q = {"lifecycle_state": {"$ne": "retired"}} if exclude_retired else {}
    cursor = get_db()[STRATEGIES].find(q, {"_id": 0, "strategy_id": 1, "signature": 1})
    return [(d["strategy_id"], frozenset(d.get("signature") or []))
            async for d in cursor]


async def events(strategy_id: str, limit: int = 50) -> list[dict]:
    from botmaximus.db.mongo import get_db
    cursor = get_db()[STRATEGY_EVENTS].find(
        {"strategy_id": strategy_id}, {"_id": 0}).sort("at", -1).limit(limit)
    return [d async for d in cursor]
