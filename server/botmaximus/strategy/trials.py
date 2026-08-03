"""Lifetime trial ledger — the input the deflated Sharpe is actually asking for.

`metrics.expected_max_sharpe` needs the number of trials the winner was selected
from. Before this module that number came from `settings.bt_candidate_trials`,
a constant set to 1, and `expected_max_sharpe` returns a benchmark of 0.0 for
`n_trials <= 1` — so the multiple-testing correction collapsed into an ordinary
probabilistic Sharpe and the gate's headline defence was a no-op. Harmless while
five hand-written seeds were the whole population; wrong the moment Pass C2
generates in bulk, which is the case the correction exists for.

**The count must be persistent and lifetime.** Search intensity does not reset
when a process restarts or a generation cycle ends; a strategy that looks
exceptional is exceptional *relative to everything ever tried against this same
price history*, across every cycle, forever.

## What counts as a trial

One trial = one (structural signature, config hash) pair ever evaluated.

- **Total evaluations, not distinct ideas.** Trying fifty variants of one idea is
  genuinely fifty looks at the data, and the max over fifty correlated variants
  is less extreme than over fifty independent ones — so this over-corrects
  slightly. That is the correct direction of error for a gate whose purpose is
  to reject: under-counting silently weakens it, over-counting only makes it
  harder to pass. Distinct signatures are tracked too, but only for reporting.
- **Re-running an identical config does not count.** Keyed on the config hash,
  so replaying the exact same evaluation — during development, after a restart,
  from a retry — is not a new look at the data. Anything that differs is.
- **Repairs count.** A §7.3 repair is a new search step by construction; it will
  differ in signature or config and land as its own trial.

The ledger is append-only and never decremented. There is no API to reset it,
deliberately: a trial count that can be lowered is a trial count that will be.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from botmaximus.strategy.schema import StrategyDefinition
from botmaximus.strategy.validator import signature

TRIAL_LEDGER = "trial_ledger"


async def ensure_indexes() -> None:
    from botmaximus.db.mongo import get_db
    db = get_db()
    await db[TRIAL_LEDGER].create_index([("sig_hash", 1), ("config_hash", 1)],
                                        unique=True)
    await db[TRIAL_LEDGER].create_index([("sig_hash", 1)])


def signature_hash(defn: StrategyDefinition) -> str:
    """Stable digest of the §5.7 structural signature. Sorted before hashing
    because a frozenset has no order and the key must survive a restart."""
    tokens = sorted(signature(defn))
    return hashlib.sha256("|".join(tokens).encode()).hexdigest()[:16]


async def record(defn: StrategyDefinition, config_hash: str) -> int:
    """Register this evaluation and return the lifetime trial count to hand to
    the deflated Sharpe.

    Upsert-on-insert: an identical (signature, config) re-run touches
    `last_seen` and increments `replays`, but does not add a trial.
    """
    from botmaximus.db.mongo import get_db
    db = get_db()
    now = datetime.now(timezone.utc)
    sig = signature_hash(defn)
    await db[TRIAL_LEDGER].update_one(
        {"sig_hash": sig, "config_hash": config_hash},
        {
            "$setOnInsert": {"sig_hash": sig, "config_hash": config_hash,
                             "strategy_id": defn.id, "first_seen": now},
            "$set": {"last_seen": now},
            "$inc": {"replays": 1},
        },
        upsert=True,
    )
    return await count()


async def count() -> int:
    """Lifetime trials. Never resets, never decrements."""
    from botmaximus.db.mongo import get_db
    return await get_db()[TRIAL_LEDGER].count_documents({})


async def distinct_ideas() -> int:
    """Distinct structural signatures ever evaluated — reporting only. This is
    NOT the number fed to the deflated Sharpe; see the module docstring."""
    from botmaximus.db.mongo import get_db
    sigs = await get_db()[TRIAL_LEDGER].distinct("sig_hash")
    return len(sigs)


async def summary() -> dict:
    return {"trials": await count(), "distinct_ideas": await distinct_ideas()}
