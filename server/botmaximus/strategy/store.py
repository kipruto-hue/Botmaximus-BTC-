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


async def ensure_indexes() -> None:
    """No-op: keys and indexes are part of `schema.sql`."""
    return None


def _definition_hash(defn: StrategyDefinition) -> str:
    import hashlib
    import json
    blob = json.dumps(defn.to_dict(), sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


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
    import json

    from botmaximus.storage import postgres
    defn_hash = _definition_hash(defn)
    async with postgres.transaction() as conn:
        # The definition is content-addressed and immutable: the same hash
        # across a backtest and a live run is what proves compilation was
        # deterministic (§3.E).
        await conn.execute(
            "INSERT INTO strategy_definitions_blob (definition_hash, definition) "
            "VALUES (%s, %s) ON CONFLICT (definition_hash) DO NOTHING",
            (defn_hash, json.dumps(defn.to_dict(), default=str)))
        # DO UPDATE touches only the mutable columns. `lifecycle_state` is
        # absent deliberately — only `record_transition` may move it, otherwise
        # re-registering would silently resurrect a retired strategy. So are
        # `created_at` (the original registration) and `last_verdict`
        # (re-registration is not a new verdict).
        await conn.execute(
            "INSERT INTO strategies (strategy_id, version, definition_hash, "
            "  lifecycle_state, origin, rationale, signature, warnings) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (strategy_id, version) DO UPDATE SET "
            "  definition_hash = EXCLUDED.definition_hash, "
            "  origin = EXCLUDED.origin, rationale = EXCLUDED.rationale, "
            "  signature = EXCLUDED.signature, warnings = EXCLUDED.warnings, "
            "  updated_at = now()",
            (defn.id, defn.version, defn_hash, defn.lifecycle_state,
             defn.origin, defn.rationale, sorted(signature(defn)),
             warnings or []))


async def record_transition(t: Transition) -> None:
    """Append the event *and* move the state — one transaction, so an audit log
    entry without a corresponding state change (or the reverse) is not
    possible."""
    import json

    from botmaximus.storage import postgres
    async with postgres.transaction() as conn:
        await conn.execute(
            "INSERT INTO strategy_lifecycle_events "
            "(strategy_id, version, from_state, to_state, reason, actor, "
            " verdict, at) "
            "SELECT %s, s.version, %s, %s, %s, %s, %s, %s FROM strategies s "
            "WHERE s.strategy_id = %s "
            "ORDER BY s.version DESC LIMIT 1",
            (t.strategy_id, t.from_state, t.to_state, t.reason, "auto",
             json.dumps(t.verdict, default=str) if t.verdict else None,
             t.at, t.strategy_id))
        if t.verdict is not None:
            await conn.execute(
                "UPDATE strategies SET lifecycle_state = %s, updated_at = %s, "
                "  last_verdict = %s WHERE strategy_id = %s",
                (t.to_state, t.at, json.dumps(t.verdict, default=str),
                 t.strategy_id))
        else:
            await conn.execute(
                "UPDATE strategies SET lifecycle_state = %s, updated_at = %s "
                "WHERE strategy_id = %s",
                (t.to_state, t.at, t.strategy_id))


async def record_verdict(strategy_id: str, verdict: dict) -> None:
    import json

    from botmaximus.storage import postgres
    await postgres.execute(
        "UPDATE strategies SET last_verdict = %s, updated_at = now() "
        "WHERE strategy_id = %s",
        (json.dumps(verdict, default=str), strategy_id))


async def get(strategy_id: str) -> dict | None:
    from botmaximus.storage import postgres
    return await postgres.fetchrow(
        "SELECT s.*, b.definition FROM strategies s "
        "LEFT JOIN strategy_definitions_blob b USING (definition_hash) "
        "WHERE s.strategy_id = %s ORDER BY s.version DESC LIMIT 1",
        (strategy_id,))


async def list_population(states: list[str] | None = None) -> list[dict]:
    from botmaximus.storage import postgres
    sql = ("SELECT s.*, b.definition FROM strategies s "
           "LEFT JOIN strategy_definitions_blob b USING (definition_hash) ")
    params: tuple = ()
    if states:
        sql += "WHERE s.lifecycle_state = ANY(%s) "
        params = (list(states),)
    sql += "ORDER BY s.created_at"
    return await postgres.fetch(sql, params)


async def signatures_for_dedupe(exclude_retired: bool = True):
    """(id, signature) pairs for the §5.7 diversity check.

    Retired strategies are excluded by default: §5.7 exists to keep the *live*
    population diverse, and a repair (§7.3) necessarily resembles the strategy
    it replaces — blocking it against its own retired ancestor would make the
    repair loop unable to ever produce anything.
    """
    from botmaximus.storage import postgres
    sql = "SELECT strategy_id, signature FROM strategies "
    if exclude_retired:
        sql += "WHERE lifecycle_state <> 'retired'"
    rows = await postgres.fetch(sql)
    return [(r["strategy_id"], frozenset(r["signature"] or [])) for r in rows]


async def events(strategy_id: str, limit: int = 50) -> list[dict]:
    from botmaximus.storage import postgres
    return await postgres.fetch(
        "SELECT * FROM strategy_lifecycle_events WHERE strategy_id = %s "
        "ORDER BY at DESC LIMIT %s", (strategy_id, limit))
