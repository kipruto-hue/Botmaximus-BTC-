r"""Storage degradation policy (Storage v2.0 §7).

Postgres is the single point of failure for decisions, and §7 is blunt about
the consequence: when it is unreachable the system **halts**. It does not queue
writes, spool to disk, or carry on and reconcile later.

That looks harsh until you name the alternative. A queued write that never
lands produces a trade the ledger has no record of — so the position is real,
the audit trail is not, and the disagreement surfaces during the incident you
needed the ledger for. A rejected trade costs an opportunity; a lost write
costs the ability to know what happened.

The asymmetry with object storage is deliberate and runs the other way:

| Store | Unreachable | Why |
|---|---|---|
| Postgres | **L2 halt** | it holds decisions; there is no safe substitute |
| Object storage | continue, defer tier-out | the hot window still has the data, and tier-out refuses to drop an unverified day |

So a Parquet outage costs archive latency, and a Postgres outage costs trading.
Neither costs data.

## The ring buffer is not a fallback store

When Postgres is what is down, a `degraded` event cannot be written to
Postgres. §7 allows a bounded in-memory ring buffer, flushed on recovery. It
holds telemetry ONLY — never records, never orders, never anything a consumer
would later read as fact. It is bounded so that a long outage drops old
telemetry rather than exhausting memory on the box that is already in trouble.
"""
from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timezone

from botmaximus.storage.postgres import PostgresUnavailable

log = logging.getLogger(__name__)

UTC = timezone.utc

#: Telemetry only, and bounded. A process in trouble must not also run out of
#: memory remembering that it is in trouble.
_ring: deque[tuple[datetime, str, str, dict]] = deque(maxlen=1000)

L2_REASON = "postgres_unreachable"


def buffer_degraded(label: str, reason: str, context: dict | None = None) -> None:
    _ring.append((datetime.now(UTC), label, reason, context or {}))


def buffered() -> list[dict]:
    return [{"at": t.isoformat(), "label": la, "reason": r, "context": c}
            for t, la, r, c in _ring]


async def flush_buffer() -> int:
    """Write buffered telemetry once Postgres answers again."""
    if not _ring:
        return 0
    import json

    from botmaximus.storage import postgres
    n = 0
    while _ring:
        at, label, reason, context = _ring[0]
        try:
            await postgres.execute(
                "INSERT INTO telemetry_events (kind, label, reason, context, at) "
                "VALUES ('degraded', %s, %s, %s, %s)",
                (label, reason, json.dumps(context, default=str), at))
        except PostgresUnavailable:
            break                       # still down; keep what is left
        _ring.popleft()
        n += 1
    if n:
        log.info("flushed %d buffered degraded event(s) after recovery", n)
    return n


async def on_postgres_unavailable(risk, error: Exception) -> None:
    """§7: halt. Not a retry loop, not a fallback path.

    `risk` is the RiskCore whose L2 stops new positions and lets existing ones
    be managed to exit. The halt is recorded in the ring buffer because the
    place it would normally be recorded is the thing that is down.
    """
    buffer_degraded(L2_REASON, str(error))
    log.critical("POSTGRES UNREACHABLE — L2 halt (%s)", error)
    try:
        await risk.kills.halt_portfolio(L2_REASON)
    except Exception:                                   # noqa: BLE001
        # Persisting the kill also needs Postgres. The in-memory flag is still
        # set, which is what blocks the next entry; on restart `load()` reads
        # from a database that is by then reachable.
        log.critical("L2 halt set in memory; could not persist it")


async def health(risk=None) -> dict:
    """Current storage health, for the dashboard and the supervisor."""
    from botmaximus.storage import postgres
    pg_ok = await postgres.ping()
    archive_ok, archive_error = True, None
    try:
        from botmaximus.storage import records as store
        a = store.archive()
        a.backend.exists(a.archive_bucket, "__healthcheck__")
    except Exception as e:                              # noqa: BLE001
        archive_ok, archive_error = False, str(e)

    if pg_ok:
        await flush_buffer()

    return {
        "postgres": pg_ok,
        "archive": archive_ok,
        "archive_error": archive_error,
        # An archive outage is survivable; a Postgres outage is not.
        "status": ("ok" if pg_ok and archive_ok
                   else "degraded" if pg_ok else "halted"),
        "buffered_events": len(_ring),
    }


def reset_for_tests() -> None:
    _ring.clear()
