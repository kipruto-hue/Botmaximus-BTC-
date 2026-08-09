r"""Degradation recorder — constitution §11: *degradation is visible, never silent.*

Every layer that falls back to a safer behaviour records it here. Two sinks,
because they answer different questions:

- an **in-memory counter**, so "is anything degraded right now?" is answerable
  without a database round-trip and shows on the dashboard;
- a **`degraded_events` document**, so "when did this start, and what were the
  circumstances?" is answerable after the fact.

## Why this exists rather than a log line

A log line is a fallback nobody counts. The failure mode this project keeps
meeting is the silent one — a socket that stays connected while delivering
nothing, a supervisor that logs its start line and dies, a backup that verifies
the wrong database. In every case the system was *degraded and reporting
success*. A degradation that increments a counter cannot hide: the number is
either zero or it is not.

## Recording is best-effort, but never silent about its own failure

If Mongo is unavailable the counter still increments and a WARNING is logged.
Losing the audit trail must not take down the caller — a degradation recorder
that raises would turn a survivable fallback into an outage.

This module deliberately lives outside `pipeline/`: the build constitution
forbids modifying the data-layer telemetry, and degradation is not a data-layer
concern anyway.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone

log = logging.getLogger(__name__)

DEGRADED_EVENTS = "degraded_events"

#: label -> count, for this process lifetime.
_counts: Counter[str] = Counter()


async def ensure_indexes() -> None:
    """No-op: indexes are part of `schema.sql`."""
    return None


async def record(label: str, reason: str, **context) -> None:
    """Note that a layer took a safer path than intended.

    `label` is a stable, low-cardinality key that can be counted and alerted on
    (`venue_fee_rate_unavailable`, `scrutiny_provider_timeout`). `reason` is the
    human sentence. `context` is whatever a later investigation would want.
    """
    _counts[label] += 1
    log.warning("DEGRADED [%s] %s %s", label, reason, context or "")
    try:
        await _write(label, reason, context)
    except Exception as e:                      # noqa: BLE001
        # The counter already moved, so the degradation is not lost. Raising
        # here would let a bookkeeping failure escalate a survivable fallback
        # into an outage.
        log.error("could not persist degraded event %s: %s", label, e)


def record_sync(label: str, reason: str, **context) -> None:
    """Synchronous callers (the risk core's deterministic checks are not async).

    The counter and the log line happen immediately; the durable event is
    scheduled onto the running loop when there is one. Outside a loop — in a
    unit test, say — the counter still moves, which is what assertions read.
    """
    import asyncio

    _counts[label] += 1
    log.warning("DEGRADED [%s] %s %s", label, reason, context or "")
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(_persist(label, reason, context))


async def _write(label: str, reason: str, context: dict) -> None:
    """Degradations land in `telemetry_events` with kind='degraded' (§7)."""
    import json

    from botmaximus.storage import postgres
    await postgres.execute(
        "INSERT INTO telemetry_events (kind, label, reason, context, at) "
        "VALUES ('degraded', %s, %s, %s, %s)",
        (label, reason, json.dumps(context, default=str),
         datetime.now(timezone.utc)))


async def _persist(label: str, reason: str, context: dict) -> None:
    try:
        await _write(label, reason, context)
    except Exception as e:                      # noqa: BLE001
        log.error("could not persist degraded event %s: %s", label, e)


def counts() -> dict[str, int]:
    """Snapshot for the dashboard/telemetry endpoint."""
    return dict(_counts)


def total() -> int:
    return sum(_counts.values())


def reset_for_tests() -> None:
    """Test-only. Never call from application code: a degradation count that
    application code can clear is a count that will be cleared."""
    _counts.clear()
