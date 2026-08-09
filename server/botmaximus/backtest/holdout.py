"""Sealed holdout (§5.4, strengthened) — the one window search may not touch.

Walk-forward inside `validation.py` is honest for a *single* strategy: sequential
folds, no shuffling. It is not out-of-sample for a *population*. Every candidate
Pass C2 generates is selected against the same 1,051,199 bars, so those folds get
re-used thousands of times and out-of-sample decays into in-sample by attrition.
Nothing in the numbers announces when that has happened.

So a slice of history is sealed: the most recent `holdout_days`. The generator
never sees it, the search path is refused if it overlaps it, and a strategy may
be evaluated on it **once**.

## Once, and then it is burned

The holdout is a consumable, not a test suite. Re-running a strategy against it
after a tweak is exactly the attrition this module exists to prevent — the
second look is already selection. So `assert_unburned` refuses a second holdout
run for a strategy id, and the burn is recorded as a lifecycle event rather than
a flag, so it survives a restart and shows up in the audit log.

A repair of a burned strategy is a new strategy with a new id, and it gets its
own single look. That is intentional: the cost of a repair should be visible.

Reserving the *most recent* window (rather than the oldest) is deliberate — it is
the segment closest to the conditions a strategy would actually trade in, and
the one whose regime the generator is least able to infer from the training
window's tail.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from botmaximus.config import settings

HOLDOUT_EVENT = "holdout_burned"


class HoldoutViolation(Exception):
    """Raised instead of silently trimming: a window that reaches into the
    holdout is a question the caller should not be asking."""


def boundary(now: datetime | None = None) -> datetime:
    """First instant of the sealed window. Everything at or after this is
    off-limits to the search path."""
    now = now or datetime.now(timezone.utc)
    return now - timedelta(days=settings.holdout_days)


def clip_end(end: datetime, now: datetime | None = None) -> datetime:
    """Latest end a *search* window may use. Callers that build their own
    windows should clip with this rather than discover the refusal later."""
    return min(end, boundary(now))


def assert_outside(start: datetime, end: datetime,
                   now: datetime | None = None) -> None:
    """Refuse a search window that overlaps the sealed slice."""
    edge = boundary(now)
    if end > edge:
        raise HoldoutViolation(
            f"window ends {end.isoformat()}, inside the sealed holdout that "
            f"begins {edge.isoformat()} ({settings.holdout_days}d) — the search "
            f"path may not read it. Clip the end, or run with holdout=True to "
            f"spend this strategy's single look.")


def window(now: datetime | None = None) -> tuple[datetime, datetime]:
    """The holdout window itself, for the one confirmation run."""
    now = now or datetime.now(timezone.utc)
    return boundary(now), now


async def is_burned(strategy_id: str) -> bool:
    from botmaximus.storage import postgres
    row = await postgres.fetchrow(
        "SELECT 1 AS x FROM strategy_events "
        "WHERE strategy_id = %s AND event = %s LIMIT 1",
        (strategy_id, HOLDOUT_EVENT))
    return row is not None


async def assert_unburned(strategy_id: str) -> None:
    if await is_burned(strategy_id):
        raise HoldoutViolation(
            f"{strategy_id} has already spent its single holdout evaluation. "
            f"A second look is selection, not validation. Repair it into a new "
            f"strategy id if you want another.")


async def record_burn(strategy_id: str, verdict: dict, window_: tuple) -> None:
    """Append-only, and written *before* the verdict is returned to the caller —
    a holdout run that crashes after the backtest still counts as spent."""
    import json

    from botmaximus.storage import postgres
    await postgres.execute(
        "INSERT INTO strategy_events (strategy_id, event, detail) "
        "VALUES (%s, %s, %s)",
        (strategy_id, HOLDOUT_EVENT, json.dumps({
            "window": [window_[0].isoformat(), window_[1].isoformat()],
            "verdict": verdict,
        }, default=str)))
