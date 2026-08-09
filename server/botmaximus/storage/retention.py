r"""Retention policy (Storage v2.0 §9).

Generous with decisions, stingy with the raw firehose. The table below is the
policy in executable form: `FOREVER` classes have no expiry mechanism at all —
not a long one, none — because a retention period that exists can be shortened
by accident, and the trial ledger and the money records are the two things this
system cannot afford to lose.

**Retention changes are operator commits, never automatic** (§9). Nothing here
runs on a timer; `expired_quarantine_objects` reports what is eligible and the
operator decides. The one class with a real expiry is quarantine (2 years), and
even that is deliberately a report rather than a delete.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

FOREVER = None

#: class → retention in days, or FOREVER.
POLICY: dict[str, int | None] = {
    "market_data": FOREVER,          # the archive is cheap; backtests need history
    "coverage_ledger": FOREVER,      # future backtests query historical coverage
    "features": FOREVER,
    "strategies": FOREVER,
    "trials": FOREVER,               # statistical honesty; see trials.py
    "backtest_metadata": FOREVER,    # reproducibility
    "orders_fills_ledger": FOREVER,  # money records
    "llm_provenance": FOREVER,       # audit, and re-running under a dead model
    "backtest_signals": 730,         # 2y, then aggregated: volume vs value
    "quarantine": 730,               # long enough for post-incident review
    "app_logs": 90,                  # a debugging window, not the audit record
}


def is_forever(cls: str) -> bool:
    if cls not in POLICY:
        raise KeyError(
            f"unknown retention class {cls!r}. Every stored thing needs an "
            f"explicit answer to 'how long', including 'forever'.")
    return POLICY[cls] is FOREVER


def cutoff(cls: str, now: datetime | None = None) -> datetime | None:
    """The instant before which `cls` data is eligible for deletion, or None if
    it is kept forever."""
    days = POLICY[cls] if cls in POLICY else None
    if days is FOREVER:
        return None
    return (now or datetime.now(timezone.utc)) - timedelta(days=days)


async def expired_quarantine_objects(now: datetime | None = None) -> list[dict]:
    """Quarantine objects past their 2-year retention.

    Reports only. §14 forbids deleting quarantined data before its retention
    expires and forbids moving it to production; deleting it *after* expiry is
    permitted but is an operator action, so this hands over a list rather than
    acting on it.
    """
    from botmaximus.config import settings
    from botmaximus.storage import postgres

    before = cutoff("quarantine", now)
    if before is None:
        return []
    return await postgres.fetch(
        "SELECT * FROM storage_manifest WHERE bucket = %s AND written_at < %s "
        "ORDER BY written_at", (settings.bucket_quarantine, before))


def summary() -> list[dict]:
    return [{"class": k, "retention_days": v, "forever": v is FOREVER}
            for k, v in POLICY.items()]
