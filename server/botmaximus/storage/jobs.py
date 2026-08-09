r"""Scheduled storage jobs (Storage v2.0 §4, §8, §11).

Three loops, run as supervised tasks alongside the collectors:

- **tier-out**, nightly at 00:15 UTC, after the day's boundaries settle;
- **integrity**, hourly;
- **checksum audit**, weekly — a full re-hash is expensive and §8 asks for it
  weekly, not hourly.

They are separate loops rather than one scheduler because their failure modes
are unrelated: the integrity checks going quiet must not stop tier-out, and a
slow checksum audit must not delay either.

## They are loud and they do not stop

Each loop logs, records to Postgres, and continues. An exception cancels one
iteration, never the loop — a storage job that dies silently leaves the exact
condition it was watching for unobserved, which is the failure mode this whole
section exists to prevent.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

UTC = timezone.utc

#: §4: "after daily boundaries settle".
TIER_OUT_HOUR = 0
TIER_OUT_MINUTE = 15


def seconds_until(hour: int, minute: int, now: datetime | None = None) -> float:
    now = now or datetime.now(UTC)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


class TierOutJob:
    name = "tier-out"

    def __init__(self, interval_s: float | None = None) -> None:
        #: Tests inject a short interval; production waits for 00:15 UTC.
        self.interval_s = interval_s

    async def run(self) -> None:
        from botmaximus.storage import snapshots, tiering
        while True:
            delay = (self.interval_s if self.interval_s is not None
                     else seconds_until(TIER_OUT_HOUR, TIER_OUT_MINUTE))
            await asyncio.sleep(delay)
            try:
                # Snapshot BEFORE tiering. The coverage ledger describes days
                # that tier-out is about to drop from the hot window, and the
                # money export reads tables tier-out does not touch — but
                # ordering them this way means a snapshot can never miss a day
                # that was archived and dropped in the same cycle.
                snap = await snapshots.run_nightly()
                log.info("[%s] nightly snapshots: %s", self.name, snap)

                results = await tiering.run_tier_out()
                dropped = [r for r in results if r.dropped]
                refused = [r for r in results if r.checks and not r.verified]
                log.info("[%s] %d day(s) considered, %d dropped, %d refused",
                         self.name, len(results), len(dropped), len(refused))
            except asyncio.CancelledError:
                raise
            except Exception as e:                      # noqa: BLE001
                log.exception("[%s] cycle failed (%s)", self.name, e)


class IntegrityJob:
    name = "integrity"

    def __init__(self, interval_s: float = 3600) -> None:
        self.interval_s = interval_s

    async def run(self) -> None:
        from botmaximus.storage import integrity
        while True:
            await asyncio.sleep(self.interval_s)
            try:
                results = await integrity.run_all()
                failed = [r.name for r in results if not r.passed]
                if failed:
                    log.error("[%s] FAILING: %s", self.name, ", ".join(failed))
                else:
                    log.info("[%s] %d check(s) passed", self.name, len(results))
            except asyncio.CancelledError:
                raise
            except Exception as e:                      # noqa: BLE001
                log.exception("[%s] cycle failed (%s)", self.name, e)


class ChecksumAuditJob:
    name = "checksum-audit"

    def __init__(self, interval_s: float = 7 * 24 * 3600) -> None:
        self.interval_s = interval_s

    async def run(self) -> None:
        from botmaximus.storage import integrity, postgres
        while True:
            await asyncio.sleep(self.interval_s)
            try:
                out = await integrity.check_archive_checksums(limit=5000)
                r = out[0]
                await postgres.execute(
                    "INSERT INTO backup_events (kind, succeeded, detail) "
                    "VALUES ('checksum_audit', %s, %s)",
                    (r.passed, f"checked {r.observed.get('checked')}, "
                               f"failed {r.observed.get('failed')}"))
                if not r.passed:
                    log.error("[%s] BIT ROT: %s", self.name, r.observed)
            except asyncio.CancelledError:
                raise
            except Exception as e:                      # noqa: BLE001
                log.exception("[%s] cycle failed (%s)", self.name, e)


def all_jobs() -> list:
    return [TierOutJob(), IntegrityJob(), ChecksumAuditJob()]
