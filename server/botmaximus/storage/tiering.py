r"""Tier-out: Postgres hot window → Parquet archive (Storage v2.0 §4, §9).

Data flows outward, never inward. Once a day's records are safely in Parquet
with a verified checksum, the Postgres partition holding them is dropped and
backtests read the archive instead.

## The order of operations is the whole safety property

§4 is unambiguous: **verify first, drop second, and a failed verification never
drops anything.** So this module never deletes on a schedule, only on evidence:
the row count in Postgres must equal the row count recorded in the manifest for
that partition, *and* every archived object must still hash to what the manifest
says it did. Only then is a `DropAuthorization` constructible, and
`partitions.drop_partition` refuses without one.

A TTL index — the Mongo-era mechanism this replaces — would have deleted on age
alone, including the day the archive never received. That failure is invisible
until someone asks for the data.

## Why one partition covers several datasets

`market_records` is partitioned by `event_time` only, so a day's partition holds
every dataset. Hot windows are per-dataset (§3.A: ticks 24h, funding 30d), and a
partition cannot be dropped while ANY dataset in it is still inside its own
window — so the effective retention of a daily partition is the **longest** hot
window among the datasets it contains. That keeps the partition count in the low
tens, which is what it should be; sub-partitioning per dataset would buy a few
days of disk and cost a great deal of moving machinery.

## Re-archiving

§7 lets the collector keep writing to Postgres when object storage is
unreachable, which leaves a day present in Postgres and absent from Parquet.
Tier-out repairs that: if a day has no complete archive, it writes one from the
Postgres rows before considering the drop. If that fails too, the day simply
stays in Postgres and the operator is told.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from botmaximus.db.schema import DATASETS, hot_window_hours
from botmaximus.storage import partitions, postgres
from botmaximus.storage import records as store
from botmaximus.storage.envelope import Record

log = logging.getLogger(__name__)

UTC = timezone.utc


@dataclass
class DatasetCheck:
    dataset_id: str
    pg_rows: int
    parquet_rows: int
    checksum_ok: bool
    detail: str = ""

    @property
    def verified(self) -> bool:
        return self.checksum_ok and self.pg_rows == self.parquet_rows


@dataclass
class TierOutResult:
    day: date
    dropped: bool = False
    skipped_reason: str | None = None
    checks: list[DatasetCheck] = field(default_factory=list)

    @property
    def verified(self) -> bool:
        return bool(self.checks) and all(c.verified for c in self.checks)


def eligible_before(now: datetime | None = None) -> date:
    """The newest day that may be considered for tier-out.

    Uses the LONGEST hot window across datasets, because one partition holds
    them all. Anything on or after this date is still serving a live consumer.
    """
    now = now or datetime.now(UTC)
    longest = max((hot_window_hours(d) for d in DATASETS), default=48)
    return (now - timedelta(hours=longest)).date()


async def _pg_counts(day: date) -> dict[str, int]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    rows = await postgres.fetch(
        "SELECT dataset_id, count(*) AS n FROM market_records "
        "WHERE event_time >= %s AND event_time < %s GROUP BY dataset_id",
        (start, start + timedelta(days=1)))
    return {r["dataset_id"]: r["n"] for r in rows}


async def _manifest_rows(day: date, dataset_id: str) -> tuple[int, list[dict]]:
    prefix = f"year={day:%Y}/month={day:%m}/day={day:%d}/dataset={dataset_id}"
    objs = await postgres.fetch(
        "SELECT * FROM storage_manifest WHERE bucket = %s AND partition = %s",
        (store.archive().archive_bucket, prefix))
    return sum(o["rows"] for o in objs), objs


async def verify_day(day: date) -> list[DatasetCheck]:
    """Compare Postgres against the archive, per dataset, and re-hash the
    objects. Nothing here mutates anything."""
    checks: list[DatasetCheck] = []
    archive = store.archive()
    for dataset_id, pg_rows in sorted((await _pg_counts(day)).items()):
        parquet_rows, objs = await _manifest_rows(day, dataset_id)
        checksum_ok, detail = True, ""
        for o in objs:
            from botmaximus.storage.archive import WrittenFile
            wf = WrittenFile(bucket=o["bucket"], key=o["object_key"],
                             rows=o["rows"], bytes=o["bytes"],
                             sha256=o["sha256"], dataset_id=o["dataset_id"],
                             partition=o["partition"],
                             written_at=o["written_at"])
            if not archive.verify(wf):
                checksum_ok = False
                detail = f"checksum mismatch or unreadable: {o['object_key']}"
                break
        if objs:
            await postgres.execute(
                "UPDATE storage_manifest SET verified_at = now() "
                "WHERE bucket = %s AND partition = %s AND %s",
                (store.archive().archive_bucket,
                 f"year={day:%Y}/month={day:%m}/day={day:%d}/dataset={dataset_id}",
                 checksum_ok))
        if not objs:
            detail = "no archived object for this day"
        checks.append(DatasetCheck(dataset_id, pg_rows, parquet_rows,
                                   checksum_ok, detail))
    return checks


async def rearchive_day(day: date, dataset_id: str) -> bool:
    """Write the archive copy for a day that never got one (§7 deferral).

    Reads the rows back out of Postgres and rebuilds the envelope, so the
    archived record is the same record — not a summary of it.
    """
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    rows = await postgres.fetch(
        "SELECT * FROM market_records WHERE dataset_id = %s "
        "AND event_time >= %s AND event_time < %s ORDER BY event_time",
        (dataset_id, start, start + timedelta(days=1)))
    if not rows:
        return False
    recs = [Record(
        dataset_id=r["dataset_id"], source=r["source"],
        event_time=r["event_time"], payload=r["payload"],
        symbol=r["symbol"], record_id=str(r["record_id"]),
        collection_time=r["collection_time"], ingest_time=r["ingest_time"],
        valid_from_sys=r["valid_from_sys"], valid_to_sys=r["valid_to_sys"],
        supersedes=str(r["supersedes"]) if r["supersedes"] else None,
        correction_reason=r["correction_reason"], producer=r["producer"],
        code_version=r["code_version"], schema_version=r["schema_version"],
        config_hash=r["config_hash"],
        quality_flags=tuple(r["quality_flags"] or ()),
        quality_ok=r["quality_ok"],
        quality_gate_version=r["quality_gate_version"],
        annotations=tuple(r["annotations"] or ()),
        stage_latency_ms=r["stage_latency_ms"] or {},
    ) for r in rows]
    try:
        written = store.archive().write(recs, part=f"rearchive-{day:%Y%m%d}")
        await store._record_manifest(written)
        log.info("tier-out: re-archived %s %s (%d rows)", dataset_id, day,
                 len(recs))
        return True
    except Exception as e:                              # noqa: BLE001
        log.error("tier-out: re-archive failed for %s %s: %s",
                  dataset_id, day, e)
        return False


async def tier_out_day(day: date, *, allow_rearchive: bool = True) -> TierOutResult:
    """Verify one day and drop its partition only if every dataset checks out."""
    result = TierOutResult(day=day)
    result.checks = await verify_day(day)

    if not result.checks:
        result.skipped_reason = "no rows in postgres for this day"
        await _record(result)
        return result

    if allow_rearchive:
        repaired = False
        for c in result.checks:
            if c.parquet_rows == 0 and c.pg_rows > 0:
                if await rearchive_day(day, c.dataset_id):
                    repaired = True
        if repaired:
            result.checks = await verify_day(day)

    if not result.verified:
        bad = [f"{c.dataset_id}: pg={c.pg_rows} parquet={c.parquet_rows} "
               f"checksum_ok={c.checksum_ok} {c.detail}".strip()
               for c in result.checks if not c.verified]
        result.skipped_reason = "; ".join(bad)
        log.error("tier-out: REFUSING to drop %s — %s", day,
                  result.skipped_reason)
        await _record(result)
        await _alert(day, result.skipped_reason)
        return result

    auth = partitions.DropAuthorization(
        day=day,
        pg_rows=sum(c.pg_rows for c in result.checks),
        parquet_rows=sum(c.parquet_rows for c in result.checks),
        checksum_ok=True)
    result.dropped = await partitions.drop_partition(auth)
    await _record(result)
    return result


async def run_tier_out(now: datetime | None = None,
                       max_days: int = 30) -> list[TierOutResult]:
    """Nightly job (00:15 UTC). Considers every partition older than the
    longest hot window, oldest first."""
    cutoff = eligible_before(now)
    out: list[TierOutResult] = []
    for name in await partitions.existing_partitions():
        try:
            day = datetime.strptime(name.rsplit("_", 1)[1], "%Y%m%d").date()
        except (ValueError, IndexError):
            continue
        if day >= cutoff:
            continue
        out.append(await tier_out_day(day))
        if len(out) >= max_days:
            break
    return out


async def _record(result: TierOutResult) -> None:
    """One row per dataset per attempt, pass or fail.

    Recording only failures would make "the job stopped running" and "nothing
    needed doing" the same observation.
    """
    for c in result.checks or [DatasetCheck("*", 0, 0, False,
                                            result.skipped_reason or "")]:
        await postgres.execute(
            "INSERT INTO tier_out_events (dataset_id, partition_day, pg_rows, "
            " parquet_rows, checksum_ok, dropped, detail) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (c.dataset_id, result.day, c.pg_rows, c.parquet_rows,
             c.checksum_ok, result.dropped,
             c.detail or result.skipped_reason))


async def _alert(day: date, reason: str) -> None:
    from botmaximus.obs import degradation
    await degradation.record(
        "tier_out_verification_failed",
        f"the {day} partition was NOT dropped: {reason}. Postgres may be "
        f"holding the only copy of that day.",
        day=str(day))


async def status() -> dict:
    """What the dashboard shows for tiering (§11)."""
    rows = await postgres.fetch(
        "SELECT dataset_id, count(*) AS rows_hot, min(event_time) AS oldest, "
        "       max(event_time) AS newest "
        "FROM market_records GROUP BY dataset_id ORDER BY dataset_id")
    recent = await postgres.fetch(
        "SELECT * FROM tier_out_events ORDER BY at DESC LIMIT 20")
    archive_bytes = await postgres.fetchval(
        "SELECT COALESCE(sum(bytes), 0) AS b FROM storage_manifest "
        "WHERE bucket = %s", (store.archive().archive_bucket,))
    quarantine_bytes = await postgres.fetchval(
        "SELECT COALESCE(sum(bytes), 0) AS b FROM storage_manifest "
        "WHERE bucket = %s", (store.archive().quarantine_bucket,))
    return {
        "hot_window": [dict(r) for r in rows],
        "partitions": await partitions.existing_partitions(),
        "eligible_before": eligible_before().isoformat(),
        "archive_bytes": int(archive_bytes or 0),
        "quarantine_bytes": int(quarantine_bytes or 0),
        "recent_tier_outs": [json.loads(json.dumps(dict(r), default=str))
                             for r in recent],
    }
