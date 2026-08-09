r"""The record write path and point-in-time reads (Storage v2.0 §4, §5, §6).

One door in, two doors out. Every record produced by a collector passes through
`write_records`, and the quality gate's verdict decides which store it reaches:

- **clean** → Postgres hot window **and** the Parquet archive **and** a
  `storage_manifest` row carrying the archive checksum.
- **failed** → the quarantine bucket, and nothing else. Not the hot window, not
  the archive, not a "pending review" table (§5, P4).

There is no third outcome and no path back. `archive.py` deliberately has no
`promote_from_quarantine`, and this module adds none.

## Which write is allowed to fail

Postgres is required; object storage is not. §7 is explicit: if the archive is
unreachable the collector keeps writing to the hot window and tier-out defers,
whereas if Postgres is unreachable the system halts. So Postgres is written
first and its failure propagates as `PostgresUnavailable` (an L2 halt), while an
archive failure is recorded as a deferral and the call still succeeds.

That ordering has a consequence worth stating: a deferred day exists in
Postgres with no Parquet counterpart, so the tier-out job will refuse to drop
that partition (§4) until the archive is written. Data is never dropped on the
strength of a copy that was never made.

## Corrections

`apply_correction` is the only place in the system that issues an UPDATE against
`market_records`, and it only ever stamps `valid_to_sys` on a row that is being
superseded. The payload is never edited: the new truth is a **new** row with the
original `event_time` and `supersedes` pointing back (§2). Per §5.1 the record
being corrected is also copied to quarantine flagged `retroactive` — it stays in
production, queryable as-of any earlier instant, and is simultaneously available
for forensics.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime

from botmaximus.config import settings
from botmaximus.storage import partitions, postgres
from botmaximus.storage.archive import Archive, LocalBackend, ObjectStorageBackend
from botmaximus.storage.envelope import Record, visible_at  # noqa: F401
from botmaximus.storage.venues import require_venue, venue_of

log = logging.getLogger(__name__)

RETROACTIVE = "retroactive"


class ArchiveUnavailable(RuntimeError):
    """Object storage could not be reached.

    §7: live collection continues, tier-out defers, and backtester operations
    that need the archive fail loudly with this rather than quietly returning a
    short window.
    """


@dataclass
class WriteResult:
    written: int = 0
    quarantined: int = 0
    archived_keys: list[str] = field(default_factory=list)
    #: Partitions whose archive write failed. Tier-out must not drop these.
    deferred_partitions: list[str] = field(default_factory=list)

    @property
    def archive_deferred(self) -> bool:
        return bool(self.deferred_partitions)


# ---------------------------------------------------------------- archive

_archive: Archive | None = None


def archive() -> Archive:
    """The process-wide archive.

    Falls back to the local filesystem backend only when no object-store
    endpoint is configured — that is a documented single-box mode (§1.B), not a
    silent degradation: if an endpoint IS configured and unreachable, the
    object-store backend raises rather than quietly writing to disk, because a
    local file the operator believes is in Tokyo is worse than an error.
    """
    global _archive
    if _archive is None:
        if settings.objectstore_endpoint:
            backend = ObjectStorageBackend(
                endpoint=settings.objectstore_endpoint,
                access_key=_secret(settings.objectstore_access_key),
                secret_key=_secret(settings.objectstore_secret_key),
                region=settings.objectstore_region)
        else:
            log.warning(
                "no OBJECTSTORE_ENDPOINT configured — archiving to the local "
                "filesystem at %s. Adequate for one box, not durable.",
                settings.archive_local_root)
            backend = LocalBackend(settings.archive_local_root)
        _archive = Archive(backend, settings.bucket_archive,
                           settings.bucket_quarantine)
    return _archive


def _secret(v) -> str:
    if v is None:
        raise RuntimeError(
            "object storage endpoint is configured but its credentials are "
            "not; refusing to attempt an anonymous write")
    return v.get_secret_value() if hasattr(v, "get_secret_value") else str(v)


def reset_for_tests() -> None:
    global _archive
    _archive = None


def set_archive_for_tests(a: Archive | None) -> None:
    global _archive
    _archive = a


# ---------------------------------------------------------------- write path

async def write_records(records: list[Record]) -> WriteResult:
    """Route a batch by quality verdict. The single entry point for collectors."""
    result = WriteResult()
    if not records:
        return result

    clean = [r for r in records if r.quality_ok]
    failed = [r for r in records if not r.quality_ok]

    if failed:
        await _quarantine(failed, result)
    if clean:
        await _write_clean(clean, result)
    return result


async def _write_clean(records: list[Record], result: WriteResult) -> None:
    # Postgres first and unconditionally: it is the store whose failure halts
    # the system, so discovering it is down after a successful archive write
    # would leave the two stores describing different worlds.
    await _insert_hot(records)
    result.written += len(records)

    for (partition, _), group in _group(records).items():
        try:
            written = archive().write(group, part=_part_key(group))
        except Exception as e:                          # noqa: BLE001
            # §7: the archive being down is not a reason to stop collecting.
            log.error("archive write deferred for %s: %s", partition, e)
            result.deferred_partitions.append(partition)
            await _telemetry("degraded", "archive_write_failed", str(e),
                             {"partition": partition, "rows": len(group)})
            continue
        await _record_manifest(written)
        result.archived_keys.append(written.key)


async def _insert_hot(records: list[Record]) -> None:
    """Insert into the partitioned hot window.

    Dedupe is on the NATURAL key — (venue, dataset_id, event_time) among
    current records — not on `record_id`, which is minted fresh per
    observation. A websocket reconnect replaying the last closed candle, or a
    backfill overlapping live data, produces a new id for a fact the store
    already holds; keying on the id would store both and inflate every count
    the coverage ledger and the integrity checks derive from these rows.

    The index is partial on `valid_to_sys IS NULL`, so a correction still
    lands: superseding the old row removes it from the index and frees the slot
    for its replacement.
    """
    days = {r.event_time.date() for r in records}
    for day in sorted(days):
        await partitions.ensure_partition(day)

    rows = [(
        r.record_id, r.dataset_id, r.source, venue_of(r.source), r.symbol,
        r.event_time, r.collection_time, r.ingest_time,
        r.valid_from_sys, r.valid_to_sys, r.supersedes, r.correction_reason,
        r.producer, r.code_version, r.schema_version, r.config_hash,
        list(r.quality_flags), r.quality_ok, r.quality_gate_version,
        list(r.annotations),
        json.dumps(r.stage_latency_ms), json.dumps(r.payload, default=str),
    ) for r in records]

    async with postgres.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO market_records ("
                " record_id, dataset_id, source, venue, symbol,"
                " event_time, collection_time, ingest_time,"
                " valid_from_sys, valid_to_sys, supersedes, correction_reason,"
                " producer, code_version, schema_version, config_hash,"
                " quality_flags, quality_ok, quality_gate_version, annotations,"
                " stage_latency_ms, payload)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                "         %s,%s,%s,%s,%s,%s)"
                " ON CONFLICT (venue, dataset_id, event_time)"
                "   WHERE valid_to_sys IS NULL DO NOTHING",
                rows)


async def _quarantine(records: list[Record], result: WriteResult,
                      retroactive: bool = False) -> None:
    """Bad data to the quarantine bucket, and a pointer row in Postgres.

    The pointer carries the failing check NAMES only — never a margin. Same
    rule as the envelope and the LLM prompt wall: a name is diagnostic, a margin
    is a gradient someone can tune against.
    """
    for (partition, _), group in _group(records).items():
        key = None
        try:
            written = archive().write_quarantine(group, part=_part_key(group))
            key = written.key
            await _record_manifest(written)
        except Exception as e:                          # noqa: BLE001
            log.error("quarantine write failed for %s: %s", partition, e)
            await _telemetry("degraded", "quarantine_write_failed", str(e),
                             {"partition": partition})
        async with postgres.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "INSERT INTO quality_events "
                    "(dataset_id, failing_check, gate_version, quarantine_key) "
                    "VALUES (%s,%s,%s,%s)",
                    [(r.dataset_id,
                      f"{RETROACTIVE}:{flag}" if retroactive else flag,
                      r.quality_gate_version, key)
                     for r in group
                     for flag in (r.quality_flags or ("unspecified",))])
    result.quarantined += len(records)


async def _record_manifest(written) -> None:
    """Index every Parquet file, with its checksum.

    The weekly audit (§8) walks object storage and reconciles against this
    table. Without it, a lost object and a bit-rotted one are both silent.
    """
    await postgres.execute(
        "INSERT INTO storage_manifest "
        "(bucket, object_key, dataset_id, partition, rows, bytes, sha256, "
        " written_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (bucket, object_key) DO UPDATE SET "
        " rows = EXCLUDED.rows, bytes = EXCLUDED.bytes, "
        " sha256 = EXCLUDED.sha256, written_at = EXCLUDED.written_at",
        (written.bucket, written.key, written.dataset_id, written.partition,
         written.rows, written.bytes, written.sha256, written.written_at))


# ---------------------------------------------------------------- corrections

async def apply_correction(original: Record, payload: dict, reason: str,
                           now: datetime | None = None) -> Record:
    """Supersede a record with a corrected one (§2 invariant 3, §5.1).

    Returns the new current record. The original is not deleted, not edited,
    and remains the answer to any as-of query pinned before this instant.
    """
    closed, new = original.supersede(payload, reason, now=now)

    async with postgres.transaction() as conn:
        # The only UPDATE this system performs on market_records, and it
        # touches one column: the close of a system-time interval. The payload
        # of the superseded row is left exactly as it was written.
        cur = await conn.execute(
            "UPDATE market_records SET valid_to_sys = %s "
            "WHERE record_id = %s AND event_time = %s AND valid_to_sys IS NULL",
            (closed.valid_to_sys, original.record_id, original.event_time))
        if cur.rowcount != 1:
            raise ValueError(
                f"record {original.record_id} was not open for correction "
                f"(matched {cur.rowcount} rows). Correcting an already-closed "
                f"record would fork the supersession chain and make as-of "
                f"queries ambiguous.")

    await _write_clean([new], WriteResult())

    # §5.1: the superseded original is ALSO copied to quarantine, flagged, so
    # a post-hoc reconciliation leaves a forensic trail without removing the
    # row that historical backtests still legitimately see.
    flagged = Record.create(
        dataset_id=original.dataset_id, source=original.source,
        event_time=original.event_time, payload=original.payload,
        symbol=original.symbol, quality_ok=False,
        quality_flags=(RETROACTIVE,),
        quality_gate_version=original.quality_gate_version,
        config_hash=original.config_hash)
    await _quarantine([flagged], WriteResult(), retroactive=True)
    return new


# ---------------------------------------------------------------- read path

async def read_as_of(dataset_id: str, start: datetime, end: datetime, *,
                     venue: str, as_of: datetime | None = None) -> list[dict]:
    """Records for an event-time range, as the system knew them at `as_of`.

    `venue` is keyword-only and required. It is not a filter you may omit for
    convenience: a window that silently spans two venues returns a price series
    that never existed on either.

    `as_of=None` means current truth — what live consumers want. A backtest
    must pass one (§6); `backtest_runs.as_of` is NOT NULL so an unpinned run
    cannot even be recorded.
    """
    require_venue(venue)
    sql = ("SELECT * FROM market_records "
           "WHERE venue = %s AND dataset_id = %s "
           "  AND event_time >= %s AND event_time <= %s ")
    params: list = [venue, dataset_id, start, end]
    if as_of is None:
        sql += "AND valid_to_sys IS NULL "
    else:
        # Exactly `envelope.visible_at`, in SQL. A test asserts the two agree
        # on the same fixture — two implementations of a point-in-time
        # predicate that disagree is the silent-revision bug wearing a
        # different hat.
        sql += "AND valid_from_sys <= %s "
        sql += "AND (valid_to_sys IS NULL OR valid_to_sys > %s) "
        params += [as_of, as_of]
    sql += "ORDER BY event_time"
    return await postgres.fetch(sql, tuple(params))


async def last_event_time(dataset_id: str, *, venue: str) -> datetime | None:
    """Newest committed event time — what a restarting collector asks for
    before deciding what to backfill (§3.C)."""
    require_venue(venue)
    return await postgres.fetchval(
        "SELECT max(event_time) AS t FROM market_records "
        "WHERE venue = %s AND dataset_id = %s AND valid_to_sys IS NULL",
        (venue, dataset_id))


# ---------------------------------------------------------------- helpers

def _group(records: list[Record]) -> dict[tuple[str, str], list[Record]]:
    """By (partition, dataset) — one Parquet file per day per dataset, which is
    what makes a daily partition atomic to write and to verify."""
    out: dict[tuple[str, str], list[Record]] = {}
    for r in records:
        out.setdefault((r.partition_path(), r.dataset_id), []).append(r)
    return out


def _part_key(group: list[Record]) -> str:
    """A stable per-batch suffix so concurrent or repeated writes for the same
    day land in distinct objects instead of overwriting one another."""
    return group[0].record_id.replace("-", "")[:12]


async def _telemetry(kind: str, label: str, reason: str, context: dict) -> None:
    try:
        await postgres.execute(
            "INSERT INTO telemetry_events (kind, label, reason, context) "
            "VALUES (%s,%s,%s,%s)",
            (kind, label, reason, json.dumps(context, default=str)))
    except Exception:                                   # noqa: BLE001
        # Telemetry about a failure must never become a second failure.
        log.exception("could not record telemetry event %s/%s", kind, label)
