r"""Daily partitions for the market-data hot window (Storage v2.0 §1.A, §3.A).

`market_records` is RANGE-partitioned on `event_time`, one partition per UTC
day. Two things follow from that, and both are load-bearing:

**Partitions are created ahead of need.** An insert into a range with no
partition fails outright — Postgres does not invent one. Creating them lazily
at write time would put a DDL statement on the collector's hot path and make
the first tick after midnight the one that discovers the problem. They are
created ahead, at boot and daily.

**The hot window stays small.** This table only ever holds 24h–7d per dataset
(§3.A); everything older lives in Parquet. So the partition count stays in the
low tens no matter how much history the system accumulates — the 2-year archive
never becomes 730 partitions here, because it was never in Postgres to begin
with. Anything that makes this table grow without bound is a bug in the
tier-out job, not a reason to add partitions.

## Why dropping needs an authorization object

§4 is unambiguous: a partition is dropped **only** after its Parquet
counterpart has been verified, and "tier-out failure never drops the Postgres
partition." A plain `drop_partition(day)` would sit in the module looking like
a reasonable cleanup helper, one call away from deleting the only copy of a
day's data. So it does not exist. `drop_partition` requires a
`DropAuthorization`, which cannot be constructed without a row count and a
matching checksum, and refuses if the verification did not pass. The safety
property is then a type signature rather than a comment asking people to be
careful.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from psycopg import sql

from botmaximus.storage import postgres

log = logging.getLogger(__name__)

PARENT = "market_records"


def partition_name(day: date) -> str:
    return f"{PARENT}_{day:%Y%m%d}"


def _validate(name: str) -> str:
    """Partition names are interpolated into DDL — psycopg cannot parameterise
    an identifier. They are derived from a date, so this can only fire if a
    caller invented one, but the check is cheap and the failure mode is
    injection."""
    if not re.fullmatch(r"market_records_\d{8}", name):
        raise ValueError(f"refusing to use {name!r} as a partition identifier")
    return name


async def ensure_partition(day: date) -> str:
    """Create the partition covering `day`, if absent. Idempotent."""
    name = _validate(partition_name(day))
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    # Partition bounds are DDL: Postgres rejects bound parameters there
    # ("there is no parameter $1"), so they must be literals. `sql.Literal`
    # does the quoting rather than an f-string, and the bounds are written as
    # explicit UTC instants so the server's own timezone cannot shift a day
    # boundary by a few hours.
    stmt = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {part} PARTITION OF {parent} "
        "FOR VALUES FROM ({start}) TO ({end})"
    ).format(part=sql.Identifier(name), parent=sql.Identifier(PARENT),
             start=sql.Literal(start), end=sql.Literal(end))
    async with postgres.connection() as conn:
        await conn.execute(stmt)
    return name


async def ensure_range(start: date, end: date) -> list[str]:
    """Partitions for every day in [start, end] inclusive."""
    if end < start:
        raise ValueError(f"end {end} precedes start {start}")
    out, day = [], start
    while day <= end:
        out.append(await ensure_partition(day))
        day += timedelta(days=1)
    return out


async def ensure_ahead(days: int = 3, now: datetime | None = None) -> list[str]:
    """Today plus `days` ahead.

    Called at boot and once a day. The lookahead exists so a collector that
    runs through midnight never waits on DDL, and so a clock skew of a few
    hours cannot land a record in a range that does not exist yet.
    """
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(timezone.utc).date()
    return await ensure_range(today, today + timedelta(days=days))


async def existing_partitions() -> list[str]:
    rows = await postgres.fetch(
        "SELECT c.relname AS name FROM pg_class c "
        "JOIN pg_inherits i ON i.inhrelid = c.oid "
        "JOIN pg_class p ON p.oid = i.inhparent "
        "WHERE p.relname = %s ORDER BY c.relname", (PARENT,))
    return [r["name"] for r in rows]


async def row_count(day: date) -> int:
    """Rows in one day's partition. The number the tier-out job compares
    against the Parquet partition before it is allowed to drop anything."""
    name = _validate(partition_name(day))
    if name not in await existing_partitions():
        return 0
    stmt = sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(name))
    async with postgres.connection() as conn:
        cur = await conn.execute(stmt)
        row = await cur.fetchone()
    return row["n"] if row else 0


@dataclass(frozen=True)
class DropAuthorization:
    """Proof that a day's Parquet partition was verified against Postgres.

    Constructed only by the tier-out job, from a real comparison. `checksum_ok`
    false or mismatched counts make `drop_partition` refuse — which is §4's
    "tier-out failure never drops the Postgres partition", expressed so that
    getting it wrong is a raised exception rather than a missing day.
    """
    day: date
    pg_rows: int
    parquet_rows: int
    checksum_ok: bool

    @property
    def verified(self) -> bool:
        return self.checksum_ok and self.pg_rows == self.parquet_rows


async def drop_partition(auth: DropAuthorization) -> bool:
    """Drop a day's partition. Requires verification that Parquet holds it.

    Returns True if a partition was dropped, False if there was nothing to
    drop. Raises if the authorization did not pass — deliberately loud: a
    silent skip here looks identical to a successful tier-out, and the
    difference is whether Postgres is still holding the only copy.
    """
    if not auth.verified:
        raise ValueError(
            f"refusing to drop the {auth.day} partition: parquet verification "
            f"did not pass (checksum_ok={auth.checksum_ok}, "
            f"pg_rows={auth.pg_rows}, parquet_rows={auth.parquet_rows}). "
            f"§4: tier-out failure never drops the Postgres partition.")
    name = _validate(partition_name(auth.day))
    if name not in await existing_partitions():
        log.info("tier-out: partition %s already absent", name)
        return False
    async with postgres.connection() as conn:
        await conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(name)))
    log.info("tier-out: dropped partition %s (%s rows, archived)",
             name, auth.pg_rows)
    return True
