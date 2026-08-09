r"""Postgres client, bootstrap and partition management (Storage v2.0 §1.A, §4).

Two groups. The first needs no database at all — identifier validation, the
drop-authorization guard and DSN redaction are pure logic, and they cover the
failure modes that would be most expensive to discover in production. The
second runs against a real Postgres when `BOTMAXIMUS_TEST_POSTGRES_DSN` is set
and skips otherwise, so the suite stays runnable on a laptop with no database.

The constraint tests deliberately assert on *refusals*. The schema's job is to
make bad states unrepresentable, and a schema is only doing that job if you can
show it rejecting the bad state.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from botmaximus.storage import partitions, postgres
from botmaximus.storage.envelope import Record

UTC = timezone.utc


# ---------------------------------------------------------------- no database

def test_partition_name_is_derived_from_the_day():
    assert partitions.partition_name(date(2026, 8, 9)) == "market_records_20260809"


@pytest.mark.parametrize("bad", [
    "market_records; DROP TABLE orders",
    "market_records_2026",
    "orders",
    "market_records_20260809x",
])
def test_partition_identifiers_are_validated(bad):
    """Partition names reach DDL by string interpolation — psycopg cannot bind
    an identifier — so the only thing between a caller and injection is this."""
    with pytest.raises(ValueError):
        partitions._validate(bad)


def test_drop_authorization_requires_matching_counts_and_checksum():
    day = date(2026, 8, 9)
    assert partitions.DropAuthorization(day, 100, 100, True).verified
    # A checksum that did not verify is the bit-rot case §8 exists for.
    assert not partitions.DropAuthorization(day, 100, 100, False).verified
    # Counts that disagree mean the archive is short — dropping here would
    # destroy the only complete copy.
    assert not partitions.DropAuthorization(day, 100, 99, True).verified


@pytest.mark.asyncio
async def test_drop_partition_refuses_without_verification():
    """§4: 'tier-out failure never drops the Postgres partition.' The refusal
    must be an exception, not a quiet skip — a silent no-op is
    indistinguishable from a successful tier-out."""
    auth = partitions.DropAuthorization(date(2026, 8, 9), 100, 99, True)
    with pytest.raises(ValueError, match="refusing to drop"):
        await partitions.drop_partition(auth)


def test_safe_dsn_redacts_the_password(monkeypatch):
    """Connection failures get logged, and a logged password is a leaked one."""
    from botmaximus.config import settings
    monkeypatch.setattr(
        settings, "postgres_dsn",
        "postgresql://botmaximus:hunter2@10.0.0.1:5432/botmaximus")
    safe = postgres._safe_dsn()
    assert "hunter2" not in safe
    assert "botmaximus" in safe and "10.0.0.1" in safe


def test_no_fallback_write_path_exists():
    """§7/§14: when Postgres is unreachable the system halts. It does not write
    somewhere else. A helper that spooled writes to disk would satisfy every
    other test in this file while destroying the guarantee, so its absence is
    asserted directly."""
    import re
    src = (postgres.SCHEMA_PATH.parent / "postgres.py").read_text(encoding="utf-8")
    # `open(` on its own would match `pool.open(`, so match a bare builtin
    # `open()` on a literal path, plus the other stand-in stores §14 names.
    forbidden = [
        r"(?<![.\w])open\(\s*[\"'f]",   # builtins.open("…")
        r"\.write_text\(", r"\.write_bytes\(",
        r"json\.dump", r"csv\.", r"sqlite3", r"\bredis\b", r"shelve",
    ]
    for pattern in forbidden:
        assert not re.search(pattern, src), (
            f"{pattern!r} appears in postgres.py — when Postgres is "
            f"unreachable the system halts (§7); it does not write elsewhere")


# ---------------------------------------------------------------- live database

@pytest.mark.asyncio
async def test_bootstrap_is_idempotent(pg):
    """Bootstrap runs on every boot. A deployment that forgot the migration
    step is a worse failure than a redundant no-op statement."""
    assert await pg.applied_version() == pg.SCHEMA_VERSION
    await pg.bootstrap()
    await pg.bootstrap()
    assert await pg.applied_version() == pg.SCHEMA_VERSION
    n = await pg.fetchval(
        "SELECT count(*) AS n FROM schema_version WHERE version = %s",
        (pg.SCHEMA_VERSION,))
    assert n == 1


@pytest.mark.asyncio
async def test_session_is_utc(pg):
    """A session in the host's local zone makes partition boundaries and as-of
    cutoffs mean different things on the Tokyo VPS than on a desktop."""
    assert await pg.fetchval("SHOW timezone") == "UTC"


@pytest.mark.asyncio
async def test_ping_is_true_when_up_and_never_raises(pg):
    assert await pg.ping() is True


@pytest.mark.asyncio
async def test_ping_returns_false_rather_than_raising(monkeypatch):
    """Health checks poll this. A health check that throws is a second outage
    on top of the first."""
    from botmaximus.config import settings
    monkeypatch.setattr(
        settings, "postgres_dsn",
        "postgresql://nobody@127.0.0.1:1/nothing")
    postgres.reset_for_tests()
    try:
        assert await postgres.ping() is False
    finally:
        await postgres.close()
        postgres.reset_for_tests()


@pytest.mark.asyncio
async def test_open_pool_raises_postgres_unavailable(monkeypatch):
    """One exception type for the degradation layer to key an L2 halt on."""
    from botmaximus.config import settings
    monkeypatch.setattr(
        settings, "postgres_dsn",
        "postgresql://nobody@127.0.0.1:1/nothing")
    postgres.reset_for_tests()
    try:
        with pytest.raises(postgres.PostgresUnavailable):
            await postgres.open_pool()
    finally:
        await postgres.close()
        postgres.reset_for_tests()


@pytest.mark.asyncio
async def test_partitions_are_created_ahead(pg):
    now = datetime(2026, 8, 9, 12, tzinfo=UTC)
    made = await partitions.ensure_ahead(days=3, now=now)
    assert made == [
        "market_records_20260809", "market_records_20260810",
        "market_records_20260811", "market_records_20260812",
    ]
    existing = await partitions.existing_partitions()
    assert set(made) <= set(existing)
    # Idempotent: the daily job re-runs over days that already exist.
    assert await partitions.ensure_ahead(days=3, now=now) == made


@pytest.mark.asyncio
async def test_insert_without_a_partition_fails(pg):
    """Postgres does not invent a partition. This is why they are created
    ahead of need rather than lazily on the collector's hot path.

    The error is `CheckViolation` ("no partition of relation ... found for
    row"), not `UndefinedTable` — routing fails before any table is named.
    """
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation, match="no partition"):
        await _insert(datetime(2031, 1, 1, tzinfo=UTC))


@pytest.mark.asyncio
async def test_row_count_and_verified_drop(pg):
    day = date(2026, 8, 9)
    await partitions.ensure_partition(day)
    for i in range(3):
        await _insert(datetime(2026, 8, 9, 0, i, tzinfo=UTC))
    assert await partitions.row_count(day) == 3

    auth = partitions.DropAuthorization(day, pg_rows=3, parquet_rows=3,
                                        checksum_ok=True)
    assert await partitions.drop_partition(auth) is True
    assert partitions.partition_name(day) not in await partitions.existing_partitions()
    # Dropping again is a no-op, not an error — the nightly job may retry.
    assert await partitions.drop_partition(auth) is False


@pytest.mark.asyncio
async def test_venue_check_blocks_a_third_venue(pg):
    """The separation that used to be two databases is now this constraint."""
    import psycopg
    await partitions.ensure_partition(date(2026, 8, 9))
    with pytest.raises(psycopg.errors.CheckViolation):
        await _insert(datetime(2026, 8, 9, 1, tzinfo=UTC), venue="kraken")


@pytest.mark.asyncio
async def test_production_table_refuses_failed_records(pg):
    """P4: quarantine never mixes with production, enforced at write time."""
    import psycopg
    await partitions.ensure_partition(date(2026, 8, 9))
    with pytest.raises(psycopg.errors.CheckViolation):
        await _insert(datetime(2026, 8, 9, 2, tzinfo=UTC), quality_ok=False)
    with pytest.raises(psycopg.errors.CheckViolation):
        await _insert(datetime(2026, 8, 9, 3, tzinfo=UTC),
                      quality_flags=["stale_price"])


@pytest.mark.asyncio
async def test_transaction_rolls_back_as_one_unit(pg):
    """The money records are the reason `transaction()` exists: a prediction
    and the order it belongs to are one atomic fact."""
    with pytest.raises(RuntimeError):
        async with pg.transaction() as conn:
            await conn.execute(
                "INSERT INTO kill_events (level, reason) VALUES ('L1','test')")
            raise RuntimeError("boom")
    assert await pg.fetchval("SELECT count(*) AS n FROM kill_events") == 0


# ---------------------------------------------------------------- helpers

async def _insert(event_time: datetime, *, venue: str = "bybit",
                  quality_ok: bool = True,
                  quality_flags: list[str] | None = None) -> None:
    await postgres.execute(
        "INSERT INTO market_records (record_id, dataset_id, source, venue, "
        "event_time, collection_time, ingest_time, valid_from_sys, producer, "
        "code_version, schema_version, quality_flags, quality_ok, "
        "quality_gate_version, payload) "
        "VALUES (gen_random_uuid(), 'btc_ohlcv_1m', 'bybit_v5_ws', %s, %s, "
        "now(), now(), now(), 'test/1', 'abc', 1, %s, %s, 1, '{}')",
        (venue, event_time, quality_flags or [], quality_ok))
