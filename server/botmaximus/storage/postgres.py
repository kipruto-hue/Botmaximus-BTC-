r"""Postgres connection pool, bootstrap and health (Storage v2.0 §1.A).

Postgres owns everything the system decides or commits. That makes it the
single point of failure for decisions, and §7 is explicit about the consequence:
when it is unreachable the system **halts**. There is no file fallback and no
queued-write side-channel in this module, and there must never be one — a
queued write that never lands is worse than a rejected trade, because the
rejected trade is visible.

## Why the pool is module-level

Same shape as the `db/mongo.py` it replaces, so porting 126 call sites stays
mechanical rather than becoming a redesign of every caller. `pool()` is lazy:
importing this module never opens a socket, which is what lets the test suite
run with no database in reach.

## open=False, and why connect errors are not swallowed

The pool is constructed with `open=False` and opened explicitly. psycopg's
default is to open in the background and let the first caller discover the
failure, which turns "the DSN is wrong" into an error at an arbitrary later
call site. Here the failure surfaces at `connect()`, where the operator is
looking.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from botmaximus.config import settings

log = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

#: Bumped when schema.sql gains objects. Additive only (§10) — a breaking
#: change is a new table alongside the old, never a rewrite of this one.
SCHEMA_VERSION = 1

#: Everything lives in the `bmx` schema; `schema.sql` creates it. Kept as a
#: constant because the tier-out job and the test fixture both need to name it.
SCHEMA = "bmx"

_pool: AsyncConnectionPool | None = None


def ensure_compatible_event_loop() -> None:
    """Windows only: select the SelectorEventLoop policy before any loop runs.

    Python's default on Windows is `ProactorEventLoop`, and psycopg's async mode
    refuses to run on it outright:

        InterfaceError: Psycopg cannot use the 'ProactorEventLoop' to run in
        async mode.

    Production is Linux (Vultr Tokyo) where this is a no-op, but the collector
    and the test suite both run on this desktop, and without it every database
    call fails in a way that reads like the database is down rather than like a
    platform default. Called explicitly from the entrypoint and the test
    fixtures — importing a module should never reconfigure the event loop of a
    process that merely imported it.
    """
    if sys.platform != "win32":
        return
    policy = asyncio.get_event_loop_policy()
    if isinstance(policy, asyncio.WindowsSelectorEventLoopPolicy):
        return
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    log.debug("selected WindowsSelectorEventLoopPolicy for psycopg compatibility")


class PostgresUnavailable(RuntimeError):
    """Postgres could not be reached.

    Callers must not degrade around this by writing somewhere else (§7, §14).
    The correct response is an L2 halt.
    """


def dsn() -> str:
    return settings.postgres_dsn


def pool() -> AsyncConnectionPool:
    """The process-wide pool. Lazy: import does not open a socket."""
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=dsn(),
            min_size=settings.postgres_pool_min,
            max_size=settings.postgres_pool_max,
            open=False,
            # Every connection lands in the bmx schema, so no query has to
            # qualify table names and none can accidentally read a like-named
            # table in `public`.
            configure=_configure,
        )
    return _pool


async def _configure(conn: AsyncConnection) -> None:
    conn.row_factory = dict_row
    # UTC is not cosmetic here. Every timestamp in this system is an instant,
    # and a session running in the host's local zone renders and parses
    # unqualified timestamps in that zone — so the Tokyo VPS and this desktop
    # would disagree about what a partition boundary or an as-of cutoff means,
    # invisibly and only near midnight.
    await conn.execute(f"SET search_path TO {SCHEMA}, public; SET timezone TO 'UTC'")
    # The pool hands out connections it expects to be idle. `SET` opens an
    # implicit transaction, so without this commit every connection is returned
    # INTRANS and discarded — the pool then fails to initialise at all, which
    # surfaces as "postgres is unreachable" rather than as a configure bug.
    await conn.commit()


async def open_pool() -> None:
    p = pool()
    try:
        await p.open(wait=True, timeout=10)
    except Exception as e:                              # noqa: BLE001
        raise PostgresUnavailable(
            f"cannot open a connection pool to Postgres at "
            f"{_safe_dsn()}: {e}. Storage v2.0 §7: this is an L2 halt, not a "
            f"condition to write around.") from e


@asynccontextmanager
async def connection():
    """A pooled connection.

    Only *connection-level* failures become `PostgresUnavailable` — the pool
    timing out, or the server going away mid-query. Query errors
    (`IntegrityError`, `CheckViolation`, `ProgrammingError`) propagate
    untouched, and the distinction matters more than it looks: §7 makes
    `PostgresUnavailable` an L2 halt, so wrapping every exception would turn a
    rejected bad write — the schema doing exactly its job — into a trading
    halt, and would bury genuine SQL bugs under a misleading "database
    unreachable".
    """
    try:
        async with pool().connection() as conn:
            yield conn
    except PostgresUnavailable:
        raise
    except (PoolTimeout, psycopg.OperationalError) as e:
        raise PostgresUnavailable(f"postgres connection failed: {e}") from e


@asynccontextmanager
async def transaction():
    """An explicit transaction.

    The money records in §3.I are the reason this exists: an order and its
    ledger prediction, or a fill and the position update it implies, are one
    atomic fact. Writing them as two sequential statements and hoping is the
    class of bug that only shows up during the incident you needed the ledger
    for.
    """
    async with connection() as conn:
        async with conn.transaction():
            yield conn


# ---- convenience readers ------------------------------------------------
# Thin on purpose. They exist so call sites stop repeating the cursor dance,
# not to become a query builder — SQL in this project stays visible at the
# call site where it can be reviewed.

async def fetch(sql: str, params: tuple | dict | None = None) -> list[dict]:
    async with connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


async def fetchrow(sql: str, params: tuple | dict | None = None) -> dict | None:
    async with connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchone()


async def fetchval(sql: str, params: tuple | dict | None = None):
    row = await fetchrow(sql, params)
    if row is None:
        return None
    return next(iter(row.values()))


async def execute(sql: str, params: tuple | dict | None = None) -> int:
    async with connection() as conn:
        cur = await conn.execute(sql, params)
        return cur.rowcount


# ---- lifecycle ----------------------------------------------------------

async def ping() -> bool:
    """True if Postgres answers. Never raises — this is what health checks and
    the degradation layer poll, and a health check that throws is a second
    outage on top of the first."""
    try:
        async with connection() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:                                   # noqa: BLE001
        return False


async def close() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def bootstrap() -> int:
    """Apply `schema.sql`. Idempotent — every object uses IF NOT EXISTS.

    Returns the schema version now recorded. Safe to run on every boot, which
    is the point: a deployment that forgot the migration step is a worse
    failure than a redundant no-op statement.
    """
    ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    async with transaction() as conn:
        await conn.execute(ddl)
        await conn.execute(
            "INSERT INTO schema_version (version, code_version) "
            "VALUES (%s, %s) ON CONFLICT (version) DO NOTHING",
            (SCHEMA_VERSION, _code_version()))
    log.info("postgres schema bootstrapped to version %s", SCHEMA_VERSION)
    return SCHEMA_VERSION


async def applied_version() -> int | None:
    return await fetchval("SELECT max(version) AS v FROM schema_version")


def _code_version() -> str:
    from botmaximus.storage.envelope import _code_version as cv
    return cv()


def _safe_dsn() -> str:
    """The DSN with its password removed. Connection errors get logged and a
    logged password is a leaked password."""
    raw = dsn()
    if "@" not in raw:
        return raw
    head, _, tail = raw.rpartition("@")
    scheme, sep, creds = head.partition("://")
    user = creds.split(":")[0] if creds else ""
    return f"{scheme}{sep}{user}:***@{tail}"


def reset_for_tests() -> None:
    """Drop the cached pool so a fixture can point the next call at a scratch
    database. Test-only; nothing in the running system reassigns the pool."""
    global _pool
    _pool = None
