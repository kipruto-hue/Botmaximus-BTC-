"""Shared fixtures.

The venue-constants fixture is autouse and deliberate. `venue.get()` raises
when the constants were never fetched, because a default would be exactly the
assumed-number bug that `execution/venue.py` exists to remove — so every test
that sizes an order has to say which venue it is sizing against.

The values below are the **real** Bybit BTCUSDT linear-perp numbers, read from
`/v5/market/instruments-info` and `/v5/market/risk-limit` on 2026-08-05. Pinning
real values rather than round test numbers means a test that passes here is
evidence about the venue we actually trade, and a venue change that breaks an
assumption shows up as a failing test instead of a surprise in production.
"""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

from botmaximus.config import settings
from botmaximus.execution import venue
from botmaximus.execution.venue import RiskTier, VenueConstants
from botmaximus.obs import degradation
from botmaximus.storage import postgres

#: Bybit BTCUSDT LinearPerpetual, captured live 2026-08-05.
BYBIT_BTCUSDT = VenueConstants(
    symbol="BTCUSDT",
    category="linear",
    qty_step=0.001,
    min_order_qty=0.001,
    max_order_qty=1500.0,
    max_market_qty=150.0,
    min_notional=5.0,            # Binance hardcode said 100.0
    tick_size=0.1,
    max_leverage=100.0,
    tiers=(
        RiskTier(limit_value=2_000_000.0, maint_margin=0.005,   # was 0.004
                 initial_margin=0.01, max_leverage=100.0),
        RiskTier(limit_value=2_600_000.0, maint_margin=0.0056,
                 initial_margin=0.0111, max_leverage=90.0),
    ),
    taker_fee_rate=0.00055,
    maker_fee_rate=0.0002,
    fee_source="test",
    host="test",
)


@pytest.fixture(autouse=True)
def venue_constants():
    venue.set_for_tests(BYBIT_BTCUSDT)
    yield BYBIT_BTCUSDT
    venue.set_for_tests(None)


@pytest.fixture(autouse=True)
def clean_degradation_counters():
    """Degradations are asserted on in several tests; a count leaking between
    tests would make those assertions depend on execution order."""
    degradation.reset_for_tests()
    yield
    degradation.reset_for_tests()


# ---------------------------------------------------------------- Postgres
#: Opt-in, via env var, and never the production DSN by default.
#:
#: These tests TRUNCATE every table between cases. Defaulting to
#: `settings.postgres_dsn` would make "ran the test suite with .env loaded"
#: indistinguishable from "wiped the live decision record", so the DSN has to
#: be named explicitly and the suite skips when it is not.
TEST_DSN_ENV = "BOTMAXIMUS_TEST_POSTGRES_DSN"

# Windows default loop is one psycopg cannot use; without this every
# Postgres-backed test skips with what looks like an unreachable database.
postgres.ensure_compatible_event_loop()


@pytest.fixture(scope="session")
def event_loop_policy():
    """pytest-asyncio builds each test's loop from this policy."""
    postgres.ensure_compatible_event_loop()
    import asyncio
    return asyncio.get_event_loop_policy()


@pytest.fixture
def pg_dsn() -> str:
    dsn = os.environ.get(TEST_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {TEST_DSN_ENV} to run the Postgres-backed tests")
    return dsn


@pytest_asyncio.fixture
async def pg(pg_dsn, monkeypatch):
    """A bootstrapped, empty `bmx` schema on the test database.

    Function-scoped on purpose: `AsyncConnectionPool` binds to the event loop
    it was opened in, and pytest-asyncio gives each test its own loop. A
    session-scoped pool would work for exactly one test and then fail in ways
    that look like database errors rather than fixture errors.
    """
    monkeypatch.setattr(settings, "postgres_dsn", pg_dsn)
    postgres.reset_for_tests()
    try:
        await postgres.open_pool()
    except postgres.PostgresUnavailable as e:
        postgres.reset_for_tests()
        pytest.skip(f"postgres not reachable at {TEST_DSN_ENV}: {e}")
    await postgres.bootstrap()
    await _truncate_all()
    try:
        yield postgres
    finally:
        await postgres.close()
        postgres.reset_for_tests()


async def _truncate_all() -> None:
    """Empty every table, including the market_records partitions.

    TRUNCATE on the partitioned parent cascades to its partitions but leaves
    them attached, so a test that created yesterday's partition does not leak
    rows into the next test while still exercising real partition routing.
    """
    rows = await postgres.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s",
        (postgres.SCHEMA,))
    # `schema_version` is not test data — it records which DDL this database
    # was brought up to (§10). Truncating it would make bootstrap look like it
    # never ran, which is the difference between "empty database" and
    # "unmigrated database".
    names = [r["tablename"] for r in rows
             if not r["tablename"].startswith("market_records_")
             and r["tablename"] != "schema_version"]
    if not names:
        return
    async with postgres.connection() as conn:
        await conn.execute(
            f"TRUNCATE {', '.join(names)} RESTART IDENTITY CASCADE")
