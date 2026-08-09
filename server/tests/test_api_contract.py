r"""Every read endpoint, against a real database (audit 2).

The storage migration rewrote what these handlers query, and a handler can be
syntactically fine while returning a shape the dashboard cannot render — or
raising on a row the new schema permits and the old one did not. The
`/api/strategies` LEFT JOIN is exactly that case: `definition` is now nullable,
and indexing into it was a 500 on the dashboard's main panel.

The handlers are called directly rather than through TestClient so the app's
lifespan does not start collectors and websockets during a unit test.
"""
from __future__ import annotations

import inspect

import pytest

from botmaximus.api import app as api

#: Every GET handler that takes no path parameter, with the kwargs it needs.
NO_ARG_ENDPOINTS = [
    (api.health, {}),
    (api.get_telemetry, {}),
    (api.get_storage, {}),
    (api.run_storage_integrity, {}),
    (api.get_risk, {}),
    (api.get_strategies, {}),
    (api.get_coverage, {}),
    (api.get_ohlcv, {"limit": 5}),
    (api.get_ticks, {"limit": 5}),
    (api.get_quarantine, {"limit": 5}),
    (api.get_calibration, {}),
    (api.get_arbiter_events, {"limit": 5}),
    (api.get_scrutiny_events, {"limit": 5}),
    (api.get_degradation, {"limit": 5}),
    (api.get_scrutiny_calibration, {}),
    (api.get_auditor_reports, {"limit": 5}),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("fn,kwargs", NO_ARG_ENDPOINTS,
                         ids=[f.__name__ for f, _ in NO_ARG_ENDPOINTS])
async def test_every_read_endpoint_answers_on_an_empty_database(pg, fn, kwargs):
    """An empty system is the state every deployment starts in. An endpoint
    that only works once data exists is broken on day one."""
    out = await fn(**kwargs) if inspect.iscoroutinefunction(fn) else fn(**kwargs)
    assert out is not None


@pytest.mark.asyncio
async def test_strategies_endpoint_survives_a_missing_definition_blob(pg):
    """Regression: `definition` comes from a LEFT JOIN and is None when the
    blob is absent — a migrated row, or one registered before its definition
    landed. `d["definition"].get(...)` made that a 500."""
    await pg.execute(
        "INSERT INTO strategies (strategy_id, version, definition_hash, "
        " lifecycle_state, origin) "
        "VALUES ('orphan', 1, 'nosuchhash', 'candidate', 'seed')")

    out = await api.get_strategies()
    assert out["count"] == 1
    s = out["strategies"][0]
    assert s["strategy_id"] == "orphan"
    assert s["direction"] is None          # absent, not an exception
    assert out["by_state"] == {"candidate": 1}


@pytest.mark.asyncio
async def test_strategies_endpoint_reads_a_real_definition(pg):
    await pg.execute(
        "INSERT INTO strategy_definitions_blob (definition_hash, definition) "
        "VALUES ('h1', '{\"direction\": \"long\", \"required_feeds\": [\"ohlcv\"]}')")
    await pg.execute(
        "INSERT INTO strategies (strategy_id, version, definition_hash, "
        " lifecycle_state, origin) VALUES ('s1', 1, 'h1', 'paper', 'seed')")

    s = (await api.get_strategies())["strategies"][0]
    assert s["direction"] == "long"
    assert s["required_feeds"] == ["ohlcv"]


@pytest.mark.asyncio
async def test_single_strategy_endpoint(pg):
    await pg.execute(
        "INSERT INTO strategies (strategy_id, version, definition_hash, "
        " lifecycle_state, origin) VALUES ('s1', 1, 'h1', 'paper', 'seed')")
    out = await api.get_strategy("s1")
    assert out["strategy"]["strategy_id"] == "s1"
    assert out["events"] == []
    assert (await api.get_strategy("nope"))["error"] == "not_found"


@pytest.mark.asyncio
async def test_health_reports_postgres_not_mongo(pg):
    """The key was renamed in the migration; a dashboard reading `mongo` would
    show a permanently red light."""
    h = await api.health()
    assert "mongo" not in h
    assert h["postgres"] is True


@pytest.mark.asyncio
async def test_dataset_endpoint_rejects_an_unknown_dataset(pg):
    out = await api.get_dataset("btc_not_a_feed", limit=5)
    assert "error" in out
    assert (await api.get_dataset("btc_ohlcv_1m", limit=5)) == []


@pytest.mark.asyncio
async def test_auditor_endpoints_are_read_only():
    """§1.2/§12: the Auditor's output is a document, not a command. An endpoint
    is a pathway, so there must not be one."""
    auditor_routes = [r for r in api.app.routes
                      if getattr(r, "path", "").startswith("/api/auditor")]
    assert auditor_routes
    for r in auditor_routes:
        assert set(r.methods) <= {"GET", "HEAD"}, f"{r.path} accepts {r.methods}"


@pytest.mark.asyncio
async def test_the_only_mutating_endpoint_is_the_operator_kill():
    """Every other route is a read. The master kill is deliberately a POST and
    deliberately token-gated — it is the one place the dashboard may change
    system state."""
    mutating = [(r.path, sorted(r.methods)) for r in api.app.routes
                if getattr(r, "methods", None)
                and not set(r.methods) <= {"GET", "HEAD"}]
    assert mutating == [("/api/risk/master_kill", ["POST"])]


@pytest.mark.asyncio
async def test_auditor_verify_endpoint_reports_a_missing_report(pg):
    out = await api.verify_auditor_report("00000000-0000-0000-0000-000000000000")
    assert out["error"] == "no such report"
