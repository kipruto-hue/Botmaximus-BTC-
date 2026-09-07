"""FastAPI surface for the dashboard (and later the Scrutiny Gate/Arbiter, §7).

The pipeline runs in the same event loop: the lifespan hook wires
collector → stages → writer and tears it down on shutdown.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from botmaximus.config import settings
from botmaximus.db.schema import DATASETS, ensure_schema
from botmaximus.orchestrator import startup as orchestrator_startup
from botmaximus.storage import degrade as storage_degrade
from botmaximus.storage import jobs as storage_jobs
from botmaximus.storage import postgres
from botmaximus.arbiter import core as arbiter_core
from botmaximus.execution import venue
from botmaximus.execution import ledger as execution_ledger
from botmaximus.obs import degradation
from botmaximus.scrutiny import gate as scrutiny_gate
from botmaximus.strategy import decay, trials
from botmaximus.pipeline import coverage
from botmaximus.pipeline.backfill import OhlcvBackfiller
from botmaximus.pipeline.bus import Pipeline
from botmaximus.pipeline.collectors.binance import BinanceCollector
from botmaximus.pipeline.collectors.binance_futures import (
    BinanceFuturesDepthCollector,
    BinanceFuturesMarketCollector,
)
from botmaximus.pipeline.collectors.binance_history import (
    FundingHistoryCollector,
    OIHistoryCollector,
)
from botmaximus.pipeline.collectors.binance_oi import BinanceOICollector
from botmaximus.pipeline.coverage import CoverageHeartbeat
from botmaximus.pipeline.parsers.binance import BinanceParser
from botmaximus.pipeline.quality.gate import QualityGate
from botmaximus.pipeline.telemetry import telemetry
from botmaximus.pipeline.writer import Writer
from botmaximus.risk.core import RiskCore
from botmaximus.strategy import store as strategy_store

log = logging.getLogger(__name__)

risk_core: RiskCore | None = None


def _venue_parser():
    """One parser per venue, both emitting the identical canonical envelope.
    Nothing downstream of here may know which venue produced a bar."""
    if settings.venue == "bybit":
        from botmaximus.pipeline.parsers.bybit import BybitParser
        return BybitParser()
    if settings.venue == "binance":
        return BinanceParser()
    raise RuntimeError(
        f"unknown venue {settings.venue!r} — expected 'bybit' or 'binance'. "
        f"Refusing to start rather than defaulting to a venue you did not pick.")


def _venue_sources(gather_q) -> list:
    """Collectors for the configured venue.

    Bybit needs one public socket for every topic; Binance needs three (its
    2026-04-23 migration split futures streams across routed paths). Both
    supply the same six datasets.
    """
    if settings.venue == "bybit":
        from botmaximus.pipeline.collectors.bybit import BybitPublicCollector
        from botmaximus.pipeline.collectors.bybit_history import (
            BybitFundingHistoryCollector,
            BybitOhlcvBackfiller,
            BybitOIHistoryCollector,
        )
        return [
            BybitPublicCollector(gather_q),
            BybitOhlcvBackfiller(gather_q),
            BybitFundingHistoryCollector(gather_q),
            BybitOIHistoryCollector(gather_q),
        ]
    return [
        BinanceCollector(gather_q),
        BinanceFuturesMarketCollector(gather_q),
        BinanceFuturesDepthCollector(gather_q),
        BinanceOICollector(gather_q),
        OhlcvBackfiller(gather_q),
        FundingHistoryCollector(gather_q),
        OIHistoryCollector(gather_q),
    ]


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global risk_core
    # The pool is lazy and opens no socket on import, so it must be opened
    # here — before ensure_schema() is the first thing to want a connection.
    # Its absence surfaced as PoolClosed wrapped in PostgresUnavailable, i.e.
    # as "the database is unreachable", which it was not. `postgres.close()`
    # in the finally below is the other half of this pair.
    await postgres.open_pool()
    await ensure_schema()
    await coverage.ensure_indexes()
    await strategy_store.ensure_indexes()
    await degradation.ensure_indexes()
    await arbiter_core.ensure_indexes()
    await scrutiny_gate.ensure_indexes()
    await execution_ledger.ensure_indexes()
    await decay.ensure_indexes()
    await trials.ensure_indexes()

    # Constitution §9: Bybit constants come from Bybit. Fetched once, here,
    # before anything can size an order. `venue.get()` raises if this was
    # skipped rather than falling back to assumed numbers.
    if settings.venue == "bybit":
        vc = await venue.init()
        log.info("venue constants: min_notional=%s maint_margin=%s taker=%s (%s)",
                 vc.min_notional, vc.tiers[0].maint_margin,
                 vc.taker_fee_rate, vc.fee_source)

    # risk core loads persisted equity/peak/kill state — a restart never resets it
    risk_core = RiskCore()
    await risk_core.load()

    writer = Writer()
    await writer.seed_dedupe()
    pipeline = Pipeline(parser=_venue_parser(), gate=QualityGate(), writer=writer)
    pipeline.start()
    # Storage jobs run alongside the collectors: tier-out nightly, integrity
    # hourly, checksum audit weekly (§4, §8, §11). They are supervised the same
    # way, because a storage job that dies silently leaves the condition it was
    # watching for unobserved.
    # The TradeLoop: OFF unless the operator turned it on. Built here, before
    # collectors start, so that an enabled-but-unwireable loop aborts startup
    # rather than coming up looking healthy (§6). When disabled this constructs
    # nothing and boot behaviour is unchanged.
    trade_loop = await orchestrator_startup.build(pipeline=pipeline,
                                                  risk_core=risk_core)

    sources = (_venue_sources(pipeline.gather_q) + [CoverageHeartbeat()]
               + storage_jobs.all_jobs())
    log.info("venue: %s (%s %s)", settings.venue, settings.symbol,
             settings.bybit_category if settings.venue == "bybit" else "futures")
    source_tasks = [
        asyncio.create_task(s.run(), name=f"{s.name}_collector") for s in sources
    ]
    log.info("pipeline started (%d sources)", len(sources))
    try:
        yield
    finally:
        for t in source_tasks:
            t.cancel()
        await asyncio.gather(*source_tasks, return_exceptions=True)
        await pipeline.stop()
        if trade_loop is not None:
            orchestrator_startup.log_stopped()
        await postgres.close()


app = FastAPI(title="BOTMAXIMUS (BTC) data layer", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


def _serialize(doc: dict) -> dict:
    doc = dict(doc)
    doc.pop("_id", None)
    for k, v in list(doc.items()):
        # uuid/Decimal are not JSON-serialisable; the dashboard only displays
        # these, so a lossless string beats a 500.
        if type(v).__name__ in ("UUID", "Decimal"):
            doc[k] = str(v)
    for k in ("event_time", "collection_time", "ingest_time"):
        if doc.get(k) is not None:
            doc[k] = doc[k].isoformat()
    payload = doc.get("payload", {})
    for k, v in list(payload.items()):
        if hasattr(v, "isoformat"):
            payload[k] = v.isoformat()
    return doc


@app.get("/api/health")
async def health():
    return {
        "status": "ok" if telemetry.ws_connected else "degraded",
        "ws_connected": telemetry.ws_connected,
        "postgres": await postgres.ping(),
        "uptime_s": round(__import__("time").time() - telemetry.started_at),
    }


@app.get("/api/telemetry")
async def get_telemetry():
    return telemetry.snapshot()


@app.get("/api/storage")
async def get_storage():
    """§11 storage telemetry: hot-window size per dataset, archive and
    quarantine growth, partitions, recent tier-outs, backup age and the last
    restore drill.

    Quarantine growth is on this panel deliberately — a spike there is a signal
    (venue outage, gate misfire, upstream corruption), and it is invisible
    anywhere else because no other consumer is allowed to read that bucket.
    """
    from botmaximus.storage import integrity, retention, tiering
    return {
        "tiering": await tiering.status(),
        "integrity": await integrity.status(),
        "retention": retention.summary(),
        "degradation": await storage_degrade.health(),
    }


@app.get("/api/auditor/reports")
async def get_auditor_reports(report_type: str | None = None, limit: int = 20):
    """§12 Auditor Feed. Read-only by construction: there is no POST, PATCH or
    DELETE anywhere under /api/auditor, because §1.2 gives the Auditor no
    pathway to act and an endpoint is a pathway."""
    from botmaximus.auditor import reports as auditor_reports
    return {"reports": await auditor_reports.latest(report_type,
                                                    min(limit, 100))}


@app.get("/api/auditor/reports/{report_id}")
async def get_auditor_report(report_id: str):
    from botmaximus.auditor import reports as auditor_reports
    row = await auditor_reports.get(report_id)
    return row or {"error": "no such report"}


@app.get("/api/auditor/verify/{report_id}")
async def verify_auditor_report(report_id: str):
    """The 'verify citation' affordance (§12): re-run a report's citations and
    show what the ledgers say now.

    Only SELECTs are executed — `citations.verify` refuses anything else. The
    citations are stored strings, and a verify endpoint that could be talked
    into running an UPDATE would be a hole in an otherwise sealed wall.
    """
    from botmaximus.auditor import citations as cit
    from botmaximus.auditor import reports as auditor_reports
    row = await auditor_reports.get(report_id)
    if not row:
        return {"error": "no such report"}
    result = await cit.verify([cit.Citation(**c) for c in row["citations"]])
    return {"report_id": report_id, "checked": result.checked,
            "passed": result.passed, "mismatches": result.mismatches,
            "unrunnable": result.unrunnable}


@app.get("/api/storage/integrity")
async def run_storage_integrity():
    """Run the §11 checks now rather than waiting for the hourly job."""
    from botmaximus.storage import integrity
    return {"results": [r.to_dict() for r in await integrity.run_all()]}


@app.get("/api/risk")
async def get_risk():
    """Risk core state: limits, kill stack, equity peak/drawdown (§4)."""
    if risk_core is None:
        return {"error": "risk core not initialised"}
    return risk_core.snapshot()


def _defn(row: dict) -> dict:
    """The compiled definition, or an empty dict when the blob is absent."""
    return row.get("definition") or {}


@app.get("/api/strategies")
async def get_strategies(state: str | None = None):
    """The strategy population and where each one sits in its lifecycle.

    Real, not simulated — but note what it is *not*: a claim that anything here
    has an edge. `candidate` means proposed and parseable; only a strategy past
    `candidate` has cleared the §5.5 gate, and in Pass C1 nothing is promoted
    automatically.
    """
    states = [s.strip() for s in state.split(",")] if state else None
    pop = await strategy_store.list_population(states)
    counts: dict[str, int] = {}
    for d in pop:
        counts[d["lifecycle_state"]] = counts.get(d["lifecycle_state"], 0) + 1
    return {
        "count": len(pop),
        "by_state": counts,
        "strategies": [
            {
                "strategy_id": d["strategy_id"],
                "version": d.get("version"),
                "lifecycle_state": d["lifecycle_state"],
                "origin": d.get("origin"),
                # `definition` comes from a LEFT JOIN on the blob table, so it
                # is None whenever the blob is absent — a migrated row, or a
                # strategy registered before its definition landed. Indexing
                # into it directly turns that into a 500 on the dashboard's
                # main panel rather than a missing field on one row.
                "direction": _defn(d).get("direction"),
                "regime_scope": _defn(d).get("regime_scope"),
                "required_feeds": _defn(d).get("required_feeds"),
                "rationale": d.get("rationale"),
                "warnings": d.get("warnings", []),
                "last_verdict": d.get("last_verdict"),
                "updated_at": d["updated_at"].isoformat() if d.get("updated_at") else None,
            }
            for d in pop
        ],
    }


@app.get("/api/strategies/{strategy_id}")
async def get_strategy(strategy_id: str):
    doc = await strategy_store.get(strategy_id)
    if doc is None:
        return {"error": "not_found", "strategy_id": strategy_id}
    for k in ("created_at", "updated_at"):
        if doc.get(k) is not None:
            doc[k] = doc[k].isoformat()
    history = await strategy_store.events(strategy_id)
    for e in history:
        e["at"] = e["at"].isoformat()
    return {"strategy": doc, "events": history}


@app.get("/api/coverage")
async def get_coverage(feed: str = "btc_ohlcv_1m", hours: int = 24):
    """Coverage-ledger summary for a feed over the last `hours` (§5.1)."""
    from datetime import datetime, timedelta, timezone
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    return await coverage.summary(feed, start, end)


async def _latest(dataset_id: str, limit: int) -> list[dict]:
    """Most recent records for a dataset, from the Postgres hot window.

    Only the hot window: anything older lives in Parquet, and §12 is explicit
    that the dashboard never reads Parquet directly. An endpoint that silently
    reached into the archive would turn a 200ms panel into a multi-second scan.
    """
    rows = await postgres.fetch(
        "SELECT * FROM market_records "
        "WHERE venue = %s AND dataset_id = %s AND valid_to_sys IS NULL "
        "ORDER BY event_time DESC LIMIT %s",
        (settings.venue, dataset_id, min(limit, 500)))
    return [_serialize(r) for r in rows]


@app.get("/api/ohlcv")
async def get_ohlcv(limit: int = 10):
    return await _latest("btc_ohlcv_1m", limit)


@app.get("/api/ticks")
async def get_ticks(limit: int = 10):
    return await _latest("btc_price_tick", limit)


@app.get("/api/data/{dataset_id}")
async def get_dataset(dataset_id: str, limit: int = 10):
    """Latest records for any dataset (funding, OI, liquidations, order book, …)."""
    if dataset_id not in DATASETS:
        return {"error": f"unknown dataset_id; one of {sorted(DATASETS)}"}
    return await _latest(dataset_id, limit)


@app.get("/api/quarantine")
async def get_quarantine(limit: int = 20):
    """Quarantine POINTERS, not payloads.

    The rejected records themselves live in the quarantine bucket (§3.J), which
    no hot path reads. What Postgres holds — and what this returns — is the
    index: which dataset, which check failed, and the archive key to go and
    look at if an operator is investigating.
    """
    rows = await postgres.fetch(
        "SELECT * FROM quality_events ORDER BY at DESC LIMIT %s",
        (min(limit, 200),))
    return [_serialize(r) for r in rows]


MASTER_KILL_TOKEN = "CONFIRM-MASTER-KILL"


@app.post("/api/risk/master_kill")
async def master_kill(token: str = "", reason: str = "operator"):
    """Operator master kill: block, cancel every order, close every position.

    Token-guarded in the same style as the L3 reset. This is the first
    non-simulated kill button in the project — before it, the dashboard's kill
    was local-only, which is the most dangerous kind of safety control: one
    that looks armed and does nothing.
    """
    if token != MASTER_KILL_TOKEN:
        return {"error": "invalid token", "hint": "POST ?token=CONFIRM-MASTER-KILL"}
    if risk_core is None:
        return {"error": "risk core not initialised"}
    result = await risk_core.kills.master_kill(f"operator:{reason}")
    return {
        "killed": True,
        "flatten": result,
        "warning": None if result else
        "no execution layer attached — entries are blocked but any open "
        "position is still on",
        "risk": risk_core.snapshot(),
    }


@app.get("/api/execution/calibration")
async def get_calibration(strategy_id: str | None = None):
    """Is the cost model telling the truth? Reports `status:
    no_realized_fills` until paper execution writes fills — deliberately, so an
    empty aggregate (every drift 0.0) is never mistaken for a calibrated one."""
    from botmaximus.execution import ledger
    return await ledger.calibration(strategy_id)


@app.get("/api/arbiter/events")
async def get_arbiter_events(limit: int = 50):
    """Real decisions, including the refusals. `reason` explains every
    no-trade: conflict, cooldown, outside_window, position_open, all_stale."""
    rows = await postgres.fetch(
        "SELECT * FROM arbiter_events ORDER BY at DESC LIMIT %s",
        (min(limit, 200),))
    return [_serialize(r) for r in rows]


@app.get("/api/scrutiny/events")
async def get_scrutiny_events(limit: int = 50):
    rows = await postgres.fetch(
        "SELECT * FROM scrutiny_events ORDER BY at DESC LIMIT %s",
        (min(limit, 200),))
    return {"provider": settings.scrutiny_provider,
            "events": [_serialize(r) for r in rows]}


@app.get("/api/degradation")
async def get_degradation(limit: int = 50):
    """Constitution §11 made visible: what has fallen back, and how often.
    A non-empty counter here is the system telling you it is not at full
    strength — silence is the only healthy reading."""
    rows = await postgres.fetch(
        "SELECT * FROM telemetry_events WHERE kind = 'degraded' "
        "ORDER BY at DESC LIMIT %s", (min(limit, 200),))
    return {"counts": degradation.counts(),
            "total": degradation.total(),
            "recent": [_serialize(r) for r in rows]}


@app.get("/api/scrutiny/calibration")
async def get_scrutiny_calibration(days: int = 7):
    """Veto precision, recall and consistency — the three numbers that judge
    the gate. Conviction is deliberately not among them: it is the model's
    self-report, and only ground truth feeds back.

    Drift is fixed by changing thresholds, `k`, or the prompt — never by
    raising temperature, which would attack the consistency being measured."""
    from botmaximus.scrutiny import calibration
    return (await calibration.report(None, days)).to_dict()


@app.get("/api/venue")
async def get_venue():
    """The constants orders are actually sized against, and where they came
    from. `fee_source: config` means the account's real fee schedule was not
    readable and an assumed rate is in use."""
    try:
        vc = venue.get()
    except RuntimeError as e:
        return {"error": str(e)}
    return {
        "host": vc.host, "symbol": vc.symbol, "category": vc.category,
        "qty_step": vc.qty_step, "min_order_qty": vc.min_order_qty,
        "min_notional": vc.min_notional, "tick_size": vc.tick_size,
        "maint_margin_lowest_tier": vc.tiers[0].maint_margin,
        "tiers": len(vc.tiers),
        "taker_fee_rate": vc.taker_fee_rate,
        "maker_fee_rate": vc.maker_fee_rate,
        "fee_source": vc.fee_source,
        "testnet": settings.bybit_testnet,
        "live_trading_enabled": settings.live_trading_enabled,
    }


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(telemetry.snapshot())
            await asyncio.sleep(1.5)
    except (WebSocketDisconnect, ConnectionError):
        pass
