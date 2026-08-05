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
from botmaximus.db import mongo
from botmaximus.db.schema import DATASET_COLLECTIONS, QUARANTINE, ensure_schema
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


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global risk_core
    await ensure_schema()
    await coverage.ensure_indexes()
    await strategy_store.ensure_indexes()

    # risk core loads persisted equity/peak/kill state — a restart never resets it
    risk_core = RiskCore(mongo.get_db())
    await risk_core.load()

    writer = Writer()
    await writer.seed_dedupe()
    pipeline = Pipeline(parser=BinanceParser(), gate=QualityGate(), writer=writer)
    pipeline.start()
    sources = [
        BinanceCollector(pipeline.gather_q),
        BinanceFuturesMarketCollector(pipeline.gather_q),
        BinanceFuturesDepthCollector(pipeline.gather_q),
        BinanceOICollector(pipeline.gather_q),
        OhlcvBackfiller(pipeline.gather_q),
        FundingHistoryCollector(pipeline.gather_q),
        OIHistoryCollector(pipeline.gather_q),
        CoverageHeartbeat(),
    ]
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
        await mongo.close()


app = FastAPI(title="BOTMAXIMUS (BTC) data layer", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


def _serialize(doc: dict) -> dict:
    doc["_id"] = str(doc["_id"])
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
        "mongo": await mongo.ping(),
        "uptime_s": round(__import__("time").time() - telemetry.started_at),
    }


@app.get("/api/telemetry")
async def get_telemetry():
    return telemetry.snapshot()


@app.get("/api/risk")
async def get_risk():
    """Risk core state: limits, kill stack, equity peak/drawdown (§4)."""
    if risk_core is None:
        return {"error": "risk core not initialised"}
    return risk_core.snapshot()


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
                "direction": d["definition"].get("direction"),
                "regime_scope": d["definition"].get("regime_scope"),
                "required_feeds": d["definition"].get("required_feeds"),
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


@app.get("/api/ohlcv")
async def get_ohlcv(limit: int = 10):
    db = mongo.get_db()
    cursor = db[DATASET_COLLECTIONS["btc_ohlcv_1m"]].find().sort("event_time", -1).limit(min(limit, 500))
    return [_serialize(d) async for d in cursor]


@app.get("/api/ticks")
async def get_ticks(limit: int = 10):
    db = mongo.get_db()
    cursor = db[DATASET_COLLECTIONS["btc_price_tick"]].find().sort("event_time", -1).limit(min(limit, 500))
    return [_serialize(d) async for d in cursor]


@app.get("/api/data/{dataset_id}")
async def get_dataset(dataset_id: str, limit: int = 10):
    """Latest records for any dataset (funding, OI, liquidations, order book, …)."""
    coll = DATASET_COLLECTIONS.get(dataset_id)
    if coll is None:
        return {"error": f"unknown dataset_id; one of {sorted(DATASET_COLLECTIONS)}"}
    db = mongo.get_db()
    cursor = db[coll].find().sort("event_time", -1).limit(min(limit, 500))
    return [_serialize(d) async for d in cursor]


@app.get("/api/quarantine")
async def get_quarantine(limit: int = 20):
    db = mongo.get_db()
    cursor = db[QUARANTINE].find().sort("event_time", -1).limit(min(limit, 200))
    return [_serialize(d) async for d in cursor]


@app.get("/api/execution/calibration")
async def get_calibration(strategy_id: str | None = None):
    """Is the cost model telling the truth? Reports `status:
    no_realized_fills` until paper execution writes fills — deliberately, so an
    empty aggregate (every drift 0.0) is never mistaken for a calibrated one."""
    from botmaximus.execution import ledger
    return await ledger.calibration(strategy_id)


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(telemetry.snapshot())
            await asyncio.sleep(1.5)
    except (WebSocketDisconnect, ConnectionError):
        pass
