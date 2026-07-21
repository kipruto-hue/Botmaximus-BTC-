# BOTMAXIMUS (BTC) — Data Layer

The data collection spine from the **Data Collection Master Prompt v1.0**:
a four-stage timed pipeline (gather → parse → quality → store) streaming
Binance BTC data into MongoDB, instrumented end-to-end (§9), guarded by the
five-layer quality gate (§8), and surfaced on a live dashboard.

## Status (§11 build order)

- [x] 1. Canonical envelope + Mongo time-series collections + indexes
- [x] 2. One streaming collector (Binance kline_1m + aggTrade) end-to-end, fully timed
- [x] 3. Five-layer quality gate (freshness, sanity/phantom, liquidity, timestamps/lookahead, source reliability) with quarantine
- [ ] 4. Funding, OI, liquidations, order book, ETF flows
- [ ] 5. RSS + event parsing, event calendar
- [ ] 6. ReactionRecord backfill
- [ ] 7. RAG indexing + temporal isolation
- [ ] 8. Interfaces out (Scrutiny Gate, Arbiter, Generator)

## Run

```powershell
# MongoDB (portable, no admin needed) — data in .\data\db
.\start-mongo.ps1

# server (pipeline + API on :8300)
cd server
.\.venv\Scripts\python.exe -m botmaximus.main

# dashboard (Vite on :5173, proxies /api and /ws to :8300)
cd dashboard
npm run dev
```

## API

- `GET /api/health` — pipeline/ws/Mongo status
- `GET /api/telemetry` — per-stage p50, end-to-end p95, freshness vs staleness budgets
- `GET /api/ohlcv?limit=10` · `GET /api/ticks?limit=10` — stored records
- `GET /api/quarantine` — records that failed the quality gate
- `WS /ws/live` — telemetry snapshot every 1.5s (feeds the dashboard)

## Layout

```
server/botmaximus/
  config.py                §10 parameter surface (.env overridable)
  pipeline/envelope.py     §3.1 canonical record envelope
  pipeline/bus.py          §2 bounded queues + timed stage workers
  pipeline/collectors/     websocket collectors (base + binance)
  pipeline/parsers/        raw payload → envelope
  pipeline/quality/gate.py §8 five-layer gate
  pipeline/writer.py       routing, dedupe, quarantine
  pipeline/telemetry.py    §9 rolling p50/p95 + freshness
  db/schema.py             §3.2 time-series collections, TTL, indexes
  api/app.py               FastAPI + /ws/live
dashboard/                 React dashboard; telemetry panel is live, SIM panels await their subsystems
```

Tests: `cd server; .\.venv\Scripts\python.exe -m pytest tests -q`
