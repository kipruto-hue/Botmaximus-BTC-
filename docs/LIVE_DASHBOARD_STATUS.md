# BOTMAXIMUS (BTC) — Live Dashboard: What Is Real, What Is Not

**Snapshot:** 2026-07-24 · engine uptime ~6h40m · BTC/USD ~$63,965 · 0 quarantined
**Repo:** main @ b717f2c · **Ports:** Mongo 27017 · engine 8300 · dashboard 5173

> Read this before trusting anything on the screen. The dashboard mixes **real
> live data** with **simulated placeholders**, and the two look alike. This
> document says exactly which is which, how the real parts work, and why the
> "trading" you see is not real yet.

---

## 1. The one thing to understand first

**Nothing is trading. No strategy exists. No decision is being made from data.**

The system today is a **data layer** (built, real, running) plus the first two
layers of a decision system — a **risk core** and a **backtest harness** (built,
real, but not yet driving any trade). Everything on the dashboard that looks
like *trading* — open positions, P&L, the strategy pool, the "scrutiny feed"
of TRADE/SKIP reasoning, the world-state posture — is a **browser-side
animation driven by `Math.random()`**. It reads from no data and influences
nothing.

| Dashboard panel | Status | Source |
|---|---|---|
| Status bar — BTC price, uptime, WS state, LIVE DATA chip | **REAL** | `/ws/live` telemetry |
| Pipeline telemetry (gather/parse/quality/store, freshness, counts) | **REAL** | `/ws/live` telemetry |
| Risk to kill (drawdown gauge) | **WAS FAKE → now REAL** | `/api/risk` (see §5) |
| Account (equity, day P&L, limits, kill stack) | **WAS FAKE → now REAL** | `/api/risk` (see §5) |
| Open positions | **SIMULATED** | `Math.random()` — no execution engine exists |
| Strategy pool (allocations, decay) | **SIMULATED** | `Math.random()` — no strategies exist |
| Scrutiny feed ("reasoning · live") | **SIMULATED** | `Math.random()` — no LLM, no gate exists |
| World state (FOMC/CPI events, posture) | **SIMULATED** | hard-coded fake events |

---

## 2. The real live data (what is actually being collected)

Six feeds stream from Binance and are stored in MongoDB after passing a quality
gate. Live counts this session:

| Feed | What it is | Cadence | Records (session) | In Mongo |
|---|---|---|---|---|
| BTC ticks | last trade price/qty | ~1/s (throttled) | 500 (rolling) | 28,207 |
| OHLCV 1m | 1-minute candles | per close | 174 | 11,966 |
| Funding rate | live mark price + funding | 30s | 351 | 784 |
| Open interest | total open futures positions | 30s poll | 347 | 807 |
| Liquidations | forced closes (event-driven) | as they happen | 216 | 344 |
| Order book | top-20 depth, spread, imbalance | 5s | 500 (rolling) | 4,513 |
| Funding 8h history | settled funding series (backtest cost) | REST backfill | — | 2,210 |
| OI 5m history | 5-min open-interest history | REST backfill | — | 8,863 |

**Totals:** 12,747 records stored this session, **0 quarantined**, 1,718
candles auto-backfilled after downtime, 61 socket reconnects survived cleanly.

### Coverage ledger (how complete the data actually is)
The system tracks, per feed per time-slot, whether data truly exists — so a
future backtest can refuse to run on windows with holes.

| Feed | Window | Complete |
|---|---|---|
| OHLCV 1m | 7 days | 86.7% (1,344 slots missing from off-hours) |
| Funding 8h hist | 30 days | 98.9% |
| OI 5m hist | 30 days | 99.5% |
| Liquidations | 24h | 12.6% |
| Order book | 24h | 12.6% |

The low liquidation/order-book coverage is **correct and honest**: those feeds
can only be captured while the process is running (there is no history endpoint
to backfill them), so every hour the PC is off is permanently missing data.
This is why 24/7 uptime (a VPS) matters before any real strategy relies on them.

---

## 3. How collection and parsing work (the real pipeline)

Every record flows through a timed four-stage pipeline and is only stored if it
passes a five-layer quality gate:

```
Binance WebSocket / REST
      │  (collector stamps collection_time on receipt)
      ▼
  gather_q ──► PARSE  ──► parse_q ──► QUALITY ──► store_q ──► WRITE ──► MongoDB
                │                        │                       │
         raw → canonical            5-layer gate            dedupe +
         envelope (§3.1)          (quarantine bad)        coverage ledger
```

- **Gather:** three WebSocket collectors (spot, futures `/market`, futures
  `/public`) plus REST pollers. Auto-reconnect with backoff; each message
  timestamped on arrival.
- **Parse:** raw Binance JSON → a canonical envelope. `event_time` always comes
  from the exchange, never the local clock (prevents lookahead).
- **Quality gate (5 layers):** lookahead protection, impossible-value checks
  (crossed book, nonpositive price, implausible funding), liquidity/staleness
  flags, phantom-jump detection, source tagging. Hard failures are
  **quarantined**, not stored (quarantine count: 0).
- **Write:** dedupe (time-series collections are insert-only), then update the
  coverage ledger. Each stored candle carries its stage latencies — the
  Pipeline telemetry panel shows these live.

This is the part of the dashboard that is genuinely live and trustworthy.

---

## 4. How the "demo trade" is done (and why it means nothing)

The Open Positions, Strategy Pool, and Scrutiny Feed panels are produced by a
single `setInterval` loop in `dashboard/src/Dashboard.jsx` that runs every
1.6 seconds and calls `Math.random()`:

- **Positions P&L** drifts on an internal random walk (`rnd(-55, 55)`) that is
  **decoupled from the real BTC price** — the position P&L can show green while
  real BTC falls, because it never reads the real price.
- **Scrutiny verdicts** ("4H bias up · 15m breakout held · funding neutral")
  are picked at random from a hard-coded list of plausible-sounding strings.
  The panel is labelled "reasoning · live" but **there is no LLM and no
  reasoning** — it is theatre.
- **Strategy decay** values wander randomly; the strategies (`MR-BAND-01`, etc.)
  do not exist anywhere in the backend.
- **World-state events** (FOMC, CPI, options expiry) are hard-coded countdowns,
  not a real economic calendar.

These panels carry a small amber **SIM** badge, but the animation is convincing
enough to be mistaken for live trading. That is the core hazard this document
exists to correct.

---

## 5. THE FLAW (found and fixed)

**Flaw:** the left-column risk panels displayed **fabricated numbers that
contradicted the real risk core**, and one of them understated danger:

| Shown on dashboard (before) | Real value (`/api/risk`) | Consequence |
|---|---|---|
| **KILL @ 18%** drawdown | **15%** hard-kill | Overstated headroom before the system flattens — the single most dangerous kind of dashboard lie |
| Drawdown gauge on a random walk (~6%) | **0.0%** actual | Showed risk that wasn't there / could hide risk that is |
| Risk / trade **0.35%** | **0.25%** | Wrong sizing assumption |
| Open risk **1.4% eq** | **0.0%** (no positions) | Fabricated exposure |
| Day P&L random | **0.0%** | Fabricated performance |

This is exactly the failure class the project's own governing document warns
against ("fictional fallbacks", "a dashboard must make the real/sim distinction
unmissable"). Because the risk core is now real and queryable at `/api/risk`,
there is no reason to show invented risk numbers.

**Fix applied:** the **Risk to kill** gauge and the **Account** panel are now
wired to `/api/risk`. They show the real equity ($10,000 paper), real peak,
real drawdown (0.0%), the real **15%** kill line, real per-trade (0.25%) and
max-open-risk (1.0%) limits, and the real kill-stack state (L1/L2/L3 all clear).
Their SIM badges are removed **only because they are now genuinely real** — the
still-simulated panels keep theirs.

**Still-open honesty gaps (documented, not yet changed):**
- The **MASTER KILL** button sets local browser state only; it is **not** wired
  to the real risk core's kill stack (that needs a dedicated, safety-reviewed
  endpoint — deliberately not added casually).
- Positions / strategy / scrutiny / world-state panels remain simulated until
  their subsystems exist (see §6–§8).

---

## 6. How past-trade data influences current trades — TODAY

**It does not. There are no past trades.** No order — paper or live — has ever
been placed by this system. The only "past run" that exists is **one backtest**
(`backtest_runs` collection, 1 document): a test SMA-momentum strategy replayed
over 24h of real BTC data, which the validation gate **correctly rejected**
(gross −$154, net −$635 after fees/funding/slippage — no edge).

So the honest answer to "how does past data influence the trades" right now is:
**the loop that would let it does not exist yet.** The pieces that will form
that loop are built bottom-up and only two of six exist:

| Layer | Purpose | Status |
|---|---|---|
| A · Risk core | hard limits, sizing, kill stack | **BUILT** |
| B · Backtest harness | prove/reject an edge on real history | **BUILT** |
| C · Strategy DSL + lifecycle | the strategies themselves | not built (blocked, see below) |
| D · Arbiter | resolve conflicting strategy signals | not built |
| E · Scrutiny gate | LLM veto using historical analogs | not built |
| F · Paper execution | actually place simulated orders | not built |

---

## 7. How decisions will form from past data, and how strategies are made

When Layers C–F exist, the decision loop will be:

```
market data ─► STRATEGIES ─► ARBITER ─► SCRUTINY GATE ─► RISK CORE ─► EXECUTION
 (6 feeds)     propose        pick one    (LLM veto,        (hard        (paper
               intents        or none     may only          limits,      orders +
                                          block)            sizing)      venue stops)
```

Past data enters this loop in two distinct places:

1. **Strategy formation (offline).** A strategy is a *parameter object* (not
   code) — an entry rule, exit rule, and risk-based sizing over named features.
   Seed strategies are hand-written from an economic rationale; later a
   generator proposes variations. **Every candidate must pass the backtest
   validation gate (Layer B) on real history before it is allowed to trade a
   cent** — minimum trade count, positive net expectancy *after costs*,
   deflated Sharpe (corrected for having tried many candidates), stability
   across ≥2 market regimes, and walk-forward consistency. This is how past
   data decides *which strategies are even allowed to exist*.

2. **Per-trade scrutiny (live, Layer E).** When a strategy proposes a trade,
   the LLM scrutiny gate will retrieve **historical analogs** — past moments
   with a similar market state and how they resolved — from a local vector
   store, and may **veto** the trade (it can never create or enlarge one). This
   is how past data will influence *individual live decisions*.

Neither path is active yet. Layer C is **blocked** on one missing input: the
**System Master Prompt v1.1** (the "constitution" that defines the strategy
schema), which has not been provided.

---

## 8. What "decaying strategies" means

A strategy is not trusted forever. It moves through a **lifecycle**, one step up
at a time, and can be demoted several steps at once:

```
candidate ─► paper ─► micro ─► full ─► retired
(no money)  (shadow) (min size)(full)   (dead, may spawn a repaired successor)
```

**Decay** is the statistical detection that a strategy's edge is *fading* —
measured properly, not "it lost today". The system tracks rolling expectancy,
hit rate, and how far live results have drifted from the backtest, using a test
that accounts for the losing streaks that are *normal* for a given win rate. A
real decay breach → the strategy is **down-weighted, then retired**; a retired
strategy can spawn a *repaired* successor that must re-pass the full validation
gate from scratch (a repair is a new strategy, not a patched live one).

On the dashboard today the "decay 41% ↯repair" numbers are **random noise** —
there is no strategy behind them. When Layer C is built, these will reflect the
real sequential decay test.

---

## 9. Summary: is the system "dynamic"?

- **The data layer is dynamic** in the resilience sense: it self-reconnects,
  self-heals candle gaps, quarantines bad data, and honestly tracks its own
  coverage and latency. That part is real and working well.
- **The trading system is not dynamic yet**, because the parts that would adapt
  to the market — strategies, arbiter, scrutiny gate, execution — do not exist.
  The multi-feed data now being collected (funding, OI, liquidations, book
  imbalance) is exactly what will make that future layer adaptive rather than a
  static one-signal bot. But the intelligence itself is unbuilt.

**Next unblock:** provide the **System Master Prompt v1.1** to start Layer C
(strategies), which is what turns the honest-but-static system into a real one.
