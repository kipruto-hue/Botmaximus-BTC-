# BOTMAXIMUS (BTC) — STORAGE ARCHITECTURE MASTER PROMPT

**Version:** 2.0 (supersedes v1.0 and the Mongo Cancellation Directive; both archived)
**Scope:** The complete, authoritative storage specification for Botmaximus. How every piece of data the system touches — raw feeds, cleaned data, quarantined data, features, strategies (proposed / validated / live / decayed / retired), trials, verdicts, orders, fills, ledger entries, LLM prompts and responses — is stored, timestamped, tiered, retained, backed up, and made reproducible.
**Host:** Vultr, Tokyo, with a Singapore mirror for the archive.
**Companions:** System v1.1 · Data Collection v1.0 · Finish-the-System v1.0 · LLM Parameters v1.0 · Strategy DSL v1.0 · Decision & Execution v1.0.
**Precedence:** System v1.1 constitution > this document > implementation choices.

> "Institutional-grade" here means a **discipline** — immutability, lineage, bitemporal time, point-in-time reproducibility, quarantine separation, tiered access — applied consistently across two stores on one hosting provider.

---

## 0. THE FOUR PROPERTIES THAT MATTER (READ FIRST)

Everything in this document exists to preserve these four properties. Any implementation choice that violates them is a defect.

**P1 — Immutability with lineage.** No record is ever overwritten. Corrections are new records that supersede prior ones; the prior records remain queryable forever. Every record carries the identity of the process that wrote it and the code version that produced it.

**P2 — Point-in-time reproducibility.** The system can answer any query "as of *T*" and return the answer it would have returned at time *T*. This is what makes backtests provable. A backtest that consults a cleaned-in-hindsight table is a lie regardless of how good the code is.

**P3 — Bitemporal separation.** *Event time* (when did it happen in the world) and *system time* (when did we learn about it) are separate axes on every record. A late correction updates event-time facts *without* moving system time. This kills the silent-revision failure mode entirely.

**P4 — Quarantine never mixes with production.** Bad data is stored — for forensics, incident review, and quality-gate tuning — in a bucket that no hot path, no backtester, and no strategy training ever touches. There is no "cleaned up and reintroduced" pathway. If in doubt, quarantine.

---

## 1. THE TWO STORES

Botmaximus uses two storage systems, both on Vultr. Each is chosen for one thing it does exceptionally well; between them they cover every workload the system has, at every latency budget it needs.

### 1.A — PostgreSQL 16 on the Vultr VPS (Tokyo)

**Owns everything the system decides, commits, or needs to serve interactively:**
- Strategy pool, lifecycle events, trials
- Arbiter events, scrutiny verdicts, LLM provenance manifests
- Orders, fills, execution ledger (predictions, realizations, drift), kill events
- Coverage ledger, quality events, storage manifest, telemetry snapshots
- **The last 48 hours of market data** as time-partitioned tables (the hot window — see §3.C)

**Why here:** these workloads demand ACID transactions (a fill-and-ledger-update must be atomic), foreign keys (a fill *must* reference an order that exists), constraints (a strategy's lifecycle state can't be `full` if trials shows it never left `paper`), and joins. Postgres finds invariant violations at write time; alternatives hide them.

**Configuration:** WAL archived continuously to Object Storage. Point-in-time recovery enabled. Data directory on encrypted volume. Time-partitioned tables for market-data hot window (§3.C) with automated partition management.

### 1.B — Parquet on Vultr Object Storage (Tokyo primary + Singapore mirror)

**Owns the permanent, immutable record of everything:**
- All market data (ticks, klines, funding, OI, book snapshots, liquidations)
- All features (per version, per day)
- Backtest per-bar signals and equity curves
- Full LLM prompts and full LLM responses (blobs; metadata is in Postgres)
- **Quarantine** in a separate bucket that no live consumer reads

**Why here:** columnar storage is 10–50× smaller than JSON for numeric time-series and orders of magnitude faster for backtest scans. Object Storage is cheap, replicated by Vultr, and independent of the VPS — if the VPS burns down, the archive survives. This is the actual institutional pattern for permanent historical data.

**Partitioning:** every dataset partitioned by **UTC date** (`year=YYYY/month=MM/day=DD/`) and within a day by dataset id. This makes point-in-time reads trivially efficient and daily writes atomic.

**Buckets:**
- `botmaximus-archive-tokyo` — primary permanent record
- `botmaximus-quarantine-tokyo` — bad-data forensics store
- `botmaximus-mirror-singapore` — last 90 days mirrored; disaster recovery

### The rule of thumb for placement

| Question | Store |
|---|---|
| Does a mistake here corrupt a decision or its audit trail? | Postgres |
| Is this the permanent record of what happened? | Parquet |
| Was this rejected by the quality gate? | Quarantine (separate Parquet bucket) |
| Is it neither? | It does not get stored. |

If a piece of data belongs in more than one, it lives in more than one — but Postgres or Parquet is the **system of record**, and the other copy is a derived view that can be rebuilt from it.

---

## 2. THE CANONICAL RECORD ENVELOPE (BITEMPORAL)

Every record in either store carries these fields. Payload is dataset-specific and typed.

```
record_id            uuid v7 (time-ordered — sortable + globally unique)
dataset_id           e.g. "btc_ohlcv_1m", "scrutiny_verdict", "trial_result"
source               e.g. "bybit_v5_ws", "generator_gpt-5.5", "backtester"
symbol               "BTCUSDT" or null (non-market records)

# --- BITEMPORAL AXES ---
event_time           UTC — when the fact was true in the world
collection_time      UTC — when this process observed it
ingest_time          UTC — when it was written to the store of record

# --- SYSTEM-TIME VERSIONING (for corrections) ---
valid_from_sys       UTC — when this record became the current truth
valid_to_sys         UTC — null while current; set on supersession
supersedes           record_id or null — the record this replaces (never deleted)
correction_reason    text or null — why the correction was made

# --- LINEAGE / REPRODUCIBILITY ---
producer             hostname + process + pid
code_version         git SHA of the writer
schema_version       int — for envelope evolution
config_hash          hash of relevant config at write time

# --- QUALITY ---
quality_flags        list of check names (never margins) — empty if clean
quality_ok           bool — set only if the full quality gate passed
quality_gate_version int — the gate version that judged this

# --- LATENCY ---
stage_latency_ms     {gather, parse, quality, store}

# --- PAYLOAD ---
payload              dataset-specific fields (typed)
```

**Three invariants:**
1. **`supersedes` chains are append-only.** A record's history is the graph of supersessions ending at it. No node is ever deleted.
2. **`event_time` is authoritative from the venue.** Never the local clock. If the venue didn't stamp it, the record is quarantined.
3. **Corrections are new records.** A late-arriving revision of a funding rate is written as a *new* record with the original `event_time`, `valid_from_sys` = now, and `supersedes` pointing to the record it replaces. The prior record's `valid_to_sys` is set to now. Backtests as-of any past `valid_from_sys` see the world as it was then.

---

## 3. WHERE EACH DATASET LIVES

### 3.A — Market data (from the collectors)

| Dataset | Postgres hot window (48h default) | Parquet archive | Notes |
|---|---|---|---|
| `btc_ohlcv_1m` | 7 days | permanent | `confirm:true` only |
| `btc_price_tick` (throttled) | 24 hours | permanent | 1s cadence |
| `btc_funding` | 30 days | permanent | REST-backfillable |
| `btc_open_interest` | 7 days | permanent | REST-backfillable (Bybit has 2y history) |
| `btc_orderbook_snap` | 24 hours | permanent | **event-driven — gaps are permanent** |
| `btc_liquidations` | 30 days | permanent | **event-driven — gaps are permanent** |

Hot-window durations are per-dataset overrides in config. The tier-out job (§4) moves data from Postgres partitions to Parquet nightly.

### 3.B — Coverage ledger

Postgres. One row per feed per slot (e.g. per minute), state `complete | partial | missing`. This is the truth the backtester consults to refuse incomplete windows. Snapshotted to Parquet nightly so historical coverage state is itself point-in-time queryable.

### 3.C — The live collector's working state

Not a store — memory. Each collector holds current tick, in-progress kline (unstored until `confirm:true`), current book snapshot with deltas applied, current funding, current OI in-process. This is what the trade loop and the dashboard's live-price WebSocket read from, so the hot path never touches a database for "current" anything.

On restart: rebuild by (a) querying Postgres for last committed timestamps, (b) REST-backfilling OHLCV/funding/OI from the venue, (c) event-driven feeds resume from now — historical gaps stay permanent. In-memory state is a working buffer, never the record of truth. Collector state-recovery-on-restart is safety-critical and gets an explicit deployment-checklist test.

### 3.D — Features

Postgres for metadata (feature id, definition, version, code hash). Parquet for the computed feature vectors, partitioned by day, one directory per feature-set version. Never recomputed on top of prior files — a schema change is a new version directory, both versions coexist until the operator retires the old one.

### 3.E — Strategies (the pool)

Postgres. Four tables:
- `strategies` — one row per strategy version. A "repair" is a new strategy with `parent_id` set, not an update. `(id, version)` unique.
- `strategy_lifecycle_events` — every state transition (`candidate → paper → micro → full → retired`), timestamped, with reason and actor (operator or auto).
- `strategy_definitions_blob` — the compiled DSL JSON as an immutable blob, hashed. Same hash across backtest and live proves compilation is deterministic.
- `strategy_provenance` — for LLM-generated strategies, foreign key to the generation manifest (§3.G).

Retired strategies stay forever. Retirement is a state, not a deletion.

### 3.F — Trials

Postgres `trials` table. Feeds `n_trials` into the deflated Sharpe correction. Every candidate that reached validation is a row here, forever. Repair attempts count. Nothing is ever deleted. This is a legal requirement of statistical honesty, not a nice-to-have.

### 3.G — LLM generation and scrutiny provenance

Postgres tables `generations` and `scrutiny_events` hold the structured fields (model_id, prompt_version, seed, temperature, parent_id, lineage_depth, timestamps, verdict, latency, etc.). Parquet holds the **full prompt text and full response text** as immutable blobs — these are large, rarely read, but must survive forever. Partitioned by day. This is what makes the "closes audit A4" provenance requirement institutional-grade rather than best-effort. A candidate whose provenance blob is missing must not enter validation.

### 3.H — Backtest results

Postgres for metadata (config hash, coverage summary, verdict, gross/net metrics, `as_of` timestamp). Parquet for equity curve, trade list, and per-bar signals — large and columnar-friendly. Linked by `backtest_run_id`.

### 3.I — Orders, fills, execution ledger

**Postgres.** These are the money records — every field ACID-critical, every join a hard requirement. Tables: `orders`, `fills`, `execution_ledger_predictions`, `execution_ledger_realizations`, `execution_ledger_drift`, `kill_events`. Nightly export to Parquet for the archive, but Postgres is the system of record.

### 3.J — Quarantine

Separate Object Storage bucket (`botmaximus-quarantine-tokyo`). Same envelope, same partitioning, but no hot path, no backtester, no strategy training ever reads from it. Written to by the quality gate on every rejection with the failing check names, the raw payload, and enough context to reconstruct why the check fired. **Never** rehabilitated back into production. Read only by:
- Operator forensic review after an incident.
- Offline quality-gate tuning (measured against known-bad examples).

---

## 4. TIERING (POSTGRES HOT WINDOW → PARQUET ARCHIVE)

Data flows outward, never inward. Records move from Postgres partitions to Parquet daily; Parquet files transition to cold storage class over time. Nothing ever moves back in production. Backtests read Parquet directly.

**Tier policy (default; per-dataset overrides allowed):**

| Age | Location | Access latency | Use |
|---|---|---|---|
| 0 – 48h | Postgres time-partitioned table + Parquet | sub-10ms Postgres | live trade loop, dashboard, recent backtests |
| 48h – 90d | Parquet (warm) | seconds | backtester, generator context, calibration |
| 90d – 2y | Parquet (warm, compressed) | seconds | walk-forward, sealed holdout |
| > 2y | Parquet (cold storage class) | minutes | audit, long-window studies |

**Tier-out job** runs nightly at 00:15 UTC (after daily boundaries settle):
1. For each dataset, verify the completed day's Parquet daily partition matches the corresponding Postgres partition (row count + checksum).
2. Only after verification, drop the Postgres partition.
3. Write a `tier_out_events` row to Postgres with counts, checksums, and result.

**Tier-out failure never drops the Postgres partition.** If Parquet verification fails, the Postgres partition stays and the operator is alerted.

**Latency budgets, confirmed workable:**

| Consumer | Budget | Source |
|---|---|---|
| Dashboard structured panels | 200ms | Postgres |
| Dashboard live price tick | ~real-time | Collector WebSocket (in-memory) |
| Scrutiny Gate analog lookup | 800ms | Parquet (as-of query) |
| Live feature reads | < 100ms | In-process feature layer |
| Executor pre-trade check | < 50ms | Postgres |
| Backtester history scan | seconds-to-minutes | Parquet |

Nothing Botmaximus does requires sub-millisecond database reads.

---

## 5. THE QUALITY GATE ↔ QUARANTINE PATHWAY

Every incoming record passes through the five-layer quality gate (freshness, sanity, stale/liquidity, timestamp reconciliation, source reliability). Outcome is binary:

- **Pass** → written to Postgres hot window + Parquet daily partition with `quality_ok=true`.
- **Fail** → written **only to quarantine** with `quality_ok=false` and the failing check names. Never enters production stores. Never surfaces to backtester or generator.

**Two operational rules:**

1. **Retroactive quarantine.** If a record that passed the gate is later found to be bad (post-hoc reconciliation, a venue-issued correction), it is *not* deleted from production. A *correction record* is written (§2, bitemporal) that supersedes it, and the original is *also* copied to quarantine with `flag=retroactive`. Both exist forever.
2. **Gate version stamped on every record.** `quality_gate_version` in the envelope. Re-tuning the gate never rewrites history — only future records see the new version.

---

## 6. POINT-IN-TIME QUERIES

The read API supports two shapes:

- **As-of latest** (`get(dataset, event_time_range)`) — returns the current truth for that event-time range. This is what live consumers use.
- **As-of *T*** (`get(dataset, event_time_range, as_of=T)`) — returns the truth the system knew at *T*: records where `valid_from_sys ≤ T` and (`valid_to_sys is null OR valid_to_sys > T`). This is what backtests use.

**Every backtest run records its `as_of` timestamp** (default = run start) in Postgres alongside its `backtest_run_id`. A re-run with the same `as_of` returns the same data, byte-for-byte. This is what makes "the backtest we ran in April" reproducible in November.

**A backtest that does not pin `as_of` is a defect.** The runner refuses to start without one.

---

## 7. DEGRADATION

Postgres is the single point of failure for decisions. It is treated accordingly.

- **Postgres unreachable → L2 halt.** Executor stops new positions; existing positions managed to exit if possible; operator alerted. No file fallback, no queued-write side-channel. A queued write that never lands is worse than a rejected trade.
- **Postgres slow (writes queue up) → back-pressure at collector.** The collector's bounded queues fill; upstream shedding starts at the collector, not at the writer. Persistent → L2 halt.
- **Parquet / Object Storage unreachable → collector continues writing to Postgres hot window; tier-out defers.** Backtester operations that need Parquet fail loudly with reason `archive_unavailable`. Live trading continues if all other checks pass.
- **Collector in-memory state lost (process restart) → rebuild per §3.C.** Never treat rebuilt state as the record of truth for anything before the restart timestamp.

All degradations write a `degraded` event to Postgres `telemetry_events` (or to a bounded in-memory ring buffer if Postgres is what's down, flushed on recovery).

---

## 8. BACKUP & DISASTER RECOVERY (VULTR)

**Postgres:**
- WAL streamed continuously to Vultr Object Storage.
- Nightly full backup + hourly incrementals.
- Recovery target: RPO ≤ 5 min, RTO ≤ 30 min.
- **Automated restore drill every 30 days:** spin up a temporary read-only replica from backup, run a checksum query, tear down. A backup that has never been restored is not a backup.

**Object Storage (Parquet archive + quarantine):**
- Vultr Object Storage is replicated at the platform level.
- **Weekly checksum audit:** iterate all objects, verify against the manifest table in Postgres, alert on any mismatch.
- **Last 90 days mirrored to Singapore** (`botmaximus-mirror-singapore`). Tokyo alone is one earthquake from a very bad day.

**Encryption at rest:** default for Vultr Block and Object Storage. Postgres data directory on encrypted volume. **API keys and secrets:** `.env` on encrypted volume, never in backups.

**Backup contents rule:** backups include Postgres, Parquet manifest, `.env.example`, config, and code (git remote is authoritative). Backups **exclude** `.env` — secrets rotate via operator, not restore.

---

## 9. RETENTION POLICY

Institutional discipline is generous with retention on decisions and stingy with raw firehose.

| Class | Retention | Rationale |
|---|---|---|
| Market data (all six feeds) | **Forever** in Parquet | Backtests need arbitrary history; the archive is cheap |
| Coverage ledger | Forever | Historical coverage state is queried by future backtests |
| Features (versioned) | Forever | Same as above |
| Strategy definitions, lifecycle events, trials | Forever | Statistical honesty (n_trials) and audit |
| Backtest metadata | Forever | Reproducibility |
| Backtest per-bar signals | 2 years, then aggregated | Volume vs value |
| Orders, fills, ledger, kill events | Forever | Money records |
| LLM generation + scrutiny provenance | Forever | Audit; also enables re-running under a deprecated model |
| Quarantine | 2 years | Long enough for post-incident review, not forever |
| Postgres hot window (per dataset) | 24h – 7d | Serving recent data; older lives in Parquet |
| Application logs | 90 days | Debugging window, not the audit record |

Retention changes are operator commits, never automatic.

---

## 10. SCHEMAS ARE VERSIONED, NEVER MUTATED

Every schema (envelope, per-dataset payload, Postgres tables, Parquet layouts) has a `schema_version` integer.
- **Backward-compatible additions** (new nullable field) bump minor, do not touch prior data.
- **Breaking changes** bump major, are written as a new dataset directory alongside the old, cut over by explicit operator action. Old data is not migrated; it is read via a compatibility view.

This is the anti-pattern that quietly destroys institutional data lakes: "let's migrate the old data to the new schema." Don't. New schema, new directory, both coexist, readers pick.

---

## 11. INTEGRITY, MONITORING, ALERTS

The storage layer is worthless if you cannot tell when it's lying.

**Continuous integrity checks (run hourly, log to Postgres, alert on anomaly):**
- Row-count parity between Postgres daily partition and Parquet daily partition (for datasets with a hot window).
- Coverage ledger totals reconcile to actual record counts per feed per day.
- Every `orders` row has a matching `execution_ledger_predictions` row (and vice versa where expected).
- Every `scrutiny_events` row has a `provenance_blob` file in Object Storage.
- No `event_time` in the future. No `event_time` before venue launch date. No negative latency.
- Every generated candidate has a `generations` provenance row.

**Storage telemetry on the dashboard:**
- Postgres hot-window size per dataset.
- Parquet archive size and growth rate.
- Quarantine growth rate (a spike is a signal — venue outage, gate misfire, or upstream corruption).
- Backup age. Last successful restore drill.
- Singapore mirror lag.

Any of these red-lining halts new writes to the affected dataset (via the L2 layer) rather than blindly continuing on corrupted or unbacked state.

---

## 12. INTERFACES (WHAT OTHER LAYERS SEE)

None of this changes the semantics named in Finish-the-System v1.0, LLM Parameters v1.0, or Strategy DSL v1.0 — it changes *where* those semantics land physically.

- **Collectors** → write to Postgres hot window + Parquet daily archive (dual-write per §4). Coverage ledger updated in Postgres. Live tick held in-process for hot-path consumers.
- **Backtester** → reads Parquet as-of the pinned timestamp; reads coverage ledger from Postgres; writes results to Postgres + Parquet. Refuses to run without `as_of` (§6).
- **Feature layer** → reads Parquet (historical) or the collector's in-process state (live); writes to Parquet feature-set version directory + Postgres feature-set catalog. Never overwrites prior versions.
- **Generator (C2)** → reads coarsened digests from Postgres (`trials`, `strategy_lifecycle_events`, `generations`); writes proposals to Postgres `strategies` + Parquet provenance blob.
- **Arbiter** → reads live signals from in-process signal bus; writes `arbiter_events` to Postgres.
- **Scrutiny** → reads analog windows from Parquet as-of `now − EMBARGO_WINDOW_S`; writes `scrutiny_events` to Postgres + prompt/response blob to Parquet.
- **Executor** → reads/writes `orders`, `fills`, `execution_ledger_*`, `kill_events` in Postgres transactionally.
- **Dashboard** → reads structured data from Postgres via FastAPI; subscribes to collector WebSocket for live prices. Never reads Parquet directly (Parquet is not for interactive reads).

---

## 13. CLEAN-INSTALLATION PLAN (VULTR)

Botmaximus starts fresh on Vultr with the two-store architecture from day one.

**Phase 1 — Object Storage online.**
Provision Vultr Object Storage buckets: `botmaximus-archive-tokyo`, `botmaximus-quarantine-tokyo`, `botmaximus-mirror-singapore`. Encryption on. Access keys in `.env`.

**Phase 2 — Postgres online.**
Stand up Postgres 16 on the VPS with WAL archiving to Object Storage. Create schemas empty. Provision the storage manifest table first (it will index every Parquet file the system produces). Time-partitioned tables prepared for market-data hot window.

**Phase 3 — Collector online, dual-write from the start.**
Collectors write to Postgres hot window and Parquet daily partitions simultaneously. Coverage ledger updates begin. In-process state serves live consumers.

**Phase 4 — Tier-out job online.**
Nightly job at 00:15 UTC: verify Parquet daily partition against Postgres partition, then drop the Postgres partition. Tested against one dataset first (`btc_ohlcv_1m`), then rolled out to the rest.

**Phase 5 — Bitemporal query interface online.**
As-of queries wired for the backtester. Backtester refuses to run without `as_of`.

**Phase 6 — Backup and monitoring live.**
Postgres WAL streaming verified. First restore drill run. Weekly Parquet checksum audit scheduled. Singapore mirror validated. Storage telemetry live on dashboard.

Each phase is verified before the next begins. If a phase fails, only that phase is fixed and retried — earlier phases don't need to be redone.

---

## 14. WHAT THE BUILD AGENT MUST NOT DO

- Do not overwrite records. Ever. Corrections are new records with `supersedes`.
- Do not delete quarantined data before its retention expires. Do not move it to production.
- Do not use local wall-clock for `event_time`.
- Do not migrate old data to a new schema. Version the schema; keep both.
- Do not skip `as_of` on a backtest — the runner must refuse.
- Do not put secrets in backups.
- Do not build a "cleanup and reintroduce quarantined data" pathway. There is none.
- Do not skip the checksum audit of Parquet against the Postgres manifest. Silent bit rot on Object Storage is a real thing.
- Do not introduce a third store (Redis, DragonflyDB, Memcached, another database) "to cache what Postgres is doing." The two-store architecture is deliberate.
- Do not write to local JSON/CSV files as a stand-in for a proper store. If it isn't Postgres or Parquet, it does not get written.
- Do not implement a fallback path that writes "somewhere else" when Postgres is unreachable. §7 defines the correct behaviour: halt.

---

## 15. THE HONEST FRAME

Institutional-grade storage is a **discipline**, not a vendor stack: nothing overwrites, everything is timestamped twice, corrections leave a trail, backtests reproduce byte-for-byte, quarantine never mixes with production, tiers match access patterns, backups are restored, secrets never leak into archives.

Botmaximus applies that discipline on two stores, both on Vultr: **Postgres for decisions and the recent hot window, Parquet on Object Storage for the immutable permanent record**, with the collector's in-process state serving the sub-second live path. Nothing else. Every strategy that ever runs, every trade that ever fires, every decision the LLM ever makes will be reconstructible years later — which is the real definition of institutional data.

---

*End of BOTMAXIMUS (BTC) Storage Architecture Master Prompt v2.0.*
