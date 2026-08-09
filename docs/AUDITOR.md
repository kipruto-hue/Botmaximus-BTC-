# BOTMAXIMUS (BTC) — AUDITOR MASTER PROMPT

**Version:** 1.0
**Feature:** The Auditor — an LLM-driven observability role that reads the system's ledgers on schedule and on events, produces human-readable reports (daily rundowns, weekly reviews, incident reports), and stores them as immutable records.
**Role:** Distinct from Generator (C2) and Scrutiny (E). Same model, third role.
**Companions:** System v1.1 · Storage Architecture v2.0 · Finish-the-System v1.0 · LLM Parameters v1.0.
**Precedence:** System v1.1 constitution > this document > implementation.

> The Auditor is Botmaximus's senior analyst on staff — the one who reads the ledgers at 09:00, writes a brief on what happened, points out what's decaying, and flags where the operator should look. It is read-only, always. It never acts. It never touches state. Its reports are commentary, not authority.

---

## 0. WHY THIS FEATURE EXISTS

Botmaximus produces enormous quantities of structured decision data across four ledgers (trials, execution, scrutiny, generations) and a coverage store. The operator cannot read all of it every day. Left unread, ledgers become archaeology — you find the important pattern six months late, in incident review.

The Auditor closes that gap. It reads what the operator can't and writes the parts worth reading. Daily rundowns for routine monitoring. Weekly reviews for pattern-spotting. Event-triggered incident reports when something significant happens. All in plain language, all backed by ledger citations, all stored permanently.

This is genuinely valuable — it's how institutional risk desks operate, minus the humans. But it's only safe under one condition, which is the whole point of the design that follows: **the Auditor observes and comments; it never touches anything.** The moment an observability layer gains authority to act, it becomes another layer the system's safety guarantees route through. Not this one.

---

## 1. NON-NEGOTIABLE CONSTITUTION (this role)

Extends System v1.1. Anything here that conflicts with v1.1, v1.1 wins.

1. **The Auditor is read-only.** It has no write access to any Postgres table, any Parquet dataset (except its own report blobs), the risk core, the strategy pool, the arbiter, or the executor. Enforced at the database role level (`auditor_read` Postgres role with `SELECT` only on the specific tables in §3), not just at application code level.
2. **The Auditor cannot trigger actions.** It cannot suspend a strategy, promote a strategy, adjust a limit, cancel an order, fire a kill, change a config, or dispatch a webhook that would do any of those. Its output is a document, not a command.
3. **The numbers boundary.** Every quantitative claim in an Auditor report is a *quoted value from a specific ledger record*, cited by table and record_id. The Auditor does not perform arithmetic. If a percentage is needed, the query computes it and the Auditor quotes the result. LLMs are unreliable at math and reliable at prose — this separation keeps reports honest.
4. **No forward-looking claims.** The Auditor never predicts, recommends trades, forecasts PnL, or estimates future outcomes. It reports what happened, points at what's degrading, and flags where operator attention is warranted. Predictions live in Scrutiny and strategies, not here.
5. **Every report is a record.** Reports are stored in Postgres (metadata + citations) and Parquet (full prose blob) with the standard bitemporal envelope and full LLM provenance. A report from August 12th is byte-for-byte recoverable in January.
6. **The Auditor sees no live market data.** It reads only from stored ledger records. It does not query current price, current book, or current features. Its inputs are historical facts by definition — this prevents any drift toward "the Auditor thinks BTC will go up."
7. **The Auditor cannot see `.env` values, API keys, secrets, or dashboard state.** Only the tables and Parquet paths listed in §3.

---

## 2. THREE REPORT TYPES, ONE ROLE

| Report | Trigger | Cadence | Consumer |
|---|---|---|---|
| **Daily Rundown** | Scheduled | 09:00 Africa/Nairobi | Operator morning read |
| **Weekly Review** | Scheduled | Monday 09:00 Africa/Nairobi | Operator strategic read |
| **Incident Report** | Event-triggered (§6) | On event, up to N/day | Operator alert channel |

All three use the same Auditor role, same parameter profile, same guards. They differ in scope (time window) and template. They share a single provenance store.

---

## 3. INPUTS — WHAT THE AUDITOR MAY READ

Read-only queries against these Postgres tables and Parquet paths, and *only* these:

**Postgres (via `auditor_read` role):**
- `strategies`, `strategy_lifecycle_events`, `strategy_definitions_blob` (metadata only, not full compiled blob for reports — hash is enough)
- `trials`
- `arbiter_events`
- `scrutiny_events` (verdict, reason, latency, provider version, realized outcome; **not** raw prompt/response)
- `orders`, `fills`, `execution_ledger_predictions`, `execution_ledger_realizations`, `execution_ledger_drift`
- `kill_events`
- `coverage_ledger`
- `quality_events` (aggregate counts and reasons; not raw quarantined records)
- `generations` (metadata only, not raw prompts/responses)
- `telemetry_events`
- `storage_manifest` (for freshness/mirror status)

**Parquet:**
- Its own prior reports (for weekly-reviews-of-daily-rundowns and consistency across cycles).

**Explicitly not accessible to the Auditor:**
- Current in-process collector state (no live prices).
- Raw quarantined payloads.
- Full LLM prompt/response text from Generator or Scrutiny (metadata is enough for reporting; the blob is for audit forensics only).
- Any table not listed above.

If the Auditor asks for data it doesn't have access to, the query returns empty and the report says so. It does not fabricate.

---

## 4. THE THREE REPORT TEMPLATES

Each template is a **prompt** + **query set**. The prompt describes what section is being written; the query set fetches the ledger facts that section will cite. The Auditor composes prose from the facts, never invents facts.

### 4.A — Daily Rundown

Window: yesterday 00:00 UTC → today 00:00 UTC. Structure:

1. **Headline** — one sentence: what mattered yesterday. (e.g. "System halted at 14:22 UTC on L2 daily-loss trigger; three strategies traded before the halt; one repair candidate promoted to paper.")
2. **Trading summary** — trades placed, fill success rate, realized-vs-predicted cost drift, PnL. Quoted from `execution_ledger_*` and `fills`. Zero trades is a valid section content ("System was flat all session.").
3. **Strategy pool status** — count by lifecycle state; any transitions yesterday; any strategies flagged by the decay monitor. Quoted from `strategies` and `strategy_lifecycle_events`.
4. **Risk events** — any kill events, halts, or breaches. Quoted from `kill_events`. Empty is fine and gets one line.
5. **Data quality** — quarantine growth rate, coverage gaps, feed staleness incidents. Quoted from `quality_events`, `coverage_ledger`, `telemetry_events`.
6. **Generator activity** — candidates proposed, pass rate, common rejection reasons (as coarsened check names). Quoted from `generations` and `trials`.
7. **Scrutiny activity** — verdicts, veto rate, average latency, timeout count. Quoted from `scrutiny_events`.
8. **What the operator should look at today** — bulleted list, each with a citation (table + record_id or count + query). Empty is fine.

**Bounded length:** 400–800 words. If there's nothing to say, the report is short — that is a feature, not a failure.

### 4.B — Weekly Review

Window: previous 7 days. Adds to the Daily template:

- **Trends** — rolling metrics compared week-over-week (from `execution_ledger` and `strategies`), quoted with prior-week values.
- **Decayed strategies** — full list with their diagnostic packet references.
- **Cost model accuracy** — realized/predicted ratio over the week, quoted per strategy.
- **Coverage progression** — how much usable microstructure history has accumulated (from `coverage_ledger`).
- **Storage health** — Parquet mirror lag, backup age, last restore drill (from `storage_manifest`, `telemetry_events`).
- **Pattern notes** — recurring rejection reasons, recurring veto reasons, recurring kill causes. Frequencies quoted from counts, not estimated.

**Bounded length:** 800–1500 words.

### 4.C — Incident Report

Triggered per §6. Structure:

1. **What happened** — event type, timestamp, one-sentence summary. Quoted from the triggering ledger row.
2. **Sequence of events** — the 15 minutes before and 30 minutes after, drawn from `arbiter_events`, `scrutiny_events`, `orders`, `fills`, `kill_events`, `telemetry_events`. Chronological. Each item cited.
3. **Contributing conditions** — coverage/quality state at the time, active strategies, recent decisions. Quoted, not inferred.
4. **Similar past incidents** — matched from prior incident reports (Parquet) by event type; up to 3 references with links.
5. **Suggested operator actions** — bulleted, non-imperative. "Consider reviewing X." "Verify Y is still fresh." Never "do Z." The Auditor advises; the operator decides.

**Bounded length:** 300–1000 words depending on incident complexity.

---

## 5. LLM PARAMETERS (THIRD ROLE)

Set separately from Generator and Scrutiny. Ships in the same `.env`, different keys.

| Parameter | Recommended | Range | Rationale |
|---|---|---|---|
| `AUD_TEMPERATURE` | `0.4` | `0.2 – 0.6` | Enough for readable prose without becoming inventive. Between Scrutiny's 0.1 (rigid) and Generator's 0.9 (creative). |
| `AUD_TOP_P` | `0.95` | — | Standard. |
| `AUD_MAX_OUTPUT_TOKENS` | `2500` | `1500 – 4000` | Sized to report bounds in §4. |
| `AUD_STOP_SEQUENCES` | `["\n\n---END---"]` | — | Deterministic close. |
| `AUD_RESPONSE_FORMAT` | `json_schema` with `{sections: [{title, prose, citations: [{table, record_id_or_query}]}]}` | — | Every citation is machine-verifiable. |
| `AUD_SEED` | rotated per report | int | Stored in provenance. |
| `AUD_N_CHOICES` | `1` | `1` | One report, one provenance record. |
| `AUD_LATENCY_BUDGET_MS` | `60000` (60s) | — | Reports are not on a hot path. Timeout → report failure, alert, retry once per cycle. |

**Why 0.4 temperature.** Reports are prose about facts. Temperature 0 makes them robotic and same-shaped every day (which paradoxically makes patterns harder to spot); 0.9 makes them fabricate. 0.4 is the sweet spot for "readable, honest analyst prose."

### 5.A System prompt (fixed, versioned)

Stored at `auditor/prompts/auditor_system.md`. Establishes:
- Role: senior systematic-trading analyst writing an internal brief.
- Absolutes: read-only; every quantitative claim cited to a ledger record; no forward-looking predictions; no imperative recommendations; if a fact is not in the provided query results, do not state it.
- Style: plain, direct, technical-but-readable; short paragraphs; no marketing language; empty sections written as one honest sentence ("No kill events in the reporting window.").
- Escalation: if the query set contains a value flagged as anomalous, name it and cite it; do not soften it, do not exaggerate it.
- Explicit reminders: the Auditor does not compute; the Auditor does not act; the Auditor does not predict.

### 5.B Prompt context (dynamic — assembled per report)

1. **Report type and window** (daily / weekly / incident + date range).
2. **The relevant query results**, as structured tables, one per section of the template. Empty result sets are passed through as `[]` — the Auditor writes "no events" from that, not from silence.
3. **The prior week's report headlines** (for consistency of framing and pattern-spotting across cycles).
4. **The schema of each ledger** (so citations can reference the correct fields).

Deliberately not in context:
- Predictions from any model.
- Current live market data.
- Full LLM prompts/responses from other roles.
- Config values (rates, thresholds) — the Auditor reports what happened, not what the config is.
- PnL forecasts or expected values.

---

## 6. INCIDENT TRIGGERS

The Auditor writes an incident report automatically when any of these events lands in the ledgers (event-driven, checked by a small watcher process):

- Any `kill_events` row (L1 suspend, L2 halt, L3 master kill).
- `execution_ledger_drift` breach — realized > predicted × `AUTO_DEMOTE_COST_MULTIPLE` over the rolling window.
- Strategy lifecycle transition to `retired` with reason other than voluntary operator action.
- Coverage-ledger gap detected in a required feed for an active strategy.
- Reconciliation divergence between local and Bybit state.
- Scrutiny provider timeout rate above threshold in a rolling window.
- Storage integrity alarm (Parquet checksum mismatch, WAL archiving failure, mirror lag beyond SLO).
- Backtester holdout burn — an event with lasting consequences that deserves the record.

**Rate limit:** max N incident reports per day (default `N=5`). If N is exceeded, further events aggregate into a "cascade report" until the rate clears — prevents alert fatigue during outages, when everything fires at once.

---

## 7. STORAGE OF REPORTS

Reports are records in the same sense as everything else in the system. Storage Architecture v2.0 applies without modification.

**Postgres — `auditor_reports` table:**
- `report_id` (uuid v7)
- `report_type` (`daily` | `weekly` | `incident`)
- `window_start`, `window_end` (UTC)
- `trigger` (`scheduled` | event type)
- `generated_at` (UTC)
- `citations` (JSONB — array of `{table, record_id_or_query, quoted_value}`)
- `sections` (JSONB — section titles and word counts)
- `provenance_ref` — foreign key to `auditor_provenance`

**Postgres — `auditor_provenance` table** (same structure as `generations`):
- `model_id`, `prompt_version`, `system_prompt_hash`, `context_hash`
- `seed`, `temperature`, `top_p`, `max_output_tokens`
- `input_query_hashes`, `output_response_hash`
- `code_version`, `producer`, `timestamp`

**Parquet — `botmaximus-archive-tokyo/auditor_reports/year=YYYY/month=MM/day=DD/`:**
- Full prose text of every report.
- Immutable, partitioned by report generation date.
- Mirrored to Singapore per Storage v2.0 §8.

**Bitemporal integrity.** A report is never edited. If a report is later found to have quoted a stale value (because a correction record superseded the value after report generation), a **follow-up report** is written that supersedes the original in the standard `supersedes` chain — both remain queryable, both remain forever.

**Retention:** forever, per Storage v2.0 §9 (LLM provenance forever).

---

## 8. INTEGRITY: HOW TO KNOW THE AUDITOR IS HONEST

Because the Auditor produces prose from data, the operator needs a fast way to verify a report against ground truth.

**Every report is spot-checkable.** Each citation is a `{table, record_id_or_query, quoted_value}` triple. Running the query returns the same value. If the value differs, the report is wrong — and this happens quietly if arithmetic drifts into the LLM. A weekly integrity check samples 5 random citations from the past week's reports and verifies them; mismatch alerts the operator.

**Coarse metrics on the reports themselves** (tracked in `telemetry_events`):
- Citation density (citations per 100 words) — too low means the LLM is generating prose without evidence.
- Novel-fact rate — claims made in the report that don't map to any citation. Should be zero. Above zero → the numbers boundary is broken and the prompt needs strengthening.
- Report bounded-length adherence — reports outside §4 word bounds signal drift.
- Timeout rate on daily generation.

Any of these red-lining is itself an incident trigger, generating an incident report *about the Auditor*. Yes, this creates the possibility of infinite regress — mitigated by not incident-reporting on incident reports about the Auditor itself.

---

## 9. INTERFACES

- **Reads:** the tables and Parquet paths in §3, via the `auditor_read` Postgres role and read-only S3 credentials.
- **Writes:** `auditor_reports`, `auditor_provenance`, `telemetry_events` in Postgres; report prose blobs to Parquet at `botmaximus-archive-tokyo/auditor_reports/`.
- **Emits (informational only):** daily/weekly reports to the operator alert channel (dashboard, email, whatever the operator has set up). Incident reports flagged with severity in the channel.
- **Consumes:** no live data. No cross-role LLM output. Only stored ledger records.
- **Never touches:** the risk core, the strategy pool, orders, fills, kill state, config, or the running system's behaviour in any way.

The Auditor's endpoints on the FastAPI layer are read-only: `GET /api/auditor/reports?...` returns lists and single reports for the dashboard. No `POST`, no `PATCH`, no `DELETE`.

---

## 10. FAILURE MODES → GUARDRAILS

| Failure | Guardrail |
|---|---|
| Auditor fabricates a number | §1.3 numbers boundary + §8 citation verification + §5.A "no fact without citation" system prompt |
| Auditor predicts market direction | §1.4 no forward-looking claims + §5.A explicit prohibition |
| Auditor recommends an action | §5.A style rule: no imperatives; §1.2 no action pathway exists to execute one anyway |
| Auditor timeouts every day | §5 latency budget → alert; report failure recorded, no fake report generated |
| LLM hallucinates a ledger row | §3 restricted access + §8 citation spot-check catches within a week |
| Report accidentally reveals a secret | §1.7 no access to secrets, `.env`, or dashboard state; nothing to leak |
| Reports drift toward same-shaped filler | §8 novel-fact rate + citation density monitoring |
| Alert fatigue during outage | §6 rate limit + cascade report |
| Report cites a value that has since been corrected | §7 bitemporal follow-up report, both remain queryable |

---

## 11. COST DISCIPLINE

The Auditor is called on schedule and on incident triggers only. Not on the hot path. Volume is bounded and predictable.

- **Daily rundowns:** 1 per day.
- **Weekly reviews:** 1 per week.
- **Incident reports:** capped at 5/day (§6).
- **Retries:** 1 per report on transient API failure.

At ~2500 output tokens per report and standard GPT-5.5 pricing, this is a small, predictable monthly line item. Track it in `telemetry_events` with `AUD_MAX_MONTHLY_USD` soft cap — on breach, pause the Auditor, alert operator, do not auto-adjust anything. A skipped weekly review is fine; a silent budget breach is not.

---

## 12. INTEGRATION WITH THE DASHBOARD

New panel: **Auditor Feed** (real, no `SIM` badge on first launch since it renders real reports from day one).

- Latest daily rundown at the top, expandable.
- Recent incident reports (with severity coloring).
- Link to weekly review.
- "Verify citation" affordance — clicking a citation runs the query and shows the current value, so the operator can spot-check inline.
- Integrity metrics (§8) as a small sub-panel.

The Auditor's reports become the dashboard's *narrative* layer, complementing the *state* layer that shows current risk, positions, and strategy pool.

---

## 13. BUILD ORDER

1. **Postgres role and access grants** (§1.1, §3) — the wall comes first.
2. **Report storage tables and Parquet path** (§7) — where reports land before there are any.
3. **Query set for daily rundown** (§4.A) — the ledger reads that the LLM will cite.
4. **Daily rundown prompt + template + one manual test run** (§4.A, §5) — verify citations work end-to-end.
5. **Integrity monitors** (§8) — before the second report is written, so drift is caught early.
6. **Scheduler for daily and weekly reports** (§2, §6) — production cadence.
7. **Incident trigger watcher and incident report template** (§6, §4.C).
8. **Dashboard Auditor Feed panel** (§12).
9. **Cost telemetry and soft cap** (§11).

Never build the incident watcher before the daily rundown — the pattern needs to prove itself on the easy case first.

---

## 14. WHAT THE BUILD AGENT MUST NOT DO

- Do not give the Auditor any write access anywhere except its own report tables and blob path. Enforced at the DB role, not just app code.
- Do not let the Auditor perform arithmetic — the query computes, the LLM quotes.
- Do not let the Auditor read live prices, current features, or the collector's in-process state.
- Do not let the Auditor read raw quarantined payloads or raw LLM prompts/responses from other roles.
- Do not let the Auditor's output trigger any system action (no webhooks to promote strategies, adjust limits, fire kills, etc.).
- Do not use the Auditor's output as an input to Scrutiny or Generator — those roles read the ledgers directly, not through Auditor prose.
- Do not skip citations. A section with prose but no citations is a broken section.
- Do not permit "predictions" or "recommendations" in reports — reject at review, tighten the system prompt if they recur.
- Do not delete or edit reports. Corrections are new reports with `supersedes`.
- Do not exceed report bounded lengths silently — an over-length report is a signal, not a feature.

---

## 15. THE HONEST FRAME

The Auditor makes ledger data legible without giving it teeth. It is a language interface to the truth the system has already recorded — the analyst who reads what the operator can't. Its power is entirely borrowed from the ledgers: it can say nothing that isn't already in them, and it can do nothing at all. That is deliberate. Observability that can act stops being observability. Botmaximus has enough authority-bearing layers; this one is authority-free by design.

Done right, the operator's morning routine becomes: read the daily rundown, check the citations that surprise them, act on what needs acting on. That is what having a senior analyst on staff feels like, and that is the feature.

---

*End of BOTMAXIMUS (BTC) Auditor Master Prompt v1.0.*

---

## IMPLEMENTATION NOTES (added during build)

Two places where the spec meets the existing system and needed a decision.

### The exclusion wall is role-specific

`llm/guards.py` blocks `pnl`, `equity`, `drawdown` and kill state from ever
reaching a prompt. Those exist for the Generator (feeding PnL to a model that
writes strategies is a fast track to fitting recent noise) and for Scrutiny
(which sits after the deterministic risk checks and may only subtract).

But §4.A **requires** the Auditor to report PnL, and §4.A.4 requires it to
report kill events. Applying the generator's wall wholesale would make the
daily rundown impossible.

So there are two walls. `auditor/guards.py` is a separate list that still
blocks secrets (§1.7), raw quarantined payloads, raw cross-role LLM text and
live market data — but permits the ledger facts the Auditor exists to report.
The Generator's wall is untouched. The reasoning is that the wall's purpose is
to stop a *generating* model from optimising against what it can see; the
Auditor generates no strategies and gates no trades, so the same data carries a
different risk.

### The numbers boundary is enforced mechanically

§1.3 says the Auditor does not perform arithmetic. That is unenforceable as a
prompt instruction alone, so the verifier extracts every numeric token from the
report prose and checks it appears in some citation's `quoted_value`. A number
in the prose that no citation supports is exactly the fabrication this boundary
exists to prevent, and it is now a countable metric rather than a hope.
