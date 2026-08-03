# BOTMAXIMUS — Structural Risk Register

Where the cracks form, ordered by *when* they bite. Written 2026-08-02 at
`main @ b836c1d` (Passes A/B/C1 complete, C2 not started).

> **Status 2026-08-02, post-hardening:** §1.1 (trial ledger), §1.3 (sealed
> holdout) and §1.4 (trade floor) are **CLOSED** — see the addendum at the foot
> of this file for what was built and what it changed. Everything else stands.

This is not a list of bugs. Bugs are found by tests; these are properties of the
design that are currently correct and will *become* wrong as the system grows
into its next stage. Each one names the code that carries it.

---

## Tier 1 — cracks that open during Pass C2 (weeks away)

### 1.1 The deflated-Sharpe correction is switched off. `bt_candidate_trials = 1`
**This is the load-bearing crack.** `config.py:89` sets it to 1;
`validation.py:60` reads it as the default; `metrics.py:67` returns benchmark
`0.0` for `n_trials <= 1`, which collapses the deflated Sharpe back to an
ordinary probabilistic Sharpe. The entire multiple-testing defence is a no-op.

That is harmless today — C1 tested five hand-written seeds. It becomes severe
the moment C2 generates candidates in bulk, which is precisely the scenario the
correction exists for. Search 5,000 strategies against one price history and
some will clear a 0.95 PSR on noise alone; with `n_trials=1` the gate will
certify them and report high confidence.

**Fix before the first generated candidate is validated:** `n_trials` must be a
*persistent lifetime* count, not per-cycle and not per-run. The right unit is
already built — `validator.py:207` computes a `structural_signature` that
deliberately treats "EMA 50 vs EMA 51" as the same idea, with a comment naming
this exact failure. Count distinct signatures ever tested, store the counter in
Mongo, and pass it in. A repair attempt is a trial. A re-run after a tweak is a
trial.

### 1.2 The repair loop is an optimizer pointed at the gate
§7's decay-diagnosis/repair loop feeds rejection reasons back to the generator —
and `seed_gate.py:90` already stores them on purpose as "the training signal
Pass C2's generator needs." That makes `validation.py` a fitness function and
the LLM a search process climbing it. Goodhart's law is not a risk here, it is
the specification: any gate optimized against stops measuring what it was built
to measure.

Rejection *reasons* are a much richer leak channel than a pass/fail bit —
`deflated_sharpe_below_threshold:0.412<0.95` tells the generator exactly how far
to push. Mitigations: cap repair attempts per lineage (3 is generous), count
every attempt as a trial (§1.1), and return coarsened feedback — the failing
check names, not the margins.

### 1.3 There is no untouched holdout. The 2-year history is a consumable
`pipeline/deep_history.py` gave the system 1,051,199 bars — one macro path
through one asset. Walk-forward (`validation.py:108`) splits the *trade sequence*
into 4 sequential folds, which is honest for a single strategy but is not
out-of-sample across a *population*: every candidate is selected on the same
bars, so the folds get re-used thousands of times and out-of-sample decays into
in-sample by attrition.

Seal a final holdout (say the most recent 90 days) that neither generator nor
validator may touch. Open it only for a strategy that has already passed
everything else, once. A strategy that has seen the holdout has burned it.

### 1.4 The 30-trade floor gives almost no statistical power
`bt_min_trades = 30` (`config.py:86`). A Sharpe estimated from 30 trades has a
standard error large enough to make the point estimate nearly uninformative.
It cleared four of five seeds in Gate 3, so it is *permissive* in practice —
combined with §1.1 it is the second half of a leaky gate. At ~5-minute holds
trades are cheap to accumulate; 200+ is a more honest floor and costs nothing
but window length.

### 1.5 A population of one bet
`structural_signature` dedupe plus `diversity_threshold = 0.85` is real
protection, but every strategy is BTC, on one venue, over the same window.
Structural diversity is not statistical independence. Correlation between
"diverse" candidates will be high, and §1.1's trial count assumes independence.

---

## Tier 2 — cracks that open at Pass F, paper→live (months away)

### 2.1 The cost model is optimistic in exactly the tail that matters
`costs.py` charges a taker fee each side, funding across settlements, and an
adverse-side slippage *floor* of 1bp. What it does not model: order-book depth,
market impact, queue position, partial fills, exchange rejects and retries, or
latency finer than whole bars (`latency_bars`). Fills happen at the bar open.

At ~5-minute holds friction is already the dominant term (see
`friction_is_the_adversary`): a seed showed friction 538× gross. A modelling
error in the dominant term is not a rounding error, and it compounds once per
trade across hundreds of trades. **Paper will underperform backtest, and the gap
will be misattributed to the strategy.**

### 2.2 Stops are assumed to fill at the stop price
`engine.py:164` takes `bar.low <= stop` and exits *at* `stop` — the "stop-first
worst case" is worst-case in *ordering*, not in *price*. BTC gaps through stops
during liquidation cascades, and venue-side stops (planned for F) are market
orders that fill worst precisely when the book is thinnest. Every drawdown
number in the system is therefore a floor, not an estimate — including the
`bt_max_drawdown_pct = 25` ceiling and the 15% L3 kill.

### 2.3 No live-vs-backtest reconciliation ledger exists
Nothing currently records, per trade, *predicted* fill/fee/funding/slippage
against *realized*. Without it, §7 decay diagnosis cannot distinguish "the edge
decayed" from "the cost model was wrong from day one" — and §2.1/§2.2 make the
second explanation likely. Build this **before** F, not as a reaction to the
first bad week. It is also the only honest calibration path for the slippage
floor.

### 2.4 Portfolio correlation is not modelled anywhere
`core.py:144` caps summed `risk_usd` at `max_open_risk_pct`, which is correct
bookkeeping under the assumption that positions fail independently. They will
not: N BTC strategies in a cascade are one position, and §2.2 means each one
loses *more* than its `risk_usd` at the same moment. The 1% open-risk cap can
therefore be breached in realized terms even though every pre-trade check
passed.

Note also `core.py:219` — `register_close(strategy_id)` drops *all* positions
for a strategy, which silently assumes one open position per strategy. The
backtester only ever runs one strategy with one position, so nothing has tested
the portfolio path.

### 2.5 Kill-switch semantics for open positions are undefined
The L1/L2/L3 stack persists to Mongo and survives restart (good), and L3 needs
`CONFIRM-RESET-L3` (good). But it is not defined whether a kill *flattens* open
positions or merely *blocks new entries*. A drawdown kill that only blocks
entries leaves full exposure on during the event that triggered it. The
dashboard MASTER KILL is still local-only. Decide this before F, and test it
with a position open.

---

## Tier 3 — slow rot, already underway

### 3.1 Uptime is data, and lost data never comes back
Liquidations and order book have **no history endpoint** — they cannot be
backfilled, ever. As of this writing: a 4-hour 1m gap from 08:04 today,
`btc_funding_8h` gappy since 2026-07-29, `btc_oi_5m` since 2026-07-28. Two of
five seeds could not be evaluated at all in the latest Gate 3 run — refused on
coverage, correctly.

This is the crack that compounds fastest and most quietly. Each outage punches a
permanent hole; the coverage gate (correctly) refuses any window containing one;
so the set of evaluable windows shrinks monotonically. On a Windows desktop that
sleeps, updates and reboots, this is a certainty rather than a risk. **A
supervised service on a VPS is the single highest-value operational fix and it
gets more expensive to defer every day.**

### 3.2 The most valuable asset in the system has no backup
A single-node portable MongoDB, data on the desktop's `C:`, no replica set, no
snapshot. It holds 1,051,199 candles, the coverage ledger, every backtest run
and the persisted kill state. One disk failure erases two years of history that
took real effort to assemble, and §3.1 means parts of it cannot be re-fetched.

### 3.3 Single venue, single symbol, hardcoded fees
Already bitten once: the 2026-04-23 Binance WS routing migration connected fine
and silently pushed nothing, and SUBSCRIBE even acked success. That class of
failure — *working, but wrong* — is the dangerous one and it will recur.
`taker_fee_rate` is a constant, so VIP-tier or schedule changes silently
invalidate every stored backtest. Venue outage, regional access change or symbol
delisting has no fallback path.

### 3.4 Regime labels are lagging by construction
6 buckets from realised vol (`regime_vol_lookback = 60`,
`regime_vol_ref_lookback = 1440`) and direction — all backward-looking. Regime
*changes* are recognised late by design, and the 2026-08-02 confirmation-bar fix
deliberately adds 5 more bars of lag to trade the churn away. Decay detection in
§7 inherits that lag: the system will notice a broken regime after paying for it.

### 3.5 `REGIME_BUCKETS = 6` but validation counts the 3-label axis
`build_regime_map` still returns the direction axis because `validation.py:80`
counts positive regimes off it. That was the right call (repointing it at 6
sparse buckets would silently weaken the gate) — but it is a live inconsistency
between what the DSL scopes on and what the gate measures, and it will confuse
whoever touches it next, including a future you.

---

## Tier 4 — the strategic cracks

### 4.1 The search space may simply be empty, and the failure mode is human
At taker fees both sides, ~5-minute holds and 1m bars, round-trip cost is the
dominant term and does not shrink with hold time while edge scales with move
size. Gate 3 was 0/5. C2 may run for months and find nothing — **and that would
be the honest result, not a malfunction.**

The crack is not in the code. It is the pressure that builds after the hundredth
rejection to loosen `PSR_THRESHOLD`, drop `bt_min_regimes_positive`, or widen a
coverage refusal. Every one of those converts a real answer into a comfortable
one. The parameters that could actually change the outcome are operator
decisions, not code changes: longer holds, maker/limit entries
(`order_style = "taker"`), or a genuinely larger per-trade edge.

### 4.2 LLM dependency is a reproducibility hole
`generation_llm` is unset by design. Once set: models get deprecated, sampling is
non-deterministic, providers have outages, and prompts drift. A strategy
generated by a model that no longer exists cannot be regenerated or audited.
Store the full prompt, response, model id, and sampling params alongside every
candidate — the DSL definition alone is not a provenance record.

### 4.3 Retrieval memory manufactures analogies
Chroma event-reaction memory (§6.2) retrieves "similar past events" to inform
present decisions. Similarity in embedding space is not causal similarity, and
this machinery sits on the scrutiny path where it can *add* confidence to a
decision. Any strategy that leans on it should be judged as if it had been
selected on the retrieved events too — because it has.

### 4.4 Nothing models capacity
$10k paper equity with no market-impact model means size is free in the
backtest. Edges that survive validation may be noise-harvesting that dies at any
size worth trading. If it only works small, that needs to be known before it is
called an edge.

---

## The three that will actually kill it

1. **`bt_candidate_trials = 1` meeting a bulk generator** (§1.1) — the gate will
   certify noise and report high confidence. Fix before C2's first candidate.
2. **Uptime destroying irreplaceable data** (§3.1) — already happening, weekly,
   silently, and it is unrecoverable rather than merely bad.
3. **The cost model's optimism meeting real fills** (§2.1/§2.2) — friction is the
   dominant term, so an error there dwarfs everything the strategy layer does.

## Cheapest high-value fixes, in order

| # | Fix | Cost | Buys |
|---|-----|------|------|
| 1 | Persistent lifetime trial counter → `n_trials` | hours | The multiple-testing defence, before it is needed |
| 2 | Supervised service + VPS | a day | Stops permanent, ongoing data loss |
| 3 | Automated `mongodump` to external storage | hours | The 2-year asset survives a disk |
| 4 | Sealed 90-day holdout, generator-invisible | hours | One honest verdict per strategy |
| 5 | Predicted-vs-realized trade ledger | a day | §7 can tell decay from mis-modelling |
| 6 | Raise `bt_min_trades` to ~200 | minutes | Real statistical power |
| 7 | Define kill-with-open-position semantics | discussion | Not being fully exposed during a kill |

---

# Addendum — pre-C2 hardening, 2026-08-02

Items 1, 4 and 6 of that table are done. All three had to land *before* C2's
first candidate: every verdict issued under the old gate would otherwise have
needed re-running, and a trial count cannot be reconstructed after the fact.

### `strategy/trials.py` — lifetime trial ledger (closes §1.1)
One trial = one (structural signature, config hash) ever evaluated, persisted in
`trial_ledger`, never reset. `run_dsl_backtest` registers the evaluation *before*
judging it and feeds the count to `validate(n_trials=…)`, so the deflated Sharpe
is finally corrected against real search intensity instead of the constant 1
that made `expected_max_sharpe` return a 0.0 benchmark.

Two judgement calls, both erring toward rejecting:
- **Total evaluations, not distinct ideas.** Fifty variants of one idea are fifty
  looks at the data. Because those variants are correlated this slightly
  over-corrects — the right direction of error for a gate whose job is to reject.
  Distinct signatures are tracked separately, for reporting only.
- **An identical re-run is not a trial.** Keyed on config hash, so restarts,
  retries and dev re-runs don't inflate the count. Anything that differs does.

There is deliberately no reset, clear or decrement entry point, and a test
asserts the module never grows one.

### `backtest/holdout.py` — sealed window (closes §1.3)
The most recent `holdout_days` (90) is invisible to the search path:
`run_dsl_backtest` refuses any window reaching into it, and `seed_gate` clips its
end to the boundary. A strategy may be judged on it **once**, via
`holdout_run=True`; the burn is written to `strategy_events` before the verdict
is returned, so a run that dies afterwards has still spent it. A failing verdict
burns it too — the window is consumed by *looking*, not by passing.

### `bt_min_trades` 30 → 200 (closes §1.4)
Immediately visible: two seeds that were previously judged on merit now reject on
`insufficient_trades:30<200` and `44<200`.

### What the hardened gate did to Gate 3
Windows moved to 2025-05-05 → 2026-05-05. Still 0/5, with better-founded
rejections. One genuine improvement and one new problem:

- **`seed_funding_extreme_contrarian` became evaluable.** It was previously
  refused on coverage because `btc_funding_8h` has been gappy since 2026-07-29 —
  which now falls *inside* the sealed window. Sealing the recent past
  incidentally routes the search around the freshest collector outages.
- **`seed_oi_divergence_exhaustion` got worse: `btc_oi_5m` 8353/8353 missing.**
  A real structural conflict, not a bug. `openInterestHist` has a hard 30-day
  venue limit, so stored OI history only extends back as far as the collector has
  been running. With the search path pushed 90 days into the past, the OI window
  lands entirely before any OI was ever collected.

  **Any feed whose accumulated history is shorter than `holdout_days` plus a
  usable window is unevaluable.** Today that is OI; it also would be liquidations
  and order book. It resolves itself as the collector accumulates history — but
  *only* if uptime holds, which makes §3.1 more load-bearing than it already was.
  The alternative is shortening `holdout_days`, which is an operator tradeoff
  between out-of-sample honesty and feed coverage, not a code decision.
