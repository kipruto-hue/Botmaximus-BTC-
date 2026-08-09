"""BOTMAXIMUS (BTC) data-layer configuration — the §10 parameter surface.

Every value can be overridden via environment variables or a `.env` file
in the server root (e.g. `MONGO_URI=...`).
"""
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # storage
    # ---- storage: two stores, both on Vultr (Storage Architecture v2.0) ----
    # Postgres owns everything the system decides or commits, plus the recent
    # hot window. Parquet on Object Storage owns the immutable permanent
    # record. There is no third store: no Redis, no cache layer, no local
    # JSON/CSV stand-in. MongoDB was cancelled permanently on 2026-08-06.
    postgres_dsn: str = "postgresql://botmaximus@127.0.0.1:5432/botmaximus"
    postgres_pool_min: int = 2
    postgres_pool_max: int = 10
    #: Per-dataset hot-window overrides (§3.A). Older data lives only in
    #: Parquet; the tier-out job drops the Postgres partition after verifying
    #: the archive copy — never before.
    hot_window_hours_default: int = 48
    hot_window_hours_ohlcv: int = 168        # 7d
    hot_window_hours_ticks: int = 24
    hot_window_hours_funding: int = 720      # 30d
    hot_window_hours_open_interest: int = 168
    hot_window_hours_orderbook: int = 24
    hot_window_hours_liquidations: int = 720

    # Vultr Object Storage (S3-compatible). Tokyo primary, Singapore mirror.
    objectstore_endpoint: str | None = None
    objectstore_region: str = "jp-tyo"
    objectstore_access_key: SecretStr | None = None
    objectstore_secret_key: SecretStr | None = None
    bucket_archive: str = "botmaximus-archive-tokyo"
    bucket_quarantine: str = "botmaximus-quarantine-tokyo"
    bucket_mirror: str = "botmaximus-mirror-singapore"
    #: Local staging/backend root. Used when no object-store endpoint is
    #: configured — a single-box deployment writing to the attached volume.
    archive_local_root: str = "data/archive"

    # Vultr host. The 256GB block volume holds the Postgres data directory and
    # archive staging; §1.A requires it encrypted and separate from the VPS root.
    vultr_region: str = "nrt"                # Tokyo
    vultr_block_mount: str = "/mnt/blockstore"

    # ---- instrument & venue ----
    # The venue the collectors read AND the venue orders would reach. These must
    # be the same: validating a strategy on one venue's prices, fees, funding
    # and liquidity while executing on another makes the cost model — already
    # the dominant term at 5-minute holds — measure the wrong market, and the
    # execution ledger could not tell that apart from genuine decay.
    venue: str = "bybit"                    # "bybit" | "binance"
    symbol: str = "BTCUSDT"                 # linear USDT-margined perpetual

    # Bybit V5. `linear` = USDT-margined, PnL linear in price, equity in USDT —
    # which is what the risk core's sizing and liquidation estimate assume.
    # `inverse` (the contract literally named BTC/USD) is coin-margined with
    # non-linear PnL and would need different maths throughout; it is not
    # supported and must not be set here without that work.
    bybit_category: str = "linear"
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    bybit_rest_url: str = "https://api.bybit.com"
    bybit_testnet_rest_url: str = "https://api-testnet.bybit.com"
    bybit_orderbook_depth: int = 50         # topic depth subscribed
    bybit_orderbook_store_levels: int = 20  # levels persisted, as on Binance

    binance_ws_url: str = "wss://stream.binance.com:9443/stream"
    binance_futures_ws_url: str = "wss://fstream.binance.com"  # routed: /market, /public
    binance_rest_url: str = "https://api.binance.com"
    binance_futures_rest_url: str = "https://fapi.binance.com"

    # pipeline flow
    queue_maxsize: int = 1000          # bounded queues → backpressure (§2)
    tick_throttle_ms: int = 1000       # store at most one price tick per second
    funding_throttle_ms: int = 30_000  # mark-price stream is 1/s; funding moves slowly
    orderbook_throttle_ms: int = 5_000 # depth snapshots stored at most once per 5s
    oi_poll_s: int = 30                # open interest REST poll cadence

    # OHLCV gap backfill (§11 step 4): heal candles missed during dead sockets
    backfill_enabled: bool = True
    backfill_scan_minutes: int = 180            # periodic cycles look this far back
    backfill_startup_scan_minutes: int = 10_080 # first cycle after boot: 7 days
    backfill_check_s: int = 60

    # staleness budgets, ms (§9) — a dataset over budget is excluded from live use
    budget_price_tick_ms: int = 3_000
    budget_ohlcv_1m_ms: int = 75_000
    budget_funding_ms: int = 90_000
    budget_open_interest_ms: int = 120_000
    budget_orderbook_ms: int = 30_000
    # liquidations are event-driven and legitimately silent for hours → no budget

    # retention / TTL, seconds (§10)
    ttl_price_ticks_s: int = 7 * 24 * 3600        # 7 days of 1/s ticks
    ttl_ohlcv_1m_s: int = 10 * 365 * 24 * 3600    # keep candles ~10 years
    ttl_funding_s: int = 2 * 365 * 24 * 3600
    ttl_open_interest_s: int = 2 * 365 * 24 * 3600
    ttl_liquidations_s: int = 365 * 24 * 3600
    ttl_orderbook_s: int = 30 * 24 * 3600         # heavy — keep 30 days of snapshots

    # quality gate (§8)
    max_minute_move_pct: float = 5.0   # phantom-tick jump threshold vs previous record
    clock_skew_tolerance_ms: int = 2_000
    illiquid_gap_ms: int = 15_000      # no tick for this long → illiquid_window flag

    # declared now, consumed by the RAG/reaction layers later (§10)
    embargo_window_s: int = 60
    reaction_lags: str = "1m,5m,15m,1h,4h,1d"

    # ---- decision layer (Decision & Execution Master Prompt §3) ----
    # Locked by operator 2026-07-23; layers that need a None value must fail loudly.
    holding_period_target_s: int = 300          # ~5min holds, high frequency
    order_style: str = "taker"
    max_drawdown_kill_pct: float = 15.0         # L3 master kill, from equity peak
    risk_per_trade_pct: float = 0.25
    max_open_risk_pct: float = 1.0
    daily_loss_limit_pct: float = 3.0           # L2 portfolio halt
    leverage_cap: float = 3.0
    margin_mode: str = "isolated"
    position_mode: str = "one_way"
    starting_equity_paper: float = 10_000.0
    liq_stop_buffer_pct: float = 20.0           # stop sits ≥ this % of stop-distance inside liq price
    llm_model: str | None = None                # set at Scrutiny Gate pass; consumers raise if unset
    paper_fill_model: str | None = None         # set at execution pass

    # ---- backtest cost model (§5.3) — never frictionless ----
    # Bybit linear-perp taker, each side. 5.5bps, up from Binance's 5.0 — a 10%
    # increase in the term that already dominates net P&L at these hold times.
    # This is the standard non-VIP rate; the account's real rate comes from
    # /v5/account/fee-rate and needs a key, so verify it before paper trading
    # and correct this rather than letting the gate judge on a stale number.
    taker_fee_rate: float = 0.00055
    slippage_bps: float = 1.0                   # conservative constant floor, never zero
    latency_bars: int = 1                       # decision→fill delay, in 1m bars

    # ---- backtest validation gate (§5.5) ----
    bt_walkforward_folds: int = 4
    bt_purge_bars: int = 5                      # ≈ holding period; drop label-overlapping samples
    bt_embargo_bars: int = 5
    # Statistical power floor. Was 30, which cleared four of five Gate-3 seeds:
    # a Sharpe estimated from 30 trades has a standard error wide enough to be
    # uninformative, and paired with an uncorrected DSR that made the gate leaky
    # from both ends. At ~5-minute holds trades are cheap; this costs only window.
    bt_min_trades: int = 200
    bt_max_drawdown_pct: float = 25.0
    bt_min_regimes_positive: int = 2
    # Fallback only, for callers with no ledger (unit tests, the ad-hoc OHLCV
    # path). The real count comes from `strategy.trials` — a persistent lifetime
    # ledger. Note `expected_max_sharpe` returns a 0.0 benchmark at n<=1, so this
    # value DISABLES the correction; it must never be the production source.
    bt_candidate_trials: int = 1
    # Most-recent slice sealed from the search path (§ backtest/holdout.py).
    # The generator never reads it; a strategy may be judged on it once.
    holdout_days: int = 90

    # ---- strategy DSL & generation (Strategy DSL Master Prompt §2) ----
    # DSL-side params are set. Generator-side params are declared here but left
    # unset on purpose: §2 requires the build to fail loudly rather than invent
    # a value, and Pass C2 is where they get decided.
    max_holding_bars_cap: int = 60          # DSL time_exit ceiling, 1m bars (1h)
    default_time_exit_bars: int = 5         # ≈ holding_period_target_s
    diversity_threshold: float = 0.85       # §5.7 similarity ceiling for dedupe
    regime_vol_lookback: int = 60           # realised-vol window for the vol axis
    regime_vol_ref_lookback: int = 1440     # trailing reference for high/low vol
    # Bars the regime label must stay out-of-scope before an open position is
    # invalidated. On the 1m grid the 6-bucket label flips constantly; at 1 the
    # Gate-3 trend seed exited on `regime` 307 times in 336 trades, paying a
    # taker fee for each flicker. Not a DSL field — it is a friction control the
    # operator owns, not a strategy choice the generator may tune.
    regime_invalidation_confirm_bars: int = 5
    vector_store: str = "chroma"            # §6.2 memory; operator lock 2026-07-23

    generation_llm: str | None = None       # §2 GENERATION_LLM — C2, fail loudly
    candidate_cap_per_cycle: int | None = None   # protects multiple-testing math
    generation_cadence_s: int | None = None      # throttled to backtest throughput
    decay_repair_trigger: str | None = None      # §7.1 sequential-test trigger

    # ---- finish-the-system build (§2 of the Finish-the-System prompt) ----
    #: Repair attempts per lineage before the parent retires for good. Caps
    #: chained Goodharting: each repair is another look at the same gate.
    lineage_repair_cap: int = 3
    #: `rules` ships in this build. `llm` exists as an interface only and is
    #: unreachable — flipping it is a v2 operator decision, not a config tweak.
    scrutiny_provider: str = "rules"
    scrutiny_latency_budget_ms: int = 800
    #: Post-entry quiet period, so a flapping signal cannot churn the book.
    arbiter_cooldown_s: int = 300
    #: "Africa/Nairobi:15:30-17:30". Unset means no window is enforced, which
    #: the executor treats as a configuration error rather than "always open".
    #: Parsed by `Session.from_settings()`.
    trade_window_local: str | None = None

    # ---- the TradeLoop (TradeLoop Orchestrator v1.0) ----
    #: OFF by default, and that is a correct boot state rather than a fault:
    #: no strategy has cleared Gate 3, so a running loop would faithfully do
    #: nothing. Enabling is an operator commit — `.env` plus a restart, never
    #: an HTTP endpoint, because a route that can start trading is an attack
    #: surface. Every boot logs which state this is in and why (§5).
    trade_loop_enabled: bool = False
    #: `taker` | `maker_first_then_taker`. The second implements the audit's
    #: maker-entry test: at 5.5bps taker each side, fee is the dominant term.
    order_entry_style: str = "taker"
    maker_timeout_ms: int = 10_000
    #: Realized/predicted cost ratio that demotes a strategy, over a rolling
    #: window of reconciled legs.
    auto_demote_cost_multiple: float = 1.5
    auto_demote_leg_window: int = 10

    # ---- LLM decoding profiles (LLM Parameters Master Prompt §2.A, §3.A) ----
    # TWO profiles, deliberately not interchangeable. They serve opposite jobs:
    # the generator wants exploration inside a safe grammar, the scrutiny gate
    # wants the same answer to the same setup. A shared constant here would be
    # a silent merge of those goals, so there is no shared constant and a test
    # asserts none appears.
    #
    # Every one is orchestrator-owned. Nothing in a model response may change
    # them (§5); a response that suggests otherwise is ignored and logged.
    gen_temperature: float = 0.9        # 0.7-1.1; below loses novelty, above loses JSON
    gen_top_p: float = 0.95
    gen_max_output_tokens: int = 2000   # truncation is a SILENT corruption mode
    gen_stop_sequences: str = "\n\n---END---"
    gen_seed: int | None = None         # rotated per cycle; reuse kills diversity
    gen_max_monthly_usd: float | None = None    # soft budget; breach pauses, never auto-adjusts
    gen_max_content_retries_per_cycle: int = 2

    # ---- Auditor: the THIRD role (Auditor Master Prompt v1.0 §5) ----
    # Deliberately its own block. §1 of LLM Parameters v1.0 forbids sharing a
    # temperature constant between roles, and the Auditor sits between the
    # other two for a reason: reports are prose about facts, so 0 makes every
    # day's report the same shape (which hides patterns) and 0.9 makes it
    # invent. 0.4 is readable-but-honest.
    auditor_llm: str | None = None      # AUDITOR_LLM — fail loudly, like C2
    aud_temperature: float = 0.4        # 0.2-0.6
    aud_top_p: float = 0.95
    aud_max_output_tokens: int = 2500   # 1500-4000, sized to the §4 word bounds
    aud_stop_sequences: str = "\n\n---END---"
    aud_seed: int | None = None         # rotated per report, stored in provenance
    aud_latency_budget_ms: int = 60_000  # not a hot path; timeout = no report
    aud_max_incidents_per_day: int = 5   # §6 rate limit, then cascade report
    aud_max_monthly_usd: float | None = None   # §11 soft cap: pause, never adjust
    #: §8 red-lines. Below the density or above the uncited count, the report is
    #: prose without evidence and that is itself an incident.
    aud_min_citation_density: float = 1.0   # citations per 100 words
    aud_max_uncited_numbers: int = 0        # §1.3: every number is quoted

    scr_temperature: float = 0.1        # 0.0-0.3; consistency IS the safety property
    scr_top_p: float = 1.0
    scr_max_output_tokens: int = 250    # enough for a verdict, not for prose that hides reasoning
    scr_stop_sequences: str = "\n---"
    scr_seed: int = 7                   # fixed per prompt_version, for veto consistency

    #: Alert thresholds for LLM degradation counters (§2.E, §5).
    llm_feature_hallucination_alert_rate: float = 0.05

    # ---- secrets ----
    # Every one is SecretStr: repr/str render as '**********', so a stray log
    # line, an exception traceback or a settings dump cannot leak a live key.
    # Read the real value with `.get_secret_value()` at the point of use, never
    # earlier — a plain str assigned to a local is exactly how keys reach logs.
    #
    # Values live in server/.env, which is gitignored. `.env.example` is the
    # committed template; it must never contain a real key.
    #
    # Venue credentials. Trading keys are the only secrets here that can move
    # money, so they get their own guard: `bybit_testnet` defaults True and
    # `live_trading_enabled` defaults False, and BOTH must be turned off/on
    # deliberately. A misconfigured deploy fails toward the harmless state.
    bybit_api_key: SecretStr | None = None
    bybit_api_secret: SecretStr | None = None
    bybit_testnet: bool = True
    live_trading_enabled: bool = False

    # Generation / scrutiny model keys (§6, §8). Which one is needed depends on
    # `generation_llm`; `require_secrets()` checks the matching pair.
    openai_api_key: SecretStr | None = None
    anthropic_api_key: SecretStr | None = None

    # api
    api_host: str = "127.0.0.1"
    api_port: int = 8300
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"

    # ---- fail-loudly guards (§2) ----
    def require(self, *names: str) -> None:
        """Assert secrets are present at the point of use, not at import.

        Import-time validation would make the whole system unstartable because a
        key for a subsystem that is not running yet is absent — the collector
        does not need a trading key, and Pass C2's model key is irrelevant to a
        backtest. Checking here keeps startup possible while making the failure
        immediate and specific when a subsystem actually reaches for a secret.
        """
        missing = [n for n in names if getattr(self, n, None) is None]
        if missing:
            raise RuntimeError(
                f"missing required secret(s): {', '.join(missing)}. Set them in "
                f"server/.env (see .env.example). Refusing to continue rather "
                f"than run with an unconfigured credential.")

    def require_trading(self) -> None:
        """Called before any order-placing path.

        `live_trading_enabled` gates **risking real money**, not trading at all.
        On testnet there is no real money, so demo orders are permitted while it
        stays False — which is what lets paper trading be proven before anyone
        argues about flipping it. Reaching a *live* account still requires the
        flag to be set deliberately, so the default configuration remains
        incapable of losing anything.

        Credentials are required either way: an unconfigured key is a
        misconfiguration on demo exactly as much as on live.
        """
        self.require("bybit_api_key", "bybit_api_secret")
        if self.bybit_testnet:
            return
        if not self.live_trading_enabled:
            raise RuntimeError(
                "bybit_testnet is False and live_trading_enabled is False — "
                "refusing to place orders against a real account. Reaching a "
                "live venue must be an explicit decision, never something a "
                "config drift enables.")


settings = Settings()
